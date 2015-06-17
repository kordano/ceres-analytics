(ns ceres-analytics.measures
  (:refer-clojure :exclude [find sort])
  (:require [ceres-analytics.helpers :refer [contacts cascades nodes mongo-time statistics format-to-table-view table-columns db broadcasters]]
            [monger.collection :as mc]
            [monger.joda-time]
            [incanter.stats :as stats]
            [clj-time.core :as t]
            [clj-time.coerce :as c]
            [monger.operators :refer :all]
            [aprint.core :refer [ap aprint]]
            [monger.query :refer :all]))

(defn neighborhood
  "Compute neighborhood of given node id"
  [id t0]
  (zipmap
   contacts
   (map #(mc/find-maps @db % {$or [{:ts {$lt t0}
                                    :source id}
                                   {:ts {$lt t0}
                                    :target id}]}) contacts)))

(defn nw-size
  "Computes size of subclass network on different temporal granularity levels"
  [subclass  t0 tmax granularity]
  (case granularity
    :overall (mc/count @db subclass {:ts {$gt t0 $lt tmax}})
    :hourly
    (->> (t/interval t0 tmax)
         t/in-hours
         range
         (map
          (fn [h]
            (mc/count @db subclass {:ts {$gt (t/plus t0 (t/hours h))
                                         $lt (t/plus t0 (t/hours (inc h)))}})))
         statistics)
    :distribution
    (->> (t/interval t0 tmax)
         t/in-hours
         range
         (map
          (fn [h]
            (mc/count @db subclass {:ts {$gt (t/plus t0 (t/hours h))
                                         $lt (t/plus t0 (t/hours (inc h)))}}))))
    :evolution
    (let [day-range (range (t/in-days (t/interval t0 tmax)))
          overall-size (mc/count @db subclass {:ts {$gt t0 $lt tmax}})]
      (->> (t/interval t0 tmax)
           t/in-days
           range
           (map
            (fn [d] (/ (mc/count @db subclass {:ts {$gt (t/plus t0 (t/days d))
                                                    $lt (t/plus t0 (t/days (inc d)))}})
                       overall-size)))))
    :time
    (let [time-range (t/interval t0 tmax)
          overall-size (/ (mc/count @db subclass {:ts {$gt t0 $lt tmax}}) (t/in-days time-range ))]
      (->> time-range
           t/in-hours
           range
           (map (fn [h]
                  {(mod h 24)
                   [(/ (mc/count @db subclass {:ts {$gt (t/plus t0 (t/hours h))
                                                    $lt (t/plus t0 (t/hours (inc h)))}})
                       overall-size)]}))
           (apply merge-with concat)
           (map (fn [[k v]] [k (statistics v)]))
           (into {})))
    :unknown))



(defn order
  "Computes order of the network at different granularity levels"
  [subclass t0 tmax granularity]
  (case granularity
    :overall (mc/count @db subclass {:ts {$gt t0 $lt tmax}})
    :hourly (->> (range (t/in-hours (t/interval t0 tmax)))
                 (map
                  (fn [h]
                    (mc/count @db subclass {:ts {$gt (t/plus t0 (t/hours h))
                                                 $lt (t/plus t0 (t/hours (inc h)))}})))
                 statistics)
    :distribution (->> (range (t/in-hours (t/interval t0 tmax)))
                       (map
                        (fn [h]
                          (mc/count @db subclass {:ts {$gt (t/plus t0 (t/hours h))
                                                       $lt (t/plus t0 (t/hours (inc h)))}}))))
    :evolution (let [overall-order (mc/count @db subclass {:ts {$gt t0 
                                                                $lt tmax}})]
                 (->> (t/interval t0 tmax)
                      t/in-days 
                      range
                      (map
                       (fn [d]
                         (/ (mc/count @db subclass {:ts {$gt (t/plus t0 (t/days d))
                                                         $lt (t/plus t0 (t/days (inc d)))}})
                            overall-order)))))
    :time (let [time-range (t/interval t0 tmax)
                day-average (/ (mc/count @db subclass {:ts {$gt t0 $lt tmax}}) (t/in-days time-range))]
            (->> time-range
                 t/in-hours 
                 range 
                 (map (fn [h]
                        {(mod h 24)
                         [(/ (mc/count @db subclass {:ts {$gt (t/plus t0 (t/hours h))
                                                          $lt (t/plus t0 (t/hours (inc h)))}})
                             day-average)]}))
                 (apply merge-with concat)
                 (map (fn [[k v]] [k (statistics v)]))
                 (into {})))
    :unrelated))


;; --- density ---
(defn density
  "Computes the density of the network at time t_0"
  [n c t0 tmax granularity]
  (case granularity
    :overall
    (let [node-count (reduce
                      +
                      (pmap
                       #(mc/count @db % {:ts {$gt t0
                                              $lt tmax}}) n))]
      ((comp float /)
       (reduce +
               (pmap #(mc/count @db % {:ts {$gt t0
                                            $lt tmax}}) c))
       (* node-count (dec node-count))))
    :hourly
    (->> (range (t/in-hours (t/interval t0 tmax)))
         (map
          (fn [h]
            (let [node-count (reduce
                              +
                              (pmap
                               #(mc/count @db % {:ts {$gt (t/plus t0 (t/hours h))
                                                      $lt (t/plus t0 (t/hours (inc h)))}}) n))]
              ((comp float /)
               (reduce +
                       (pmap #(mc/count @db % {:ts {$gt (t/plus t0 (t/hours h))
                                                    $lt (t/plus t0 (t/hours (inc h)))}}) c))
               (* node-count (dec node-count))))))
         statistics)
    :daily
    (let [day-range (range (t/in-days (t/interval t0 tmax)))]
      (zipmap
       day-range
       (map
        (fn [d]
          (let [node-count (reduce
                            +
                            (pmap
                             #(mc/count @db % {:ts {$gt (t/plus t0 (t/days d))
                                                    $lt (t/plus t0 (t/days (inc d)))}}) n))]
            ((comp float /)
             (reduce +
                     (pmap #(mc/count @db % {:ts {$gt (t/plus t0 (t/days d))
                                                  $lt (t/plus t0 (t/days (inc d)))}}) c))
             (* node-count (dec node-count)))))
        day-range)))
    :time
    (->> (range (t/in-hours (t/interval t0 tmax)))
         (map
          (fn [h]
            (let [node-count (reduce
                              +
                              (map
                               #(mc/count @db % {:ts {$gt (t/plus t0 (t/hours h))
                                                      $lt (t/plus t0 (t/hours (inc h)))}}) n))]
              {(mod h 24)
               [((comp float /)
                 (reduce +
                         (map #(mc/count @db % {:ts {$gt (t/plus t0 (t/hours h))
                                                     $lt (t/plus t0 (t/hours (inc h)))}}) c))
                 (* node-count (dec node-count)))]})))
         (apply merge-with concat)
         (map (fn [[k v]] [k (statistics v)]))
         (into {}))
    :unrelated))



;; --- total degree ---
(defn total-degree
  "Computes total degree for given contact types between given start and end point and a given temporal granularity"
  [subclass t0 tmax granularity]
  (case granularity
    :overall (* 2 (mc/count @db subclass (mongo-time t0 tmax))) 
    :hourly (->> (t/interval t0 tmax)
                 t/in-hours
                 range
                 (map
                  (fn [h]
                    (* 2
                       (mc/count @db subclass
                                 (mongo-time (t/plus t0 (t/hours h))
                                             (t/plus t0 (t/hours (inc h))))))))
                 statistics)
    :distribution (->> (t/interval t0 tmax)
                       t/in-hours
                       range
                       (map
                        (fn [h]
                          (* 2
                             (mc/count @db subclass
                                       (mongo-time (t/plus t0 (t/hours h))
                                                   (t/plus t0 (t/hours (inc h)))))))))
    :evolution (let [overall-degree (* 2 (mc/count @db subclass (mongo-time t0 tmax)))]
                 (->> (t/interval t0 tmax)
                      t/in-days 
                      range 
                      (pmap
                       (fn [d]
                         (/ (* 2
                               (mc/count @db subclass
                                         (mongo-time (t/plus t0 (t/days d))
                                                     (t/plus t0 (t/days (inc d))))))
                            overall-degree)))))
    :time (let [time-range (t/interval t0 tmax)
                average-degree (/ (* 2 (mc/count @db subclass (mongo-time t0 tmax))) (t/in-days time-range))]
            (->> time-range t/in-hours
                 range
                 (map
                  (fn [h]
                    {(mod h 24)
                     [(/ (* 2 (mc/count @db subclass (mongo-time (t/plus t0 (t/hours h))
                                                                 (t/plus t0 (t/hours (inc h))))))
                         average-degree)]}))
                 (apply merge-with concat)
                 (map (fn [[k v]] [k (statistics v)]))
                 (into {})))
    :unrelated))


(defn contact-latency
  "Compute contact latency on given temporal granularity level for cascades"
  [label t0 tmax granularity]
  (case granularity
    :statistics
    (->> (mc/find-maps @db label {:ts {$gt t0
                                       $ne nil
                                       $lt tmax}
                                  :source {$ne nil}
                                  :target {$ne nil}})
         (pmap
          (fn [{:keys [target ts]}]
            (if-let [target-ts (:ts (mc/find-map-by-id @db "messages" target))]
              (/ (t/in-seconds
                  (t/interval target-ts ts))
                 3600)
              nil)))
         (remove nil?)
         statistics)
    :distribution
    (->> (mc/find-maps @db label {:ts {$gt t0 $lt tmax} :target {$ne nil}})
         (pmap
          (fn [{:keys [target ts]}]
            (if-let [target-ts (:ts (mc/find-map-by-id @db "messages" target))]
              (t/in-seconds (t/interval target-ts ts))
              nil)))
         (remove nil?))
    :evolution
    (->> (t/interval t0 tmax)
         t/in-days
         range
         (mapv
          (fn [d]
            (->> (mc/find-maps @db label {:ts  {$gt t0
                                                $lt (t/plus t0 (t/days (inc d)))}
                                          :source {$ne nil}
                                          :target {$ne nil}})
                 (pmap
                  (fn [{:keys [target ts]}]
                    (if-let [target-ts (:ts (mc/find-map-by-id @db "messages" target))]
                      (/ (t/in-seconds (t/interval target-ts ts)) 3600)
                      nil)))
                 (remove nil?)
                 statistics))))
    :unknown))

(defn inter-contact-time
  "Compute intercontact times of given cascades between given t0 and tmax on given granularity level "
  [label t0 tmax granularity]
  (case granularity
    :statistics
    (->> (mc/find-maps @db label {:ts {$gt t0
                                       $ne nil
                                       $lt tmax}})
         (pmap :ts)
         (remove nil?)
         (clojure.core/sort t/before?)
         (partition 2 1)
         (pmap (fn [[c1 c2]] (t/in-seconds (t/interval c1 c2))))
         statistics)
    :distribution
    (->> (mc/find-maps @db label {:ts {$gt t0 $lt tmax}})
         (pmap :ts)
         (remove nil?)
         (partition 2 1)
         (pmap (fn [[c1 c2]] (t/in-seconds (t/interval c1 c2)))))
    
    :evolution
    (->> (range (t/in-days (t/interval t0 tmax)))
         (pmap
          (fn [d]
            (->> (mc/find-maps @db label {:ts {$gt t0
                                           $lt (t/plus t0 (t/days (inc d)))}})
                 (pmap :ts)
                 (remove nil?)
                 (partition 2 1)
                 (pmap (fn [[c1 c2]] (t/in-seconds (t/interval c1 c2))))
                 statistics))))
    :unknown))



(defn degree
  "Compute degree of given node id"
  [t0 tmax granularity label]
  (case granularity
    :statistics
    (case label
      "users" (->> (mc/find-maps @db "pubs" (mongo-time t0 tmax))
                   (map :source)
                   frequencies
                   vals
                   statistics)
      "messages"
      (let [non-zero-degrees (->> (mapcat
                                   (fn [c]
                                     (mc/find-maps @db c
                                                   (merge {:target {$ne nil}}
                                                          (mongo-time t0 tmax))))
                                   cascades)
                                  (map :target)
                                  frequencies
                                  vals)
            overall-count (reduce + (map (fn [c] (mc/count @db c (merge (mongo-time t0 tmax)))) cascades))
            zero-count (repeat (- overall-count (count non-zero-degrees)) 0)]
        (statistics (concat non-zero-degrees zero-count)))
      "tags" (->> (mc/find-maps @db "tagrefs" (mongo-time t0 tmax))
                  (map :target)
                  frequencies
                  vals
                  statistics)
      :unrelated)
    :distribution
    (case label
      "users" (->> (mc/find-maps @db "pubs" (mongo-time t0 tmax))
                   (map :source)
                   frequencies
                   vals)
      "messages" (let [non-zero-degrees (->> (mapcat (fn [c] (mc/find-maps @db c (merge {:target {$ne nil}} (mongo-time t0 tmax)))) cascades)
                                             (map :target)
                                             frequencies
                                             vals
                                             (map inc))
                       overall-count (reduce + (map (fn [c] (mc/count @db c (merge (mongo-time t0 tmax)))) cascades))
                       zero-count (repeat (- overall-count (count non-zero-degrees)) 1)]
                   (concat non-zero-degrees zero-count))
      "tags" (->> (mc/find-maps @db "tagrefs" (mongo-time t0 tmax))
                  (map :source)
                  frequencies
                  vals)
      :unrelated)
    :evolution
    (let [day-range (range (t/in-days (t/interval t0 tmax)))]
      (case label
        "users" (->> day-range
                     (pmap
                      (fn [d]
                        (->> (mongo-time t0 (t/plus t0 (t/hours (inc d))))
                             (mc/find-maps @db "pubs")
                             (map :source)
                             frequencies
                             vals
                             statistics))))
        "messages"
        (->> day-range
             (pmap
              (fn [d]
                (let [non-zero-degrees
                      (->> (mapcat
                            (fn [c]
                              (mc/find-maps @db c (merge {:target {$ne nil}}
                                                         (mongo-time t0
                                                                     (t/plus t0 (t/days (inc d)))))))
                            cascades)
                           (map :target)
                           frequencies
                           vals)
                      overall-count
                      (reduce + (map
                                 (fn [c]
                                   (mc/count @db c
                                             (merge (mongo-time t0 tmax))))
                                 cascades))
                      zero-count (repeat (- overall-count (count non-zero-degrees)) 0)]
                  (statistics (concat non-zero-degrees zero-count))))))
        "tags" (->> day-range
                    (pmap
                     (fn [d]
                       (->> (mongo-time t0 (t/plus t0 (t/days (inc d))))
                            (mc/find-maps @db "tagrefs" )
                            (map :source)
                            frequencies
                            vals
                            statistics))))
        :unrelated))
    :unrelated))

(comment

  (def t0 (t/date-time 2015 4 5))

  (def tmax (t/date-time 2015 4 15))

  (->> (mc/find-maps @db "shares" {:ts {$gt t0
                                 $lt tmax}})
       (pmap :ts)
       (remove nil?)
       (partition 2 1)
       (pmap (fn [[c1 c2]]
               (t/in-seconds (t/interval c1 c2)))))

  (ap)

  (->> (inter-contact-times cascades t0 tmax :evolution)
       vals
       first
       (map :count)
       statistics
       time)

  (time (degree t0 tmax :evolution))

  (map #(order % t0 tmax :time) contacts)

  )
