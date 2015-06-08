(ns ceres-analytics.measures
  (:refer-clojure :exclude [find sort])
  (:require [ceres-analytics.db :refer [db broadcasters]]
            [monger.collection :as mc]
            [monger.joda-time]
            [incanter.stats :as stats]
            [clj-time.core :as t]
            [clj-time.coerce :as c]
            [monger.operators :refer :all]
            [aprint.core :refer [ap aprint]]
            [monger.query :refer :all]))

(def contacts ["shares" "replies" "retweets" "tagrefs" "pubs" "unknown"])
(def cascades ["shares" "replies" "retweets"])
(def nodes ["users" "messages" "tags"])

(defn mongo-time[t0 tmax]
  {:ts {$gt t0 $lt tmax}})

(defn statistics [coll]
  (let [percentiles {:q0 0 :q50 0.5 :q100 1}
        quantiles (zipmap (keys percentiles) (stats/quantile coll :probs (vals percentiles)))]
    (merge quantiles
           {:mean (stats/mean coll)
            :count (count coll)
            :sd (stats/sd coll)})))


(defn format-to-table-view
  "Formats statistics to mean, sd, median, minimum, maximum"
  [{:keys [count mean sd q0 q50 q100]}]
  [mean sd q50 q0 q100 count])

(def table-columns ["Label" "Average" "Standard Deviation" "Medium" "Minimum" "Maximum" "Count"])


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
  "Computes size of network on different temporal granularity levels"
  [cs t0 tmax granularity]
  (case granularity
    :hourly
    (zipmap
     cs
     (map
      #(let [hour-range (range (t/in-hours (t/interval t0 tmax)))]
         (statistics (map
                      (fn [h] (mc/count @db % {:ts {$gt (t/plus t0 (t/hours h))
                                                    $lt (t/plus t0 (t/hours (inc h)))}}))
                      hour-range)))
      cs))
    :daily
    (zipmap
     cs
     (map
      #(let [day-range (range (t/in-days (t/interval t0 tmax)))
             day-values (map
                         (fn [d] (mc/count @db % {:ts {$gt (t/plus t0 (t/days d))
                                                       $lt (t/plus t0 (t/days (inc d)))}}))
                         day-range)
             day-sum (reduce + day-values)]
         (zipmap
          day-range
          (map (fn [v] (* 100 (/ v day-sum))) day-values)))
      cs))
    :time
    (zipmap
     cs
     (map
      #(->> (range (t/in-hours (t/interval t0 tmax)))
            (map (fn [h]
                   {(mod h 24)
                    [(mc/count @db % {:ts {$gt (t/plus t0 (t/hours h))
                                           $lt (t/plus t0 (t/hours (inc h)))}})]}))
            (apply merge-with concat)
            (map (fn [[k v]] [k (statistics v)]))
            (into {}))
      cs))
    :unknown))



(defn order
  "Computes order of the network at different granularity levels"
  [n t0 tmax granularity]
  (case granularity
    :hourly
    (zipmap
     n
     (map
      #(->> (range (t/in-hours (t/interval t0 tmax)))
            (map
             (fn [h]
               (mc/count @db % {:ts {$gt (t/plus t0 (t/hours h))
                                     $lt (t/plus t0 (t/hours (inc h)))}})))
            statistics)
      n))
    :daily
    (zipmap
     n
     (map
      #(let [day-range (range (t/in-days (t/interval t0 tmax)))]
         (zipmap
          day-range
          (map
           (fn [d] (mc/count @db % {:ts {$gt (t/plus t0 (t/days d))
                                         $lt (t/plus t0 (t/days (inc d)))}}))
           day-range)))
      n))
    :time
    (zipmap
     n
     (map
      #(->> (range (t/in-hours (t/interval t0 tmax)))
            (map (fn [h]
                   {(mod h 24)
                    [(mc/count @db % {:ts {$gt (t/plus t0 (t/hours h))
                                           $lt (t/plus t0 (t/hours (inc h)))}})]}))
            (apply merge-with concat)
            (map (fn [[k v]] [k (statistics v)]))
            (into {}))
      n))
    :unknown))


;; --- density ---
(defn density
  "Computes the density of the network at time t_0"
  [n c t0 tmax granularity]
  (case granularity
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
    :unknown))



;; --- total degree ---
(defn total-degree
  "Computes total degree for given contact types between given start and end point and a given temporal granularity"
  [cs t0 tmax granularity]
  (case granularity
    :hourly nil
    :daily
    (let [day-range (range (t/in-days (t/interval t0 tmax)))]
      (->> cs
           (map
            (fn [c]
              (->> day-range
                   (pmap
                    #(* 2
                        (mc/count @db c
                                  (mongo-time (t/plus t0 (t/days %))
                                              (t/plus t0 (t/days (inc %)))))))
                   (zipmap day-range))))
           (zipmap cs)))
    :time
    (->> cs
         (map
          (fn [c]
            (->> (t/interval t0 tmax)
                 t/in-hours
                 range
                 (map
                  (fn [h]
                    {(mod h 24)
                     [(* 2
                         (mc/count @db c
                                   (mongo-time (t/plus t0 (t/hours h))
                                               (t/plus t0 (t/hours (inc h))))))]}))
                 (apply merge-with concat)
                 (map (fn [[k v]] [k (statistics v)]))
                 (into {})
                 )))
         (zipmap cs))
    :unrelated))


(defn contact-latency
  "Compute contact latency on given temporal granularity level for cascades"
  [cs t0 tmax granularity]
  (case granularity
    :statistics
    (->> cs
         (map
          (fn [c]
            (->> (mc/find-maps @db c {:ts {$gt t0
                                           $ne nil
                                           $lt tmax}
                                      :source {$ne nil}
                                      :target {$ne nil}})
                 (pmap
                  (fn [{:keys [target ts]}]
                    (if-let [target-ts (:ts (mc/find-map-by-id @db "messages" target))]
                      (t/in-seconds
                       (t/interval target-ts ts))
                      nil)))
                 (remove nil?)
                 statistics)))
         (zipmap cs))
    :distribution
    (->> cs
         (map
          (fn [c]
            (->> (mc/find-maps @db c {:ts {$gt t0 $lt tmax} :target {$ne nil}})
                 (pmap
                  (fn [{:keys [target ts]}]
                    (if-let [target-ts (:ts (mc/find-map-by-id @db "messages" target))]
                       (t/in-seconds (t/interval target-ts ts))
                      nil)))
                 (remove nil?))))
         (zipmap cs))
    :evolution
    (->> cs
         (map
          (fn [c]
            (->> (t/interval t0 tmax)
                 t/in-days
                 range
                 (mapv
                  (fn [d]
                    (->> (mc/find-maps @db c {:ts  {$gt t0
                                                    $lt (t/plus t0 (t/days (inc d)))}
                                              :source {$ne nil}
                                              :target {$ne nil}})
                         (pmap
                          (fn [{:keys [target ts]}]
                            (if-let [target-ts (:ts (mc/find-map-by-id @db "messages" target))]
                              (/ (t/in-seconds (t/interval target-ts ts)) 3600)
                              nil)))
                         (remove nil?)
                         statistics))))))
         (zipmap cs))
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
      "messages" (->> (mapcat
                       (fn [c]
                         (mc/find-maps @db c
                                       (merge {:target {$ne nil}}
                                              (mongo-time t0 tmax))))
                       cascades)
                      (map :target)
                      frequencies
                      vals
                      statistics)
      "tags" (->> (mc/find-maps @db "tagrefs" (mongo-time t0 tmax))
                  (map :source)
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
      "messages" (->> (mapcat
                       (fn [c]
                         (mc/find-maps @db c
                                       (merge {:target {$ne nil}}
                                              (mongo-time t0 tmax))))
                       cascades)
                      (map :target)
                      frequencies
                      vals)
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
        "messages" (->> day-range
                        (pmap
                         (fn [d]
                           (->> cascades
                                (mapcat
                                 (fn [c]
                                   (mc/find-maps @db c (merge {:target {$ne nil}}
                                                              (mongo-time t0
                                                                          (t/plus t0 (t/days (inc d))))))))
                                (map :target)
                                frequencies
                                vals
                                statistics))))
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

  )
