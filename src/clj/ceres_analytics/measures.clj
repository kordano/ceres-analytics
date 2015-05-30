(ns ceres-analytics.measures
  (:refer-clojure :exclude [find sort])
  (:require [ceres-analytics.db :refer [db broadcasters]]
            [monger.collection :as mc]
            [monger.joda-time]
            [incanter.stats :as stats]
            [clj-time.core :as t]
            [clj-time.coerce :as c]
            [monger.operators :refer :all]
            [aprint.core :refer [ap]]
            [monger.query :refer :all]))

(def contacts ["shares" "replies" "retweets" "tagrefs" "pubs" "unknown"])
(def cascades ["shares" "replies" "retweets"])
(def nodes ["users" "messages" "tags"])


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


(defn degree
  "Compute degree of given node id"
  [id t0]
  (->> (neighborhood id t0) vals (apply concat) count))


(defn lifetime
  "Computes lifetime of contact set"
  [cs]
  (zipmap
   (keys cs)
   (map
    #(let [sorted-cs (sort-by :ts %)]
       (-> (t/interval (-> % first :ts) (-> % last :ts))
           t/in-seconds
           (/ 3600)
           float))
    (vals cs))))



(defn nw-size
  "Computes size of network on different temporal granularity levels"
  [cs t0 tmax granularity]
  (case granularity
    :hourly
    (zipmap
     cs
     (map
      #(let [hour-range (range (t/in-hours (t/interval t0 tmax)))]
         (statistics
          (map
           (fn [h] (mc/count @db % {:ts {$gt (t/plus t0 (t/hours h))
                                         $lt (t/plus t0 (t/hours (inc h)))}}))
           hour-range)))
      cs))
    :daily
    (zipmap
     cs
     (map
      #(let [day-range (range (t/in-days (t/interval t0 tmax)))]
         (zipmap
          day-range
          (map
           (fn [d] (mc/count @db % {:ts {$gt (t/plus t0 (t/days d))
                                         $lt (t/plus t0 (t/days (inc d)))}}))
           day-range)))
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



(defn subset-size
  "Computes size of given contact set"
  [cs]
  (zipmap
   (keys cs)
   (map count (vals cs))))



(defn intercontact-times
  "Computes average inter-contact times"
  [cs]
  (zipmap
   (keys cs)
   (map
    (fn [sub-cs]
      (let [sorted-cs (sort-by :ts sub-cs)]
        (reduce + #(t/in-seconds (t/interval (:ts %1) (:ts %2))))))
    (vals cs))))

;; --- total degree ---
(defn daily-total-degree
  "Compute total degree of contact set"
  [cs t0 daily-range]
  (zipmap
   cs 
   (map
    (fn [c]
      (zipmap
       daily-range
       (map
        #(* 2
            (mc/count @db c {:ts {$gt (t/plus t0 (t/days %))
                                  $lt (t/plus t0 (t/days (inc %)))}}))
        daily-range)))
    cs)))

(defn hourly-total-degree
  "Compute total degree of contact set"
  [cs t0 hourly-range]
  (map
   #(* 2 (mc/count @db cs {:ts {$gt (t/plus t0 (t/hours %))
                                $lt (t/plus t0 (t/hours (inc %)))}}))
   hourly-range))


(defn tod-total-degree
  "Compute total degree for each day of time of day"
  [cs t0 tmax]
  (zipmap
    cs
    (map
     (comp
      #(into {} %)
      #(map (fn [[k v]] [k (* v 2)]) %)
      frequencies
      (fn [cs] (map (comp t/hour :ts) cs))
      #(mc/find-maps @db % {:ts {$gt t0 $lt tmax}}))
     cs)))


(defn modularity
  "Computes the modularity of between two entites"
  [v0 v1 cs t0]
  (/ (* (degree v0 t0) (degree v1 t0))
     (* 2 (count cs))))


(defn topological-eccentricity
  "Compute topological eccentriciy of given entity"
  [v0 cs t0]
  )


(defn contact-latency
  "Compute contact latency on given temporal granularity level for cascades"
  [cs t0 tmax granularity]
  (case granularity
    :hourly
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
                      (/
                       (t/in-seconds
                        (t/interval target-ts ts))
                       3600)
                      nil)))
                 (remove nil?)
                 statistics)))
         (zipmap cs))  
    :daily
    (->> cs
         (map
          (fn [c]
            (let [day-range (->> (t/interval t0 tmax)
                                 t/in-days
                                 range)]
              (->> day-range
                   (mapv
                    (fn [d]
                      (->> (mc/find-maps @db c {:ts  {$gt (t/plus t0 (t/days d))
                                                      $lt (t/plus t0 (t/days (inc d)))}
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
                           statistics)))
                   (zipmap day-range)))))
         (zipmap cs))  
    :time
    (->> cs
         (map
          (fn [c]
            (->> (t/interval t0 tmax)
                 t/in-hours
                 range
                 (mapv
                  (fn [h]
                    (->> (mc/find-maps @db c {:ts  {$gt (t/plus t0 (t/hours h))
                                                    $lt (t/plus t0 (t/hours (inc h)))}
                                              :source {$ne nil}
                                              :target {$ne nil}})
                         (pmap
                          (fn [{:keys [target ts]}]
                            (if-let [target-ts (:ts (mc/find-map-by-id @db "messages" target))]
                              {(mod h 24)
                               [(/ (t/in-seconds
                                    (t/interval target-ts ts))
                                   3600)]}
                              nil)))
                         (remove nil?)
                         (apply merge-with concat))))
                 (apply merge-with concat)
                 (map (fn [[k v]] [k (statistics v)]))
                 (into {}))))
         (zipmap cs))
    :unknown))

(defn inter-contact-times
  ""
  [cs t0 tmax granularity]
  (case granularity
    :hourly
    (->> cs
         (pmap
          (fn [c]
            (->> (mc/find-maps @db c {:ts {$gt t0
                                           $ne nil
                                           $lt tmax}})
                 (pmap :ts)
                 (remove nil?)
                 (clojure.core/sort t/before?)
                 (partition 2 1)
                 (pmap (fn [[c1 c2]]
                         (t/in-seconds (t/interval c1 c2))))
                 statistics)))
         (zipmap cs))
    :daily
    (->> cs
         (map
          (fn [c]
            (let [day-range (range (t/in-days (t/interval t0 
                                                          tmax)))]
              (->> day-range
                   (map
                    (fn [d]
                      (->> (mc/find-maps @db c {:ts {$gt (t/plus t0 (t/days d))
                                                     $lt (t/plus t0 (t/days (inc d)))}})
                           (pmap :ts)
                           (remove nil?)
                           (partition 2 1)
                           (pmap (fn [[c1 c2]]
                                   (t/in-seconds (t/interval c1 c2))))
                           statistics)))
                   (zipmap day-range)))))
         (zipmap cs))
    :time
    (->> cs
         (map
          (fn [c]
            (let [hour-range (range (t/in-hours (t/interval t0 
                                                            tmax)))]
              (->> hour-range
                   (pmap
                    (fn [h]
                      (->> (mc/find-maps @db c {:ts {$gt (t/plus t0 (t/hours h))
                                                     $lt (t/plus t0 (t/hours (inc h)))}})
                           (pmap :ts)
                           (remove nil?)
                           (partition 2 1)
                           (pmap (fn [[c1 c2]]
                                   {(mod h 24)
                                    [(t/in-seconds (t/interval c1 c2))]}))
                           (apply merge-with concat))))
                   (apply merge-with concat)
                   (map (fn [[k v]] [k (statistics v)]))
                   (into {})))))
         (zipmap cs))
    :unknown))



(comment

  (def t0 (t/date-time 2015 4 5))
  
  (def tmax (t/date-time 2015 5 5))

  
  (def day-range (range 0 31))

  (def hour-range (range 0 (inc (* 24 30))))
 
  (contact-latency cascades t0 tmax :hourly)

  (ap)


  (def p1 (contact-latency ["pubs"] t0 (t/date-time 2015 4 25) :time))

  (def p2 (contact-latency ["pubs"] (t/date-time 2015 4 25) (t/date-time 2015 5 5) :time))

  
  ;; Calculate pubs contact latency in 15 day steps
  (merge p1
         (into {} (map (fn [[k v]] [(+ k 10) v]) p2))
         (into {} (map (fn [[k v]] [(+ k 20) v]) p3)))


  (mc/count @db "pubs" {:ts {$gt (t/date-time 2015 4 5)
                             $lt (t/date-time 2015 4 25)}})

  (mc/count @db "pubs" {:ts {$gt (t/date-time 2015 4 25)
                             $lt (t/date-time 2015 5 5)}})
  
  )
