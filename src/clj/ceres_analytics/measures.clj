(ns ceres-analytics.measures
  (:refer-clojure :exclude [find sort])
  (:require [ceres-analytics.db :refer [db broadcasters]]
            [monger.collection :as mc]
            [monger.joda-time]
            [incanter.stats :as stats]
            [clj-time.core :as t]
            [monger.operators :refer :all]
            [aprint.core :refer [ap]]
            [monger.query :refer :all]))

(def contacts ["shares" "replies" "retweets" "tagrefs" "sources" "unknown"])
(def cascades ["shares" "replies" "retweets"])
(def nodes ["users" "messages" "tags"])

(def news-authors (take 14 (map #(select-keys % [:name :_id]) (mc/find-maps @db "users" {:name {$in broadcasters}}))))

(defn dispatch-entity [entity]
  (case entity
    :nodes nodes
    :users "users"
    :messages "messages"
    :topics "tags"
    :contacts contacts
    :publications "pubs"
    :cascades cascades
    :assignments "tagrefs"
    :unknown "unknown"
    :unrelated))


(defn dynamic-expansion
  "Computes the dynamic expansion of specific
  entity type between t1 and t2"
  [entity t1 t2]
  (let [colls (dispatch-entity entity)]
    (if (vector? colls)
      {colls
       (mc/find-maps @db colls {:ts {$gt t1
                                     $lt t2}})})))


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


(defn density
  "Computes the density of the network at time t_0"
  [entity t0]
  (when (= entity :full)
    (let [node-count (reduce + (map #(mc/count @db % {:ts {$lt t0}}) nodes))]
      ((comp float /) (reduce + (map #(mc/count @db % {:ts {$lt t0}}) contacts))
         (* node-count (dec node-count))))))

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


(defn daily-total-degree
  "Compute total degree of contact set"
  [cs t0 daily-range]
  (map
   #(* 2 (mc/count @db cs {:ts {$gt (t/plus t0 (t/days %))
                                $lt (t/plus t0 (t/days (inc %)))}}))
   daily-range))

(defn hourly-total-degree
  "Compute total degree of contact set"
  [cs t0 hourly-range]
  (map
   #(* 2 (mc/count @db cs {:ts {$gt (t/plus t0 (t/hours %))
                                $lt (t/plus t0 (t/hours (inc %)))}}))
   hourly-range))


(defn max-degree
  "Compute maximum degree"
  [entity t0]
  (let [cs (dispatch-entity entity)]
    (if (vector? cs)
      (zipmap
       (keys cs)
       (map
        (fn [sub-cs]
          (map
           (comp
            #(apply max %)
            #(map degree %)
            #(mc/find-maps @db % {:ts {$lt t0}}))
           sub-cs))))
      (apply max (pmap #(degree (:_id %) t0 ) (mc/find-maps @db cs {:ts {$lt t0}}))))))

(defn min-degree
  "Compute minimum degree"
  [entity t0]
  (let [cs (dispatch-entity entity)]
    (if (vector? cs)
      (zipmap
       (keys cs)
       (map
        (fn [sub-cs]
          (map
           (comp
            #(apply min %)
            #(map degree %)
            #(mc/find-maps @db % {:ts {$lt t0}}))
           sub-cs))))
      (apply min (pmap #(degree (:_id %) t0 ) (mc/find-maps @db cs {:ts {$lt t0}}))))))


(defn modularity
  "Computes the modularity of between two entites"
  [v0 v1 cs t0]
  (/ (* (degree v0 t0) (degree v1 t0))
     (* 2 (count cs))))


(defn topological-eccentricity
  "Compute topological eccentriciy of given entity"
  [v0 cs t0]
  )


(defn daily-expansion
  "Calculates daily expansion of a given time interval"
  [coll t0 day-range]
  (map
   #(mc/count @db coll
              {:ts {$gt (t/plus t0 (t/days %))
                    $lt (t/plus t0 (t/days (inc %)))}})
   day-range))


(defn hourly-expansion
  "Calculates hourly expansion given a collection
  a time interval and a starting time"
  [coll t0 hour-range]
  (map
   #(mc/count @db coll {:ts {$gt (t/plus t0 (t/hours %))
                             $lt (t/plus t0 (t/hours (inc %)))}})
   hour-range))

(defn news-daily-expansion
  "Calculates daily expansion of a given time interval"
  [news-users t0 day-range]
  (map
   (fn [d]
     (map
      (fn [{:keys [_id name]}]
        [name
         (mc/count @db "pubs"
                   {:source _id
                    :ts {$gt (t/plus t0 (t/days d))
                         $lt (t/plus t0 (t/days (inc d)))}})
         d])
      news-users))
   day-range))

(defn news-hourly-expansion
  "Calculates hourly expansion given a collection
  a time interval and a starting time"
  [t0 hour-range]
  (apply merge-with concat
         (map
          (fn [h] (->> news-authors
                       (map
                        (fn [{:keys [_id name]}]
                          {name
                           [(mc/count @db "pubs" {:source _id
                                                   :ts {$gt (t/plus t0 (t/hours h))
                                                        $lt (t/plus t0 (t/hours (inc h)))}})]}) )
                       (apply merge)))
          hour-range)))


(defn statistics [coll]
  (let [percentiles [0 0.5 1]
        quantiles (zipmap [:q0 :q50 :q100] (stats/quantile coll :probs percentiles))]
    (merge quantiles
    {:mean (stats/mean coll)
     :sd  (stats/sd coll)})))

(comment

  (def t0 (t/date-time 2015 4 3))

  (def day-range (range 0 31))

  (def hour-range (range 0 (inc (* 24 30))))
   
 
  (ap)
  
  
  )
