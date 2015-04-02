(ns ceres-analytics.metrics
  (:refer-clojure :exclude [find sort])
  (:require [monger.collection :as mc]
            [ceres-analytics.core :refer [db news-accounts links nodes]]
            [clojure.data.priority-map :refer [priority-map]]
            [clj-time.core :as t]
            [clj-time.periodic :as p]
            [incanter.stats :refer [mean]]
            [clj-time.format :as f]
            [monger.core :as mg]
            [monger.joda-time]
            [monger.operators :refer :all]
            [aprint.core :refer [aprint]]
            [monger.query :refer :all]))

(def refs ["mentions" "shares" "replies" "retweets" "urlrefs" "tagrefs" "unknown" "pubs"])

(defn in-degree
  "doc-string"
  [{:keys [_id]}]
  (into {} (map (fn [c] [c (mc/count @db c {:target _id})]) refs)))


(defn out-degree
  "doc-string"
  [{:keys [_id]}]
  (into {} (map (fn [c] [c (mc/count @db c {:source _id})]) refs)))


(defn normalized-degree
  [v coll]
  ((comp float /)
   (merge + (in-degree v) (out-degree v))
   (reduce + (map #(mc/count @db %) ["users" "tags" "urls" "messages"]))))


(defn find-neighbors [[id t]]
  (if (=  t "pub")
    (map
     (fn [{:keys [source type]}] [source type])
     (mc/find-maps @db "refs" {:source id :type "pub"}))
    (map
     (fn [{:keys [source type]}] [source type])
     (mc/find-maps @db "refs" {:target id :type {$in ["retweet" "reply" "share" "pub"]}}))))


(defn dijkstra
  "Dijkstra on social news graph"
  [start]
  (loop [q (priority-map [start "source"] 0)
         r {}]
    (if-let [[v d] (peek q)]
      (let [n (remove #(contains? r (first %)) (find-neighbors v))
            dist (zipmap n (repeat (count n) (inc d)))]
        (recur
         (merge-with min (pop q) dist)
         (assoc r (first v) [(second v) d])))
      r)))





(comment

  (def users  (mc/find-maps @db "users" {:name {$in news-accounts}}))

  (->> nodes (map (fn [d] [d (mc/count @db d)])) (into {}) aprint)

  (->> links (map (fn [d] [d (mc/count @db d)])) (into {}) aprint)

  (let [messages (mc/find-maps @db "messages")]
    (->> messages
         (pmap in-degree)
         #_(reduce (fn [[id1 od1] [id2 od2]] [(+ id1 id2) (+ od1 od2)]))))



  ;; avg avg out degree
  (->> refs
       (pmap (fn [d] [d
                     (->> (mc/find-maps @db d)
                          (map :source)
                          frequencies
                          vals
                          mean)]))
       vec
       aprint
       time)

  ;; avg in degree
  (->> refs
       (pmap (fn [d]
               [d
                (->> (mc/find-maps @db d)
                     (map :target)
                     frequencies
                     vals
                     mean)]))
       vec
       aprint
       time)


  (->> (map
        (fn [h] [h (mc/find-maps @db "pubs" {:ts {$gt (t/date-time 2015 3 13 h)
                                                 $lt (t/date-time 2015 3 13 h 59 59 999)}})])
        (range 0 24))
       (into {})
       (pmap (fn [[k v]] [k]))
       aprint)

  (->> {:ts {$gt (t/date-time 2015 3 13 8)
             $lt (t/date-time 2015 3 13 8 59 59 999)}}
       (mc/find-maps @db "pubs")
       (pmap (fn [{:keys [target]}] (reduce + (pmap #(mc/count @db % {:target target
                                                                     :ts {$gt (t/date-time 2015 3 13 8)
                                                                          $lt (t/date-time 2015 3 13 8 59 59 999)}}) ["retweets" "replies" "shares"])) ))
       frequencies
       (sort-by val >)
       aprint
       time)



  (map (fn [r] [r (mc/count @db r)]) refs)


  (->> (mc/find-maps @db "shares" {:ts {$gt (t/date-time 2015 3 31)}}))

  )
