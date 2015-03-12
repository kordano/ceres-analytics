(ns ceres-analytics.metrics
  (:refer-clojure :exclude [find sort])
  (:require [monger.collection :as mc]
            [ceres-analytics.core :refer [db news-accounts]]
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

  (def dbs ["users" "messages" "tweets" "htmls" "urls" "tags" "mentions" "shares" "replies" "retweets" "urlrefs" "tagrefs" "unknown"])

  (->> dbs
       (map (fn [d] [d (mc/count @db d)]))
       (into {})
       aprint)

  (->> users
       (map
        (fn [{:keys [name _id] :as n}]
          {:name name
           :in (in-degree n)
           :out (out-degree n)}))
       (into {})
       aprint)

  )
