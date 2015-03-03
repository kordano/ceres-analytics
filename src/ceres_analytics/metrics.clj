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


(defn in-degree
  "doc-string"
  [{:keys [_id]}]
  (mc/count @db "refs" {:target _id}))

(defn out-degree
  "doc-string"
  [{:keys [_id]}]
  (mc/count @db "refs" {:source _id}))

(defn dijkstra
  "Dijkstra on social news graph"
  [start]
  (loop [q (priority-map [start "source"] 0)
         r {}]
    (if-let [[v d] (peek q)]
      (let [n (map (fn [{:keys [source type]}] [source type]) (mc/find-maps @db "refs" {:target (first v) :type {$in ["retweet" "reply" "share" "pub"]}}))
            dist (zipmap n (repeat (count n) (inc d)))]
        (recur
         (merge-with min (pop q) dist)
         (assoc r v d)))
      r)))


(defn type-distribution []
  (map #(mc/count @db "refs" {:type %}) ["share" "unrelated" "source" "reply" "retweet"]))


(comment

  (def users  (mc/find-maps @db "users" {:name {$in news-accounts}}))

  (reduce + (map #(mc/count @db %) ["users" "messages" "tweets" "htmls" "urls" "tags"]))

  (map #(mc/count @db "refs" {:type %}) ["pub" "reply" "retweet" "share"])


  (->> (mc/find-maps @db "refs")
       (map :type)
       frequencies
       aprint)

  (->> users
       (map #(mc/count @db "refs" {:source (:_id %)})))


  (let [vs (mc/find-maps @db "refs" {:source {$in (map :_id users)}})]
    (->> (map (fn [{:keys [target]}] [target (dijkstra target)]) vs)
         count
         time
         ))


  (repeat (count '()) (inc 0))

  )
