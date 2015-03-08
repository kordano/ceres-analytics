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


(defn normalized-degree
  [v]
  ((comp float /)
   (+ (in-degree v) (out-degree v))
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

  (def refs ["pub" "reply" "retweet" "share" "tag"])

  (def dbs ["users" "messages" "tweets" "htmls" "urls" "tags" "refs"])

  (->> dbs
       (map (fn [d] [d (mc/count @db d)]))
       (into {})
       aprint)

  (let [ref-counts (into {} (map (fn [ref] [ref (mc/count @db "refs" {:type ref})]) refs))
        ref-nil-counts (into {} (map (fn [ref] [ref (mc/count @db "refs" {:type ref :target nil})]) refs))]
    (aprint [ref-counts (merge-with (comp float /) ref-nil-counts ref-counts)]))


  (->> (mc/find-maps @db "refs")
       (map :source)
       frequencies
       (map second)
       frequencies
       aprint
       )


  (->> users
       (map
        (fn [{:keys [name _id]}]
          [name (mc/count @db "refs" {:source _id :type "pub"})]))
       (into {})
       aprint)


  )
