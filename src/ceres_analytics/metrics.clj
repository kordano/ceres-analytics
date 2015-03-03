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



(defn map-vals [m f]
  (into {} (for [[k v] m] [k (f v)])))

(defn remove-keys [m pred]
  (->> (keys m)
       (filter (complement pred))
       (select-keys m)))

(defn dijkstra
  "Code from http://www.ummels.de/2014/06/08/dijkstra-in-clojure/"
  [start f]
  (loop [q (priority-map start 0) r {}]
    (if-let [[v d] (peek q)]
      (let [dist (-> (f v) (remove-keys r) (map-vals (partial + d)))]
        (recur (merge-with min (pop q) dist) (assoc r v d)))
      r)))

(comment

  (def users  (mc/find-maps @db "users" {:name {$in news-accounts}}))

  (reduce + (map #(mc/count @db %) ["users" "messages" "tweets" "htmls" "urls" "tags"]))

  (map #(mc/count @db "refs" {:type %}) ["pub" "reply" "retweet" "share"])


  (->> (mc/find-maps @db "refs")
       (map :type)
       frequencies
       aprint)


  )
