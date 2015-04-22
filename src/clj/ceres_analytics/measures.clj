(ns ceres-analytics.measures
  (:refer-clojure :exclude [find sort])
  (:require [ceres-analytics.db :refer [db]]
            [monger.collection :as mc]
            [monger.joda-time]
            [clj-time.core :as t]
            [monger.operators :refer :all]
            [monger.query :refer :all]))

(def contacts ["shares" "replies" "retweets" "tagrefs" "pubs"])
(def cascades ["shares" "replies" "retweets"])
(def nodes ["users" "messages" "tags"])

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
    :unrelated))


(defn dynamic-expansion
  "Computes the dynamic expansion of specific
  entity type between t1 and t2"
  [entity t1 t2]
  (let [colls (dispatch-entity entity)]
    (if (vector? colls)
      (zipmap colls
              (map #(mc/find-maps @db % {:ts {$gt t1
                                              $lt t2}}) colls))
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
  (let [sorted-cs (sort-by :ts cs)]
    (t/in-seconds (t/interval (last cs) (first cs)))))
