(ns ceres-analytics.measures
  (:refer-clojure :exclude [find sort])
  (:require [ceres-analytics.db :refer [db]]
            [monger.collection :as mc]
            [monger.joda-time]
            [monger.operators :refer :all]
            [monger.query :refer :all]))


(defn dispatch-entity [entity]
  (case entity
    :nodes ["users" "messages" "tags"]
    :users "users"
    :messages "messages"
    :topics "tags"
    :contacts ["shares" "replies" "retweets" "tagrefs" "pubs"]
    :publications "pubs"
    :cascades ["shares" "replies" "retweets"]
    :assignments "tagrefs"
    :unrelated))


(defn dynamic-expansion
  "Computes the dynamic expansion of specific
  entity type between t1 and t2"
  [entity t1 t2]
  (let [colls (dispatch-entity entity)]
    (if (vector? colls)
      (reduce + (map #(mc/count @db %) colls))
      (mc/count @db colls))))
