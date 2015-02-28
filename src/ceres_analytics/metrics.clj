(ns ceres-analytics.metrics
  (:refer-clojure :exclude [find sort])
  (:require [monger.collection :as mc]
            [ceres-analytics.core :refer [db news-accounts]]
            [clj-time.core :as t]
            [clj-time.periodic :as p]
            [clj-time.format :as f]
            [monger.core :as mg]
            [monger.joda-time]
            [monger.operators :refer :all]
            [monger.query :refer :all]))

(defn distance [v1 v2]
  (let [conns (->> {$or [{:source v1 :target v2}
                         {:source v2 :target v1}]}
                   (mc/count @db "refs"))]
    (if (pos? conns)
      1
      0)))

(comment

  )
