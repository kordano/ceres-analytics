(ns ceres-analytics.db
  (:refer-clojure :exclude [find sort])
  (:require [monger.core :as mg]
            [monger.collection :as mc]
            [monger.operators :refer :all]
            [monger.query :refer :all]))
(def news-users (map :_id (take 13 (mc/find-maps @db "users" {:name {$in broadcasters}}))))
