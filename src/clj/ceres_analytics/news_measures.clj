(ns ceres-analytics.news-measures
  (:refer-clojure :exclude [find sort])
  (:require [ceres-analytics.db :refer [db broadcasters]]
            [ceres-analytics.measures :refer [contacts cascades statistics format-to-table-view]]
            [ceres-analytics.cascade :refer [compounds]]
            [monger.collection :as mc]
            [monger.joda-time]
            [incanter.stats :as stats]
            [clj-time.core :as t]
            [clj-time.coerce :as c]
            [monger.operators :refer :all]
            [aprint.core :refer [ap]]
            [monger.query :refer :all]))

(def news-authors
  (->> (mc/find-maps @db "users" {:name {$in broadcasters}})
       (map #(select-keys % [:name :_id]))
       (take 13)))


(comment

  (def t0 (t/date-time 2015 4 5))
  
  (def tmax (t/date-time 2015 4 10))



  (-> (compounds [{:name "SZ"}] t0 tmax)
      (get "SZ")
      :links
      count)
 
  )
