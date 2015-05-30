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
       (take 14)))


(defn news-daily-expansion
  "Calculates daily expansion of a given time interval"
  [news-users t0 day-range]
  (map
   (fn [d]
     (map
      (fn [{:keys [_id name]}]
        [name
         (mc/count @db "pubs"
                   {:source _id
                    :ts {$gt (t/plus t0 (t/days d))
                         $lt (t/plus t0 (t/days (inc d)))}})
         d])
      news-users))
   day-range))

(defn news-hourly-expansion
  "Calculates hourly expansion given a collection
  a time interval and a starting time"
  [authors t0 tmax]
  (->> authors
       (map
        (fn [{:keys [_id name]}]
          (->> (t/in-hours (t/interval t0 tmax))
               range
               (map
                (fn [h]
                  (mc/count @db "pubs" {:source _id
                                        :ts {$gt (t/plus t0 (t/hours h))
                                             $lt (t/plus t0 (t/hours (inc h)))}})
                  (apply merge)))
               (apply merge-with concat))))))


(defn overall-size
  "Compute overall size of each given author for each cascade label"
  [authors t0 tmax granularity]
  (case granularity
    :hourly
    (->> news-authors
         (pmap
          (fn [{:keys [_id name]}]
            (->> (t/interval t0 tmax)
                 t/in-hours
                 range
                 (pmap
                  (fn [h]
                    (mc/count @db "pubs" {:source _id
                                          :ts {$gt (t/plus t0 (t/hours h))
                                               $lt (t/plus t0 (t/hours (inc h)))}})))
                 statistics)))
         (zipmap (map :name news-authors)))
    :daily
    (->> news-authors
         (map
          (fn [{:keys [_id name]}]
            (let [day-range (range (t/in-days (t/interval t0 tmax)))]
              (->> day-range
                   (map
                    (fn [d]
                      (mc/count @db "pubs" {:source _id 
                                            :ts {$gt (t/plus t0 (t/days d))
                                                 $lt (t/plus t0 (t/days (inc d)))}})))
                   (zipmap day-range)))))
         (zipmap (map :name news-authors)))
    :time
    (->> news-authors
         (pmap
          (fn [{:keys [_id name]}]
            (->> (t/interval t0 tmax)
                 t/in-hours
                 range
                 (pmap
                  (fn [h]
                    {(mod h 24)
                     [(mc/count @db "pubs" {:source _id
                                            :ts {$gt (t/plus t0 (t/hours h))
                                                 $lt (t/plus t0 (t/hours (inc h)))}})]}))
                 (apply merge-with concat)
                 (map (fn [[k v]] [k (statistics v)]))
                 (into {}))))
         (zipmap (map :name news-authors)))
    :unknown))


(comment

  (def t0 (t/date-time 2015 4 5))
  
  (def tmax (t/date-time 2015 5 5))

  (overall-size news-authors t0 tmax :daily)
  
  (ap)
  
  )
