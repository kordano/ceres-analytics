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
            [aprint.core :refer [ap aprint]]
            [monger.query :refer :all]))

(def news-authors
  (->> (mc/find-maps @db "users" {:name {$in broadcasters}})
       (map #(select-keys % [:name :_id]))
       (take 14)))




(defn overall-size
  "Compute overall size of each given author for each cascade label"
  [authors t0 tmax granularity]
  (case granularity
    :hourly
    (->> authors
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
         (zipmap (map :name authors)))
    :daily
    (->> authors
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
         (zipmap (map :name authors)))
    :time
    (->> authors
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
         (zipmap (map :name authors)))
    :unknown))

(defn temporal-diameter
  "Computes temporal diameter of a compound"
  [{:keys [links nodes] :as compound}]
  (pmap
   (fn [[l ls]]
     (let [node-times (map (comp c/to-long :ts) ls)]
       (if (empty? node-times)
         0
         (->> (t/interval
               (c/from-long (apply min node-times))
               (c/from-long (apply max node-times)))
              t/in-minutes
              Math/floor))))
   links))

(defn temporal-radius
  "Computes temporal radius of a compound"
  [{:keys [links nodes] :as compound}]
  (->> links
       (pmap
        (fn [[l ls]]
          (let [contact-times (map (comp c/to-long :ts) ls)]
            (if (empty? contact-times)
              0
              (t/in-minutes (t/interval
                             (:ts l)
                             (->> contact-times (apply min) c/from-long)))))))
       statistics))


(defn lifetime
  "Compute lifetime of a compound"
  [{:keys [links nodes] :as compound}]
  (->> links
       (pmap
        (fn [[l ls]]
          (let [contact-times (map (comp c/to-long :ts) ls)]
            (if (empty? contact-times)
              0
              (t/in-minutes
               (t/interval
                (:ts l)
                (-> (apply max contact-times)
                    c/from-long)))))))
       statistics))

(defn contact-latency
  ""
  []
  )

(defn inter-contact-times
  "Compute inter-contact times in given compound set"
  [{:keys [links nodes] :as compound} t0 tmax granularity]
  (case granularity
    :overall (let [icts (->> links
                             (mapv
                              (fn [[l ls]]
                                (let [contact-times (map :ts ls)]
                                  (if (< (count contact-times) 2)
                                    [0.0]
                                    (->> contact-times
                                         (remove nil?)
                                         (clojure.core/sort t/before?)
                                         (partition 2 1)
                                         (pmap (fn [[c1 c2]] (t/in-seconds (t/interval c1 c2))))))))))]
               {:stats (statistics (map stats/mean icts))
                :distribution (apply concat icts)})
    :daily nil
    :time nil
    :unrelated))

(defn source-degree
  ""
  [broadcaster t0 tmax granularity]
  
  )


(comment

  (def t0 (t/date-time 2015 4 5))

  (def tmax (t/date-time 2015 5 5))
  

  (ap)

  (for [na (take 2 news-authors)]
(->> (inter-contact-times (get (compounds (filter #{na} news-authors) t0 tmax) (:name na)) :overall)
         aprint
         time))

  
  )
