(ns ceres-analytics.news-measures
  (:refer-clojure :exclude [find sort])
  (:require [ceres-analytics.helpers :refer [news-authors contacts cascades statistics format-to-table-view db]]
            [ceres-analytics.cascade :refer [compounds get-user-tree]]
            [monger.collection :as mc]
            [monger.joda-time]
            [incanter.stats :as stats]
            [clj-time.core :as t]
            [clj-time.coerce :as c]
            [monger.operators :refer :all]
            [aprint.core :refer [ap aprint]]
            [monger.query :refer :all]))


(defn compound-size
  "Compute lifetime of a compound"
  [author t0 tmax granularity]
  (case granularity
    :fractions
    (let [links (:links (get-user-tree author t0 tmax)) ]
      {:overall  (count links)
       :zero (->> links (pmap (comp count second)) (filter #{0}) count)})
    :statistics
    (->> (get-user-tree author t0 tmax)
         :links
         (pmap (comp count second))
         statistics)
    :distribution
    (let [links (:links (get-user-tree author t0 tmax))
          lsize (count links)]
      (->> links
           (pmap (comp count second))
           frequencies
           (pmap (fn [[k v]] [k (/ v lsize)]))))
    :evolution
    (->> (t/interval t0 tmax)
         t/in-days
         range
         (map
          #(->> (get-user-tree author t0 (t/plus t0 (t/days (inc %))))
                :links
                (pmap (comp count second))
                statistics)))
    :unrelated))


(defn temporal-diameter
  "Computes temporal diameter of a compound"
  [author t0 tmax granularity]
  (case granularity
    :statistics
    (->> (get-user-tree author t0 tmax)
         :links
         (pmap
          (fn [[l ls]]
            (let [node-times (map (comp c/to-long :ts) ls)]
              (if (empty? node-times)
                nil
                (/ (->> (t/interval
                         (c/from-long (apply min node-times))
                         (c/from-long (apply max node-times)))
                        t/in-seconds)
                   3600)))))
         (remove nil?)
         statistics)
    :distribution
    (->> (get-user-tree author t0 tmax)
         :links
         (pmap
          (fn [[l ls]]
            (let [node-times (map (comp c/to-long :ts) ls)]
              (if (empty? node-times)
                nil
                (/ (->> (t/interval
                         (c/from-long (apply min node-times))
                         (c/from-long (apply max node-times)))
                        t/in-seconds)
                   3600)))))
         (remove nil?))
    :evolution
    (->> (t/interval t0 tmax)
         t/in-days
         range
         (map
          #(->> (get-user-tree author t0 (t/plus t0 (t/days (inc %))))
                :links
                (pmap
                 (fn [[l ls]]
                   (let [node-times (map (comp c/to-long :ts) ls)]
                     (if (empty? node-times)
                       nil
                       (/ (->> (t/interval
                              (c/from-long (apply min node-times))
                              (c/from-long (apply max node-times)))
                             t/in-seconds)
                          3600)))))
                (remove nil?)
                statistics)))
    :unrelated))





(defn temporal-radius
  "Computes temporal radius of a compound"
  [author t0 tmax granularity]
  (case granularity
    :statistics
    (->> (get-user-tree author t0 tmax)
         :links
         (pmap
          (fn [[l ls]]
            (let [contact-times (map (comp c/to-long :ts) ls)]
              (if (empty? contact-times)
                nil
                (/ (t/in-seconds (t/interval
                                  (:ts l)
                                  (->> contact-times (apply min) c/from-long)))
                   60)))))
         (remove nil?)
         statistics)
    :distribution
    (->> (get-user-tree author t0 tmax)
         :links
         (pmap
          (fn [[l ls]]
            (let [contact-times (map (comp c/to-long :ts) ls)]
              (if (empty? contact-times)
                nil
                (/ (t/in-seconds (t/interval
                                  (:ts l)
                                  (->> contact-times (apply min) c/from-long)))
                   60)))))
         (remove nil?)
         )
    :evolution
    (->> (t/interval t0 tmax)
         t/in-days
         range
         (map
          #(->> (get-user-tree author t0 (t/plus t0 (t/days (inc %))))
                :links
                (pmap
                 (fn [[l ls]]
                   (let [contact-times (map (comp c/to-long :ts) ls)]
                     (if (empty? contact-times)
                       nil
                       (/ (t/in-seconds (t/interval
                                         (:ts l)
                                         (->> contact-times (apply min) c/from-long)))
                          60)))))
                (remove nil?)
                statistics)))
    :unrelated))





(defn lifetime
  "Compute lifetime of a compound"
  [author t0 tmax granularity]
  (case granularity
    :statistics
    (->> (get-user-tree author t0 tmax)
         :links
         (pmap
          (fn [[l ls]]
            (let [contact-times (map (comp c/to-long :ts) ls)]
              (if (empty? contact-times)
                0
                (/ (t/in-seconds
                    (t/interval
                     (:ts l)
                     (-> (apply max contact-times)
                         c/from-long)))
                   3600)))))
         statistics)
    :distribution
    (->> (get-user-tree author t0 tmax)
         :links
         (pmap
          (fn [[l ls]]
            (let [contact-times (map (comp c/to-long :ts) ls)]
              (if (empty? contact-times)
                0
                (/
                 (t/in-seconds
                  (t/interval
                   (:ts l)
                   (-> (apply max contact-times)
                       c/from-long)))
                 3600))))))
    :evolution
    (->> (t/interval t0 tmax)
         t/in-days
         range
         (map
          #(->> (get-user-tree author t0 (t/plus t0 (t/days (inc %))))
                :links
                (pmap
                 (fn [[l ls]]
                   (let [contact-times (map (comp c/to-long :ts) ls)]
                     (if (empty? contact-times)
                       0
                       (/
                        (t/in-seconds
                         (t/interval
                          (:ts l)
                          (-> (apply max contact-times)
                              c/from-long)))
                        3600)
                       ))))
                statistics)))
    :unrelated))


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


(defn center-degree
  "Computes the degree of the compound's origin node"
  [author t0 tmax granularity]
  (let [user (mc/find-one-as-map @db "users" {:name author})]
    (case granularity
      :statistics
      (->> (mc/find-maps @db "pubs" {:source (:_id user) :ts {$gt t0 $lt tmax}})
           (map :target)
           (into #{})
           (map
            (fn [id]
              (reduce
               +
               (map
                #(mc/count @db % {:target id :ts {$gt t0 $lt tmax}})
                ["replies" "retweets" "shares"]))))
           statistics)
      :distribution
      (let [pubs (->> (mc/find-maps @db "pubs" {:source (:_id user) :ts {$gt t0 $lt tmax}})
                      (map :target)
                      (into #{}))
            pcount (count pubs)]
        (->> pubs
             (map
              (fn [id]
                (reduce
                 +
                 (map
                  #(mc/count @db % {:target id :ts {$gt t0 $lt tmax}})
                  ["replies" "retweets" "shares"]))))
             frequencies
             (map (fn [[k v]] [k (/ v pcount)]))))
      
      :evolution
      (->> (t/interval t0 tmax)
           t/in-days
           range
           (map
            (fn [d]
              (->> (mc/find-maps @db "pubs" {:source (:_id user) :ts {$gt t0 $lt (t/plus t0 (t/days (inc d)))}})
                   (map :target)
                   (into #{})
                   (map
                    (fn [id]
                      (reduce
                       +
                       (map
                        #(mc/count @db % {:target id :ts {$gt t0 $lt tmax}})
                        ["replies" "retweets" "shares"]))))
                   statistics))))
      :unrelated))
  )



(comment

  (def t0 (t/date-time 2015 4 5))

  (def tmax (t/date-time 2015 4 15))

  (ap)

  (for [na (map :name news-authors)]
    (do
      (aprint na)
      (aprint (compound-size na t0 tmax :statistics))))
  
  )
