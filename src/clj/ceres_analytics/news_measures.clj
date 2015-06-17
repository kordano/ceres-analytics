(ns ceres-analytics.news-measures
  (:refer-clojure :exclude [find sort])
  (:require [ceres-analytics.helpers :refer [news-authors broadcasters contacts cascades statistics format-to-table-view db]]
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
  "Compute size of a compound"
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


(defn center-degree
  "Computes the degree of the compound's origin node"
  [author-id t0 tmax granularity]
  (let [user (mc/find-one-as-map @db "users" {:id author-id})]
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
      :unrelated)))


(defn temporal-distance
  "Compute average temporal distance for each compound"
  [author t0 tmax granularity]
  (case granularity
    :statistics
    (->> (get-user-tree author t0 tmax)
         :links 
         (pmap
          (fn [[l ls]]
            (if (empty? ls)
              nil
              (stats/mean (pmap #(t/in-seconds (t/interval (:ts l) (:ts %))) ls)))))
         (remove nil?)
         statistics)
    :distribution
    (->> (get-user-tree author t0 tmax)
         :links 
         (pmap
          (fn [[l ls]]
            (if (empty? ls)
              nil
              (statistics (pmap #(/ (t/in-seconds (t/interval (:ts l) (:ts %))) 3600) ls)))))
         (remove nil?))
    :evolution
    (->> (t/interval t0 tmax)
         (t/in-days)
         range
         (pmap
          (fn [d]
            (->> (get-user-tree author t0 (t/plus t0 (t/days d)))
                 :links 
                 (pmap
                  (fn [[l ls]]
                    (if (empty? ls)
                      nil
                      (stats/mean (pmap #(/ (t/in-seconds (t/interval (:ts l) (:ts %))) 3600) ls)))))
                 (remove nil?)
                 statistics))))
    :unrelated))


(defn size-lifetime
  "Computes size-lifetime tuple distribution and evolution for any author's news compound"
  [authors t0 tmax granularity]
  (case granularity
    :correlation
    (let [result  (->> authors
                       (mapcat
                        (fn [author]
                          (->> (get-user-tree author t0 tmax)
                               :links
                               (pmap
                                (fn [[l ls]]
                                  (let [contact-times (map (comp c/to-long :ts) ls)]
                                    (if (empty? contact-times)
                                      [0 0]
                                      [(count contact-times)
                                       (/
                                        (t/in-seconds
                                         (t/interval
                                          (:ts l)
                                          (-> (apply max contact-times)
                                              c/from-long)))
                                        3600)]))))))))]
      (stats/correlation (map first result) (map second result)))
    :distribution
    (let [links (mapcat  #(get (get-user-tree (key %) t0 tmax) :links) authors)
          lsize (count links)]
      (->> links
           (pmap
            (fn [[l ls]]
              (let [contact-times (map (comp c/to-long :ts) ls)]
                (if (empty? contact-times)
                  [0 0]
                  [(count contact-times)
                   (/
                    (t/in-seconds
                     (t/interval
                      (:ts l)
                      (-> (apply max contact-times)
                          c/from-long)))
                    3600)]))))))
    :unrelated))


(defn size-center-degree
  "Computes size-center-degree tuple distribution and evolution for any author's news compound"
  [authors t0 tmax granularity]
  (case granularity
    :distribution
    (let [links (mapcat #(:links (get-user-tree (key %) t0 tmax)) authors)]
      (->> links
           (pmap
            (fn [[l ls]]
              [(count ls)
               (reduce
                +
                (map
                 #(mc/count @db % {:target (:source l)  :ts {$gt t0 $lt tmax}})
                 ["replies" "retweets" "shares"]))]))))
    :unrelated))

(defn size-radius
  "Computes size-radius tuple distribution and evolution for any author's news compound"
  [authors t0 tmax granularity]
  (case granularity
    :distribution
    (let [links (mapcat #(:links (get-user-tree (key %) t0 tmax)) authors)
          lsize (count links)]
      (->> links
           (pmap
            (fn [[l ls]]
              (let [contact-times (map (comp c/to-long :ts) ls)]
                (if (empty? contact-times)
                  [0 0]
                  [(count contact-times)
                   (t/in-seconds
                    (t/interval
                     (:ts l)
                     (-> (apply min contact-times)
                         c/from-long)))]))))))
    :evolution nil
    :unrelated))

(defn lifetime-degree
  "Computes lifetime-degree tuple distribution and evolution for any author's news compound"
  [authors t0 tmax granularity]
  (case granularity
    :distribution
    (->> authors
         (mapcat #(:links (get-user-tree (key %) t0 tmax)))
         (pmap
          (fn [[l ls]]
            (let [contact-times (map (comp c/to-long :ts) ls)]
              (if (empty? contact-times)
                [0 0]
                [(/
                  (t/in-seconds
                   (t/interval
                    (:ts l)
                    (-> (apply max contact-times)
                        c/from-long)))
                  3600)
                 (reduce
                  +
                  (map
                   #(mc/count @db % {:target (:source l)  :ts {$gt t0 $lt tmax}})
                   ["replies" "retweets" "shares"]))])))))
    :evolution nil
    :unrelated))





(comment

  (def t0 (t/date-time 2015 4 5))

  (def tmax (t/date-time 2015 4 10))

  (ap)

  (for [na (map :name news-authors)]
    (time
     (do
       (aprint na)
       (aprint (count (filter #(< % 60) (map :mean (temporal-distance na t0 tmax :distribution))))))))


  (lifetime-degree broadcasters t0 tmax :distribution)
  
  )
