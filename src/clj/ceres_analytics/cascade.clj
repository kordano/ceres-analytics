(ns ceres-analytics.cascade
  (:refer-clojure :exclude [find sort])
  (:require [monger.collection :as mc]
            [aprint.core :refer [aprint]]
            [monger.joda-time]
            [monger.operators :refer :all]
            [monger.query :refer :all]
            [ceres-analytics.core :refer [db]]
            [clj-time.core :as t]))



(defn find-publications
  "Find all messages published by user in specific interval"
  [uid [start end]]
  (map
   (fn [{:keys [target]}]
     (assoc (mc/find-map-by-id @db "messages" target) :type :source))
   (mc/find-maps @db "pubs" {:source uid
                             :ts {$gt (t/date-time 2015 3 26 start )
                                  $lt (t/date-time 2015 3 26 end)}})))


(defn find-reactions
  "doc-string"
  [mid]
  (let [group {"replies" 3 "retweets" 4 "shares" 5}]
    [mid
     (map
      (fn [coll]
        (map
         (comp find-reactions :source)
         (mc/find-maps @db coll {:target mid})))
      (keys group))]))


(let [articles (->> (mc/find-maps @db "sources" {:ts {$gt (t/date-time 2015 3 26 12)
                                                      $lt (t/date-time 2015 3 26 18)}})
                    (map #(mc/find-map-by-id @db "messages" (:target %))))
      pubs (->> articles
                (map
                 (fn [a]
                   { (->> {:target (:_id a)}
                          (mc/find-one-as-map @db "pubs" )
                          :source
                          (mc/find-map-by-id @db "users" )
                          :name)
                     (vector (:_id a))}))
                (apply merge-with (comp vec concat)))]
  (time (count (vec (map
                     (fn
                       [[u ps]]
                       [u (apply merge (map find-reactions ps))])
                     pubs)))))
