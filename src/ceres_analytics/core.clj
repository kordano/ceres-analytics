(ns ceres-analytics.core
  (:refer-clojure :exclude [find sort])
  (:require [monger.collection :as mc]
            [clj-time.core :as t]
            [clj-time.periodic :as p]
            [clj-time.format :as f]
            [incanter.stats :refer [mean sd quantile]]
            [ceres-analytics.tree :refer [full-reaction-tree reaction-tree tree-summary]]
            [aprint.core :refer [aprint]]
            [monger.core :as mg]
            [monger.joda-time]
            [monger.operators :refer :all]
            [monger.query :refer :all]))


(def db (atom
         (let [^MongoOptions opts (mg/mongo-options :threads-allowed-to-block-for-connection-multiplier 300)
               ^ServerAddress sa  (mg/server-address (or (System/getenv "DB_PORT_27017_TCP_ADDR") "127.0.0.1") 27017)]
           (mg/get-db (mg/connect sa opts) "athena"))))


(def time-interval {$gt (t/date-time 2014 8 1) $lt (t/date-time 2014 9 1)})


(def custom-formatter (f/formatter "E MMM dd HH:mm:ss Z YYYY"))


(def news-accounts #{"FAZ_NET" "dpa" "tagesschau" "SPIEGELONLINE" "SZ" "BILD" "DerWesten" "ntvde" "tazgezwitscher" "welt" "ZDFheute" "N24_de" "sternde" "focusonline"} )


(def degrees
  (future
    (->> (mc/find-maps @db "publications")
         (pmap
          (fn [p]
            [(:_id p)
             (+ (mc/count @db "reactions" {:publication (:_id p)})
                (mc/count @db "reactions" {:source (:_id p)}))])))))


(comment

  (->> @degrees count time)

  (->> (mc/find-maps @db "publications" {:ts {$gt (t/date-time 2015 2 15)} :type :share})
       (map #(mc/find-one-as-map @db "reactions" {:publication (:_id %)})))

  (def suids (->> (mc/find-maps @db "users" {:screen_name {$in news-accounts}})
                  (map (fn [{:keys [_id screen_name]}] [_id screen_name ]) )
                  (into {})))

  (keys suids)

  (->> (mc/find-maps @db "publications" {:user {$in (keys suids)}})
       (pmap :user)
       frequencies
       (map (fn [[k v]] [(suids k) v]))
       )

  (mc/count @db "publications" {:user {$in (keys suids)}})
  (mc/count @db "publications" {:user {$nin (keys suids)}})

  (def tree-summaries
    (future
      (time
       (->> (mc/find-maps @db "publications" {:user {$in (keys suids)}})
            (pmap (comp tree-summary reaction-tree :_id))))))


  (->>  (sort-by :size > tree-summaries)
        (take 50)
        (pmap (fn [{:keys [source height size]}]
                [height size
                 (->> source
                      (mc/find-map-by-id @db "publications")
                      :tweet
                      (mc/find-map-by-id @db "tweets"))]))
        (pmap (fn [[height size {:keys [text user]}]]
                [(:screen_name user) text size height ((comp float /) height size)]))
        aprint)

  (->>  (sort-by :height > tree-summaries)
        (take 50)
        (pmap (fn [{:keys [source height size]}]
                [height size
                 (->> source
                      (mc/find-map-by-id @db "publications")
                      :tweet
                      (mc/find-map-by-id @db "tweets"))]))
        (pmap (fn [[height size {:keys [text user]}]]
                [(:screen_name user) text size height ((comp float /) height size)]))
        aprint)


  )
