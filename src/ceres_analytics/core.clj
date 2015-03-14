(ns ceres-analytics.core
  (:refer-clojure :exclude [find sort])
  (:require [monger.collection :as mc]
            [clj-time.core :as t]
            [clj-time.periodic :as p]
            [clj-time.format :as f]
            [incanter.stats :refer [mean sd quantile variance ]]
            [aprint.core :refer [aprint]]
            [monger.core :as mg]
            [monger.joda-time]
            [monger.operators :refer :all]
            [monger.query :refer :all])
  (:import [edu.uci.ics.jung.graph DirectedSparseGraph]
           [edu.uci.ics.jung.graph.util EdgeType]
           [edu.uci.ics.jung.algorithms.metrics Metrics StructuralHoles TriadicCensus]))


(def db (atom
         (let [^MongoOptions opts (mg/mongo-options {:threads-allowed-to-block-for-connection-multiplier 300})
               ^ServerAddress sa  (mg/server-address (or (System/getenv "DB_PORT_27017_TCP_ADDR") "127.0.0.1") 27017)]
           (mg/get-db (mg/connect sa opts) "juno"))))


(def time-interval {$gt (t/date-time 2014 8 1) $lt (t/date-time 2014 9 1)})

(def custom-formatter (f/formatter "E MMM dd HH:mm:ss Z YYYY"))

(def news-accounts #{"FAZ_NET" "dpa" "tagesschau" "SPIEGELONLINE" "SZ" "BILD" "DerWesten" "ntvde" "tazgezwitscher" "welt" "ZDFheute" "N24_de" "sternde" "focusonline"})

(defn short-metrics [coll]
  {:mean (mean coll)
   :std (sd coll)
   :quantiles (let [probs [0.0 0.001 0.25 0.5 0.75 0.999 1.0]]
                (zipmap probs (quantile coll :probs probs)))})

(def nodes ["users" "messages" "htmls" "urls" "tags" ])

(def links ["mentions" "shares" "replies" "retweets" "urlrefs" "tagrefs" "unknown"])
