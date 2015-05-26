(ns ceres-analytics.db
  (:require [monger.core :as mg]))


(def db (atom
         (let [^MongoOptions opts (mg/mongo-options {:threads-allowed-to-block-for-connection-multiplier 300})
               ^ServerAddress sa  (mg/server-address (or (System/getenv "DB_PORT_27017_TCP_ADDR") "127.0.0.1") 27017)]
           (mg/get-db (mg/connect sa opts) "juno"))))

(def broadcasters #{"FAZ_NET" "dpa" "tagesschau" "SPIEGELONLINE" "SZ" "BILD" "DerWesten" "ntvde" "tazgezwitscher" "welt" "ZDFheute" "N24_de" "sternde" "focusonline"})
