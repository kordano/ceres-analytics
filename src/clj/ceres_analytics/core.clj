 (ns ceres-analytics.core
  (:gen-class :main true)
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
            [monger.query :refer :all]
            [ceres-analytics.cascade :as cascade]
            [clojure.java.io :as io]
            [compojure.route :refer [resources]]
            [compojure.core :refer [GET POST defroutes]]
            [compojure.handler :refer [site api]]
            [taoensso.timbre :refer [info debug error warn] :as timbre]
            [org.httpkit.server :refer [with-channel on-receive on-close run-server send!]]))


(timbre/refer-timbre)

(def db (atom
         (let [^MongoOptions opts (mg/mongo-options {:threads-allowed-to-block-for-connection-multiplier 300})
               ^ServerAddress sa  (mg/server-address (or (System/getenv "DB_PORT_27017_TCP_ADDR") "127.0.0.1") 27017)]
           (mg/get-db (mg/connect sa opts) "juno"))))


(def time-interval {$gt (t/date-time 2014 8 1) $lt (t/date-time 2014 9 1)})

(def custom-formatter (f/formatter "E MMM dd HH:mm:ss Z YYYY"))

(def t0 (t/date-time 2015 4 5))
(def tmax (t/date-time 2015 5 5))

(def day-range (range 0 31))
(def hour-range (range 0 (inc (* 24 30))))

(def news-accounts #{"FAZ_NET" "dpa" "tagesschau" "SPIEGELONLINE" "SZ" "BILD" "DerWesten" "ntvde" "tazgezwitscher" "welt" "ZDFheute" "N24_de" "sternde" "focusonline"})

(defn short-metrics [coll]
  {:mean (mean coll)
   :std (sd coll)
   :quantiles (let [probs [0.0 0.001 0.25 0.5 0.75 0.999 1.0]]
                (zipmap probs (quantile coll :probs probs)))})

(def nodes ["users" "messages" "tags" ])

(def cascades ["mentions" "shares" "replies" "retweets" "pubs" "unknown"])

(defn get-random-links [{:keys [user interval]}]
  (let [[start-time end-time] interval]
    (apply merge-with (comp vec concat)
           (map
            (fn [[user messages]]
              {:nodes (concat [{:name (str (:_id user)) :value (:name user) :group 1}]
                              (map (fn [m] {:name (str (:_id m)) :value (:text m) :group 2}) messages))
               :links (map (fn [m] {:source (str (:_id user)) :target (str (:_id m)) }) messages)})
            (map
             (fn [{:keys [_id name] :as user}]
               [user
                (map
                 #(mc/find-map-by-id @db "messages" (:target %))
                 (mc/find-maps @db "pubs" {:source _id
                                           :ts {$gt (t/date-time 2015 3 19 start-time)
                                                $lt (t/date-time 2015 3 19 end-time)}}))])
             (mc/find-maps @db "users" {:name {$in news-accounts}}))))))


(defn dispatch-request
  "Dispatch incoming requests"
  [{:keys [topic data]}]
  (case topic
    :graph (get-random-links data)
    :user-tree (cascade/get-user-tree data)
    :unrelated))


(defn ws-handler
  "Handle incoming websocket requests"
  [request]
  (with-channel request channel
    (on-close channel (fn [msg] (println "Channel closed!")))
    (on-receive channel (fn [msg]
                          (do
                            (info "sending data")
                            (send! channel (pr-str (dispatch-request (read-string msg)))))))))


(defroutes handler
  (resources "/")
  (GET "/data/ws" [] ws-handler)
  (GET "/*" [] (io/resource "public/index.html")))


(defn -main [& args]
  (info "warming up...")
  (run-server (site #'handler) {:port 8091 :join? false})
  (info "running!\nVisit http://localhost:8091"))


(comment

  (def stop-server (run-server (site #'handler) {:port 8091 :join? false}))

  (stop-server)

  )
