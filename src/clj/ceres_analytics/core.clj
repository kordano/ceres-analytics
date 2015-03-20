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

(def news-accounts #{"FAZ_NET" "dpa" "tagesschau" "SPIEGELONLINE" "SZ" "BILD" "DerWesten" "ntvde" "tazgezwitscher" "welt" "ZDFheute" "N24_de" "sternde" "focusonline"})

(defn short-metrics [coll]
  {:mean (mean coll)
   :std (sd coll)
   :quantiles (let [probs [0.0 0.001 0.25 0.5 0.75 0.999 1.0]]
                (zipmap probs (quantile coll :probs probs)))})

(def nodes ["users" "messages" "htmls" "urls" "tags" ])

(def links ["mentions" "shares" "replies" "retweets" "urlrefs" "tagrefs" "unknown"])

(defn get-random-links []
  (let [publications (map
                      (fn [{:keys [_id name] :as user}]
                        [user
                         (map
                          #(mc/find-map-by-id @db "messages" (:target %))
                          (mc/find-maps @db "pubs" {:source _id
                                                    :ts {$gt (t/date-time 2015 3 19 8)
                                                         $lt (t/date-time 2015 3 19 9)}}))])
                      (mc/find-maps @db "users" {:name {$in news-accounts}}))]
    (loop [pubs publications
           result {:nodes []
                   :links []}
           user-counter 0]
      (if (empty? pubs)
        (-> (update-in result [:nodes] vec)
            (update-in [:links] vec))
        (let [[user messages] (first pubs)
              n (count messages)
              nodes (concat [{:name (str (:_id user)) :value (:name user) :group 1}] (map (fn [{:keys [text _id]}] {:name (str _id) :value text :group 2}) messages))
              links (mapv (fn [i] {:source i :target user-counter}) (range (inc user-counter) (+ user-counter n 1)))]
          (recur (rest pubs)
                 (-> result
                     (update-in [:nodes] concat nodes)
                     (update-in [:links] concat links))
                 (+ user-counter n 1)))))))


(defn dispatch-request
  "Dispatch incoming requests"
  [{:keys [topic data]}]
  (case topic
    :graph (get-random-links)
    :unrelated))


(defn ws-handler
  "Handle incoming websocket requests"
  [request]
  (with-channel request channel
    (on-close channel (fn [msg] (println "Channel closed!")))
    (on-receive channel (fn [msg] (send! channel (pr-str (dispatch-request (read-string msg))))))))


(defroutes handler
  (resources "/")
  (GET "/data/ws" [] ws-handler)
  (GET "/*" [] (io/resource "public/index.html")))


(defn -main [& args]
  (run-server (site #'handler) {:port 8091 :join? false})
  (println "Server up and running!")
  (println  "Visit http://localhost:8091"))


(comment

  (def stop-server (run-server (site #'handler) {:port 8091 :join? false}))

  (stop-server)

  )
