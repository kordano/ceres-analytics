 (ns ceres-analytics.core
  (:gen-class :main true)
  (:refer-clojure :exclude [find sort])
  (:require [ceres-analytics.cascade :as cascade]
            [clojure.java.io :as io]
            [compojure.route :refer [resources]]
            [compojure.core :refer [GET POST defroutes]]
            [compojure.handler :refer [site api]]
            [taoensso.timbre :refer [info debug error warn] :as timbre]
            [clj-time.core :as t]
            [org.httpkit.server :refer [with-channel on-receive on-close run-server send!]]))

(timbre/refer-timbre)

(defn dispatch-request
  "Dispatch incoming requests"
  [{:keys [topic data]}]
  (let [{:keys [broadcaster day]} data]
    (case topic
      :user-tree (cascade/get-user-tree-d3 broadcaster (t/date-time 2015 4 day) (t/date-time 2015 4 (inc day)))
      :unrelated)))

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
