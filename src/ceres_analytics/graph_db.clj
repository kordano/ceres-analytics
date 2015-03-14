(ns ceres-analytics.graph-db
  (:refer-clojure :exclude [find sort])
  (:require [clojurewerkz.neocons.rest :as nr]
            [clojurewerkz.neocons.rest.nodes :as nn]
            [clojurewerkz.neocons.rest.relationships :as nrl]
            [clojurewerkz.neocons.rest.labels :as nl]
            [clojurewerkz.neocons.rest.cypher :as cy]
            [monger.collection :as mc]
            [ceres-analytics.core :refer [db news-accounts]]
            [clj-time.core :as t]
            [aprint.core :refer [aprint]]
            [monger.joda-time]
            [monger.operators :refer :all]
            [monger.query :refer :all])
  (:import org.bson.types.ObjectId))

(def conn (nr/connect "http://localhost:7474/db/data"))

(defn drop-database [conn]
  (cy/tquery conn "MATCH (n) OPTIONAL MATCH (n)-[r]-() DELETE n,r"))


(comment

  (def users  (mc/find-maps @db "users" {:name {$in news-accounts}}))

  (def all-users (mc/find-maps @db "users"))


  (def user->neo
    (doall
     (pmap
      (fn [{:keys [_id name ts]}]
        (let [node (nn/create conn {:name name :mongo-id (str _id) :ts ts})]
          (nl/add conn node "Agent"))
        all-users))))

  (def message->neo
    (pmap
     (fn [{:keys [id data]}]
       (let [messages (doall (pmap (fn [{:keys [target]}]
                                     (mc/find-map-by-id @db "messages" target))
                                   (mc/find-maps @db "refs" {:source (ObjectId. (data :mongo-id))})))
             neo-messages (doall
                           (pmap
                            (fn [{:keys [_id text ts]}]
                              (nn/create conn {:text text
                                               :mongo-id (str _id)
                                               :ts ts}))
                            messages))]
         (pmap #(nl/add conn % "Message") neo-messages)
         (doall (map #(nrl/create conn id (:id %) :PUBLISHES) neo-messages))))
     user->neo))

  (count message->neo)

  (cy/tquery conn "MATCH u-[:PUBLISHES]->(m) RETURN u.name,count(m) ORDER BY count(m) DESC LIMIT 20;")


  (let [uid (-> users first :_id)]
    (->> {:source uid}
         (mc/find-maps @db "refs")
         (map (comp (fn [{:keys [_id]}] (map :type (mc/find-maps @db "refs" {:target _id})))
                    (fn [{:keys [target]}] (mc/find-map-by-id @db "messages" target))))
         aprint))



  (drop-database conn)

  )
