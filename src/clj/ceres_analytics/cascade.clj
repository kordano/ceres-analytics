(ns ceres-analytics.cascade
  (:refer-clojure :exclude [find sort])
  (:require [monger.collection :as mc]
            [monger.core :as mg]
            [aprint.core :refer [aprint]]
            [monger.joda-time]
            [monger.operators :refer :all]
            [monger.query :refer :all]
            [clj-time.coerce :as c]
            [clj-time.core :as t]))


(def day {$gt (t/date-time 2015 4 5) $lt (t/date-time 2015 4 7)})

(def db (atom
         (let [^MongoOptions opts (mg/mongo-options {:threads-allowed-to-block-for-connection-multiplier 300})
               ^ServerAddress sa  (mg/server-address (or (System/getenv "DB_PORT_27017_TCP_ADDR") "127.0.0.1") 27017)]
           (mg/get-db (mg/connect sa opts) "juno"))))


(defn find-links [{:keys [source target group ts] :as link}]
  (let [colls {"replies" 3 "retweets" 4 "shares" 5}]
    (conj (apply concat
                 (map (fn [coll]
                        (apply concat
                               (map
                                (fn [{:keys [source target ts]}]
                                  (find-links {:source source
                                               :target target
                                               :ts (c/to-string ts)
                                               :group (colls coll)}))
                                (mc/find-maps @db coll {:target source
                                                        :ts day}))))
                      (keys colls)))
          link)))


(defn get-user-tree [username]
  (let [user (mc/find-one-as-map @db "users" {:name username})
        user-node {:name (:_id user) :value (:name user) :group 1}
        pubs (into #{} (map :target (mc/find-maps @db "pubs" {:source (:_id user) :ts day})))
        links (->> (mc/find-maps @db "sources" {:ts day :target {$in pubs}})
                   (map (comp find-links
                              (fn [{:keys [source target ts]}]
                                {:source target
                                 :target (:_id user)
                                 :ts ts
                                 :group 2})
                              #(mc/find-one-as-map @db "pubs" {:target (:_id %)})
                              #(mc/find-map-by-id @db "messages" (:target %))))
                   (apply concat))
        nodes (->> links
                   (map (comp (fn [{:keys [_id text group ts]}]
                                {:name _id :value text :group group :ts ts})
                              (fn [{:keys [source group]}]
                                (assoc (mc/find-map-by-id @db "messages" source) :group group)))))]
    {:nodes (mapv #(update-in % [:name] str) nodes)
     :links (mapv (comp #(update-in % [:source] str) #(update-in % [:target] str)) (remove #(= (:name user-node) (:target %)) links))}))


(comment


  (->> (mc/find-maps @db "messages")
       (map (comp (fn [d] [(t/month d) (t/day d)]) :ts))
       )

  (->> (mc/find-maps @db "messages" {:ts {$gt (t/date-time 2015 4 3)}})
       (pmap (comp t/day :ts))
       frequencies
       (sort-by first))

  (mc/count @db "messages" {:ts {$gt (t/date-time 2015 4 1)}})


  (->> (get-user-tree "tagesschau")
      :nodes
      (sort-by :ts t/before?)
      (partition-by (comp t/day :ts))
      first
      (partition-by (comp t/hour :ts))
      first
      (partition-by (comp t/minute :ts)))



  )
