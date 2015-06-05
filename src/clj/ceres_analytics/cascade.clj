(ns ceres-analytics.cascade
  (:refer-clojure :exclude [find sort])
  (:require [monger.collection :as mc]
            [monger.core :as mg]
            [aprint.core :refer [ap]]
            [incanter.stats :as stats]
            [monger.joda-time]
            [monger.operators :refer :all]
            [monger.query :refer :all]
            [clj-time.coerce :as c]
            [clj-time.core :as t]))


(def time-slice-0 {$gt (t/date-time 2015 4 5) $lt (t/date-time 2015 5 5)})


(def db (atom
         (let [^MongoOptions opts (mg/mongo-options {:threads-allowed-to-block-for-connection-multiplier 300})
               ^ServerAddress sa  (mg/server-address (or (System/getenv "DB_PORT_27017_TCP_ADDR") "127.0.0.1") 27017)]
           (mg/get-db (mg/connect sa opts) "juno"))))


(defn find-links [{:keys [source target group ts] :as link} t0 tmax]
  (let [colls {"replies" 3 "retweets" 4 "shares" 5}]
    (conj (apply concat
                 (pmap (fn [coll]
                        (apply concat
                               (pmap
                                (fn [{:keys [source target ts]}]
                                  (find-links {:source source
                                               :target target
                                               :ts ts 
                                               :group (colls coll)}
                                              t0 tmax))
                                (mc/find-maps @db coll {:target source
                                                        :ts {$gt t0 $lt tmax}}))))
                      (keys colls)))
          link)))


(defn get-user-tree [username t0 tmax]
  (let [user (mc/find-one-as-map @db "users" {:name username})
        user-node {:name (:_id user) :value (:name user) :group 1}
        pubs (into #{} (map :target (mc/find-maps @db "pubs" {:source (:_id user)
                                                              :ts {$gt t0 $lt tmax}})))
        links (->> (mc/find-maps @db "sources" {:ts {$gt t0 $lt tmax} :target {$in pubs}})
                   (pmap (fn [s]
                           (let [original-message ((comp (fn [{:keys [source target ts]}]
                                                           {:source target
                                                            :target (:_id user)
                                                            :root true
                                                            :ts ts
                                                            :group 2})
                                                         #(mc/find-one-as-map @db "pubs" {:target (:_id %)})
                                                         #(mc/find-map-by-id @db "messages" (:target %))
                                                         ) s)]
                             [original-message (find-links original-message t0 tmax)]))))
        nodes (->> links
                    (pmap second)
                    (apply concat)
                    (pmap (comp (fn [{:keys [_id text group ts]}]
                                  {:name _id :value text :group group :ts ts})
                                (fn [{:keys [source group]}]
                                  (assoc (mc/find-map-by-id @db "messages" source) :group group)))))]
    {:nodes (mapv #(update-in % [:name] str) nodes)
     :links (->> links
                 (pmap
                  (fn [[k v]] [k (remove (fn [l] (= (:name user-node)  (:target l))) v)])
                  ;; only for frontend visualisation needed
                  #_(comp #(mapv (comp (fn [l] (update-in l [:source] str))
                                       (fn [l] (update-in l [:target] str))) %))) 
                 vec)}))


(defn compounds [users t0 tmax]
  (zipmap (pmap :name users) 
          (pmap #(get-user-tree (:name %) {$gt t0 $lt tmax}) users)))

(comment
  
  (->> (mc/find-maps @db "tagrefs" {:ts {$gt (t/date-time 2015 4 5)
                                         $lt (t/date-time 2015 5 5)}})
       (map :target)
       frequencies
       (sort-by val >)
       (take 20)
       (map (fn [[k v]] [(:text (mc/find-map-by-id @db "tags" k)) v])))

  (def t0 (t/date-time 2015 4 5))
  
  (def tmax (t/date-time 2015 4 15))
  
  (get-user-tree "SZ" t0 tmax)
  
  (ap)

  )
 
