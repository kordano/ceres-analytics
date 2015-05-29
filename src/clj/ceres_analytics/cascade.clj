(ns ceres-analytics.cascade
  (:refer-clojure :exclude [find sort])
  (:require [monger.collection :as mc]
            [monger.core :as mg]
            [aprint.core :refer [ap]]
            [incanter.stats :as stats]
            [monger.joda-time]
            [monger.operators :refer :all]
            [monger.query :refer :all]
            [ceres-analytics.measures :refer [news-authors]]
            [clj-time.coerce :as c]
            [clj-time.core :as t]))


(def time-slice-0 {$gt (t/date-time 2015 4 5) $lt (t/date-time 2015 5 5)})

(def db (atom
         (let [^MongoOptions opts (mg/mongo-options {:threads-allowed-to-block-for-connection-multiplier 300})
               ^ServerAddress sa  (mg/server-address (or (System/getenv "DB_PORT_27017_TCP_ADDR") "127.0.0.1") 27017)]
           (mg/get-db (mg/connect sa opts) "juno"))))


(defn find-links [{:keys [source target group ts] :as link} time-slice]
  (let [colls {"replies" 3 "retweets" 4 "shares" 5}]
    (conj (apply concat
                 (pmap (fn [coll]
                        (apply concat
                               (pmap
                                (fn [{:keys [source target ts]}]
                                  (find-links {:source source
                                               :target target
                                               :ts (c/to-string ts)
                                               :group (colls coll)}
                                              time-slice))
                                (mc/find-maps @db coll {:target source
                                                        :ts time-slice}))))
                      (keys colls)))
          link)))


(defn get-user-tree [username time-slice]
  (let [user (mc/find-one-as-map @db "users" {:name username})
        user-node {:name (:_id user) :value (:name user) :group 1}
        pubs (into #{} (map :target (mc/find-maps @db "pubs" {:source (:_id user) :ts time-slice})))
        links (->> (mc/find-maps @db "sources" {:ts time-slice :target {$in pubs}})
                   (pmap (comp #(find-links % time-slice)
                              (fn [{:keys [source target ts]}]
                                {:source target
                                 :target (:_id user)
                                 :ts ts
                                 :group 2})
                              #(mc/find-one-as-map @db "pubs" {:target (:_id %)})
                              #(mc/find-map-by-id @db "messages" (:target %)))))
        nodes (->> links
                   (apply concat)
                   (pmap (comp (fn [{:keys [_id text group ts]}]
                                {:name _id :value text :group group :ts ts})
                              (fn [{:keys [source group]}]
                                (assoc (mc/find-map-by-id @db "messages" source) :group group)))))]
    {:nodes (mapv #(update-in % [:name] str) nodes)
     :links (->> links
                 (pmap
                  #(remove (fn [l] (= (:name user-node) (:target l))) %)
                  ;; only for frontend visualisation needed
                  #_(comp #(mapv (comp (fn [l] (update-in l [:source] str))
                                       (fn [l] (update-in l [:target] str))) %))) 
                 vec)}))


(defn lifetimes
  ""
  [users compounds]
  (->> users
       (pmap
        (fn [{:keys [name]}]
          [name
           (->> (get compounds name)
                :links
                (pmap (fn [ls] (let [contact-times (map (comp c/to-long :ts) ls)]
                                 (if (empty? contact-times)
                                   0
                                   (->> (t/interval
                                         (c/from-long (apply min contact-times))
                                         (c/from-long (apply max contact-times)))
                                        t/in-minutes
                                        Math/floor)))))
                stats/mean)]))
       (into {})))


(defn compounds [users time-slice]
  (zipmap (pmap :name users) 
          (pmap #(get-user-tree (:name %) time-slice) users)))

(comment
  
  (->> (mc/find-maps @db "tagrefs" {:ts {$gt (t/date-time 2015 4 5)
                                         $lt (t/date-time 2015 5 5)}})
       (map :target)
       frequencies
       (sort-by val >)
       (take 20)
       (map (fn [[k v]] [(:text (mc/find-map-by-id @db "tags" k)) v])))

  
  (ap)

  )
 
