(ns ceres-analytics.cascade
  (:refer-clojure :exclude [find sort])
  (:require [monger.collection :as mc]
            [ceres-analytics.helpers :refer [db]]
            [aprint.core :refer [ap]]
            [monger.joda-time]
            [monger.operators :refer :all]
            [monger.query :refer :all]
            [clj-time.coerce :as c]
            [clj-time.core :as t]))



(defn find-links
  "Retrieve all neighbors of given contact"
  [{:keys [source target group ts] :as link} t0 tmax]
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
                                (mc/find-maps @db coll {:target source :ts {$gt t0 $lt tmax}}))))
                      (keys colls)))
          link)))


(defn get-user-tree
  "Create user reaction tree"
  [userid t0 tmax]
  (let [user (mc/find-one-as-map @db "users" {:id userid :ts {$lt (t/date-time 2015 4 1)}})
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
