(ns ceres-analytics.cascade
  (:refer-clojure :exclude [find sort])
  (:require [monger.collection :as mc]
            [aprint.core :refer [aprint]]
            [monger.joda-time]
            [monger.operators :refer :all]
            [monger.query :refer :all]
            [ceres-analytics.core :refer [db]]
            [clj-time.core :as t]))



(defn find-publications
  "Find all messages published by user in specific interval"
  [uid [start end]]
  (map
   (fn [{:keys [target]}]
     (assoc (mc/find-map-by-id @db "messages" target) :type :source))
   (mc/find-maps @db "pubs" {:source uid
                             :ts {$gt (t/date-time 2015 3 26 start )
                                  $lt (t/date-time 2015 3 26 end)}})))


(defn find-links [{:keys [source target group ts] :as link}]
  (let [colls {"replies" 2 "retweets" 3 "shares" 4}]
    (conj (apply concat
                 (map (fn [coll]
                        (apply concat
                               (map
                                (fn [{:keys [source target ts]}]
                                  (find-links {:source source :target target :group (colls coll)}))
                                (mc/find-maps @db coll {:target source}))))
                      (keys colls)))
          link)))


(defn get-user-tree [username]
  (let [user (mc/find-one-as-map @db "users" {:name username})
        user-node {:name (:_id user) :value (:name user) :group 1}
        pubs (into #{} (map :target (mc/find-maps @db "pubs" {:source (:_id user)
                                                              :ts {$gt (t/date-time 2015 3 26 12) $lt (t/date-time 2015 3 26 13)}})))
        links (->> (mc/find-maps @db "sources" {:ts {$gt (t/date-time 2015 3 26 12) $lt (t/date-time 2015 3 26 13)}
                                                :target {$in pubs}})
                   (map (comp find-links
                              (fn [{:keys [source target ts]}]
                                {:source target
                                 :target (:_id user)
                                 :group 1})
                              #(mc/find-one-as-map @db "pubs" {:target (:_id %)})
                              #(mc/find-map-by-id @db "messages" (:target %))))
                   (apply concat))
        nodes (->> links
                   (map (comp (fn [{:keys [_id text ts]}]
                                {:name _id :value text :group 2})
                              #(mc/find-map-by-id @db "messages" (:source %)))))]
    {:nodes (mapv #(update-in % [:name] str) (conj nodes user-node))
     :links (mapv (comp #(update-in % [:source] str) #(update-in % [:target] str) ) links)}))
