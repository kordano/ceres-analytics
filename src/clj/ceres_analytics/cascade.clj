(ns ceres-analytics.cascade
  (:refer-clojure :exclude [find sort])
  (:require [monger.collection :as mc]
            [aprint.core :refer [aprint]]
            [monger.joda-time]
            [monger.operators :refer :all]
            [monger.query :refer :all]
            [ceres-analytics.core :refer [db]]
            [clj-time.coerce :as c]
            [clj-time.core :as t]))


(def day {$gt (t/date-time 2015 3 26) $lt (t/date-time 2015 3 27)})


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
                                                        :ts {$gt (t/date-time 2015 3 26)
                                                             $lt (t/date-time 2015 3 27)}}))))
                      (keys colls)))
          link)))


(defn get-user-tree [username]
  (let [user (mc/find-one-as-map @db "users" {:name username})
        user-node {:name (:_id user) :value (:name user) :group 1 :ts (-> user :ts c/to-string)}
        pubs (into #{} (map :target (mc/find-maps @db "pubs" {:source (:_id user) :ts day})))
        links (->> (mc/find-maps @db "sources" {:ts day :target {$in pubs}})
                   (map (comp find-links
                              (fn [{:keys [source target ts]}]
                                {:source target
                                 :target (:_id user)
                                 :ts (c/to-string ts)
                                 :group 2})
                              #(mc/find-one-as-map @db "pubs" {:target (:_id %)})
                              #(mc/find-map-by-id @db "messages" (:target %))))
                   (apply concat))
        nodes (->> links
                   (map (comp (fn [{:keys [_id text group]}]
                                {:name _id :value text :group group})
                              (fn [{:keys [source group]}]
                                (assoc (mc/find-map-by-id @db "messages" source) :group group)))))]
    {:nodes (mapv #(update-in % [:name] str) (conj nodes user-node))
     :links (mapv (comp #(update-in % [:source] str) #(update-in % [:target] str) ) links)}))



(comment


  (->> (get-user-tree "SPIEGELONLINE") time)

  )
