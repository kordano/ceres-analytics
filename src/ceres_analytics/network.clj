(ns ceres-analytics.network
  (:refer-clojure :exclude [find sort])
  (:require [monger.collection :as mc]
            [ceres-analytics.core :refer [db news-accounts]]
            [clj-time.core :as t]
            [clj-time.periodic :as p]
            [clj-time.format :as f]
            [aprint.core :refer [aprint]]
            [monger.core :as mg]
            [monger.joda-time]
            [monger.operators :refer :all]
            [monger.query :refer :all])
  (:import [edu.uci.ics.jung.graph DirectedSparseGraph]
           [edu.uci.ics.jung.graph.util EdgeType]
           [edu.uci.ics.jung.algorithms.metrics Metrics StructuralHoles TriadicCensus]))



(defn create-message-network [adj-list]
  (let [g (DirectedSparseGraph.)]
    (doall
     (pmap
      (fn [[vertex edges]]
        (.addVertex g vertex)
        (doall
         (pmap
          (fn [{:keys [_id target]}]
            (.addVertex g target)
            (.addEdge g _id target vertex)) edges)))
      adj-list))
    g))


(comment


  (def news-users (mc/find-maps @db "users" {:name {$in news-accounts}}))

  (def news-pubs
    (map
     (fn [{:keys [_id]}]
       [_id (mc/find-maps @db "pubs" {:source _id :target {$ne nil}})])
     news-users))

  (def message-nw (create-message-network news-pubs))


  )
