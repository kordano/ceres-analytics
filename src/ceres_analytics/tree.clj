(ns ceres-analytics.tree
  (:refer-clojure :exclude [sort find])
  (:require [monger.collection :as mc]
            [monger.operators :refer :all]
            [monger.query :refer :all]
            [clojure.data.json :as json]
            [monger.joda-time]
            [clojure.string :refer [split join lower-case]]
            [clojure.walk :as walk]
            [clojure.zip :as zip]
            [aprint.core :refer [aprint]]
            [clj-time.core :as t]
            [clj-time.format :as f]
            [clj-time.coerce :as c]
            [clj-time.periodic :as p]
            [clojure.pprint :refer [pprint]]
            [opennlp.nlp :refer [make-tokenizer make-detokenizer]]
            [incanter.stats :refer [mean variance quantile]]
            [ceres-analytics.core :refer [db custom-formatter news-accounts suids]])
 (:import org.bson.types.ObjectId))

(defrecord Message [source reactions])


(defn find-reactions [pid]
  (let [reactions (mc/find-maps @db "refs" {:target pid
                                            :type {$in ["reply" "share" "retweet"]}})]
    (Message. pid (vec (pmap #(find-reactions (:source %)) reactions)))))


(defn reaction-tree [pub]
  (zip/zipper
   (fn [node] true)
   (fn [node] (:reactions node))
   (fn [node new-children] (assoc-in node [:reactions] new-children))
   (find-reactions pub)))


(defn find-full-reactions
  "extended reaction tree recursion"
  [pid]
  (let [publication (mc/find-map-by-id @db "publications" pid)
        reactions (mc/find-maps @db "reactions" {:source pid})]
    (Message. publication (vec (pmap #(find-full-reactions (:publication %)) reactions)))))


(defn full-reaction-tree [pub]
  (zip/zipper
   (fn [node] true)
   (fn [node] (:reactions node))
   (fn [node new-children] (assoc-in node [:reactions] new-children))
   (find-full-reactions pub)))


(defn tree-summary [tree]
  (loop [size 0
         max-path 0
         loc tree]
    (if (zip/end? loc)
      {:size size
       :source (-> (zip/root tree) :source)
       :height max-path}
      (recur
       (if (zip/node loc) (inc size) size)
       (if (zip/node loc) (-> loc zip/path count (max max-path)) max-path)
       (zip/next loc)))))


(defn analyze-delays
  "Create tree analyzing delay times relativ to first post time"
  [tree]
  (loop [delays []
         loc tree]
    (if (zip/end? loc)
      {:source (-> (zip/root tree) :source :_id)
       :delays delays}
      (recur
       (if (zip/node loc)
         (let [pub-time (-> (zip/root tree) :source :ts)
               post-delay (if (t/after? (-> loc zip/node :source :ts) pub-time)
                            (t/interval pub-time (-> loc zip/node :source :ts))
                            (t/interval (-> loc zip/node :source :ts) pub-time))]
           (conj delays (t/in-seconds post-delay)))
         delays)
       (zip/next loc)))))


(defn analyze-hashtags
  "Analyze hashtag development in tree"
  [tree]
  (loop [hashtags []
         loc tree]
    (if (zip/end? loc)
      {:source (-> (zip/root tree) :source :_id)
       :hashtags hashtags}
      (recur
       (if-let [node (zip/node loc)]
         (conj hashtags [(get-in node [:source :ts]) (get-in node [:source :hashtags])])
         hashtags)
       (zip/next loc)))))


(defn create-d3-graph
  "Converts zipper into d3 readable format"
  [tree]
  (loop [counter 0
         types {}
         nodes []
         texts {}
         links []
         loc tree]
    (if (zip/end? loc)
      {:nodes nodes
       :types types
       :texts texts
       :links links}
      (if-let [node (zip/node loc)]
        (let [status-text (->> node :source :tweet (mc/find-map-by-id @db "tweets") :text str)
              id (-> node :source :_id str)]
          (recur
           (inc counter)
           (assoc types id (-> node :source :type))
           (vec (conj nodes id))
           (assoc texts id status-text)
           (if-not (= node (zip/root tree))
             (conj links {:source counter :target (.indexOf nodes (-> loc zip/up zip/node :source :_id str))})
             links)
           (zip/next loc)))
        (recur counter types nodes texts links (zip/next loc))))))

(def tree-summaries
    (future
      (->> (mc/find-maps @db "publications" {:user {$in (keys suids)}})
           (pmap (comp tree-summary reaction-tree :_id)))))

(def full-summaries
  (future
    (->> (mc/find-maps @db "publications" {:user {$in (keys suids)}})
         (pmap (comp full-reaction-tree :_id)))))


(comment


  (def users (mc/find-maps @db "users" {:name {$in news-accounts}}))


  )
