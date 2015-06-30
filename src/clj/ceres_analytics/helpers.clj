(ns ceres-analytics.helpers
  (:refer-clojure :exclude [find sort])
  (:require [incanter.stats :as stats]
            [monger.core :as mg]
            [monger.collection :as mc]
            [clj-time.core :as t]
            [aprint.core :refer [ap]]
            [monger.operators :refer :all]
            [monger.query :refer :all]))

(def t0 (t/date-time 2015 4 5))
(def tmax (t/date-time 2015 5 5))

(def db
  (atom
   (let [^MongoOptions opts (mg/mongo-options {:threads-allowed-to-block-for-connection-multiplier 300})
         ^ServerAddress sa  (mg/server-address (or (System/getenv "DB_PORT_27017_TCP_ADDR") "127.0.0.1") 27017)]
     (mg/get-db (mg/connect sa opts) "juno"))))

(def broadcasters
  {2834511 "Spiegel"
   5494392 "Focus"
   5734902 "Tagesschau"
   8720562 "Welt"
   9204502 "Bild"
   15071293 "Westen"
   15243812 "TAZ"
   ;15738602 "N24"
   18016521 "FAZ"
   18774524 "Stern"
   19232587 "ntv"
   40227292 "dpa"
   114508061 "SZ"
   1101354170 "ZDF"})

(def news-authors
  (->> (mc/find-maps @db "users" {:id {$in (keys broadcasters)}
                                  :ts {$lt (t/date-time 2015 4 1)}})
       (map #(select-keys % [:name :_id]))))


(def contacts ["shares" "replies" "retweets" "tagrefs" "pubs" "unknown" "sources"])
(def cascades ["shares" "replies" "retweets"])
(def nodes ["users" "messages" "tags"])

(defn mongo-time [t0 tmax] {:ts {$gt t0 $lt tmax}})

(defn statistics [coll]
  (let [percentiles {:q0 0 :q50 0.5 :q100 1 :q95 0.95}
        quantiles (zipmap (keys percentiles) (stats/quantile coll :probs (vals percentiles)))]
    (merge quantiles
           {:mean (stats/mean coll)
            :variance (stats/variance coll)
            :count (count coll)
            :sd (stats/sd coll)})))


(defn format-to-table-view
  "Formats statistics to mean, sd, median, minimum, maximum"
  [{:keys [count mean sd q0 q50 q100 q95 variance]}]
  [mean q50 sd variance q0 q100 q95 count])


(def table-columns ["Subclass" "Average" "Median" "Standard Deviation" "Variance" "Minimum" "Maximum" "95 Percentile" "Count"])


(defn element-name [e]
  (case e
    "pubs" "Publication"
    "unknown" "Unknown"
    "tagrefs" "Assignment"
    "shares" "Share"
    "retweets" "Re-tweet"
    "replies" "Reply"
    "users" "Author"
    "messages" "Message"
    "tags" "Topic"
    "sources" "Article"
    e))


(defn element-color [e]
  (case e
    "unknown" "#001f3f"
    "pubs" "#3d9970"
    "tagrefs" "#ff851b"
    "retweets" "#b10dc9"
    "replies" "#007dd9"
    "shares" "#7fdbff"
    "users" "#39cccc"
    "messages" "#111111"
    "tags" "#2ecc40"
    :unrelated))


(comment
  
  (->> (mc/find-maps @db "users" {:id {$in (keys broadcasters)}
                                  :ts {$lt (t/date-time 2015 4 1)}})
       (map (comp (fn [[k v]] [v k]) vec vals #(select-keys % [:id :name])))
       (into {}))

  (ap))
