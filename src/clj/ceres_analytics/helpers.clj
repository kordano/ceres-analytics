(ns ceres-analytics.helpers
  (:refer-clojure :exclude [find sort])
  (:require [incanter.stats :as stats]
            [monger.operators :refer :all]
            [monger.query :refer :all]))

(def contacts ["shares" "replies" "retweets" "tagrefs" "pubs" "unknown"])
(def cascades ["shares" "replies" "retweets"])
(def nodes ["users" "messages" "tags"])

(defn mongo-time[t0 tmax]
  {:ts {$gt t0 $lt tmax}})

(defn statistics [coll]
  (let [percentiles {:q0 0 :q50 0.5 :q100 1}
        quantiles (zipmap (keys percentiles) (stats/quantile coll :probs (vals percentiles)))]
    (merge quantiles
           {:mean (stats/mean coll)
            :count (count coll)
            :sd (stats/sd coll)})))


(defn format-to-table-view
  "Formats statistics to mean, sd, median, minimum, maximum"
  [{:keys [count mean sd q0 q50 q100]}]
  [mean sd q50 q0 q100 count])

(def table-columns ["Label" "Average" "Standard Deviation" "Medium" "Minimum" "Maximum" "Count"])

