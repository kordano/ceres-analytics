;; gorilla-repl.fileformat = 1

;; **
;;; # Overall Network
;;; 
;;; In the following the complete network is studying by using a subset of measures explained in Section (ref). This worksheet compiles measures between 3 April 2015 and 3 May 2015 by computing statistics for hourly expansions, distributions over 30 days, as well as the average value at a time of day on the complete dataset. 
;; **

;; @@
(ns overall-network
  (:refer-clojure :exclude [find sort])
  (:require [gorilla-plot.core :as plot]
            [gorilla-repl.table :as table]
            [gg4clj.core :as gg4clj]
            [monger.collection :as mc]
            [ceres-analytics.core :refer [t0 hour-range day-range]]
            [ceres-analytics.db :refer [db broadcasters news-users]]
            [ceres-analytics.measures :as ms]
            [ceres-analytics.cascade :as cs]
            [clojure.data.priority-map :refer [priority-map]]
            [clj-time.core :as t]
            [clj-time.coerce :as c]
            [clj-time.periodic :as p]
            [incanter.stats :as stats]
            [clj-time.format :as f]
            [monger.core :as mg]
            [monger.joda-time]
            [monger.operators :refer :all]
            [aprint.core :refer [aprint]]
            [monger.query :refer :all]))
;; @@
;; =>
;;; {"type":"html","content":"<span class='clj-nil'>nil</span>","value":"nil"}
;; <=

;; @@

;; @@
