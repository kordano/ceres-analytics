;; gorilla-repl.fileformat = 1

;; **
;;; # News Compounds
;;; 
;;; This worksheet studies the evolutionary patterns in news compounds for each broadcaster.
;; **

;; @@
(ns news-comppounds
  (:refer-clojure :exclude [find sort])
  (:require [gorilla-plot.core :as plot]
            [gorilla-repl.table :as table]
            [gg4clj.core :as gg4clj]
            [monger.collection :as mc]
            [ceres-analytics.core :refer [t0 tmax hour-range day-range]]
            [ceres-analytics.db :refer [db broadcasters]]
            [ceres-analytics.measures :as ms]
            [ceres-analytics.news-measures :as nm]
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
(def cmpnds (cs/compounds nm/news-authors t0 tmax))
;; @@

;; @@

;; @@
;; =>
;;; {"type":"list-like","open":"<span class='clj-vector'>[</span>","close":"<span class='clj-vector'>]</span>","separator":" ","items":[{"type":"html","content":"<span class='clj-nil'>nil</span>","value":"nil"},{"type":"list-like","open":"<span class='clj-map'>{</span>","close":"<span class='clj-map'>}</span>","separator":", ","items":[{"type":"list-like","open":"","close":"","separator":" ","items":[{"type":"html","content":"<span class='clj-keyword'>:nodes</span>","value":":nodes"},{"type":"list-like","open":"<span class='clj-vector'>[</span>","close":"<span class='clj-vector'>]</span>","separator":" ","items":[],"value":"[]"}],"value":"[:nodes []]"},{"type":"list-like","open":"","close":"","separator":" ","items":[{"type":"html","content":"<span class='clj-keyword'>:links</span>","value":":links"},{"type":"list-like","open":"<span class='clj-vector'>[</span>","close":"<span class='clj-vector'>]</span>","separator":" ","items":[],"value":"[]"}],"value":"[:links []]"}],"value":"{:nodes [], :links []}"}],"value":"[nil {:nodes [], :links []}]"}
;; <=

;; @@

;; @@
