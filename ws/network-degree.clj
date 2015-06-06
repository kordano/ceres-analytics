;; gorilla-repl.fileformat = 1

;; **
;;; # Network degree
;;; 
;;; This worksheet examines the degree by studying the distribution and density curve for each label, as well as overall statistics and evolution of the average over the observation period.
;; **

;; @@
(ns fuscia-leaves
 (:require [gorilla-plot.core :as plot]
            [gorilla-repl.table :as table]
            [gg4clj.core :as gg4clj]
            [ceres-analytics.core :refer [t0 tmax]]
            [ceres-analytics.measures :refer [degree format-to-table-view table-columns cascades contacts]]
            [incanter.stats :as stats]
            [incanter.core :as incntr]))
;; @@
;; =>
;;; {"type":"html","content":"<span class='clj-nil'>nil</span>","value":"nil"}
;; <=

;; @@
(table/table-view (map (fn [[k v]] (concat [k] (format-to-table-view v))) (inter-contact-times contacts t0 tmax :statistics) ) :columns table-columns)
;; @@
