;; gorilla-repl.fileformat = 1

;; **
;;; # News Compounds Statistical Analysis
;;; 
;;; Some correlations maybe...
;; **

;; @@
(ns zealous-sunset  
  (:require [gorilla-plot.core :as plot]
            [gorilla-repl.table :as table]
            [gg4clj.core :as gg4clj]
            [ceres-analytics.news-measures :refer [size-lifetime size-center-degree lifetime-degree size-radius]]
            [ceres-analytics.helpers :refer [t0 tmax db element-color element-name broadcasters table-columns format-to-table-view]]
            [incanter.stats :as stats]
            [incanter.core :as incntr]))
;; @@
;; =>
;;; {"type":"html","content":"<span class='clj-nil'>nil</span>","value":"nil"}
;; <=

;; **
;;; # Size vs Lifetime
;; **

;; @@
(let [result (size-lifetime broadcasters t0 tmax :distribution)
         dat {:xvar (map second result)
              :yvar (map first result)}]
    (gg4clj/view
      [[:<- :d (gg4clj/data-frame dat)]
       (gg4clj/r+
         [:ggplot :d [:aes :xvar :yvar]]
         [:geom_point {:size 1}]
         [:xlab "Hours"]
         [:ylab "Size"]
         [:theme_bw]
         [:ggtitle "Lifetime vs Size"])]
      {:width 5 :height 5}))
;; @@

;; **
;;; ## Sive vs Center-Degree
;; **

;; @@
(let [result (size-center-degree broadcasters t0 tmax :distribution)
      dat {:xvar (map first result)
           :yvar (map second result)}]
  (gg4clj/view
      [[:<- :d (gg4clj/data-frame dat)]
       (gg4clj/r+
         [:ggplot :d [:aes :xvar :yvar]]
         [:geom_point {:size 1}]
         [:xlab "Size"]
         [:ylab "Center Degree"]
         [:theme_bw]
         [:ggtitle "Size vs Center-Degree"])]
      {:width 5 :height 5}))
;; @@

;; **
;;; ## Lifetime vs Center-Degree
;; **

;; @@
(let [result (lifetime-degree broadcasters t0 tmax :distribution)
      dat {:xvar (map first result)
           :yvar (map second result)}]
  (gg4clj/view
      [[:<- :d (gg4clj/data-frame dat)]
       (gg4clj/r+
         [:ggplot :d [:aes :xvar :yvar]]
         [:geom_point {:shape 1 :size 1}]
         [:xlab "Hours"]
         [:ylab "Center Degree"]
         [:theme_bw]
         [:ggtitle "Lifetime vs Center-Degree"])]
      {:width 5 :height 5}))
;; @@

;; **
;;; # Radius vs Size
;; **

;; @@
(let [result (size-radius broadcasters t0 tmax :distribution)
      dat {:xvar (map second result)
           :yvar (map first result)}]
  (gg4clj/view
      [[:<- :d (gg4clj/data-frame dat)]
       (gg4clj/r+
         [:ggplot :d [:aes :xvar :yvar]]
         [:geom_point {:size 1}]
         [:scale_x_log10]
         [:xlab "Seconds"]       
         [:ylab "Size"]
         [:theme_bw]
         [:ggtitle "Radius vs Size"])]
      {:width 5 :height 5}))
;; @@
