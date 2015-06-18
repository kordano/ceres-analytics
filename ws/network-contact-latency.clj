;; gorilla-repl.fileformat = 1

;; **
;;; # Network Contact Latency 
;;; 
;;; This worksheet examines the contact latency by studying the distribution and density curve for each label, as well as overall statistics and evolution of the average over the observation period.
;; **

;; @@
(ns fuscia-cove
  (:require [gorilla-plot.core :as plot]
            [gorilla-repl.table :as table]
            [gg4clj.core :as gg4clj]
            [ceres-analytics.measures :refer [contact-latency]]
             [ceres-analytics.helpers :refer [element-color element-name contacts cascades nodes table-columns format-to-table-view t0 tmax]]
            [incanter.stats :as stats]
            [incanter.core :as incntr]))
;; @@
;; =>
;;; {"type":"html","content":"<span class='clj-nil'>nil</span>","value":"nil"}
;; <=

;; **
;;; ## Overall Statistics
;; **

;; @@
(table/table-view
  (map (fn [[k v]] 
         (concat [(element-name k)] 
                 (format-to-table-view v))) 
       (zipmap cascades 
               (map #(contact-latency % t0 tmax :statistics) cascades)))
  :columns table-columns)
;; @@
;; =>
;;; {"type":"list-like","open":"<center><table>","close":"</table></center>","separator":"\n","items":[{"type":"list-like","open":"<tr><th>","close":"</th></tr>","separator":"</th><th>","items":[{"type":"html","content":"<span class='clj-string'>&quot;Subclass&quot;</span>","value":"\"Subclass\""},{"type":"html","content":"<span class='clj-string'>&quot;Average&quot;</span>","value":"\"Average\""},{"type":"html","content":"<span class='clj-string'>&quot;Standard Deviation&quot;</span>","value":"\"Standard Deviation\""},{"type":"html","content":"<span class='clj-string'>&quot;Median&quot;</span>","value":"\"Median\""},{"type":"html","content":"<span class='clj-string'>&quot;Minimum&quot;</span>","value":"\"Minimum\""},{"type":"html","content":"<span class='clj-string'>&quot;Maximum&quot;</span>","value":"\"Maximum\""},{"type":"html","content":"<span class='clj-string'>&quot;95 Percentile&quot;</span>","value":"\"95 Percentile\""},{"type":"html","content":"<span class='clj-string'>&quot;Count&quot;</span>","value":"\"Count\""}],"value":"[\"Subclass\" \"Average\" \"Standard Deviation\" \"Median\" \"Minimum\" \"Maximum\" \"95 Percentile\" \"Count\"]"},{"type":"list-like","open":"<tr><td>","close":"</td></tr>","separator":"</td><td>","items":[{"type":"html","content":"<span class='clj-string'>&quot;Re-tweet&quot;</span>","value":"\"Re-tweet\""},{"type":"html","content":"<span class='clj-double'>5.620070604089126</span>","value":"5.620070604089126"},{"type":"html","content":"<span class='clj-double'>28.120401508699366</span>","value":"28.120401508699366"},{"type":"html","content":"<span class='clj-double'>0.415</span>","value":"0.415"},{"type":"html","content":"<span class='clj-double'>0.0</span>","value":"0.0"},{"type":"html","content":"<span class='clj-double'>903.536388888889</span>","value":"903.536388888889"},{"type":"html","content":"<span class='clj-double'>19.85472222222222</span>","value":"19.85472222222222"},{"type":"html","content":"<span class='clj-unkown'>154681</span>","value":"154681"}],"value":"(\"Re-tweet\" 5.620070604089126 28.120401508699366 0.415 0.0 903.536388888889 19.85472222222222 154681)"},{"type":"list-like","open":"<tr><td>","close":"</td></tr>","separator":"</td><td>","items":[{"type":"html","content":"<span class='clj-string'>&quot;Reply&quot;</span>","value":"\"Reply\""},{"type":"html","content":"<span class='clj-double'>4.424451023925566</span>","value":"4.424451023925566"},{"type":"html","content":"<span class='clj-double'>23.811763784188983</span>","value":"23.811763784188983"},{"type":"html","content":"<span class='clj-double'>0.3608333333333333</span>","value":"0.3608333333333333"},{"type":"html","content":"<span class='clj-double'>0.0</span>","value":"0.0"},{"type":"html","content":"<span class='clj-double'>891.4425</span>","value":"891.4425"},{"type":"html","content":"<span class='clj-double'>15.833611111111114</span>","value":"15.833611111111114"},{"type":"html","content":"<span class='clj-unkown'>56151</span>","value":"56151"}],"value":"(\"Reply\" 4.424451023925566 23.811763784188983 0.3608333333333333 0.0 891.4425 15.833611111111114 56151)"},{"type":"list-like","open":"<tr><td>","close":"</td></tr>","separator":"</td><td>","items":[{"type":"html","content":"<span class='clj-string'>&quot;Share&quot;</span>","value":"\"Share\""},{"type":"html","content":"<span class='clj-double'>24.319763958084433</span>","value":"24.319763958084433"},{"type":"html","content":"<span class='clj-double'>84.70524580841283</span>","value":"84.70524580841283"},{"type":"html","content":"<span class='clj-double'>3.64</span>","value":"3.64"},{"type":"html","content":"<span class='clj-double'>0.0</span>","value":"0.0"},{"type":"html","content":"<span class='clj-double'>1242.893055555556</span>","value":"1242.893055555556"},{"type":"html","content":"<span class='clj-double'>91.24694444444444</span>","value":"91.24694444444444"},{"type":"html","content":"<span class='clj-unkown'>11441</span>","value":"11441"}],"value":"(\"Share\" 24.319763958084433 84.70524580841283 3.64 0.0 1242.893055555556 91.24694444444444 11441)"}],"value":"#gorilla_repl.table.TableView{:contents ((\"Re-tweet\" 5.620070604089126 28.120401508699366 0.415 0.0 903.536388888889 19.85472222222222 154681) (\"Reply\" 4.424451023925566 23.811763784188983 0.3608333333333333 0.0 891.4425 15.833611111111114 56151) (\"Share\" 24.319763958084433 84.70524580841283 3.64 0.0 1242.893055555556 91.24694444444444 11441)), :opts (:columns [\"Subclass\" \"Average\" \"Standard Deviation\" \"Median\" \"Minimum\" \"Maximum\" \"95 Percentile\" \"Count\"])}"}
;; <=

;; **
;;; ## Distributions
;; **

;; @@
(defn plot-cl-dist [coll]
  (let [dat {:latency (map incntr/log10 (remove #{0} (contact-latency coll t0 tmax :distribution)))}]
    (gg4clj/view
      [[:library "scales"]
        [:<- :d (gg4clj/data-frame dat)]
       (gg4clj/r+
         [:ggplot :d [:aes :latency]]
         [:geom_histogram [:aes {:y :..density.. }] {:binwidth 0.25 :colour "black" :fill "white"}]
         [:geom_density {:alpha 0.2 :fill "red"}]
         [:xlab "in 10^x Seconds"]
         [:ylab "Density"]
         [:theme_bw]
         [:ggtitle (str "Latency Histogram of " (element-name coll))])]
      {:width 5 :height 5})) )
;; @@
;; =>
;;; {"type":"html","content":"<span class='clj-var'>#&#x27;fuscia-cove/plot-cl-dist</span>","value":"#'fuscia-cove/plot-cl-dist"}
;; <=

;; @@
#_(for [c cascades]
  (plot-cl-dist c))
;; @@

;; **
;;; ## Average Evolution
;; **

;; @@
(defn plot-latency-evolution [coll]
(let [curr (map :mean (contact-latency coll t0 tmax :evolution))
       dat {:ltncs curr
            :days (range (count curr))}]
    (gg4clj/view
      [[:<- :d (gg4clj/data-frame dat)]
       (gg4clj/r+
         [:ggplot :d [:aes :days :ltncs]]
         [:geom_line {:color "#999999"}]
         [:xlab "Day"]
         [:ylab "Average latency in hours"]
         [:theme_bw]
         [:ggtitle (str "Average latency evolution of " (element-name coll))])]
      {:width 5 :height 5})))
;; @@
;; =>
;;; {"type":"html","content":"<span class='clj-var'>#&#x27;fuscia-cove/plot-latency-evolution</span>","value":"#'fuscia-cove/plot-latency-evolution"}
;; <=

;; @@
#_(for [c cascades]
  (plot-latency-evolution c))
;; @@
