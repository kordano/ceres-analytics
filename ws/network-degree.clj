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

;; **
;;; ## Statistics
;; **

;; @@
(let [avgs (degree t0 tmax :statistics)
      totals {"TOTALS" (apply merge-with + (vals avgs))}]
(table/table-view (map (fn [[k v]] (concat [k] (format-to-table-view v))) (merge avgs totals) ) :columns table-columns))
;; @@
;; =>
;;; {"type":"list-like","open":"<center><table>","close":"</table></center>","separator":"\n","items":[{"type":"list-like","open":"<tr><th>","close":"</th></tr>","separator":"</th><th>","items":[{"type":"html","content":"<span class='clj-string'>&quot;Label&quot;</span>","value":"\"Label\""},{"type":"html","content":"<span class='clj-string'>&quot;Average&quot;</span>","value":"\"Average\""},{"type":"html","content":"<span class='clj-string'>&quot;Standard Deviation&quot;</span>","value":"\"Standard Deviation\""},{"type":"html","content":"<span class='clj-string'>&quot;Medium&quot;</span>","value":"\"Medium\""},{"type":"html","content":"<span class='clj-string'>&quot;Minimum&quot;</span>","value":"\"Minimum\""},{"type":"html","content":"<span class='clj-string'>&quot;Maximum&quot;</span>","value":"\"Maximum\""},{"type":"html","content":"<span class='clj-string'>&quot;Count&quot;</span>","value":"\"Count\""}],"value":"[\"Label\" \"Average\" \"Standard Deviation\" \"Medium\" \"Minimum\" \"Maximum\" \"Count\"]"},{"type":"list-like","open":"<tr><td>","close":"</td></tr>","separator":"</td><td>","items":[{"type":"html","content":"<span class='clj-string'>&quot;TOTALS&quot;</span>","value":"\"TOTALS\""},{"type":"html","content":"<span class='clj-double'>11.171928301853388</span>","value":"11.171928301853388"},{"type":"html","content":"<span class='clj-double'>130.90222337677696</span>","value":"130.90222337677696"},{"type":"html","content":"<span class='clj-double'>4.0</span>","value":"4.0"},{"type":"html","content":"<span class='clj-double'>3.0</span>","value":"3.0"},{"type":"html","content":"<span class='clj-double'>23458.0</span>","value":"23458.0"},{"type":"html","content":"<span class='clj-long'>234586</span>","value":"234586"}],"value":"(\"TOTALS\" 11.171928301853388 130.90222337677696 4.0 3.0 23458.0 234586)"},{"type":"list-like","open":"<tr><td>","close":"</td></tr>","separator":"</td><td>","items":[{"type":"html","content":"<span class='clj-string'>&quot;users&quot;</span>","value":"\"users\""},{"type":"html","content":"<span class='clj-double'>4.1861173494411315</span>","value":"4.1861173494411315"},{"type":"html","content":"<span class='clj-double'>32.28243060767183</span>","value":"32.28243060767183"},{"type":"html","content":"<span class='clj-double'>1.0</span>","value":"1.0"},{"type":"html","content":"<span class='clj-double'>1.0</span>","value":"1.0"},{"type":"html","content":"<span class='clj-double'>3191.0</span>","value":"3191.0"},{"type":"html","content":"<span class='clj-unkown'>77478</span>","value":"77478"}],"value":"(\"users\" 4.1861173494411315 32.28243060767183 1.0 1.0 3191.0 77478)"},{"type":"list-like","open":"<tr><td>","close":"</td></tr>","separator":"</td><td>","items":[{"type":"html","content":"<span class='clj-string'>&quot;messages&quot;</span>","value":"\"messages\""},{"type":"html","content":"<span class='clj-double'>5.314043985833588</span>","value":"5.314043985833588"},{"type":"html","content":"<span class='clj-double'>97.63695266250838</span>","value":"97.63695266250838"},{"type":"html","content":"<span class='clj-double'>2.0</span>","value":"2.0"},{"type":"html","content":"<span class='clj-double'>1.0</span>","value":"1.0"},{"type":"html","content":"<span class='clj-double'>20255.0</span>","value":"20255.0"},{"type":"html","content":"<span class='clj-unkown'>45742</span>","value":"45742"}],"value":"(\"messages\" 5.314043985833588 97.63695266250838 2.0 1.0 20255.0 45742)"},{"type":"list-like","open":"<tr><td>","close":"</td></tr>","separator":"</td><td>","items":[{"type":"html","content":"<span class='clj-string'>&quot;tags&quot;</span>","value":"\"tags\""},{"type":"html","content":"<span class='clj-double'>1.6717669665786685</span>","value":"1.6717669665786685"},{"type":"html","content":"<span class='clj-double'>0.982840106596729</span>","value":"0.982840106596729"},{"type":"html","content":"<span class='clj-double'>1.0</span>","value":"1.0"},{"type":"html","content":"<span class='clj-double'>1.0</span>","value":"1.0"},{"type":"html","content":"<span class='clj-double'>12.0</span>","value":"12.0"},{"type":"html","content":"<span class='clj-unkown'>111366</span>","value":"111366"}],"value":"(\"tags\" 1.6717669665786685 0.982840106596729 1.0 1.0 12.0 111366)"}],"value":"#gorilla_repl.table.TableView{:contents ((\"TOTALS\" 11.171928301853388 130.90222337677696 4.0 3.0 23458.0 234586) (\"users\" 4.1861173494411315 32.28243060767183 1.0 1.0 3191.0 77478) (\"messages\" 5.314043985833588 97.63695266250838 2.0 1.0 20255.0 45742) (\"tags\" 1.6717669665786685 0.982840106596729 1.0 1.0 12.0 111366)), :opts (:columns [\"Label\" \"Average\" \"Standard Deviation\" \"Medium\" \"Minimum\" \"Maximum\" \"Count\"])}"}
;; <=

;; **
;;; ## Distributions
;; **

;; @@

;; @@
