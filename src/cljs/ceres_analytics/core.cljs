(ns ceres-analytics.core
  (:require [strokes :refer [d3]]))

(enable-console-print!)

(strokes/bootstrap)

(def graph-data-1 {:nodes [{:name "a1" :group 1} {:name "a2" :group 1} {:name "m1" :group 2} {:name "m2" :group 2}]
                   :links [{:source 0 :target 2}{:source 1 :target 3} {:source 3 :target 2} ]})

(println "Hell yeah!")

(defn clear-canvas [frame]
  (.. d3
      (select frame)
      (select "svg")
      remove))


(defn draw-fdg
  "Draw force-directed graph"
  [data frame]
  (let [width 1080
        height 920
        color (.. d3 -scale category10)
        force (.. d3 -layout force (charge -100)  (linkDistance 20) (size [width height]))
        svg (.. d3
                (select frame)
                (append "svg")
                (attr {:width width
                        :height height}))]
    (.. force
        (nodes (.-nodes data))
        (links (.-links data))
        start)
    (let [link (.. svg
                   (selectAll ".link")
                   (data (.-links data))
                   enter
                   (append "line")
                   (attr {:class "link"})
                   (style {:stroke-width 1
                           :stroke "grey"})
                   )
          node (.. svg
                   (selectAll ".node")
                   (data (.-nodes data))
                   enter
                   (append "circle")
                   (attr {:class "node"
                           :r "4"})
                   (style {:fill (fn [d] (color (.-group d)))})
                   (call (.-drag force)))]
      (do
        (.. node (append "title") (text (fn [d] (.-value d))))
        (.. force
            (on "tick"
                 (fn []
                   (.. link
                       (attr
                        {:x1 #(.. % -source -x)
                         :y1 #(.. % -source -y)
                         :x2 #(.. % -target -x)
                         :y2 #(.. % -target -y)}))
                   (.. node
                       (attr {:cx #(.-x %)
                               :cy #(.-y %)})))))))))

(draw-fdg graph-data-1 "#graph-container")
