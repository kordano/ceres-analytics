(ns ceres-analytics.core
  (:require [strokes :refer [d3]]
            [chord.client :refer [ws-ch]]
            [cljs.reader :as reader]
            [cljs.core.async :refer [<! >! put! close! timeout]])
  (:require-macros [cljs.core.async.macros :refer [go]]))

(enable-console-print!)

(strokes/bootstrap)

(def graph-data-1 {:nodes [{:name "a1" :group 1} {:name "a2" :group 1} {:name "m1" :group 2} {:name "m2" :group 2} {:name "m3" :group 2} {:name "m4" :group 2}]
                   :links [{:source 0 :target 2}{:source 1 :target 3} {:source 3 :target 2} ]})

(def graph-data-2 {:nodes [{:name "a1" :group 1} {:name "a2" :group 1} {:name "m1" :group 2} {:name "m2" :group 2} {:name "m3" :group 2} {:name "m4" :group 2}]
                   :links [{:source 0 :target 2}{:source 1 :target 3} {:source 3 :target 2} {:source 0 :target 4} {:source 5 :target 4} {:source 1 :target 5}]})


(def test-data {:nodes {"x1" {:name "a1" :group 1 :value 42} "x2" {:name "a2" :group 2 :value 43} "m1" {:name "a3" :group 3 :value 44}
                        "m2" {:name "a4" :group 1 :value 45} "m3" {:name "a5" :grouo 2 :value 46}}
                :links [{:source "x1" :target "m1"} {:source "x2" :target "m2"} {:source "x2" :target "m3"}]})

(println "Hell yeah!")

(defn clear-canvas [frame]
  (.. d3
      (select frame)
      (select "svg")
      remove))


(defn update-fdg
  "Update function for data in nodes and links"
  [svg data]
  (let [width 1080
        height 920
        color (.. d3 -scale category10)
        force (.. d3 -layout force (charge -25)  (linkDistance 20) (size [width height]))]
    (.. force
        (nodes (:nodes data))
        (links (:links data))
        start)
    (let [link (.. svg
                   (selectAll ".link")
                   (data (:links data)))
          node (.. svg
                   (selectAll ".node")
                   (data (:nodes data)))]
      (do
        (.. node
            (attr {:class "update"}))
        (.. node
            enter
            (append "circle")
            (attr {:class "node" :r 4})
            (style {:fill (fn [d] (color (:group d)))})
            (call (.-drag force)))
        (.. node (append "title") (text (fn [d] (:value d))))
        (.. link
            (attr {:class "update"}))
        (.. link
            enter
            (append "line")
            (attr {:class "link"})
            (style {:stroke-width 1
                    :stroke "grey"}))
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
                             :cy #(.-y %)})))))
        #_(.. link exit remove)
        #_(.. node exit remove)))))


(defn create-fdg-svg
  "Draw force-directed graph"
  [frame]
  (let [width 1080
        height 920]
    (.. d3
        (select frame)
        (append "svg")
        (attr {:width width
               :height height}))))



#_(go
  (let [{:keys [ws-channel error]} (<! (ws-ch "ws://localhost:8091/data/ws"))
        svg (create-fdg-svg "#graph-container")]
    (if-not error
      (do
        (>! ws-channel {:topic :graph :data 5})
        (loop [{:keys [message error] :as in} (<! ws-channel)]
          (when in
            (if error
              (println "Error on incomming message: " error)
              (do
                (println "Incoming message!")
                (update-fdg svg message)
                (recur (<! ws-channel)))))))
      (js/console.log "Error on connection: " (pr-str error)))))

(let [svg (create-fdg-svg "#graph-container")]
    (go
      (<!
       (go
         (update-fdg svg graph-data-1)
         (<! (timeout 3000))
         (update-fdg svg graph-data-2)))))

#_(draw-fdg graph-data-1 "#graph-container")
