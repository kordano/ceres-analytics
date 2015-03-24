(ns ceres-analytics.core
  (:require [strokes :refer [d3]]
            [chord.client :refer [ws-ch]]
            [cljs.reader :as reader]
            [cljs.core.async :refer [<! >! put! close! timeout]])
  (:require-macros [cljs.core.async.macros :refer [go]]))

(enable-console-print!)

(strokes/bootstrap)

(println "Hell yeah!")

(def app-state (atom {:node->index {}
                      :d3 {:nodes []
                           :links []}}))


(defn clear-canvas [frame]
  (.. d3
      (select frame)
      (select "svg")
      remove))


(defn update-client-state [old new-state]
  (let [{:keys [nodes links]} new-state
        n (-> @old :d3 :nodes count)
        old-nodes (into #{} (keys (:node->index @old)))
        old-links (into #{} (get-in @old [:d3 :links]))
        new-nodes (vec (remove #(get old-nodes (:name %)) nodes))
        {:keys [node->index]} (swap! old update-in [:node->index] merge (apply merge (map (fn [i] {(-> new-nodes (get i) :name) (+ i n)}) (range (count new-nodes)))))
        new-links (remove old-links (map (fn [{:keys [source target]}] {:source (get node->index source) :target (get node->index target)}) links))]
    (swap! old update-in [:d3 :nodes] (comp vec concat) new-nodes)
    (swap! old update-in [:d3 :links] (comp vec concat) new-links)))


(defn calculate-links [data]
  (let [{:keys [links nodes]} data
        node->index (apply merge (map (fn [i] {(-> nodes (get i) :name) i}) (range (count nodes))))]
    {:nodes nodes
     :links (mapv (fn [{:keys [source target]}] {:source (get node->index source) :target (get node->index target)}) links)}))

(defn update-fdg
  "Update function for data in nodes and links"
  [svg data]
  (let [width 1080
        height 920
        color ["red" "steelblue"]
        force (.. d3 -layout force (charge -10)  (linkDistance 10) (size [width height]))]
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
            (attr {:class "node" :r 1})
            (style {:fill (fn [d] (color (dec (:group d))))})
            (call (.-drag force)))
        (.. node (append "eitle") (text (fn [d] (:value d))))
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
        (.. link exit remove)
        (.. node exit remove)))))


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



(go
  (let [{:keys [ws-channel error]} (<! (ws-ch "ws://localhost:8091/data/ws"))
        svg ]
    (if-not error
      (do
        (>! ws-channel {:topic :graph :data 1})
        (loop [{:keys [message error] :as in} (<! ws-channel)]
          (when in
            (if error
              (println "Error on incomming message: " error)
              (do
                (println "Incoming message!")
                (clear-canvas "#graph-container")
                (update-fdg (create-fdg-svg "#graph-container")(calculate-links message))
                (recur (<! ws-channel)))))))
      (js/console.log "Error on connection: " (pr-str error)))))
