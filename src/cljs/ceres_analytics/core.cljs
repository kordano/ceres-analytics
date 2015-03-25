(ns ceres-analytics.core
  (:require [strokes :refer [d3]]
            [chord.client :refer [ws-ch]]
            [cljs.reader :as reader]
            [cljs.core.async :refer [<! >! put! close! timeout]])
  (:require-macros [cljs.core.async.macros :refer [go]]))

(enable-console-print!)

(defn log [m & ms] (.log js/console (apply str m ms)) )


(strokes/bootstrap)

(println "Hell yeah!")

(def graph-state (atom {:svg nil
                        :width 960
                        :height 450
                        :frame "#graph-container"
                        :color nil
                        :force nil
                        :node->index {}
                        :data {:nodes []
                               :links []}}))


(defn clear-canvas [frame]
  (.. d3
      (select frame)
      (select "svg")
      remove))


(defn update-graph
  "Updates nodes and links in graph"
  [state]
  (let [{:keys [svg width height data force color]} @state
        link (.. svg
                 (selectAll ".link")
                 (data (:links data)))
        node (.. svg
                 (selectAll "g.node")
                 (data (:nodes data)))]
    (.. link enter (append "line")
        (attr {:class "link"})
        (style {:stroke-width 1 :stroke "grey"}))
    (.. link exit remove)
    (let [node-enter (.. node
                         enter
                         (append "g")
                         (attr {:class "node"})
                         (call (.-drag force)))]
      (.. node-enter
          (append "svg:circle")
          (attr {:class "node-stroke" :r 3})
          (style {:fill (fn [d] (color (dec (:group d))))
                  :stroke "#fff"}))
      (.. node-enter (append "title") (text (fn [d] (:value d)))))
    (.. node exit remove)
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
                  (attr #_{:cx #(.-x %)
                           :cy #(.-y %)}
                        {:transform #(str "translate(" (.-x %) "," (.-y %) ")")})))))
    (.. force
        (charge -200)
        (linkDistance 10)
        (size [width height])
        (nodes (:nodes data))
        (links (:links data))
        start))
  state)


(defn add-node [state {:keys [name] :as new-node}]
  (log "adding node " name)
  (swap! state assoc-in [:node->index name] (count (get-in @state [:data :nodes])))
  (swap! state update-in [:data :nodes] conj new-node)
  (update-graph state))


(defn add-link
  "Add new link to graph structure"
  [state {:keys [source target] :as new-link}]
  (swap! state update-in [:data :links] conj
         {:source (get-in @state [:node->index source])
          :target (get-in @state [:node->index target])})
  (update-graph state))


(defn init-graph [state]
  (let [{:keys [width height frame data]} @state
        force (.. d3 -layout force)]
    (swap! state assoc-in [:svg] (.. d3
                                      (select frame)
                                      (append "svg")
                                      (attr {:width width
                                             :height height})))
    (swap! state assoc-in [:force] force)
    (swap! state assoc-in [:color] ["red" "steelblue"])
    (update-graph state)))



(defn run [state]
  (init-graph state)
  (go
    (let [{:keys [ws-channel error]} (<! (ws-ch "ws://localhost:8091/data/ws"))]
      (if-not error
        (do
          (>! ws-channel {:topic :graph :data 1})
          (loop [{:keys [message error] :as in} (<! ws-channel)]
            (when in
              (if error
                (println "Error on incomming message: " error)
                (do
                  (let [{:keys [nodes links]} message]
                    (doall (map #(add-node state %) nodes))
                    (doall (map #(add-link state %) links)))
                  (recur (<! ws-channel)))))))
        (js/console.log "Error on connection: " (pr-str error))))))


(run graph-state)
