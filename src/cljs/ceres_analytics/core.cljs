(ns ceres-analytics.core
  (:require [strokes :refer [d3]]
            [chord.client :refer [ws-ch]]
            [cljs.reader :as reader]
            [cljs-time.core :as t]
            [cljs-time.coerce :as c]
            [cljs.core.async :refer [<! >! put! close! timeout]])
  (:require-macros [cljs.core.async.macros :refer [go-loop go]]))

(enable-console-print!)

(defn log [m & ms] (.log js/console (apply str m ms)) )


(strokes/bootstrap)

(println "Hell yeah!")

(def graph-state (atom {:svg nil
                        :width 2000
                        :height 2000
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
        (style {:stroke-width 1 :stroke "#aaa"}))
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
        (charge -550)
        (gravity 0.5)
        (linkDistance 8)
        (friction 0.1)
        (size [width height])
        (nodes (:nodes data))
        (links (:links data))
        start))
  state)


(defn add-node [state {:keys [name] :as new-node}]
  (when-not ((into #{} (keys (get-in @state [:node->index]))) name)
    (do
      (swap! state assoc-in [:node->index name] (count (get-in @state [:data :nodes])))
      (swap! state update-in [:data :nodes] conj new-node)
      (update-graph state))))


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
    (swap! state assoc-in [:color] ["purple" "steelblue" "orange" "green" "red" ])
    (update-graph state)))


(defn start-vis [state]
  (let [nodes (get-in @state [:data :new-nodes])
        links (get-in @state [:data :new-links])]
    (println "Starting vis...")
    (go-loop [k 5]
      (if (= k 10)
        (println "done")
        (do
          (loop [i 0]
            (if (= i 24)
              (println (str "day " (inc k)))
              (do
                (loop [j 0]
                  (if (= j 60)
                    nil
                    (do
                      (println (str i ":" j))
                      (<! (timeout 2000))
                      (let [k (t/interval (t/date-time 2015 4 k i j) (t/date-time 2015 4 k i (+ j 20)))
                            new-nodes (filter #(t/within? k (:ts %)) nodes)
                            new-links (filter #(t/within? k (:ts %)) links)]
                        (doall (map #(add-node state %) new-nodes))
                        (doall (map #(add-link state %) new-links))
                        (recur (+ j 20))))))
                (recur (inc i)))))
          (recur (inc k)))))))


(defn run [state]
  (init-graph state)
  (go
    (let [{:keys [ws-channel error]} (<! (ws-ch "ws://localhost:8091/data/ws"))]
      (swap! state assoc-in [:ws-channel] ws-channel)
      (>! ws-channel {:topic :user-tree :data "tagesschau"})
      (if-not error
        (loop [{:keys [message error] :as in} (<! ws-channel)]
          (when in
            (if error
              (println "Error on incomming message: " error)
              (do
                (let [{:keys [nodes links]} message
                      formated-nodes (map (fn [n] (update-in n [:ts] c/from-string)) nodes)
                      formated-links (map (fn [l] (update-in l [:ts] c/from-string)) links)]
                  (swap! state assoc-in [:data :new-nodes] formated-nodes)
                  (swap! state assoc-in [:data :new-links] formated-links)
                  (<! (timeout 1000))
                  (start-vis state))
                (recur (<! ws-channel))))))
        (js/console.log "Error on connection: " (pr-str error))))))



(run graph-state)
