(ns ceres-analytics.core
  (:require [strokes :refer [d3]]
            [chord.client :refer [ws-ch]]
            [cljs.reader :as reader]
            [cljs-time.core :as t]
            [cljs-time.coerce :as c]
            [cljs.core.async :refer [<! >! chan put! close! timeout]])
  (:require-macros [cljs.core.async.macros :refer [go-loop go]]))

(enable-console-print!)

(defn log [m & ms] (.log js/console (apply str m ms)) )

(def broadcasters
  {2834511 "Spiegel"
   5494392 "Focus"
   5734902 "Tagesschau"
   8720562 "Welt"
   9204502 "Bild"
   15071293 "Westen"
   15243812 "TAZ"
   ;15738602 "N24"
   18016521 "FAZ"
   18774524 "Stern"
   19232587 "ntv"
   40227292 "dpa"
   114508061 "SZ"
   1101354170 "ZDF"})

(strokes/bootstrap)

(def graph-state (atom {:svg nil
                        :width 1024
                        :height 1024
                        :frame "#graph-container"
                        :color nil
                        :force nil
                        :time-controller (chan)
                        :node->index {}
                        :data {:nodes []
                               :links []}}))


(defn clear-canvas [frame]
  (.. d3
      (select frame)
      (select "svg")
      remove))


(defn draw-border
  "Draw dark borders around visible field"
  [state]
  (let [{:keys [svg width height]} @state]
    (.. svg
        (append "rect")
        (attr {:x 0
               :y 0
               :width width
               :height height})
        (style {:stroke "#777777"
                :fill "none"
                :stroke-width 3}))))





(defn update-graph
  "Updates nodes and links in graph"
  [state]
  (let [{:keys [svg width height data force color circle-size link-dist link-charge node-gravity line-weight]} @state
        link (.. svg
                 (selectAll ".link")
                 (data (:links data)))
        node (.. svg
                 (selectAll "g.node")
                 (data (:nodes data)))]
    (draw-border state)
    (.. link enter (append "line")
        (attr {:class "link"})
        (style {:stroke-width line-weight :stroke "#777777"}))
    (.. link exit remove)
    (let [node-enter (.. node
                         enter
                         (append "g")
                         (attr {:class "node"})
                         (call (.-drag force)))]
      (.. node-enter
          (append "svg:circle")
          (attr {:class "node-stroke" :r circle-size})
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
        (charge link-charge)
        (gravity node-gravity)
        (linkDistance link-dist)
        (friction 0.1)
        (size [width height])
        (nodes (:nodes data))
        (links (:links data))
        start)
    (.. d3
        (selectAll "g.node")
        moveToFront)
    )
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
    (set! (.. d3 -selection -prototype -moveToFront)
      (fn []
        (this-as this
                 (.each this (fn []
                               (this-as this
                                        (.. this -parentNode (appendChild this))))))))
    (swap! state assoc-in [:svg] (.. d3
                                      (select frame)
                                      (append "svg")
                                      (attr {:width width
                                             :xmlns "http://www.w3.org/2000/svg"
                                             :id "graph-svg"
                                             :height height})))
    (swap! state assoc-in [:force] force)
    (swap! state assoc-in [:circle-size] (/ height 200))
    (swap! state assoc-in [:link-dist] (/ height 128))
    (swap! state assoc-in [:link-charge] (* 8 (/ -100000 height)))
    (swap! state assoc-in [:node-gravity] (/ 400 height))
    (swap! state assoc-in [:line-weight] (/ height 512))
    (swap! state assoc-in [:color] ["purple" "#6c71c4" "#2aa198" "#859900" "#d33682" ])
    (println (select-keys @state [:circle-size :link-dist :link-charge]))
    (update-graph state)))


(defn start-vis [state]
  (let [nodes (get-in @state [:data :new-nodes])
        links (get-in @state [:data :new-links])
        day (get-in @state [:day])
        cntrl (get-in @state [:time-controller])]
    (set! (.-onkeydown js/document)
          (fn [e]
            (when (= (.-keyCode e) 78)
              (go (>! cntrl true)))))
    (go-loop [k day]
      (if (= k (inc day))
        (set! (.-innerHTML (.getElementById js/document "current-time")) "DONE")
        (do
          (loop [i 0]
            (if (= i 24)
              (println (str "day " (inc k)))
              (do
                (loop [j 0]
                  (if (= j 60)
                    nil
                    (do
                      (set! (.-innerHTML (.getElementById js/document "current-time")) (str  i ":" (if (= j 0) "00" j)))
                      (<! cntrl)
                      (let [k (t/interval (t/date-time 2015 4 k i j) (t/date-time 2015 4 k i (+ j 20)))
                            new-nodes (filter #(t/within? k (:ts %)) nodes)
                            new-links (filter #(t/within? k (:ts %)) links)]
                        (when-not (empty? new-nodes) (doall (map #(add-node state %) new-nodes)))
                        (when-not (empty? new-links) (doall (map #(add-link state %) new-links)))
                        (recur (+ j 20))))))
                (recur (inc i)))))
          (recur (inc k)))))))


(defn run [state]
  (init-graph state)
  (go
    (let [{:keys [ws-channel error]} (<! (ws-ch "ws://localhost:8091/data/ws"))
          broadcaster 2834511 #_(rand-nth (keys broadcasters))
          random-day 8 #_(rand-int 31)]
      (swap! state assoc-in [:ws-channel] ws-channel)
      (swap! state assoc-in [:day] random-day)
      (>! ws-channel {:topic :user-tree :data {:broadcaster broadcaster :day random-day}})
      (set! (.-innerHTML (.getElementById js/document "graph-title")) (get broadcasters broadcaster))
      (set! (.-innerHTML (.getElementById js/document "current-day")) (str random-day " April 2015"))
      (if-not error
        (loop [{:keys [message error] :as in} (<! ws-channel)]
          (when in
            (if error
              (println "Error on incomming message: " error)
              (do
                (let [{:keys [nodes links]} message
                      formated-nodes (map (fn [n] (update-in n [:ts] c/from-long)) nodes)
                      formated-links (map (fn [l] (update-in l [:ts] c/from-long)) links)]
                  (swap! state assoc-in [:data :new-nodes] formated-nodes)
                  (swap! state assoc-in [:data :new-links] formated-links)
                  (<! (timeout 1000))
                  (start-vis state))
                (recur (<! ws-channel))))))
        (js/console.log "Error on connection: " (pr-str error))))))


(run graph-state)
