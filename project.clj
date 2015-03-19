(defproject ceres-analytics "0.1.0-SNAPSHOT"

  :description "Basic analytics of ceres collections"

  :url "http://example.com/FIXME"

  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}

  :dependencies [[org.clojure/clojure "1.6.0"]
                 [org.clojure/core.async "0.1.303.0-886421-alpha"]
                 [org.clojure/clojurescript "0.0-2411"]
                 [clj-time "0.9.0"]
                 [enlive "1.1.5"]
                 [gg4clj "0.1.0"]
                 [cheshire "5.4.0"]
                 [com.novemberain/monger "2.1.0"]
                 [incanter "1.9.0"]
                 [net.sf.jung/jung-algorithms "2.0.1"]
                 [net.sf.jung/jung-api "2.0.1"]
                 [net.sf.jung/jung-graph-impl "2.0.1"]
                 [org.clojure/data.priority-map "0.0.6"]
                 [com.taoensso/timbre "3.4.0"]

                 ;; backend
                 [http-kit "2.1.19"]
                 [ring "1.3.2"]
                 [enlive "1.1.5"]
                 [compojure "1.3.2"]

                 ;; frontend

                 [jarohen/chord "0.6.0"]
                 [com.facebook/react "0.12.2.4"]
                 [om "0.7.3"]
                 [kioo "0.4.0"]
                 [net.drib/strokes "0.5.1"]

                 [aprint "0.1.3"]]

  :source-paths ["src/cljs" "src/clj"]

  :main ceres-analytics.core

  :min-lein-version "2.0.0"

  :uberjar-name "ceres-analytics-standalone.jar"

  :plugins [[lein-gorilla "0.3.4"]
            [lein-cljsbuild "1.0.3"]]

  :cljsbuild
  {:builds
   [{:source-paths ["src"]
     :compiler
     {:output-to "resources/public/js/compiled/main.js"
      :output-dir "resources/public/js/compiled/out"
      :optimizations :none
      :pretty-print false
      :source-map "main.js.map"}}]}
  )
