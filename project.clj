(defproject ceres-analytics "0.1.0-SNAPSHOT"

  :description "Basic analytics of ceres collections"

  :url "http://example.com/FIXME"

  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}

  :dependencies [[org.clojure/clojure "1.6.0"]
                 [org.clojure/core.async "0.1.303.0-886421-alpha"]
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

                 ;; backend
                 [http-kit "2.1.19"]
                 [ring "1.3.2"]
                 [com.cemerick/friend "0.2.1"]
                 [enlive "1.3.2"]
                 [compojure "0.6.0"]

                 ;; frontend
                 [jarohen/chord "0.12.2.4"]
                 [com.facebook/react "0.12.1"]
                 [om "0.7.3"]
                 [kioo "0.4.0"]
                 [net.drib/strokes "0.5.1"]

                 [aprint "0.1.3"]]

  :source-paths ["src/cljs" "src/clj"]

  :main ceres-analytics.core

  :min-lein-version "2.0.0"

  :uberjar-name "ceres-analytics-standalone.jar"

  :plugins [[lein-gorilla "0.3.4"]]

  )
