(defproject ceres-analytics "0.1.0-SNAPSHOT"

  :description "Basic analytics of ceres collections"

  :url "http://example.com/FIXME"

  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}

  :dependencies [[org.clojure/clojure "1.6.0"]
                 [org.clojure/core.async "0.1.303.0-886421-alpha"]
                 [clj-time "0.7.0"]
                 [enlive "1.1.5"]
                 [gg4clj "0.1.0"]
                 [cheshire "5.4.0"]
                 [com.datomic/datomic-free "0.9.4899"]
                 [com.novemberain/monger "2.0.0-rc1"]
                 [incanter "1.5.5"]
                 [clojure-opennlp "0.3.2"]
                 [aprint "0.1.0"]]

  :main ceres-analytics.core

  :min-lein-version "2.0.0"

  :uberjar-name "ceres-analytics-standalone.jar"


  :plugins [[lein-gorilla "0.3.4"]]

  )
