(ns ceres-analytics.graph-db
  (:require [clojurewerkz.neocons.rest :as nr]
            [clojurewerkz.neocons.rest.nodes :as nn]
            [clojurewerkz.neocons.rest.relationships :as nrl]))

(def conn (nr/connect "http://localhost:7474/db/data"))
