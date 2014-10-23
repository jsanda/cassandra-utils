(ns cassandra-utils.strint
  (:import (java.io StringWriter))
  (:require [clojure.java.io :as io])
  (:import [org.rhq.core.util TokenReplacingReader]
           [java.io StringReader StringWriter]))

(defn replace-tokens
  "Replaces any of the variables found in src with the mappings defined by
  vars. The updated string is returned. Currently vars should be a map where
  both keys and values are strings."
  [src vars]
  ;; TODO Need to close the writer
  (let [w (StringWriter.)]
    (io/copy (TokenReplacingReader. src vars) w)
    (str w)))