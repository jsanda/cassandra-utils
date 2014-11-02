(ns cassandra-utils.config
  (:require [clojure.java.io :as io] ))

(defn property
  ([n v] (property n v false))
  ([n v commented-out?]
   {:name (name n) :value v :commented-out? commented-out?})
  ([n v commented-out? update-fn]
   {:name (name n) :value v :commented-out? commented-out?
    :update-fn update-fn}))

(defn config-template [template-path properties]
  {:text (slurp (io/file (io/resource template-path)))
   :properties properties})

(defn- enable-vnodes [template]
  (assoc-in template [:properties :initial_token :commented-out?] true))

(defn- disable-vnodes [template]
  (assoc-in template [:properties :num_tokens :value] 1))

(def templates
  {:2.1.0
    (config-template
      "cassandra-2.1.0.yaml"
      {:cluster_name (property :cluster_name "'Test Cluster'")
       :num_tokens (property :num_tokens 256 false)
       :initial_token (property :initial_token "" true disable-vnodes)
       :hinted_handoff_enabled (property :hinted_handoff_enabled true false)})})