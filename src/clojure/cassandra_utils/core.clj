(ns cassandra-utils.core
  (:require [cassandra-utils.strint :as strint]
            [clj-yaml.core :as yaml]
            [clojure.set :as sets]))

(defn load-yaml
  "Parses the yaml document and eagerly loads it into a map"
  [path]
  (yaml/parse-string (slurp path)))

(defn- str-prop [p]
  (if (keyword? p) (name p) (str p)))

(defn str-map [m]
  "Converts the map keys/values into string. This is used since
  TokenReplacingReader only handles strings currently."
  (into {} (for [[k, v] m] [(str-prop k) (str-prop v)])))

(defn token-map [m]
  (reduce
     (fn [m [k v]] (assoc m (keyword (str (name k) ".name")) (name k)
                            (keyword (str (name k) ".value")) v))
     {} m))

(defn get-removed [old template]
  (sets/difference (set (keys old)) (set (keys (:properties template)))))

(defn get-added [old template]
  (sets/difference (set (keys (:properties template))) (set (keys old))))

(defn get-output [old-yaml template]
  (strint/replace-tokens
    (:text template)
    (str-map
      (token-map
        (select-keys
         (merge (into {} (for [[k v] (:properties template)] [k (:value v)]))
                old-yaml)
         (keys (:properties template)))))))

;(defn merge-config [old new mappings]
;  (let [old-yaml (yaml/parse-string old)]
;    {:output (get-output old-yaml new mappings)
;     :removed (get-removed old-yaml mappings)
;     :added (get-added old-yaml mappings)}))
