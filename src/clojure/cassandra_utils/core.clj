(ns cassandra-utils.core
  (:require [cassandra-utils.strint :as strint]
            [clj-yaml.core :as yaml]
            [clojure.set :as sets]
            [clojure.string :as str]))

(defn load-yaml
  "Parses the yaml document and eagerly loads it into a map"
  [path]
  (yaml/parse-string (slurp path)))

(defn- str-prop [p]
  (if (keyword? p) (name p) (str p)))

(defn- property-set? [p] (not (str/blank? (str-prop p))))

(defn- str-map [m]
  "Converts the map keys/values into string. This is used since
  TokenReplacingReader only handles strings currently."
  (into {} (for [[k, v] m] [(str-prop k) (str-prop v)])))

(defn- get-template-property [token template]
  "A token is a key/value pair that represents either a property name or a
  property value from a Cassandra yaml configuration file. Consider the
  cluster_name property for example. There will be two tokens -
  :cluster_name.name and :cluster_name.value. Given either of those two tokens
  this function returns the corresponding template property."
  (keyword (first (str/split (name (key token)) #"\."))) (:properties template))

(defn- comment-out-property? [k v template]
  (and
    (str/blank? (str-prop v))
    (get-in template [:properties k :commented-out?])))

(defn- get-token-name [k v template]
  (if (comment-out-property? k v template)
    (str "# " (name k))
    (name k)))

(defn- token-map
  "Produces a map of tokens from a map that was created by merging a yaml
  configuration with template properties. For each yaml configuration property,
  there will be two corresponding entries in the token map. For example,
  consider the cluster_name yaml configuration property. The token map will have

  {:cluster_name.name \"cluster_name\"
   :cluster_name.value \"Test Cluster\"}

  The name token provides the name of the yaml configuration property. The value
  token provides the value of the yaml configuration property.

  Token names will be commented out if the merged property value is blank and
  if the corresponding template property has its commente-out flag set. This is
  more easily explained with an example. Once vnodes were introduced the
  initial_token yaml configuration property is commented out by default. If
  the merged property map does not set initial_token, then the token map will
  produce,

  {:initial_token.name \"# initial_token\"
  :initial_token.value \"\"}
  "
  [m template]
  (reduce
    (fn [m [k v]]
      (assoc m
        (keyword (str (name k) ".name")) (get-token-name k v template)
        (keyword (str (name k) ".value")) v))
    {} m))



(defn- get-removed [old template]
  (sets/difference (set (keys old)) (set (keys (:properties template)))))

(defn- get-added [old template]
  (sets/difference (set (keys (:properties template))) (set (keys old))))

(defn merge-props [m template]
  (select-keys
    (merge (into {} (for [[k v] (:properties template)] [k (:value v)]))
           m)
    (keys (:properties template))))

(defn- get-output [old-yaml template]
  (strint/replace-tokens
    (:text template)
    (str-map
      (token-map
        (select-keys
         (merge (into {} (for [[k v] (:properties template)] [k (:value v)]))
                old-yaml)
         (keys (:properties template)))
        template))))

(defn merge-config [old-yaml template]
  {:output (get-output old-yaml template)
   :removed (get-removed old-yaml template)
   :added (get-added old-yaml template)})