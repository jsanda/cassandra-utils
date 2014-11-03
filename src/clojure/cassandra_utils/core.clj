(ns cassandra-utils.core
  (:require [cassandra-utils.config :as config]
            [cassandra-utils.strint :as strint]
            [clj-yaml.core :as yaml]
            [clojure.set :as sets]
            [clojure.string :as str]))

(defn load-yaml
  "Parses the yaml document and eagerly loads it into a map. See
  clojure.java.io/reader for a complete list of supported types for path."
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
  "Return true if v is blank and if the configuration property is commented out
  by default in the template. For example, once vnodes was introduced, the
  initial_token property has been commented out by default."
  (and
    (str/blank? (str-prop v))
    (get-in template [:properties k :commented-out?])))

(defn- get-token-name
  [k v template]
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

(defn- merge-props [m template]
  (select-keys
    (merge (into {} (for [[k v] (:properties template)] [k ((:value v) m)]))
           m)
    (keys (:properties template))))

(defn- get-output [old-yaml template]
  (strint/replace-tokens
    (:text template)
    (str-map
      (token-map
        (select-keys
         (merge
           (into {}
                 (for [[k v] (:properties template)]
                   [k ((:value v) old-yaml)]))
           old-yaml)
         (keys (:properties template)))
        template))))

(defn versions
  "Returns a set of the supported versions to which upgrades can be performed."
  [] (set (keys config/templates)))

(defn merge-config
  "old-yaml
  Should be a map of yaml configuration settings. See the load-yaml function
  for generating a yaml configuration map.

  template
  An entry from the cassandra-utils.config/templates. In addition to providing
  a template for the new configuration file, it also specifies default values
  and whether or not properties are commented out by default.

  The map returned contains three keys,

  :output
  The text of the new configuration with the properties from the old
  configuration merged in.

  :removed
  A set of property names that were in the original configuration but have been
  removed in the new configuration.

  :added
  A set of property names that were not in the original configuration but have
  been added in the new configuration."
  [old-yaml template]
  {:output (get-output old-yaml template)
   :removed (get-removed old-yaml template)
   ;; NOTE the set of added properties currently includes commented out
   ;; properties which could be misleading. Commented out properties should not
   ;; be included in this set.
   :added (get-added old-yaml template)})

(defn update-config
  "path Should specify a location of the existing yaml configuration file.

  version is the target version of the upgrade. Use the versions function to
  see a set of supported versions. Note that the version should be specified as
  a keyword.

  The map returned contains three keys,

  :output
  The text of the new configuration with the properties from the old
  configuration merged in.

  :removed
  A set of property names that were in the original configuration but have been
  removed in the new configuration.

  :added
  A set of property names that were not in the original configuration but have
  been added in the new configuration."
  [path version]
  (if (contains? config/templates version)
    (merge-config (load-yaml path) (version config/templates))
    ;; TODO Figure out what to return if a bad version is specified
    {:old-config (load-yaml path) :updated? false}))