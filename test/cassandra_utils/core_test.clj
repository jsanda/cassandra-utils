(ns cassandra-utils.core-test
  (:require [clojure.test :refer :all]
            [cassandra-utils.core :refer :all]
            [cassandra-utils.config :as config]))

(def vnodes-config "
# Cassandra storage config YAML

# NOTE:
#   See http://wiki.apache.org/cassandra/StorageConfiguration for
#   full explanations of configuration directives
# /NOTE

# The name of the cluster. This is mainly used to prevent machines in
# one logical cluster from joining another.
cluster_name: 'My Cluster'

# This defines the number of tokens randomly assigned to this node on the ring
# The more tokens, relative to other nodes, the larger the proportion of data
# that this node will store. You probably want all nodes to have the same number
# of tokens assuming they have equal hardware capability.
#
# If you leave this unspecified, Cassandra will use the default of 1 token for legacy compatibility,
# and will use the initial_token as described below.
#
# Specifying initial_token will override this setting on the node's initial start,
# on subsequent starts, this setting will apply even if initial token is set.
#
# If you already have a cluster with 1 token per node, and wish to migrate to
# multiple tokens per node, see http://wiki.apache.org/cassandra/Operations
num_tokens: 256

# initial_token allows you to specify tokens manually.  While you can use # it with
# vnodes (num_tokens > 1, above) -- in which case you should provide a
# comma-separated list -- it's primarily used when adding nodes # to legacy clusters
# that do not have vnodes enabled.
# initial_token:

# See http://wiki.apache.org/cassandra/HintedHandoff
# May either be \"true\" or \"false\" to enable globally, or contain a list
# of data centers to enable per-datacenter.
# hinted_handoff_enabled: DC1,DC2
hinted_handoff_enabled: true
")

(deftest merge-config-override-prop
  (testing
"When a property is set in both the old config and in the template, the old
value should be used"
    (let [old {:cluster_name "My Cluster"}
          template {:text "# Cassandra config\n${cluster_name.name}: '${cluster_name.value}'"
                    :properties
                          {:cluster_name (config/property "cluster_name" "Test Cluster")}}
          merged-config (merge-config old template)]
      (is (=
            "# Cassandra config\ncluster_name: 'My Cluster'"
            (:output merged-config))))))

(deftest merge-config-remove-prop
  (testing ""
    (let [old {:cluster_name "My Cluster" :legacy_prop 55}
          template {:text "# Cassandra config\n${cluster_name.name}: '${cluster_name.value}'"
                    :properties
                     {:cluster_name (config/property "cluster_name", "Test Cluster")}}
          merged-config (merge-config old template)]
      (is (= #{:legacy_prop} (:removed merged-config))))))

(deftest merge-config-add-prop
  (testing
"When a property is not in the old config but is set in the new config, that
property should be included in the merged, updated config."
    (let [old {:cluster_name "My Cluster"}
          template {:text "${cluster_name.name}: '#{cluster_name.value}'\n${authenticator.name}: ${authenticator.value}"
                    :properties {:cluster_name (config/property "cluster_name" "Test Cluster")
                                 :authenticator (config/property "authenticator" "AllowAuthenticator")}}
          merged-config (merge-config old template)]
      (is (= #{:authenticator} (:added merged-config)))
      (is (= "cluster_name: 'My Cluster'\nauthenticator: AllowAuthenticator")))))

;
;(deftest merge-config-preserve-comment-in-new-config
;  (testing
;"Comments in the new config should be preserved."
;    (let [old "# Cassandra storage config YAML\ncluster_name: 'My Cluster'"
;          new "# Cassandra storage config YAML\ncluster_name: '${:cluster_name}'"
;          mappings {:prop-names #{:cluster_name}
;                    :props {:cluster_name "'Test Cluster'"}}
;          merged (merge-config old new mappings)]
;      (is (= "# Cassandra storage config YAML\ncluster_name: 'My Cluster'"
;             (:output merged))))))