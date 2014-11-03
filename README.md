# cassandra-utils

A small library that provides utility functions for working with a Cassandra installation. Currently it provides functions for upgrading configuration settings in cassandra.yaml.

## Usage
```
(require '[cassandra-utils.core :as cutil])
=> nil
(cutil/versions)
=> #{:2.1.0}
(def new-config (cutil/update-config "/path/to/cassandra.yaml" :2.1.0))
=> (var cassandra-utils.core/new-config)
(spit "new-cassandra.yaml" (:output new-config))
=> nil
(:added new-config)
=> #{:num_tokens}
(:removed new-config)
=> #{:sliced_buffer_size_in_kb}
```
So far I only have support for a subset of properties for Cassandra 2.1.0.

For more examples please see the unit tests.

