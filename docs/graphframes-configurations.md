---
layout: global
displayTitle: Configurations
title: Configurations
license: |
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
---

GraphFrames provides several configuration options that can be used to tune the behavior of algorithms and operations. This page documents all available configurations, their descriptions, default values, and usage examples.

## Configuration Table

The following table lists all available GraphFrames configurations:

| Configuration | Description | Default Value |
|---------------|-------------|---------------|
| `spark.graphframes.useLocalCheckpoints` | Tells the connected components algorithm to use local checkpoints. If set to "true", the algorithm checkpoints to the local disks of the executors instead of to persistent storage. For "two_phase" this removes the need to configure a checkpoint directory. "randomized_contraction" always needs one, because it stores its intermediate tables there. Local checkpoints are faster but less reliable: they do not survive the loss of an executor. Used by the "two_phase" and "randomized_contraction" algorithms. | Optional (default: `false`) |
| `spark.graphframes.useLabelsAsComponents` | Tells the connected components algorithm to use the minimum original vertex label as the component ID. When "false", component IDs have the data type LONG. The effect depends on the algorithm and on the vertex ID type, see [Traversals](graphframes-traversals.html for the details. Used by the "two_phase" and "randomized_contraction" algorithms. | Optional (default: `false`) |
| `spark.graphframes.connectedComponents.algorithm` | Sets the connected components algorithm to use. Supported algorithms: <br>- "two_phase": Uses alternating large star and small star iterations proposed in [Connected Components in MapReduce and Beyond](http://dx.doi.org/10.1145/2670979.2670997). <br>- "randomized_contraction": Uses the randomized algorithm proposed in [In-database connected component analysis](https://arxiv.org/pdf/1802.09478). <br>- "graphx": Converts the graph to a GraphX graph and then uses the connected components implementation in GraphX. <br>- "graphframes": Deprecated alias for "two_phase". | Optional (default: `two_phase`) |
| `spark.graphframes.connectedComponents.broadcastthreshold` | Sets broadcast threshold in propagating component assignments. If a node degree is greater than this threshold at some iteration, its component assignment will be collected and then broadcasted back to propagate the assignment to its neighbors. Otherwise, the assignment propagation is done by a normal Spark join. Set it to `-1` to disable manual broadcasting and let Adaptive Query Execution handle the skew instead; that mode is considerably faster on most graphs. This parameter is only used by the "two_phase" algorithm. | Optional (default: `1000000`) |
| `spark.graphframes.connectedComponents.checkpointinterval` | Sets checkpoint interval in terms of number of iterations. Checkpointing regularly helps recover from failures, clean shuffle files, shorten the lineage of the computation graph, and reduce the complexity of plan optimization. As of Spark 2.0, the complexity of plan optimization would grow exponentially without checkpointing. Hence, disabling or setting longer-than-default checkpoint intervals are not recommended. Checkpoint data is saved under the directory set by `org.apache.spark.SparkContext.setCheckpointDir`, or, when that is not set, under the `spark.checkpoint.dir` configuration; the algorithm writes into a sub-directory prefixed with its own name ("connected-components" or "randomized-contraction"). If neither is set, "randomized_contraction" throws a `java.io.IOException`, and so does "two_phase" unless local checkpoints are enabled. Set a nonpositive value to disable checkpointing. Used by the "two_phase" and "randomized_contraction" algorithms; note that "two_phase" in AQE mode (`broadcastthreshold` set to `-1`) relies on Spark's own DataFrame checkpointing instead of its own sub-directory. | Optional (default: `2`) |
| `spark.graphframes.connectedComponents.intermediatestoragelevel` | Sets storage level for intermediate datasets that require multiple passes. Used by all the connected components algorithms. | Optional (default: `MEMORY_AND_DISK`) |

## Setting Configurations

GraphFrames configurations can be set in several ways:

### Spark Configuration

You can set configurations when creating a SparkSession:

<div class="codetabs">

<div data-lang="python" markdown="1">

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
  .appName("GraphFrames Example") \
  .config("spark.graphframes.connectedComponents.algorithm", "two_phase") \
  .config("spark.graphframes.connectedComponents.checkpointinterval", 3) \
  .getOrCreate()
```
</div>

<div data-lang="scala" markdown="1">

```scala
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder()
  .appName("GraphFrames Example")
  .config("spark.graphframes.connectedComponents.algorithm", "two_phase")
  .config("spark.graphframes.connectedComponents.checkpointinterval", 3)
  .getOrCreate()
```
</div>

</div>

### Runtime Configuration

You can also set configurations at runtime:

<div class="codetabs">

<div data-lang="python" markdown="1">

```python
spark.conf.set("spark.graphframes.connectedComponents.algorithm", "two_phase")
spark.conf.set("spark.graphframes.connectedComponents.checkpointinterval", 3)
```
</div>

<div data-lang="scala" markdown="1">

```scala
spark.conf.set("spark.graphframes.connectedComponents.algorithm", "two_phase")
spark.conf.set("spark.graphframes.connectedComponents.checkpointinterval", 3)
```
</div>

</div>

## Example: Connected Components with Custom Configurations

This example shows how to run the Connected Components algorithm with custom configurations:

<div class="codetabs">

<div data-lang="python" markdown="1">

```python
from pyspark.graphframes.examples import Graphs

# Get example graph
g = Graphs(spark).friends()

# Set configurations
spark.conf.set("spark.graphframes.connectedComponents.algorithm", "two_phase")
spark.conf.set("spark.graphframes.connectedComponents.checkpointinterval", 3)
spark.conf.set("spark.graphframes.useLocalCheckpoints", "true")

# Run connected components with custom configurations
result = g.connectedComponents()
result.show()
```
</div>

<div data-lang="scala" markdown="1">

```scala
import org.apache.spark.graphframes.GraphFrame
import org.apache.spark.graphframes.examples

// Get example graph
val g = examples.Graphs.friends

// Set configurations
spark.conf.set("spark.graphframes.connectedComponents.algorithm", "two_phase")
spark.conf.set("spark.graphframes.connectedComponents.checkpointinterval", 3)
spark.conf.set("spark.graphframes.useLocalCheckpoints", true)

// Run connected components with custom configurations
val result = g.connectedComponents.run()
result.show()
```
</div>

</div>

## Notes on Configuration Usage

- **Checkpoint Directory**: For configurations related to checkpointing, make sure to set a checkpoint directory using `spark.sparkContext.setCheckpointDir("path/to/checkpoint/dir")` before running algorithms that use checkpointing. Where `sparkContext` is not reachable, for example from a Spark Connect client, set the `spark.checkpoint.dir` configuration instead; GraphFrames falls back to it. For "two_phase" you can instead enable `spark.graphframes.useLocalCheckpoints`, which needs no checkpoint directory. "randomized_contraction" always needs one.
- **Storage Levels**: When setting the `spark.graphframes.connectedComponents.intermediatestoragelevel` configuration, use one of the following values: `MEMORY_ONLY`, `MEMORY_AND_DISK`, `MEMORY_ONLY_SER`, `MEMORY_AND_DISK_SER`, `DISK_ONLY`, `MEMORY_ONLY_2`, `MEMORY_AND_DISK_2`, etc.
- **Algorithm Selection**: The choice of algorithm for connected components can significantly impact performance. The DataFrame-based algorithms, "two_phase" (the default) and "randomized_contraction", are generally more scalable for large graphs, while the RDD-based "graphx" algorithm may be faster for smaller graphs. See [Traversals](graphframes-traversals.html for a comparison of the algorithms.
- **Local Checkpoints**: Local checkpoints are faster and can put strain on the local disk if insufficiently large. They are also less reliable than checkpoints in persistent storage, because they do not survive the loss of an executor. Because local checkpoints do not require setting `checkpointDir` for "two_phase", they are a convenient option for shorter runs.
