---
layout: global
displayTitle: Traversals and Connectivity
title: Traversals and Connectivity
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

## Shortest paths

Computes shortest paths from each vertex to the given set of landmark vertices, where landmarks are specified by the
vertex ID. Note that this takes an edge direction into account.

See [Wikipedia](https://en.wikipedia.org/wiki/Shortest_path_problem) for a background.

---

**NOTE**

_Be aware, that returned `DataFrame` is persistent and should be unpersisted manually after processing to avoid memory leaks!_

---

<div class="codetabs">

<div data-lang="python" markdown="1">

For API details, refer to the [shortestPaths](api/python/reference/api/pyspark.graphframes.GraphFrame.html).

```python
from pyspark.graphframes.examples import Graphs

g = Graphs(spark).friends()  # Get example graph

results = g.shortestPaths(landmarks=["a", "d"])
results.select("id", "distances").show()
```
</div>

<div data-lang="scala" markdown="1">

For API details, refer to the [ShortestPaths](api/scala/org/apache/spark/graphframes/lib/ShortestPaths.html).

```scala
import org.apache.spark.graphframes.{examples, GraphFrame}

val g: GraphFrame = examples.Graphs.friends // get example graph

val results = g.shortestPaths.landmarks(Seq("a", "d")).run()
results.select("id", "distances").show()
```
</div>

</div>

### Arguments

- `landmarks`

The list (`Seq`) of vertices that are used as landmarks to compute shortest paths from them to all other vertices.

- `algorithm`

Possible values are `graphx` and `graphframes`. Both implementations are based on the same logic. GraphX is faster for small-medium sized graphs but requires more memory due to less efficient RDD serialization and it's triplets-based nature. GraphFrames requires much less memory due to efficient Thungsten serialization and because the core structures are edges and messages, not triplets.

- `checkpoint_interval`

For `graphframes` only. To avoid exponential growing of the Spark' Logical Plan, DataFrame lineage and query optimization time, it is required to do checkpointing periodically. While checkpoint itself is not free, it is still recommended to set this value to something less than `5`.

- `use_local_checkpoints`

For `graphframes` only. By default, GraphFrames uses persistent checkpoints. They are reliable and reduce the errors rate. The downside of the persistent checkpoints is that they are requiride to set up a `checkpointDir` in persistent storage like `S3` or `HDFS`. By providing `use_local_checkpoints=True`, user can say GraphFrames to use local disks of Spark' executurs for checkpointing. Local checkpoints are faster, but they are less reliable: if the executur lost, for example, is taking by the higher priority job, checkpoints will be lost and the whole job fails.

- `storage_level`

The level of storage for intermediate results and the output `DataFrame` with components. By default it is memory and disk deserialized as a good balance between performance and reliability. For very big graphs and out-of-core scenarios, using `DISK_ONLY` may be faster.

- `is_directed`

By default this is true and algorithm will look for only directed paths. By passing false, graph will be considered as undirected and algorithm will look for any shortest path.

## All paths

Computes all simple paths between source and destination vertices up to a specified maximum path length. A "simple" path means no repeated vertices — for example, if there are paths A-B-C and A-D-C and an edge B-A, querying for all paths between A and C returns A-B-C and A-D-C but not A-B-A-D-C.

The algorithm supports both directed and undirected traversal as well as optional edge filtering.

---

**WARNING:**

_Depending on the value of `max_path_length`, this algorithm can be **extremely slow** or even lead to **out-of-memory (OOM) errors** or **huge disk spills**. Setting `max_path_length` on the scale of the graph diameter causes the algorithm to attempt to enumerate (almost) all simple paths in the graph, which grows combinatorially. Use with caution, especially on large or densely connected graphs. Start with a small value and increase gradually._

---

<div class="codetabs">

<div data-lang="python" markdown="1">

For API details, refer to the [all_paths](api/python/reference/api/pyspark.graphframes.GraphFrame.html).

```python
from pyspark.graphframes.examples import Graphs

g = Graphs(spark).friends()  # Get example graph

# Find all simple paths from vertex "a" to vertex "d" with at most 3 edges
paths = g.all_paths(
    from_expr="id = 'a'",
    to_expr="id = 'd'",
    max_path_length=3,
)
paths.select("path", "len").show()

# Search with edge filter and undirected traversal
paths = g.all_paths(
    from_expr="id = 'a'",
    to_expr="id = 'd'",
    edge_filter="relationship != 'friend'",
    max_path_length=3,
    is_directed=False,
)
paths.select("path", "len").show()
```
</div>

<div data-lang="scala" markdown="1">

For API details, refer to the [AllPaths](api/scala/org/apache/spark/graphframes/lib/AllPaths.html).

```scala
import org.apache.spark.graphframes.{examples, GraphFrame}

val g: GraphFrame = examples.Graphs.friends // get example graph

// Find all simple paths from vertex "a" to vertex "d" with at most 3 edges
val paths = g.allPaths
  .fromExpr("id = 'a'")
  .toExpr("id = 'd'")
  .maxPathLength(3)
  .run()
paths.select("path", "len").show()

// Search with edge filter
val pathsFiltered = g.allPaths
  .fromExpr("id = 'a'")
  .toExpr("id = 'd'")
  .edgeFilter("relationship != 'friend'")
  .maxPathLength(3)
  .run()
pathsFiltered.select("path", "len").show()
```
</div>

</div>

### Returned DataFrame Schema

The returned `DataFrame` contains the following columns:

- `path`: an array of vertex IDs in traversal order (e.g., `[a, b, d]`)
- `len`: the number of edges in the path (Long)

### Arguments

- `from_expr` (Python) / `fromExpr` (Scala)

A Column or SQL expression identifying the source (starting) vertices.

- `to_expr` (Python) / `toExpr` (Scala)

A Column or SQL expression identifying the destination (target) vertices.

- `max_path_length` (Python) / `maxPathLength` (Scala)

Maximum number of edges in a path; must be greater than 0. Default is 5. Setting a large value (e.g., on the scale of the graph diameter) may cause severe performance degradation or out-of-memory errors.

- `edge_filter` (Python) / `edgeFilter` (Scala)

An optional Column or SQL expression applied to edges during traversal. Only edges satisfying this condition are considered.

- `is_directed` (Python) / directed mode (Scala)

Whether to use directed traversal. Default is `True`. If `False`, the graph is treated as undirected by internally unioning edges with reversed edges. Note: it is assumed that the graph does not have multi-edges. Results may be unstable for graphs with multi-edges.

## Breadth-first search (BFS)

Breadth-first search (BFS) finds the shortest path(s) from one vertex (or a set of vertices) to another vertex (or a set
of vertices). The beginning and end vertices are specified as Spark DataFrame expressions.

See [Wikipedia on BFS](https://en.wikipedia.org/wiki/Breadth-first_search) for more background.

The following code snippets use BFS to find the path between vertex with name "Esther" to a vertex with `age < 32`.

<div class="codetabs">

<div data-lang="python" markdown="1">

For API details, refer to the [bfs](api/python/reference/api/pyspark.graphframes.GraphFrame.html).

```python
g = Graphs(spark).friends()  # Get example graph

# Search from "Esther" for users of age < 32

paths = g.bfs("name = 'Esther'", "age < 32")
paths.show()

# Specify edge filters or max path lengths

g.bfs("name = 'Esther'", "age < 32",
      edgeFilter="relationship != 'friend'", maxPathLength=3)
```
</div>

<div data-lang="scala" markdown="1">

For API details, refer to [BFS](api/scala/org/apache/spark/graphframes/lib/BFS.html).

```scala
import org.apache.spark.graphframes.{examples, GraphFrame}

val g: GraphFrame = examples.Graphs.friends // get example graph

// Search from "Esther" for users of age < 32.
val paths = g.bfs.fromExpr("name = 'Esther'").toExpr("age < 32").run()
paths.show()

// Specify edge filters or max path lengths.
val paths = {
  g.bfs.fromExpr("name = 'Esther'").toExpr("age < 32")
    .edgeFilter("relationship != 'friend'")
    .maxPathLength(3).run()
}
paths.show()
```
</div>

</div>

## Connected components

Computes the connected component membership of each vertex and returns a graph with each vertex assigned a component ID.

See [Wikipedia](<https://en.wikipedia.org/wiki/Connected_component_(graph_theory)>) for the background.

---

**NOTE:**

_With GraphFrames 0.3.0 and later releases, the default Connected Components algorithm requires setting a Spark checkpoint directory. Users can revert to the old algorithm using `connectedComponents.setAlgorithm("graphx")`. Starting from GraphFrames 0.9.3 release, users can also use `localCheckpoints` that does not require setting a Spark checkpoint directory. To use `localCheckpoints` users can set the config `spark.graphframes.useLocalCheckpoints` to `true` or use the API `connectedComponents.setUseLocalCheckpoints(true)`. While `localCheckpoints` provides better performance they are not as reliable as the persistent checkpointing._

**NOTE**

_Be aware, that returned `DataFrame` is persistent and should be unpersisted manually after processing to avoid memory leaks!_

**NOTE**

_From all the provided algorithms the "randomized_contraction" is the best one by all the characteristics. It is not the default only because of back-compatibility concerns._

---

<div class="codetabs">

<div data-lang="python" markdown="1">

For API details, refer to the [connectedComponents](api/python/reference/api/pyspark.graphframes.GraphFrame.html).

```python
from pyspark.graphframes.examples import Graphs

sc.setCheckpointDir("/tmp/spark-checkpoints")

g = Graphs(spark).friends()  # Get example graph

result = g.connectedComponents()
result.select("id", "component").orderBy("component").show()
```
</div>

<div data-lang="scala" markdown="1">

For API details, refer to the [ConnectedComponents](api/scala/org/apache/spark/graphframes/lib/ConnectedComponents.html).

```scala
import org.apache.spark.graphframes.{examples, GraphFrame}

val g: GraphFrame = examples.Graphs.friends // get example graph

val result = g.connectedComponents.setUseLocalCheckpoints(true).run()
result.select("id", "component").orderBy("component").show()
```
</div>

</div>

### Algorithms

GraphFrames provides three algorithm implementations, selectable via the `algorithm` argument:

#### `graphx`

A naive Pregel-based implementation backed by Apache Spark GraphX. It propagates the minimum vertex ID along edges one hop per iteration, so convergence requires a number of iterations equal to the diameter of the graph. While it may be slightly faster on very small graphs, it has poor convergence complexity on large or wide graphs and requires significantly more memory due to less efficient RDD serialization and its triplets-based nature. Use this only as a fallback or for compatibility.

#### `two_phase` (default)

A DataFrame-native implementation based on the large-star / small-star label propagation approach described in:

> Kiveris, Raimondas, et al. _"Connected components in MapReduce and beyond."_ Proceedings of the ACM Symposium on Cloud Computing. 2014. https://dl.acm.org/doi/abs/10.1145/2670979.2670997

This algorithm has much better convergence complexity than `graphx` and requires significantly less memory thanks to efficient Tungsten serialization. It is the recommended default for most workloads.

Component IDs produced by `two_phase` are stable `Long` values. For graphs whose vertex IDs are already integral types (`Long`, `Int`, `Short`, `Byte`), the component ID will be the minimum original vertex ID within the component. For `String`-typed (or other non-integral) vertex IDs, the component ID will be a random `Long` unless `useLabelsAsComponents=True` is set (see below).

This algorithm has two internal join modes — see [AQE-broadcast mode](#aqe-broadcast-mode) below for details.

#### `randomized_contraction`

A DataFrame-native implementation based on randomized graph contraction, described in:

> Bögeholz, Harald, Michael Brand, and Radu-Alexandru Todor. _"In-database connected component analysis."_ 2020 IEEE 36th International Conference on Data Engineering (ICDE). IEEE, 2020.

This algorithm iteratively contracts the graph using random linear functions until no edges remain, then reconstructs component identifiers in a reverse pass. It has similar convergence characteristics to `two_phase` (AQE mode) and performs better on benchmarks than `two_phase` with AQE. This algorithm requires around 2x less amount of memory to perform compared to "two_phase"

Unlike `two_phase`, `randomized_contraction` **always** produces random `Long` component IDs regardless of the input vertex ID type, unless `useLabelsAsComponents=True` is set.

#### Deprecation notice

The algorithm name `graphframes` is a deprecated alias for `two_phase` and will be removed in a future release. Replace any usage of `setAlgorithm("graphframes")` with `setAlgorithm("two_phase")`.

#### Performance summary

| Algorithm                 | Convergence complexity                     | Memory usage     | Component ID type                                  |
| ------------------------- | ------------------------------------------ | ---------------- | -------------------------------------------------- |
| `graphx`                  | O(diameter) iterations                     | High (RDD-based) | Min vertex ID in component                         |
| `two_phase` (skewed join) | Fast                                       | Low (DataFrame)  | Min original ID (integral) or random Long (String) |
| `two_phase` (AQE, `-1`)   | Fast, ~5x faster than skewed join          | Low (DataFrame)  | Min original ID (integral) or random Long (String) |
| `randomized_contraction`  | Fast, slightly slower than `two_phase` AQE | Low (DataFrame)  | Always random Long                                 |

### Arguments

- `algorithm` (Python) / `setAlgorithm` (Scala)

Selects the algorithm. Supported values: `graphx`, `two_phase` (default), `randomized_contraction`. The value `graphframes` is a deprecated alias for `two_phase`.

- `max_iter` (Python) / `maxIter` (Scala)

For `graphx` only. Limits the maximum number of Pregel iterations. The default is effectively unlimited, and it is generally not recommended to change this value.

- `checkpointInterval` (Python) / `setCheckpointInterval` (Scala)

For `two_phase` and `randomized_contraction`. To avoid exponential growth of the Spark logical plan, DataFrame lineage, and query optimization time, checkpointing is performed periodically. It is recommended to keep this value at `2` or below.

- `broadcastThreshold` (Python) / `setBroadcastThreshold` (Scala)

For `two_phase` only. See [AQE-broadcast mode](#aqe-broadcast-mode) below for details.

- `useLabelsAsComponents` (Python) / `setUseLabelsAsComponents` (Scala)

For `two_phase` and `randomized_contraction`. By default, component IDs are `Long` values. For `two_phase` with integral vertex ID types, the component ID is the minimum original vertex ID in the component. For `String`-typed vertices (or any non-integral type), and always for `randomized_contraction`, the component ID is a random `Long`. By setting `useLabelsAsComponents=True`, GraphFrames will instead use the minimum original vertex label as the component ID. This requires an additional `groupBy` + `agg` + `join` and is not free.

- `use_local_checkpoints` (Python) / `setUseLocalCheckpoints` (Scala)

For `two_phase` and `randomized_contraction`. By default, GraphFrames uses persistent checkpoints, which are reliable but require a `checkpointDir` to be configured in persistent storage (e.g. S3 or HDFS). Setting `use_local_checkpoints=True` uses the local disks of Spark executors instead. Local checkpoints are faster but less reliable: if an executor is lost, the checkpoint is lost and the job will fail. `randomized_contraction` still requires a `checkpointDir`, because it writes its intermediate tables there.

- `storage_level` (Python) / `setIntermediateStorageLevel` (Scala)

The storage level for intermediate datasets and the output DataFrame. Default is `MEMORY_AND_DISK`. For very large graphs or out-of-core scenarios, `DISK_ONLY` may be preferable.

### AQE-broadcast mode

_Starting from 0.10.0_

For `two_phase` only. During iterations, this algorithm can produce edges with highly skewed degree distributions, where some vertices have very high degree. In earlier versions of GraphFrames, this was handled by manually broadcasting high-degree nodes. However, this manual broadcasting is incompatible with Apache Spark Adaptive Query Execution (AQE), which is why AQE was previously disabled for Connected Components.

From GraphFrames 0.10+, you can disable manual broadcasting and instead rely on AQE to handle skewness automatically. To enable this mode, pass `-1` as the `broadcastThreshold` (or call `setBroadcastThreshold(-1)`). Based on benchmarks, this mode provides approximately **5x speed-up** over the manual skewed-join mode. It is possible that in a future release, `-1` will become the default value for `broadcastThreshold`.

### Advanced: skipping graph preparation

_For advanced JVM users only._

Internally, both `two_phase` and `randomized_contraction` perform a graph preparation step before running the algorithm (re-indexing vertices, symmetrizing edges, removing self-loops, etc.). The preparation steps differ between the two algorithms and are **not interchangeable**.

If you have already performed the exact preparation steps yourself and fully understand what they entail for the specific algorithm you are using, you can skip the internal preparation by calling `setIsGraphPrepared(true)`. This is an internal API intended only for users who have studied the source code of the algorithm in detail.

**Warning:** Incorrect use of this flag will produce silently wrong results with no error or warning at runtime.

### Strongly connected components

Compute the strongly connected component (SCC) of each vertex and return a graph with each vertex assigned to the SCC
containing that vertex. At the moment, SCC in GraphFrames is a wrapper around GraphX implementation.

See [Wikipedia](https://en.wikipedia.org/wiki/Strongly_connected_component) for the background.

---

**NOTE**

_Be aware, that returned `DataFrame` is persistent and should be unpersisted manually after processing to avoid memory leaks!_

---

<div class="codetabs">

<div data-lang="python" markdown="1">

For API details, refer to the [stronglyConnectedComponents](api/python/reference/api/pyspark.graphframes.GraphFrame.html).

```python
from pyspark.graphframes.examples import Graphs

sc.setCheckpointDir("/tmp/spark-checkpoints")

g = Graphs(spark).friends()  # Get example graph

result = g.stronglyConnectedComponents(maxIter=10)
result.select("id", "component").orderBy("component").show()
```
</div>

<div data-lang="scala" markdown="1">

For API details, refer to the [StronglyConnectedComponents](api/scala/org/apache/spark/graphframes/lib/StronglyConnectedComponents.html).

```scala
import org.apache.spark.graphframes.{examples, GraphFrame}

val g: GraphFrame = examples.Graphs.friends // get example graph

val result = g.stronglyConnectedComponents.maxIter(10).run()
result.select("id", "component").orderBy("component").show()
```
</div>

</div>

## Triangle count

Triangle count computes the number of triangles passing through each vertex. A triangle is a set of three vertices where each pair is connected by an edge (A is connected to B, B to C, and C to A).

### Performance and Use Cases

Counting triangles is a fundamental task in network analysis:

- **Clustering Coefficient**: It is used to compute the local and global clustering coefficients, which measure the degree to which nodes in a graph tend to cluster together.
- **Community Detection**: A high density of triangles often indicates the presence of a tightly knit community or "clique."
- **Spam and Fraud Detection**: In social networks and financial transactions, unusual triangle patterns can help identify botnets or money-laundering rings.

### How it works

The core logic of the algorithm is based on **neighborhood intersection**. For every edge (u, v) in the graph, the algorithm finds the intersection of the neighbor sets of u and v. Every common neighbor w completes a triangle with u and v.

### Algorithms and Trade-offs

GraphFrames provides two implementations with different performance characteristics:

- **Exact**: This is the default algorithm. It computes the precise intersection of adjacency lists.
  - **Pros**: 100% accuracy.
  - **Cons**: Extremely memory-intensive. For high-degree nodes (hubs), collecting and intersecting large neighbor sets can lead to Out-of-Memory (OOM) errors or severe skew.
- **Approximate** (Starting from Spark 4.1): This version uses **DataSketches (Theta sketches)** to estimate the size of the intersection.
  - **Pros**: Highly scalable. It uses a fixed-size probabilistic structure to represent neighborhoods, dramatically reducing memory overhead and execution time.
  - **Cons**: Provides an estimate rather than an exact count.

### Selection Guide

- **Sparse/Regular Graphs**: For graphs like grids, spatial meshes, or infrastructure networks where the maximum degree is relatively low and there is no power-law distribution, the **Exact** algorithm is recommended.
- **Power-Law/Scale-Free Networks**: For social networks, web graphs, or biological networks containing "hubs" (vertices with thousands or millions of connections), the **Approximate** algorithm is often the only viable choice to avoid job failures.

<div class="codetabs">

<div data-lang="python" markdown="1">

For API details, refer to the [triangleCount](api/python/reference/api/pyspark.graphframes.GraphFrame.html).

```python
from pyspark.graphframes.examples import Graphs

g = Graphs(spark).friends()  # Get example graph

results = g.triangleCount()
results.select("id", "count").show()
```
</div>

<div data-lang="scala" markdown="1">

For API details, refer to the [TriangleCount](api/scala/org/apache/spark/graphframes/lib/TriangleCount.html).

```scala
import org.apache.spark.graphframes.{examples, GraphFrame}

val g: GraphFrame = examples.Graphs.friends // get example graph

val results = g.triangleCount.run()
results.select("id", "count").show()
```
</div>

</div>

## Cycles Detection

GraphFrames provides an implementation of
the [Rocha–Thatte cycle detection algorithm](https://en.wikipedia.org/wiki/Rocha%E2%80%93Thatte_cycle_detection_algorithm).

---

**NOTE**

_Be aware, that returned `DataFrame` is persistent and should be unpersisted manually after processing to avoid memory leaks!_

**WARNING:**

- _This algorithm collects the full sequences and may require a lot of cluste memory for power-law graphs_

---

<div class="codetabs">

<div data-lang="python" markdown="1">

```python
from pyspark.graphframes import GraphFrame

vertices = spark.createDataFrame(
    [(1, "a"), (2, "b"), (3, "c"), (4, "d"), (5, "e")], ["id", "attr"]
)
edges = spark.createDataFrame(
    [(1, 2), (2, 3), (3, 1), (1, 4), (2, 5)], ["src", "dst"]
)
graph = GraphFrame(vertices, edges)
res = graph.detectingCycles(
    checkpoint_interval=3,
    use_local_checkpoints=True,
)
res.show(False)

# Output:
# +----+--------------+
# | id | found_cycles |
# +----+--------------+
# |1   |[1, 3, 1]     |
# |1   |[1, 2, 1]     |
# |1   |[1, 2, 5, 1]  |
# |2   |[2, 1, 2]     |
# |2   |[2, 5, 1, 2]  |
# |3   |[3, 1, 3]     |
# |5   |[5, 1, 2, 5]  |
# +----+--------------+
```
</div>

<div data-lang="scala" markdown="1">

```scala
import org.apache.spark.graphframes.GraphFrame

val graph = GraphFrame(
  spark
    .createDataFrame(Seq((1L, "a"), (2L, "b"), (3L, "c"), (4L, "d"), (5L, "e")))
    .toDF("id", "attr"),
  spark
    .createDataFrame(Seq((1L, 2L), (2L, 1L), (1L, 3L), (3L, 1L), (2L, 5L), (5L, 1L)))
    .toDF("src", "dst"))
val res = graph.detectingCycles.setUseLocalCheckpoints(true).run()
res.show(false)

// Output:
// +--------------+
// | found_cycles |
// +--------------+
// |[1, 3, 1]     |
// |[1, 2, 1]     |
// |[1, 2, 5, 1]  |
```
</div>

</div>

### Arguments

- `checkpoint_interval`

For `graphframes` only. To avoid exponential growing of the Spark' Logical Plan, DataFrame lineage and query optimization time, it is required to do checkpointing periodically. While checkpoint itself is not free, it is still recommended to set this value to something less than `5`.

- `use_local_checkpoints`

For `graphframes` only. By default, GraphFrames uses persistent checkpoints. They are reliable and reduce the errors rate. The downside of the persistent checkpoints is that they are requiride to set up a `checkpointDir` in persistent storage like `S3` or `HDFS`. By providing `use_local_checkpoints=True`, user can say GraphFrames to use local disks of Spark' executurs for checkpointing. Local checkpoints are faster, but they are less reliable: if the executur lost, for example, is taking by the higher priority job, checkpoints will be lost and the whole job fails.

- `storage_level`

The level of storage for intermediate results and the output `DataFrame` with components. By default it is memory and disk deserialized as a good balance between performance and reliability. For very big graphs and out-of-core scenarios, using `DISK_ONLY` may be faster.

## Aggregate Neighbors

Aggregate Neighbors is a powerful multi-hop traversal algorithm that allows you to explore the graph up to a specified number of hops while accumulating values along paths using customizable accumulator expressions. It supports both stopping conditions (when to stop exploring) and target conditions (when to collect a result).

### Motivation and Use Cases

This algorithm is particularly useful for:

- **Path Finding**: Finding all paths between vertices while tracking accumulated values (distance, cost, etc.)
- **Graph Exploration**: Systematically exploring neighborhoods with custom aggregation logic
- **Pattern Matching**: Finding vertices that satisfy complex conditions based on path properties
- **Accumulative Computations**: Computing values that depend on the entire path (e.g., product of edge weights, sum of node values)

Unlike single-hop algorithms like BFS or shortest paths, Aggregate Neighbors allows you to:

- Track multiple accumulators simultaneously
- Define custom stopping criteria
- Access vertex and edge attributes during traversal
- Filter which edges can be traversed

---

**NOTE**

_Be aware, that returned `DataFrame` is persistent and should be unpersisted manually after processing to avoid memory leaks!_

---

### How It Works

The algorithm performs a breadth-first traversal from starting vertices:

1. **Initialization**: Starting vertices are marked as active with accumulators initialized
2. **Iteration**: For each hop, active vertices send messages to their neighbors
3. **Accumulation**: Accumulator values are updated based on traversal state
4. **Stopping**: Paths stop when stopping condition is met or max hops reached
5. **Collection**: Results are collected when target condition is satisfied

<div class="codetabs">

<div data-lang="python" markdown="1">

For API details, refer to the [aggregate_neighbors](api/python/reference/api/pyspark.graphframes.GraphFrame.html).

```python
from pyspark.graphframes import GraphFrame
from pyspark.graphframes.graphframe import AggregateNeighbors
from pyspark.sql import functions as F

# Create a graph
vertices = spark.createDataFrame(
    [(1, "A"), (2, "B"), (3, "C"), (4, "D")], ["id", "name"]
)
edges = spark.createDataFrame(
    [(1, 2), (2, 3), (3, 4), (1, 3)], ["src", "dst"]
)
g = GraphFrame(vertices, edges)

# Find all paths from vertex 1 to vertex 4, tracking path length
result = g.aggregate_neighbors(
    starting_vertices=F.col("id") == 1,
    max_hops=5,
    accumulator_names=["path"],
    accumulator_inits=[F.lit("1")],
    accumulator_updates=[F.concat(F.col("path"), F.lit("->"), AggregateNeighbors.dst_attr("id").cast("string"))],
    target_condition=AggregateNeighbors.dst_attr("id") == 4
)

result.select("id", "hop", "path").show()
```
</div>

<div data-lang="scala" markdown="1">

For API details, refer to the [AggregateNeighbors](api/scala/org/apache/spark/graphframes/lib/AggregateNeighbors.html).

```scala
import org.apache.spark.graphframes.GraphFrame
import org.apache.spark.graphframes.lib.AggregateNeighbors
import org.apache.spark.sql.functions._

// Create a graph
val vertices = spark.createDataFrame(
  Seq((1L, "A"), (2L, "B"), (3L, "C"), (4L, "D"))
).toDF("id", "name")

val edges = spark.createDataFrame(
  Seq((1L, 2L), (2L, 3L), (3L, 4L), (1L, 3L))
).toDF("src", "dst")

val g = GraphFrame(vertices, edges)

// Find all paths from vertex 1 to vertex 4, tracking path length
val result = g.aggregateNeighbors
  .setStartingVertices(col("id") === 1)
  .setMaxHops(5)
  .addAccumulator(
    "path",
    lit("1"),
    concat(col("path"), lit("->"), AggregateNeighbors.dstAttr("id").cast("string"))
  )
  .setTargetCondition(AggregateNeighbors.dstAttr("id") === lit(4))
  .run()

result.select("id", "hop", "path").show()
```
</div>

</div>

### Accumulators

Accumulators are the core concept of Aggregate Neighbors. Each accumulator tracks a value as the algorithm traverses the graph:

- **Name**: Becomes a column in the result DataFrame
- **Initial Value**: Expression evaluated on starting vertices
- **Update Expression**: Evaluated at each hop, can reference:
  - Source vertex attributes via `src_attr("attrName")`
  - Destination vertex attributes via `dst_attr("attrName")`
  - Edge attributes via `edge_attr("attrName")`
  - Current accumulator value via its column name

**Example with Multiple Accumulators:**

```python
# Track both path length and sum of node values
result = g.aggregate_neighbors(
    starting_vertices=F.col("id") == 1,
    max_hops=5,
    accumulator_names=["path_length", "sum_values"],
    accumulator_inits=[F.lit(0), F.lit(0)],
    accumulator_updates=[
        F.col("path_length") + 1,
        F.col("sum_values") + AggregateNeighbors.dst_attr("value")
    ],
    target_condition=AggregateNeighbors.dst_attr("id") == 10,
    required_vertex_attributes=["id", "value"]
)
```

### Stopping vs Target Conditions

- **Stopping Condition**: Stops traversal along a path when true. Use for avoiding cycles or limiting search depth.
- **Target Condition**: Marks vertices as results when true. Only accumulators reaching target vertices are returned.
- At least one must be provided.

**Example with Stopping Condition:**

```python
# Stop when revisiting a vertex (avoid cycles)
result = g.aggregate_neighbors(
    starting_vertices=F.col("id") == 1,
    max_hops=10,
    accumulator_names=["visited", "path_length"],
    accumulator_inits=[F.array(F.lit(1)), F.lit(0)],
    accumulator_updates=[
        F.array_append(F.col("visited"), AggregateNeighbors.dst_attr("id")),
        F.col("path_length") + 1
    ],
    stopping_condition=F.array_contains(
        F.col("visited"),
        AggregateNeighbors.dst_attr("id")
    )
)
```

### Performance Considerations

- **Required Attributes**: Use `required_vertex_attributes` and `required_edge_attributes` to limit columns carried through traversal, reducing memory usage.
- **Edge Filtering**: Use `edge_filter` to limit traversable edges early in the pipeline.
- **Self-Loops**: Set `remove_loops=True` to exclude self-loop edges if not needed.
- **Checkpointing**: Use `checkpoint_interval` to prevent logical plan growth on deep traversals.
- **Max Hops**: Be cautious with high `max_hops` on dense graphs (risk of OOM).

### Arguments

- `starting_vertices`

Column expression selecting seed vertices for the traversal (e.g., `F.col("id") == 1`).

- `max_hops`

Maximum number of hops to explore from starting vertices. Must be a positive integer.

- `accumulator_names`, `accumulator_inits`, `accumulator_updates`

Lists defining the accumulators to track. All three lists must have the same length.

- `stopping_condition` (optional)

Boolean column expression that stops traversal along a path when true.

- `target_condition` (optional)

Boolean column expression that marks vertices as results when true.

- `required_vertex_attributes` (optional)

List of vertex column names to carry through traversal. If None, all columns are carried.

- `required_edge_attributes` (optional)

List of edge column names to carry through traversal. If None, all columns are carried.

- `edge_filter` (optional)

Boolean column expression to filter which edges can be traversed.

- `remove_loops`

If True, exclude self-loop edges (where src == dst). Default: False.

- `checkpoint_interval`

Checkpoint every N iterations to prevent logical plan growth. 0 = disabled (default).

- `use_local_checkpoints`

Use local checkpoints (faster but less reliable than persistent checkpoints).

- `storage_level`

Storage level for intermediate results. Default: MEMORY_AND_DISK_DESER.
