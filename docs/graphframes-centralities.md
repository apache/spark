---
layout: global
displayTitle: Centrality Metrics
title: Centrality Metrics
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

## Degrees

GraphFrames provides three main APIs for computing degrees:

- `inDegrees`
- `outDegrees`
- `degrees`

<div class="codetabs">

<div data-lang="python" markdown="1">

```python
from pyspark.graphframes.examples import Graphs

g = Graphs(spark).friends()
in_degrees = g.inDegrees()
out_degrees = g.outDegrees()
degrees = g.degrees()
```
</div>

<div data-lang="scala" markdown="1">

```scala
import org.apache.spark.graphframes.{examples,GraphFrame}

val g: GraphFrame = examples.Graphs.friends

val inDegrees: DataFrame = g.inDegrees
val outDegrees: DataFrame = g.outDegrees
val degrees: DataFrame = g.degrees
```
</div>

</div>

## PageRank

There are two implementations of PageRank.

- The first one uses the `org.apache.spark.graphx.graph` interface with `aggregateMessages` and runs PageRank for a fixed number of iterations. This can be executed by setting `maxIter`.
- The second implementation uses the `org.apache.spark.graphx.Pregel` interface and runs PageRank until convergence and this can be run by setting `tol`.

Both implementations support non-personalized and personalized PageRank, where setting a `sourceId` personalizes the results for that vertex.

See [Wikipedia](https://en.wikipedia.org/wiki/PageRank) for a background.

**NOTE:** The `pageRank` API at the moment is the only API in GraphFrames that returns a `GraphFrame` object instead of a `DataFrame`. Most probably, this behavior will change in the nearest major release for the API consistency. It is strongly recommended do not rely on the returned `edges` at all.

<div class="codetabs">

<div data-lang="python" markdown="1">

For API details, refer to the [pageRank](api/python/reference/api/pyspark.graphframes.GraphFrame.html).

```python
from pyspark.graphframes.examples import Graphs

g = Graphs(spark).friends()  # Get example graph

# Run PageRank until convergence to tolerance "tol"

results = g.pageRank(resetProbability=0.15, tol=0.01)

# Display resulting pageranks and final edge weights

# Note that the displayed pagerank may be truncated, e.g., missing the E notation

results.vertices.select("id", "pagerank").show()
results.edges.select("src", "dst", "weight").show()

# Run PageRank for a fixed number of iterations

results2 = g.pageRank(resetProbability=0.15, maxIter=10)

# Run PageRank personalized for vertex "a"

results3 = g.pageRank(resetProbability=0.15, maxIter=10, sourceId="a")

# Run PageRank personalized for vertex ["a", "b", "c", "d"] in parallel

results4 = g.parallelPersonalizedPageRank(resetProbability=0.15, sourceIds=["a", "b", "c", "d"], maxIter=10)
```
</div>

<div data-lang="scala" markdown="1">

For API details, refer to the [PageRank](api/scala/org/apache/spark/graphframes/lib/PageRank.html).

```scala
import org.apache.spark.graphframes.{examples,GraphFrame}

val g: GraphFrame = examples.Graphs.friends // get example graph

// Run PageRank until convergence to tolerance "tol".
val results: GraphFrame = g.pageRank.resetProbability(0.15).tol(0.01).run()

// Display resulting pageranks and final edge weights
// Note that the displayed pagerank may be truncated, e.g., missing the E notation.
results.vertices.select("id", "pagerank").show()
results.edges.select("src", "dst", "weight").show()

// Run PageRank for a fixed number of iterations.
val results2 = g.pageRank.resetProbability(0.15).maxIter(10).run()

// Run PageRank personalized for vertex "a"
val results3 = g.pageRank.resetProbability(0.15).maxIter(10).sourceId("a").run()

// Run PageRank personalized for vertex ["a", "b", "c", "d"] in parallel
val results4 = g.parallelPersonalizedPageRank.resetProbability(0.15).maxIter(10).sourceIds(Array("a", "b", "c", "d"))
.run()
results4.vertices.show()
results4.edges.show()
```
</div>

</div>

### Parallel personalized PageRank

GraphFrames also supports parallel personalized PageRank that allows users to compute ranks "from the subset of source vertices".

For the API details refer to:

- Scala API: [ParallelPersonalizedPageRank](api/scala/org/apache/spark/graphframes/lib/ParallelPersonalizedPageRank.html)
- Python API: [parallelPersonalizedPageRank](api/python/reference/api/pyspark.graphframes.GraphFrame.html)

## K-Core

K-Core decomposition is a method used to identify the most tightly connected subgraphs within a network. A k-core is a maximal subgraph where every vertex has at least degree k. This metric helps in understanding the inner structure of networks by filtering out less connected nodes, revealing cores of highly interconnected entities. K-Core centrality can be applied in various domains such as social network analysis to find influential users, in biology to detect stable protein complexes, or in infrastructure networks to assess robustness and vulnerability.

The provided implementation of K-Core decomposition in GraphFrames is based on the research described in the paper available at [IEEE Xplore](https://ieeexplore.ieee.org/abstract/document/8258018). Using think-like-a-vertex paradigm, the proposed method utilizes a message passing paradigm for solving k-core decomposition, thus reducing the I/O cost substantially.

For more information, see:

> A. Farajollahi, S. G. Khaki, and L. Wang, "Efficient distributed k-core decomposition for large-scale graphs," _2017 IEEE International Conference on Big Data (Big Data)_, Boston, MA, USA, 2017, pp. 1430-1435.

### Arguments

- `checkpoint_interval`

For `graphframes` only. To avoid exponential growing of the Spark' Logical Plan, DataFrame lineage and query optimization time, it is required to do checkpointing periodically. While checkpoint itself is not free, it is still recommended to set this value to something less than `5`.

- `use_local_checkpoints`

For `graphframes` only. By default, GraphFrames uses persistent checkpoints. They are reliable and reduce the errors rate. The downside of the persistent checkpoints is that they are requiride to set up a `checkpointDir` in persistent storage like `S3` or `HDFS`. By providing `use_local_checkpoints=True`, user can say GraphFrames to use local disks of Spark' executurs for checkpointing. Local checkpoints are faster, but they are less reliable: if the executur lost, for example, is taking by the higher priority job, checkpoints will be lost and the whole job fails.

- `storage_level`

The level of storage for intermediate results and the output `DataFrame` with components. By default it is memory and disk deserialized as a good balance between performance and reliability. For very big graphs and out-of-core scenarios, using `DISK_ONLY` may be faster.

<div class="codetabs">

<div data-lang="python" markdown="1">

```python
from pyspark.graphframes import GraphFrame

v = spark.createDataFrame([(i, f"v{i}") for i in range(30)], ["id", "name"])

# Build edges to create a hierarchical structure:
# Core (k=5): vertices 0-4 - fully connected
core_edges = [(i, j) for i in range(5) for j in range(i + 1, 5)]

# Next layer (k=3): vertices 5-14 - each connects to multiple core vertices
mid_layer_edges = [
    (5, 0),
    (5, 1),
    (5, 2),  # Connect to core
    (6, 0),
    (6, 1),
    (6, 3),
    (7, 1),
    (7, 2),
    (7, 4),
    (8, 0),
    (8, 3),
    (8, 4),
    (9, 1),
    (9, 2),
    (9, 3),
    (10, 0),
    (10, 4),
    (11, 2),
    (11, 3),
    (12, 1),
    (12, 4),
    (13, 0),
    (13, 2),
    (14, 3),
    (14, 4),
]

# Outer layer (k=1): vertices 15-29 - sparse connections
outer_edges = [
    (15, 5),
    (16, 6),
    (17, 7),
    (18, 8),
    (19, 9),
    (20, 10),
    (21, 11),
    (22, 12),
    (23, 13),
    (24, 14),
    (25, 15),
    (26, 16),
    (27, 17),
    (28, 18),
    (29, 19),
]

all_edges = core_edges + mid_layer_edges + outer_edges
e = spark.createDataFrame(all_edges, ["src", "dst"])
g = GraphFrame(v, e)
result = g.k_core(
    checkpoint_interval=args.checkpoint_interval,
    use_local_checkpoints=args.use_local_checkpoints,
    storage_level=args.storage_level,
)
```
</div>

<div data-lang="scala" markdown="1">

```scala
import org.apache.spark.graphframes.GraphFrame

val v = spark.createDataFrame((0L until 30L).map(id => (id, s"v$id"))).toDF("id", "name")

// Build edges to create a hierarchical structure:
// Core (k=5): vertices 0-4 - fully connected
// Next layer (k=3): vertices 5-14 - each connects to multiple core vertices
// Outer layer (k=1): vertices 15-29 - sparse connections
val coreEdges = for {
  i <- 0 until 5
  j <- (i + 1) until 5
} yield (i.toLong, j.toLong)

val midLayerEdges = Seq(
  (5L, 0L),
  (5L, 1L),
  (5L, 2L), // Connect to core
  (6L, 0L),
  (6L, 1L),
  (6L, 3L),
  (7L, 1L),
  (7L, 2L),
  (7L, 4L),
  (8L, 0L),
  (8L, 3L),
  (8L, 4L),
  (9L, 1L),
  (9L, 2L),
  (9L, 3L),
  (10L, 0L),
  (10L, 4L),
  (11L, 2L),
  (11L, 3L),
  (12L, 1L),
  (12L, 4L),
  (13L, 0L),
  (13L, 2L),
  (14L, 3L),
  (14L, 4L))

val outerEdges = Seq(
  (15L, 5L),
  (16L, 6L),
  (17L, 7L),
  (18L, 8L),
  (19L, 9L),
  (20L, 10L),
  (21L, 11L),
  (22L, 12L),
  (23L, 13L),
  (24L, 14L),
  (25L, 15L),
  (26L, 16L),
  (27L, 17L),
  (28L, 18L),
  (29L, 19L))

val allEdges = coreEdges ++ midLayerEdges ++ outerEdges
val e = spark.createDataFrame(allEdges).toDF("src", "dst")
val g = GraphFrame(v, e)
val result = g.kCore.run()
```
</div>

</div>

## Approximate Neighbor Functions

HyperANF (Hyper Approximated Neighbourhood Function) is an algorithm that uses HyperLogLog sketches to approximate the number of nodes reachable from each vertex within a given number of hops. It provides a scalable way to compute neighborhood functions on very large graphs without the exponential cost of exact enumeration.

This implementation is inspired by the paper:

> P. Boldi, M. Rosa, and S. Vigna, "HyperANF: Approximating the Neighbourhood Function of Very Large Graphs on a Budget," _Proceedings of the 20th International Conference on World Wide Web (WWW '11)_, ACM, 2011, pp. 587–596. arXiv preprint: [arXiv:1011.5599](https://arxiv.org/pdf/1011.5599).

The input graph is treated as **directed**: for each vertex, reachability is computed by following outgoing edges from `src` to `dst`.

### How It Works

The algorithm builds HyperLogLog sketches iteratively. At each hop, it unions the sketches of all neighbors reached so far to produce the sketch for the next hop. The returned `DataFrame` contains one row per source vertex and one sketch column per hop:

- `hop_0`: a HyperLogLog sketch containing only the source vertex itself.
- `hop_k` (for `k >= 1`): a HyperLogLog sketch of the set of vertices reachable in **exactly** `k` directed hops.

To obtain the **cumulative** approximate neighborhood size up to hop `k`, union `hop_0` through `hop_k` using `hll_union` and then apply `hll_sketch_estimate` to the merged sketch. This gives the total number of distinct vertices reachable within `k` hops.

### Arguments

- `n_hops`

Maximum hop distance to compute (positive integer). The result will contain columns `hop_0` through `hop_N` where `N = n_hops`. Default is 3.

- `lg_nom_entries`

Log2 of nominal entries used by HLL sketch aggregations. Must be between 4 and 21 (inclusive). Higher values increase accuracy at the cost of memory. Default is 12.

- `edge_filter`

Optional column expression or SQL expression string applied to edges before computation. Only edges satisfying this predicate participate in the directed reachability expansion. A common use case is filtering on `src`, for example `src IN (1, 2, 3)`, to obtain sketches only for a selected set of starting vertices.

- `checkpoint_interval`

For `graphframes` only. To avoid exponential growing of the Spark' Logical Plan, DataFrame lineage and query optimization time, it is required to do checkpointing periodically. While checkpoint itself is not free, it is still recommended to set this value to something less than `5`.

- `use_local_checkpoints`

For `graphframes` only. By default, GraphFrames uses persistent checkpoints. They are reliable and reduce the errors rate. The downside of the persistent checkpoints is that they are required to set up a `checkpointDir` in persistent storage like `S3` or `HDFS`. By providing `use_local_checkpoints=True`, user can say GraphFrames to use local disks of Spark' executors for checkpointing. Local checkpoints are faster, but they are less reliable: if the executor is lost, for example is taken by a higher priority job, checkpoints will be lost and the whole job fails.

- `storage_level`

The level of storage for intermediate results and the output `DataFrame` with components. By default it is memory and disk deserialized as a good balance between performance and reliability. For very big graphs and out-of-core scenarios, using `DISK_ONLY` may be faster.

<div class="codetabs">

<div data-lang="python" markdown="1">

```python
from pyspark.graphframes import GraphFrame
from pyspark.sql import functions as F

# Create a small directed graph
vertices = spark.createDataFrame([(i, f"v{i}") for i in range(6)], ["id", "name"])
edges = spark.createDataFrame([
    (0, 1), (0, 2), (1, 3), (2, 3),
    (3, 4), (4, 5), (1, 4)
], ["src", "dst"])
g = GraphFrame(vertices, edges)

# Run HyperANF with 3 hops
result = g.hyper_anf(n_hops=3)
result.columns  # ['id', 'hop_0', 'hop_1', 'hop_2', 'hop_3']

# Compute cumulative neighborhood sizes up to each hop
# by unioning the per-hop HLL sketches and estimating cardinality
cumulative = result
for k in range(1, 4):
    # Start with hop_0, then union hop_1, hop_2, ..., hop_k
    union_expr = result["hop_0"]
    for j in range(1, k + 1):
        union_expr = F.hll_union(union_expr, result[f"hop_{j}"])
    cumulative = cumulative.withColumn(
        f"neighbors_within_{k}",
        F.hll_sketch_estimate(union_expr),
    )
cumulative.select("id", "neighbors_within_1", "neighbors_within_2", "neighbors_within_3").show()
```
</div>

<div data-lang="scala" markdown="1">

```scala
import org.apache.spark.graphframes.GraphFrame
import org.apache.spark.sql.functions._

// Create a small directed graph
val vertices = spark.createDataFrame((0L until 6L).map(i => (i, s"v$i"))).toDF("id", "name")
val edges = spark.createDataFrame(Seq(
  (0L, 1L), (0L, 2L), (1L, 3L), (2L, 3L),
  (3L, 4L), (4L, 5L), (1L, 4L)
)).toDF("src", "dst")
val g = GraphFrame(vertices, edges)

// Run HyperANF with 3 hops
val result = g.hyperANF.setNHops(3).run()
result.columns // Seq("id", "hop_0", "hop_1", "hop_2", "hop_3")

// Compute cumulative neighborhood sizes up to each hop
// by unioning the per-hop HLL sketches and estimating cardinality
var cumulative = result
for (k <- 1 to 3) {
  var unionExpr = result("hop_0")
  for (j <- 1 to k) {
    unionExpr = hll_union(unionExpr, result(s"hop_$j"))
  }
  cumulative = cumulative.withColumn(s"neighbors_within_$k", hll_sketch_estimate(unionExpr))
}
cumulative.select("id", "neighbors_within_1", "neighbors_within_2", "neighbors_within_3").show()
```
</div>

</div>
