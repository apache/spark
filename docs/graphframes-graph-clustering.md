---
layout: global
displayTitle: Community Detection
title: Community Detection
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

## Label Propagation (LPA)

Run a static Label Propagation Algorithm for detecting communities in networks. Each node in the network is initially assigned to its own community. At every superstep, nodes send their community affiliation to all neighbors and update their state to the mode community affiliation of incoming messages. LPA is a standard community detection algorithm for graphs. It is very inexpensive computationally, although (1) convergence is not guaranteed and (2) one can end up with trivial solutions (all nodes are identified into a single community).

See [Wikipedia](https://en.wikipedia.org/wiki/Label_Propagation_Algorithm) for the background.

---

**NOTE**

_Be aware, that returned `DataFrame` is persistent and should be unpersisted manually after processing to avoid memory leaks!_

---

<div class="codetabs">

<div data-lang="python" markdown="1">

For API details, refer to the [labelPropagation](api/python/reference/api/pyspark.graphframes.GraphFrame.html).

```python
from pyspark.graphframes.examples import Graphs

g = Graphs(spark).friends()  # Get example graph

result = g.labelPropagation(maxIter=5)
result.select("id", "label").show()
```
</div>

<div data-lang="scala" markdown="1">

For API details, refer to the [LabelPropagation](api/scala/org/apache/spark/graphframes/lib/LabelPropagation.html).

```scala
import org.apache.spark.graphframes.{examples,GraphFrame}

val g: GraphFrame = examples.Graphs.friends // get example graph

val result = g.labelPropagation.maxIter(5).run()
result.select("id", "label").show()
```
</div>

</div>

### Arguments

- `maxIter`

An amount of Pregel iterations. While in theory, Label Propagation algorithm should converge sooner or later to some stable state, there are a lot of problems with it on a real-world graphs. The first one is oscillations: even if the algorithm is almost converged, on a big graphs some vertices at the border between detected communities may contibue oscillate from one iteration to another. The biggest problem, however, is that algorithm may easily converge to the state when all vertices has the same label. It is strongly recommended to set `maxIter` to some reasonable value from `5` to `10` and do some experiments depends of the task and the goal.

- `algorithm`

Possible values are `graphx` and `graphframes`. Both implementations are based on the same logic. GraphX is faster for small-medium sized graphs but requires more memory due to less efficient RDD serialization and it's triplets-based nature. GraphFrames requires much less memory due to efficient Thungsten serialization and because the core structures are edges and messages, not triplets.

- `checkpoint_interval`

For `graphframes` only. To avoid exponential growing of the Spark' Logical Plan, DataFrame lineage and query optimization time, it is required to do checkpointing periodically. While checkpoint itself is not free, it is still recommended to set this value to something less than `5`.

- `use_local_checkpoints`

For `graphframes` only. By default, GraphFrames uses persistent checkpoints. They are reliable and reduce the errors rate. The downside of the persistent checkpoints is that they are requiride to set up a `checkpointDir` in persistent storage like `S3` or `HDFS`. By providing `use_local_checkpoints=True`, user can say GraphFrames to use local disks of Spark' executurs for checkpointing. Local checkpoints are faster, but they are less reliable: if the executur lost, for example, is taking by the higher priority job, checkpoints will be lost and the whole job fails.

- `storage_level`

The level of storage for intermediate results and the output `DataFrame` with components. By default it is memory and disk deserialized as a good balance between performance and reliability. For very big graphs and out-of-core scenarios, using `DISK_ONLY` may be faster.

## Neighborhood-Aware CDLP

Neighborhood-Aware (or Structure-Aware) CDLP is a weighted variant of label propagation. Instead of counting every incoming label vote equally, it gives more influence to neighbors that are structurally similar to the destination vertex.

This implementation is inspired by:

Xie, Jierui, and Boleslaw K. Szymanski. "Community detection using a neighborhood strength driven label propagation algorithm." 2011 IEEE Network Science Workshop (NSW), 2011.

### Intuition

Each incoming label vote has two parts:

- direct-link baseline (can be enabled or disabled),
- structural-overlap part (`structuralSimilarityMultiplier * commonNeighbors`).

Simple intuition for `structuralSimilarityMultiplier`:

- smaller values: behavior is closer to standard LPA,
- larger values: labels from structurally similar neighbors (many common neighbors) get stronger weight.

### Important differences from the paper

Compared to NSW 2011, this GraphFrames implementation intentionally extends and adapts behavior:

- The paper is for **undirected graphs**; this implementation supports **directed and undirected** execution.
- The paper does **not** support turning off direct-link contribution; this implementation adds `ignoreDirectLinks`.
- The paper uses structural factor in **\[0, 1\]**; this implementation exposes a non-negative multiplier, allowing a wider tuning range.

### Directed common-neighbor definition: pragmatic, not canonical

For directed mode, common neighbors are defined as **shared out-neighbors** of source and destination.

This is a practical choice for scalable message-passing pipelines and consistent directional semantics, but it is not the only valid definition in directed graphs. Other definitions (for example, shared in-neighbors or mixed in/out motifs) may emphasize different structural patterns and lead to different communities.

### Important approximation warning (sketch-based implementation)

This implementation uses **Theta sketches** to estimate common-neighbor overlap.

That makes it scalable, but it is still an approximation. Estimation noise can accumulate through iterative label propagation, and the effect is usually more visible when overlap is small (for example, edges between low-degree vertices where true common-neighbor counts are tiny).

In short: treat very small overlap differences with caution, especially on sparse/low-degree regions.

### Relationship to classical CDLP

You can make this algorithm behave close to classical CDLP/LPA by using:

- `ignoreDirectLinks = false`
- `structuralSimilarityMultiplier = 0`

This effectively removes structural-overlap amplification and leaves direct-link voting only.

However, this is **not recommended** as a replacement for classical CDLP in production. If you need classical behavior, use the dedicated CDLP API (`labelPropagation`) because it is more optimized.

<div class="codetabs">

<div data-lang="python" markdown="1">

For API details, refer to the [neighborhood_aware_cdlp](api/python/reference/api/pyspark.graphframes.GraphFrame.html).

```python
from pyspark.graphframes.examples import Graphs

g = Graphs(spark).friends()

result = g.neighborhood_aware_cdlp(
    max_iter=5,
    structural_similarity_multiplier=0.5,
    ignore_direct_links=False,
)
result.select("id", "label").show()
```
</div>

<div data-lang="scala" markdown="1">

For API details, refer to the [StructureAwareLabelPropagation](api/scala/org/apache/spark/graphframes/lib/StructureAwareLabelPropagation.html).

```scala
import org.apache.spark.graphframes.{examples, GraphFrame}

val g: GraphFrame = examples.Graphs.friends

val result = g.structureAwareLabelPropagation
  .maxIter(5)
  .setStructuralSimilarityMultiplier(0.5)
  .setIgnoreDirectLinks(false)
  .run()

result.select("id", "label").show()
```
</div>

</div>

### Arguments

- `max_iter`

Maximum number of propagation iterations.

- `structural_similarity_multiplier`

Non-negative factor controlling the strength of structural-overlap voting.

- `ignore_direct_links`

If `False`, direct edges contribute baseline vote mass. If `True`, only structural overlap contributes.

- `initial_label_col`

Optional vertex column used as initial labels. By default, vertex `id` is used.

- `is_directed`

Controls whether edges are treated as directed (`True`) or internally symmetrized (`False`).

- `lg_nom_entries`

Theta-sketch precision parameter used in approximate common-neighbor estimation.

- `checkpoint_interval`, `use_local_checkpoints`, `storage_level`

Runtime controls with the same meaning as in other iterative GraphFrames algorithms.

## Power Iteration Clustering (PIC)

GraphFrames provides a wrapper for the [Power Iteration Clustering](https://www.cs.cmu.edu/~frank/papers/icml2010-pic-final.pdf) algorithm from the SparkML library.

---

**NOTE**

_Be aware, that returned `DataFrame` is persistent and should be unpersisted manually after processing to avoid memory leaks!_

---

<div class="codetabs">

<div data-lang="python" markdown="1">

```python
g = GraphFrame(vertices, edges)
g.powerIterationClustering(k=2, maxIter=40, weightCol="weight")
```
</div>

<div data-lang="scala" markdown="1">

```scala
val gf = GraphFrame(vertices, edges)
val clusters = gf
  .powerIterationClustering(k = 2, maxIter = 40, weightCol = Some("weight"))
```
</div>

</div>

## Maximal Independent Set

An MIS is a set of vertices such that no two vertices in the set are adjacent (i.e., there is no edge between any two vertices in the set), and the set is maximal, meaning that adding any other vertex to the set would violate the independence property. Note that maximal independent set is not necessarily maximum independent set; that is, it ensures no more vertices can be added to the set, but does not guarantee that the set has the largest possible number of vertices among all possible independent sets in the graph. Finding a maximum independent set is NP-hard problem.

The algorithm implemented in GraphFrames is based on the paper: Ghaffari, Mohsen. "An improved distributed algorithm for maximal independent set." Proceedings of the twenty-seventh annual ACM-SIAM symposium on Discrete algorithms. Society for Industrial and Applied Mathematics, 2016. ([https://doi.org/10.1137/1.9781611974331.ch20](https://doi.org/10.1137/1.9781611974331.ch20))

Algorithm is useful, for example, if you are planning a marketing campaign and want to cover at most of users of the social network by minimizing communications. In that case, you need to find a big subset of nodes does not connected to each other but connected to other nodes of the network. It is exactly what Maximal Independent Set do. It returns a set of not connected vertices so no more vertices can be added to the set without violation of the independence.

Note: This is a randomized, non-deterministic algorithm. The result may vary between runs even if a fixed random seed is provided because how Apache Spark works.

<div class="codetabs">

<div data-lang="python" markdown="1">

```python
vertices = spark.createDataFrame(
    [(0, "a"), (1, "b"), (2, "c"), (3, "d")], ["id", "name"]
)

edges = spark.createDataFrame([(0, 1, "edge1")], ["src", "dst", "name"])

graph = GraphFrame(vertices, edges)
mis = graph.maximal_independent_set(storage_level=storage_level, seed=12345)
```
</div>

<div data-lang="scala" markdown="1">

```scala
val vertices =
  spark.createDataFrame(Seq((0L, "a"), (1L, "b"), (2L, "c"), (3L, "d"))).toDF("id", "name")

val edges = spark.createDataFrame(Seq((0L, 1L, "edge1"))).toDF("src", "dst", "name")

val graph = GraphFrame(vertices, edges)
val mis = graph.maximalIndependentSet.run(seed = 12345L)
```
</div>

</div>
