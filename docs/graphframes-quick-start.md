---
layout: global
displayTitle: GraphFrames Quick Start
title: GraphFrames Quick Start
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

This quick-start guide shows how to get started using GraphFrames. After you work through this guide, move on to
the [User Guide](graphframes-creating-graphframes.html) to learn more about the many queries and algorithms
supported by GraphFrames.

The following example shows how to create a GraphFrame, query it, and run the PageRank algorithm.

<div class="codetabs">

<div data-lang="python" markdown="1">

```python
# Create a Vertex DataFrame with unique ID column "id"
v = spark.createDataFrame([
    ("a", "Alice", 34),
    ("b", "Bob", 36),
    ("c", "Charlie", 30),
], ["id", "name", "age"])

# Create an Edge DataFrame with "src" and "dst" columns
e = spark.createDataFrame([
    ("a", "b", "friend"),
    ("b", "c", "follow"),
    ("c", "b", "follow"),
], ["src", "dst", "relationship"])
# Create a GraphFrame
from pyspark.graphframes import *

g = GraphFrame(v, e)

# Query: Get in-degree of each vertex.
g.inDegrees.show()

# Query: Count the number of "follow" connections in the graph.
g.edges.filter("relationship = 'follow'").count()

# Run PageRank algorithm, and show results.
results = g.pageRank(resetProbability=0.01, maxIter=20)
results.vertices.select("id", "pagerank").show()
```
</div>

<div data-lang="scala" markdown="1">

```scala
// import GraphFrames package

import org.apache.spark.graphframes._

// Create a Vertex DataFrame with unique ID column "id"
val v = spark.createDataFrame(List(
  ("a", "Alice", 34),
  ("b", "Bob", 36),
  ("c", "Charlie", 30)
)).toDF("id", "name", "age")

// Create an Edge DataFrame with "src" and "dst" columns
val e = spark.createDataFrame(List(
  ("a", "b", "friend"),
  ("b", "c", "follow"),
  ("c", "b", "follow")
)).toDF("src", "dst", "relationship")
// Create a GraphFrame

import org.apache.spark.graphframes.GraphFrame

val g = GraphFrame(v, e)

// Query: Get in-degree of each vertex.
g.inDegrees.show()

// Query: Count the number of "follow" connections in the graph.
g.edges.filter("relationship = 'follow'").count()

// Run PageRank algorithm, and show results.
val results = g.pageRank.resetProbability(0.01).maxIter(20).run()
results.vertices.select("id", "pagerank").show()
```
</div>

</div>

## Graph Algorithms

Historically, Apache Spark shipped a built-in graph processing module named `GraphX`, based on RDDs
(the pre-Spark 2.x way of doing things). GraphX provides a set of graph algorithms, like `PageRank`,
`LabelPropagation`, etc. GraphFrames instead represents graphs using Spark's `Dataset` / `DataFrame`
API. It also provides a set of standard graph algorithms, and this set is growing. For algorithms
implemented in `GraphX` but currently not supported natively in `GraphFrames`, the library also
provides conversion methods (see [GraphX Conversions](graphframes-graphx-conversion.html)). The
following table shows the currently supported algorithms:

| Algorithm                      | GraphX (GX) | GraphFrames (GF) | Recommendations                                 |
| ------------------------------ | ----------- | ---------------- | ----------------------------------------------- |
| BFS                            | Yes         | Yes              | GF provides smoother API                        |
| Connected Components           | Yes         | Yes              | For small graphs and streaming GX, otherwise GF |
| Strongly Connected Components  | Yes         | No               | GX                                              |
| Label Propagation Algorithm    | Yes         | Yes              | GF is faster on both small and big graphs       |
| PageRank                       | Yes         | No               | GX                                              |
| Parallel Personalized PageRank | Yes         | No               | GX                                              |
| Shortest Paths                 | Yes         | Yes              | For small graphs and streaming GX, otherwise GF |
| Triangle Count                 | Yes         | Yes              | GF provides smoother API                        |
| SVD++                          | Yes         | No               | GX                                              |
| Cycles Detection               | No          | Yes              | GF                                              |
| K-Core                         | No          | Yes              | GF                                              |
| Maximal Independent Set        | No          | Yes              | GF                                              |
| Approximate Neighbor Functions | No          | Yes              | GF                                              |
| All Simple Paths               | No          | Yes              | GF                                              |
| Random Walk Embeddings         | No          | Yes              | GF: Word2Vec if \|V\| < 15M or Hash2Vec         |
