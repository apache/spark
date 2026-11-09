---
layout: global
displayTitle: Subgraphs
title: Subgraphs
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

In GraphX, the `subgraph()` method takes an edge triplet (edge, src vertex, and dst vertex, plus attributes) and allows the user to select a subgraph based on triplet and vertex filters.

GraphFrames provide an even more powerful way to select subgraphs based on a combination of motif finding and DataFrame filters. We provide three helper methods for subgraph selection. `filterVertices(condition)`, `filterEdges(condition)`, and `dropIsolatedVertices()`.

## Simple subgraph
The following example shows how to select a subgraph based upon vertex and edge filters.

<div class="codetabs">

<div data-lang="python" markdown="1">

```python
from pyspark.graphframes.examples import Graphs

g = Graphs(spark).friends()  # Get example graph

# Select subgraph of users older than 30, and relationships of type "friend"
# Drop isolated vertices (users) which are not contained in any edges (relationships)
g1 = g.filterVertices("age > 30").filterEdges("relationship = 'friend'").dropIsolatedVertices()
```
</div>

<div data-lang="scala" markdown="1">

```scala
import org.apache.spark.graphframes.{examples,GraphFrame}

val g: GraphFrame = examples.Graphs.friends

// Select subgraph of users older than 30, and relationships of type "friend".
// Drop isolated vertices (users) which are not contained in any edges (relationships).
val g1 = g.filterVertices("age > 30").filterEdges("relationship = 'friend'").dropIsolatedVertices()
```
</div>

</div>

## Complex subgraph: triplet filters

The following example shows how to select a subgraph based upon triplet filters which operate on an edge and its src and dst vertices.  This example could be extended to go beyond triplets by using more complex motifs.

<div class="codetabs">

<div data-lang="python" markdown="1">

```python
from pyspark.graphframes.examples import Graphs

g = Graphs(spark).friends()  # Get example graph

# Select subgraph based on edges "e" of type "follow"
# pointing from a younger user "a" to an older user "b"
paths = g.find("(a)-[e]->(b)")\
  .filter("e.relationship = 'follow'")\
  .filter("a.age < b.age")

# "paths" contains vertex info. Extract the edges
e2 = paths.select("e.src", "e.dst", "e.relationship")

# Construct the subgraph
g2 = GraphFrame(g.vertices, e2)
```
</div>

<div data-lang="scala" markdown="1">

```scala
import org.apache.spark.graphframes.{examples,GraphFrame}

val g: GraphFrame = examples.Graphs.friends  // get example graph

// Select subgraph based on edges "e" of type "follow"
// pointing from a younger user "a" to an older user "b".
val paths = { g.find("(a)-[e]->(b)")
  .filter("e.relationship = 'follow'")
  .filter("a.age < b.age") }
// "paths" contains vertex info. Extract the edges.
val e2 = paths.select("e.*")

// Construct the subgraph
val g2 = GraphFrame(g.vertices, e2)
```
</div>

</div>

## Property Graphs

For more advanced subgraph selection, see the [Property Graphs](graphframes-property-graphs.html) section.
