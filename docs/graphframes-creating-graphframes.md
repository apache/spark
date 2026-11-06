---
layout: global
displayTitle: Creating GraphFrames
title: Creating GraphFrames
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

Users can create GraphFrames from vertex and edge DataFrames.

* *Vertex DataFrame*: A vertex DataFrame should contain a special column named "id" which specifies unique IDs for each vertex in the graph.
* *Edge DataFrame*: An edge DataFrame should contain two special columns: "src" (source vertex ID of edge) and "dst" (destination vertex ID of edge).

Both DataFrames can have arbitrary other columns. Those columns can represent vertex and edge attributes.

A GraphFrame can also be constructed from a single DataFrame containing edge information. The vertices will be inferred from the sources and destinations of the edges.

The following example demonstrates how to create a GraphFrame from vertex and edge DataFrames.

<div class="codetabs">

<div data-lang="python" markdown="1">

```python
# Vertex DataFrame
v = spark.createDataFrame([
    ("a", "Alice", 34),
    ("b", "Bob", 36),
    ("c", "Charlie", 30),
    ("d", "David", 29),
    ("e", "Esther", 32),
    ("f", "Fanny", 36),
    ("g", "Gabby", 60)
], ["id", "name", "age"])

# Edge DataFrame
e = spark.createDataFrame([
    ("a", "b", "friend"),
    ("b", "c", "follow"),
    ("c", "b", "follow"),
    ("f", "c", "follow"),
    ("e", "f", "follow"),
    ("e", "d", "friend"),
    ("d", "a", "friend"),
    ("a", "e", "friend")
], ["src", "dst", "relationship"])
# Create a GraphFrame
g = GraphFrame(v, e)
```

The GraphFrame constructed above is available in the GraphFrames package (not available in Spark Connect):

```python
from pyspark.graphframes.examples import Graphs

g = Graphs(spark).friends()  # Get example graph
```
</div>

<div data-lang="scala" markdown="1">

```scala
import org.apache.spark.graphframes.GraphFrame

// Vertex DataFrame
val v = spark.createDataFrame(List(
  ("a", "Alice", 34),
  ("b", "Bob", 36),
  ("c", "Charlie", 30),
  ("d", "David", 29),
  ("e", "Esther", 32),
  ("f", "Fanny", 36),
  ("g", "Gabby", 60)
)).toDF("id", "name", "age")

// Edge DataFrame
val e = spark.createDataFrame(List(
  ("a", "b", "friend"),
  ("b", "c", "follow"),
  ("c", "b", "follow"),
  ("f", "c", "follow"),
  ("e", "f", "follow"),
  ("e", "d", "friend"),
  ("d", "a", "friend"),
  ("a", "e", "friend")
)).toDF("src", "dst", "relationship")
// Create a GraphFrame
val g = GraphFrame(v, e)
```

The GraphFrame constructed above is available in the GraphFrames package:

```scala
import org.apache.spark.graphframes.{examples, GraphFrame}

val g: GraphFrame = examples.Graphs.friends
```
</div>

</div>
