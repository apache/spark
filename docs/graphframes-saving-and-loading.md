---
layout: global
displayTitle: Saving and Loading
title: Saving and Loading
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

At the moment, GraphFrames does not support out-of-the-box saving and loading, and one needs to use Spark SQL to save and load `vertices` and `edges` as DataFrames. Refer to the [Spark SQL User Guide on datasources](sql-data-sources.html) for more details.

**NOTE:** Maintainers of `GraphFrames` are working at the moment on adding support for saving and loading GraphFrames into various formats, like GraphML, Apache GraphAr (incubating), and others.

The below example shows how to save and then load a graph as parquet files for vertices and edges.

<div class="codetabs">

<div data-lang="python" markdown="1">

```python
from pyspark.graphframes.examples import Graphs


g = Graphs(spark).friends()  # Get example graph

# Save vertices and edges as Parquet to some location
g.vertices.write.parquet("hdfs://myLocation/vertices")
g.edges.write.parquet("hdfs://myLocation/edges")

# Load the vertices and edges back
sameV = spark.read.parquet("hdfs://myLocation/vertices")
sameE = spark.read.parquet("hdfs://myLocation/edges")

# Create an identical GraphFrame
sameG = GraphFrame(sameV, sameE)
```
</div>

<div data-lang="scala" markdown="1">

```scala
import org.apache.spark.graphframes.{examples,GraphFrame}

val g: GraphFrame = examples.Graphs.friends  // get example graph

// Save vertices and edges as Parquet to some location.
g.vertices.write.parquet("hdfs://myLocation/vertices")
g.edges.write.parquet("hdfs://myLocation/edges")

// Load the vertices and edges back.
val sameV = spark.read.parquet("hdfs://myLocation/vertices")
val sameE = spark.read.parquet("hdfs://myLocation/edges")

// Create an identical GraphFrame.
val sameG = GraphFrame(sameV, sameE)
```
</div>

</div>
