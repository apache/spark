---
layout: global
displayTitle: Pregel API
title: Pregel API
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

Pregel API is one of the core backbones of GraphFrames. It is based on the implementation of the [Pregel algorithm](https://blog.lavaplanets.com/wp-content/uploads/2023/12/p135-malewicz.pdf) in terms of relational operations using the Apache Spark `DataFrame` / `Dataset` API.

---

**NOTE**

_Be aware, that returned `DataFrame` is persistent and should be unpersisted manually after processing to avoid memory leaks!_

---

## API

For the API details, please refer to:

- Scala API: [Pregel](api/scala/org/apache/spark/graphframes/lib/Pregel.html)
- Python API: [Pregel](api/python/reference/api/pyspark.graphframes.lib.Pregel.html)

## Arguments

### Message Columns

Internally, GraphFrames handles all the messages via a specific column:

```scala
val pregelMessageColumn = org.apache.spark.graphframes.lib.Pregel.msg
val pregelMessageColumnName = org.apache.spark.graphframes.lib.Pregel.MSG_COL_NAME
```

This name is applied to both raw and aggregated messages and should be used in conditions and aggregations. For example, in the simplest implementation of the PageRank algorithm it may look like this:

```scala
val aggregateMessagesExpression = sum(Pregel.msg)
val updateExpression = coalesce(Pregel.msg, lit(0.0)) * (1 - alpha) + alpha / numVertices
```

### Triplet Column

At the stage of message generation, GraphFrames internally creates triplets, that contains `StructType` columns for source vertex, edge and destination vertex. These structs contain all the columns from the original source, destination and edge, including not only initial attributes, but also Pregel columns, like activity column, all the vertex columns, etc. To access these data, user should rely on the following API:

```scala
val sourceVertexColumn1 = Pregel.src("vertexColumn1")
val sourceVertexColumn2 = Pregel.src("vertexColumn2")
val destinationVertexColumn1 = Pregel.dst("vertexColumn1")
val edgeWeight = Pregel.edge("weight")
```

Under the hood, the passed name of the column will be resolved to get the corresponding element of the triplet structs.

### Memory Optimization for Triplets

By default, all vertex columns are included when constructing triplets. For algorithms with large per-vertex state (e.g., cycle detection storing sequences, random walks), this can create huge intermediate datasets in memory.

To reduce memory usage, you can specify only the columns that are actually needed using `requiredSrcColumns`, `requiredDstColumns`, and `requiredEdgeColumns`:

<div class="codetabs">

<div data-lang="python" markdown="1">

```python
graph.pregel \
  .withVertexColumn("distances", ...) \
  .sendMsgToDst(Pregel.src("distances")) \
  .sendMsgToDst(Pregel.edge("weight")) \
  .required_src_columns("distances") \
  .required_dst_columns("distances") \
  .required_edge_columns("weight") \
  .aggMsgs(...) \
  .run()
```
</div>

<div data-lang="scala" markdown="1">

```scala
graph.pregel
  .withVertexColumn("distances", ...)
  .sendMsgToDst(Pregel.src("distances"))  // Only needs "distances" from source
  .sendMsgToDst(Pregel.edge("weight"))    // Needs "weight" from edge
  .requiredSrcColumns("distances")         // Only include "distances" in src struct
  .requiredDstColumns("distances")         // Only include "distances" in dst struct
  .requiredEdgeColumns("weight")           // Only include "weight" in edge struct
  .aggMsgs(...)
  .run()
```
</div>

</div>

The `id` column (and `src`/`dst` for edges) and the active flag column (if used) are always included automatically, so you don't need to specify them.

---

**NOTE**

_Before 0.12.0, all edge columns were always included in triplets by default. This was caused by an unspecified bug that kept and persisted all edge columns as a `StructType`, creating significant memory pressure. Starting from 0.12.0, only the source and destination ID columns from edges are included by default. If your message expressions reference edge properties (e.g., `Pregel.edge("weight")`), you must explicitly specify them using `requiredEdgeColumns`._

---

### Sending Messages

GraphFrames Pregel API support arbitrary number of messages per vertex. Inside the Pregel API **graphs are always considered directed**. This means that if a vertex has an outgoing edge to another vertex, then the message will be sent to the destination vertex. To emulate the behavior of the undirected graph, the user can send the same message to both the source and the destination vertex.

```scala
graph
  .pregel
  .sendMsgToDst(Pregel.src("vertexColumn")) // Sending the vertex column of the destination vertex to the source vertex.
  .sendMsgToSrc(Pregel.dst("vertexColumn")) // Sending the vertex column of the source vertex to the destination vertex.
  .run()
```

### Aggregation

GraphFrames Pregel API requires the user to specify the aggregation function for the messages. It is called over all the messages sent to the vertex no matter the direction.

```scala
graph.pregel.aggMsgs(sum(Pregel.msg))
```

### Termination Conditions

GraphFrames Pregel API provides the following termination conditions:

- **By a number of iterations.** Users can specify the maximum number of iterations with `setMaxIter(value: Int)`.
- **In case of no new messages are sent.** User can say GF to terminate the computations if all the messages sent on the iteration are empty (`null`). To do this, user should specify `setEarlyStopping(value: Boolean)`. **Be careful, because the checking of nullity is a not free operation, but Apache Spark action!** So, for example, if messages cannot be empty, this condition should be set to `false`. For example, in algorithms like `ShortestPaths`, this condition should be set to `true`, but for algorithms like `PageRank`, this condition should be set to `false` because the messages cannot be empty.
- **By vertex voting.** Users can specify the participation condition per vertex with `setInitialActiveVertexExpression(expression: Column)` and `setUpdateActiveVertexExpression(expression: Column)`. In the case if `setStopIfAllNonActiveVertices(value: Boolean)` is set to `true`, the computation will stop if all the vertices are inactive. This is useful for algorithms like `LabelPropagation`, when messages are always not `null`, but if no vertex changed a label on the last iteration, the computation should stop. **Be careful, because the checking of vertex status is a not free operation, but Apache Spark action!**
