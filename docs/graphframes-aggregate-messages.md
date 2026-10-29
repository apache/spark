---
layout: global
displayTitle: Message Passing
title: Message Passing
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

Like GraphX, GraphFrames provides primitives for developing graph algorithms. The two key components are:

* `aggregateMessages`: Send messages between vertices, and aggregate messages for each vertex. GraphFrames provides a native `aggregateMessages` method implemented using DataFrame operations. This may be used analogously to the GraphX API.
* joins: Join message aggregates with the original graph. GraphFrames rely on `DataFrame` joins, which provide the full functionality of GraphX joins.

---

**NOTE**

*Be aware, that returned `DataFrame` is persistent and should be unpersisted manually after processing to avoid memory leaks!*

---

The below code snippets show how to use `aggregateMessages` to compute the sum of the ages of adjacent users.

<div class="codetabs">

<div data-lang="python" markdown="1">

For API details, refer to the [aggregateMessages](api/python/reference/api/pyspark.graphframes.GraphFrame.html).

```python
from pyspark.graphframes.lib import AggregateMessages as AM
from pyspark.graphframes.examples import Graphs
from pyspark.sql.functions import sum as sqlsum


g = Graphs(spark).friends()  # Get example graph

# For each user, sum the ages of the adjacent users
msgToSrc = AM.dst["age"]
msgToDst = AM.src["age"]
agg = g.aggregateMessages(
    sqlsum(AM.msg).alias("summedAges"),
    sendToSrc=msgToSrc,
    sendToDst=msgToDst)
agg.show()
```
</div>

<div data-lang="scala" markdown="1">

For API details, refer to the [AggregateMessages](api/scala/org/apache/spark/graphframes/lib/AggregateMessages.html).

```scala
import org.apache.spark.graphframes.{examples,GraphFrame}
import org.apache.spark.graphframes.lib.AggregateMessages

val g: GraphFrame = examples.Graphs.friends  // get example graph

// We will use AggregateMessages utilities later, so name it "AM" for short.
val AM = AggregateMessages

// For each user, sum the ages of the adjacent users.
val msgToSrc = AM.dst("age")
val msgToDst = AM.src("age")
val agg = { g.aggregateMessages
  .sendToSrc(msgToSrc)  // send destination user's age to source
  .sendToDst(msgToDst)  // send source user's age to destination
  .agg(sum(AM.msg).as("summedAges")) } // sum up ages, stored in AM.msg column
agg.show()
```
</div>

</div>

For a more complex example, look at the code used to implement the [BeliefPropagation](api/python/reference/api/pyspark.graphframes.examples.BeliefPropagation.html).
