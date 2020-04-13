---
layout: global
title: Join Hints
displayTitle: Join Hints
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

### Description

Join Hints allow users to suggest the join strategy that Spark should use. Prior to Spark 3.0, only the `BROADCAST` Join Hint was supported. `MERGE`, `SHUFFLE_HASH` and `SHUFFLE_REPLICATE_NL` Joint Hints support was added in 3.0. When different join strategy hints are specified on both sides of a join, Spark prioritizes hints in the following order: `BROADCAST` over `MERGE` over `SHUFFLE_HASH` over `SHUFFLE_REPLICATE_NL`. When both sides are specified with the `BROADCAST` hint or the `SHUFFLE_HASH` hint, Spark will pick the build side based on the join type and the sizes of the relations. Since a given strategy may not support all join types, Spark is not guaranteed to use the join strategy suggested by the hint.

### Join Hints Types

<dl>
  <dt><code><em>BROADCAST</em></code></dt>
  <dd>
    Suggests that Spark use broadcast join. The join side with the hint will be broadcast regardless of <code>autoBroadcastJoinThreshold</code>. If both sides of the join have the broadcast hints, the one with the smaller size (based on stats) will be broadcast. The aliases for <code>BROADCAST</code> are <code>BROADCASTJOIN</code> and <code>MAPJOIN</code>.
  </dd>
</dl>

<dl>
  <dt><code><em>MERGE</em></code></dt>
  <dd>
     Suggests that Spark use shuffle sort merge join. The aliases for <code>MERGE</code> are <code>SHUFFLE_MERGE</code> and <code>MERGEJOIN</code>.
  </dd>
</dl>

<dl>
  <dt><code><em>SHUFFLE_HASH</em></code></dt>
  <dd>
     Suggests that Spark use shuffle hash join. If both sides have the shuffle hash hints, Spark chooses the smaller side (based on stats) as the build side.
  </dd>
</dl>

<dl>
  <dt><code><em>SHUFFLE_REPLICATE_NL</em></code></dt>
  <dd>
    Suggests that Spark use shuffle-and-replicate nested loop join.
  </dd>
</dl>

### Examples

{% highlight sql %}
-- Join Hints for broadcast join
SELECT /*+ BROADCAST(t1) */ * FROM t1 INNER JOIN t2 ON t1.key = t2.key;
SELECT /*+ BROADCASTJOIN (t1) */ * FROM t1 left JOIN t2 ON t1.key = t2.key;
SELECT /*+ MAPJOIN(t2) */ * FROM t1 right JOIN t2 ON t1.key = t2.key;

-- Join Hints for shuffle sort merge join
SELECT /*+ SHUFFLE_MERGE(t1) */ * FROM t1 INNER JOIN t2 ON t1.key = t2.key;
SELECT /*+ MERGEJOIN(t2) */ * FROM t1 INNER JOIN t2 ON t1.key = t2.key;
SELECT /*+ MERGE(t1) */ * FROM t1 INNER JOIN t2 ON t1.key = t2.key;

-- Join Hints for shuffle hash join
SELECT /*+ SHUFFLE_HASH(t1) */ * FROM t1 INNER JOIN t2 ON t1.key = t2.key;

-- Join Hints for shuffle-and-replicate nested loop join
SELECT /*+ SHUFFLE_REPLICATE_NL(t1) */ * FROM t1 INNER JOIN t2 ON t1.key = t2.key;

-- When different join strategy hints are specified on both sides of a join, Spark
-- prioritizes the BROADCAST hint over the MERGE hint over the SHUFFLE_HASH hint
-- over the SHUFFLE_REPLICATE_NL hint.
-- Spark will issue Warning in the following example
-- org.apache.spark.sql.catalyst.analysis.HintErrorLogger: Hint (strategy=merge)
-- is overridden by another hint and will not take effect.
SELECT /*+ BROADCAST(t1) */ /*+ MERGE(t1, t2) */ * FROM t1 INNER JOIN t2 ON t1.key = t2.key;
{% endhighlight %}

### Related Statements

 * [JOIN](sql-ref-syntax-qry-select-join.html)
 * [SELECT](sql-ref-syntax-qry-select.html)
