---
layout: global
title: State Data Source
displayTitle: State Data Source
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

Since Spark 3.2, Spark supports state data source, which reads state from Structured Streaming query.

### Read from the state

The state data source produces a DataFrame with the columns state of the streaming query is stored:

* `key`: StructType
* `value`: StructType

Both columns are nested - the columns used for grouping will belong to the `key` column in most cases.
The columns belong to the `value` column depend on the query, as Spark uses various state formats to optimize
based to the operations and aggregate functions.

Hopefully, in most cases you won't need to know about the schema and provide it explicitly. Since Spark 3.1,
Structured Streaming writes the schema of the state in checkpoint location, which is read by state data source.
You can simply query about the schema in prior to know the schema of the state to craft the query you want to
run with state data.

In some cases, the schema of state is changed on compatible way. The changes of column name or the nullability from non-nullable
to nullable are compatible for state and allowed from Spark. (Spark may change the schema of state via compatible way across versions.)
For such case, the inferred schema may not reflect the latest schema of state, though the difference is only the name or nullability.
You can simply use projection to correct, or provide the schema explicitly.

You may want to provide the schema explicitly via `.schema(manualSchema)` for some extreme cases, like schema information
is not available in checkpoint location.
Possible cases: Spark failed to write the information, or the query was only run from Spark prior to 3.1.

State data source supports the following case-insensitive options on reading:

<table class="table">
  <thead>
    <tr><th>Property Name</th><th>Default</th><th>Meaning</th></tr>
  </thead>
  <tbody>
    <tr>
      <td>checkpointLocation</td>
      <td>(required)</td>
      <td>Provide the checkpoint location on streaming query.</td>
    </tr>
    <tr>
      <td>version</td>
      <td>The latest committed batch ID.</td>
      <td>The version (batch id) to read.</td>
    </tr>
    <tr>
      <td>operatorId</td>
      <td>0</td>
      <td>The operator ID to read.</td>
    </tr>
    <tr>
      <td>storeName</td>
      <td>"default"</td>
      <td>The store name to read.</td>
    </tr>
  </tbody>
</table>

The `checkpointLocation` is only required if your query only have single state and you'd like to read
the latest value. You may want to specify other options if your query has multiple stateful operations,
or the stateful operation leverages multiple state stores (e.g. stream-stream join). You may also want to
specify the version when you want to read the old version of the state.

The code to read the state in simplest use case would follow:

<div class="codetabs">
<div data-lang="scala" markdown="1">
{% highlight scala %}

spark.read.format("state").option("checkpointLocation", "/path/to/checkpoint").load()

{% endhighlight %}
</div>

<div data-lang="java" markdown="1">
{% highlight java %}

spark.read.format("state").option("checkpointLocation", "/path/to/checkpoint").load();

{% endhighlight %}
</div>

<div data-lang="python" markdown="1">
{% highlight python %}

spark.read.format("state").option("checkpointLocation", "/path/to/checkpoint").load()

{% endhighlight %}
</div>
</div>

Once you get Dataframe, you can execute `printSchema()` and check the schema of the state,
and write your query according to the schema.

If your query uses mapGroupsWithState/flatMapGroupsWithState, you can even convert the value of the state
to the instance your query used as a state accumulator, like below:

<div class="codetabs">
<div data-lang="scala" markdown="1">
{% highlight scala %}

implicit val encoder = Encoders.product[SessionInfo]
val newDf = stateReadDf.selectExpr("value.groupState.*").as[SessionInfo]

// if you're reading the state created from Spark 2.3 (state format version 1), you'll want to do below instead
// stateReadDf.selectExpr("value.*").drop("timeoutTimestamp").as[SessionInfo]

{% endhighlight %}
</div>

<div data-lang="java" markdown="1">
{% highlight java %}

Dataset<SessionInfo> newDf = stateReadDf.selectExpr("value.groupState.*").as(Encoders.bean(SessionInfo.class));

// if you're reading the state created from Spark 2.3 (state format version 1), you'll want to do below instead
// Dataset<SessionInfo> newDf = stateReadDf.selectExpr("value.*").drop("timeoutTimestamp").as(Encoders.bean(SessionInfo.class));

{% endhighlight %}
</div>

</div>

As of Spark 3.2, state data source doesn't support writing state yet, and Spark community plans to add the feature.