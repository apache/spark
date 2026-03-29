---
layout: global
displayTitle: Structured Streaming Programming Guide
title: Structured Streaming Programming Guide
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

# Overview

TransformWithState is the new arbitrary stateful operator in Structured Streaming since the Apache Spark 4.0 release. This operator is the next generation replacement for the old mapGroupsWithState/flatMapGroupsWithState API in Scala and the applyInPandasWithState API in Python for arbitrary stateful processing in Apache Spark.

This operator has support for an umbrella of features such as object-oriented stateful processor definition, composite types, automatic TTL based eviction, timers etc and can be used to build business-critical operational use-cases.

# Language Support

`TransformWithState` is available in Scala, Java and Python.

Note that in Python, there are two operators named `transformWithStateInPandas` which works with Pandas interface, and `transformWithState` which works with Row interface.

Based on popularity of Pandas and its rich set of API with vectorization, `transformWithStateInPandas` may be the preferred API for most users. The `transformWithState` API is more suitable to handle high key cardinality use case, since the cost of conversion is considerably high for Pandas API. If users aren't familiar with Pandas, Row type API might be easier to learn.

# Components of a TransformWithState Query

A transformWithState query typically consists of the following components:
- Stateful Processor - A user-defined stateful processor that defines the stateful logic
- Output Mode - Output mode for the query such as Append, Update etc
- Time Mode - Time mode for the query such as EventTime, ProcessingTime etc
- Initial State - An optional initial state batch dataframe used to pre-populate the state

In the following sections, we will go through the above components in more detail.

## Defining a Stateful Processor

A stateful processor is the core of the user-defined logic used to operate on the input events. A stateful processor is defined by extending the StatefulProcessor class and implementing a few methods.

A typical stateful processor deals with the following constructs:
- Input Records - Input records received by the stream
- State Variables - Zero or more class specific members used to store user state
- Output Records - Output records produced by the processor. Zero or more output records may be produced by the processor.

A stateful processor uses the object-oriented paradigm to define the stateful logic. The stateful logic is defined by implementing the following methods:
  - `init` - Initialize the stateful processor and define any state variables as needed
  - `handleInputRows` - Process input rows belonging to a grouping key and emit output if needed
  - `handleExpiredTimer` - Handle expired timers and emit output if needed
  - `close` - Perform any cleanup operations if needed
  - `handleInitialState` - Optionally handle the initial state batch dataframe

The methods above will be invoked by the Spark query engine when the operator is executed as part of a streaming query.

Note also that not all types of operations are supported in each of the methods. For eg, users cannot register timers in the `init` method. Similarly, they cannot operate on input rows in the `handleExpiredTimer` method. The engine will detect unsupported/incompatible operations and fail the query, if needed.

### Using the StatefulProcessorHandle

Many operations within the methods above can be performed using the `StatefulProcessorHandle` object. The `StatefulProcessorHandle` object provides methods to interact with the underlying state store. This object can be retrieved within the StatefulProcessor by invoking the `getHandle` method.

### Using State Variables

State variables are class specific members used to store user state. They need to be declared once and initialized within the `init` method of the stateful processor.

Initializing a state variable typically involves the following steps:
- Provide a unique name for the state variable (unique within the stateful processor definition)
- Provide a type for the state variable (ValueState, ListState, MapState) - depending on the type, the appropriate method on the handle needs to be invoked
- Provide a state encoder for the state variable (in Scala - this can be skipped if implicit encoders are available)
- Provide an optional TTL config for the state variable

### Types of state variables

State variables can be of the following types:
- Value State
- List State
- Map State

Similar to collections for popular programming languages, the state types could be used to model data structures optimized for various types of operations for the underlying storage layer. For example, appends are optimized for ListState and point lookups are optimized for MapState.

### Providing state encoders

State encoders are used to serialize and deserialize the state variables. In Scala, the state encoders can be skipped if implicit encoders are available. In Java and Python, the state encoders need to be provided explicitly.
Built-in encoders for primitives, case classes and Java Bean classes are provided by default via the Spark SQL encoders.

#### Providing implicit encoders in Scala

In Scala, implicit encoders can be provided for case classes and primitive types. The `implicits` object is provided as part of the `StatefulProcessor` class. Within the StatefulProcessor definition, the user can simply import implicits as `import implicits._` and then they do not require to pass the encoder type explicitly.

### Providing TTL for state variables

State variables can be configured with an optional TTL (Time-To-Live) value. The TTL value is used to automatically evict the state variable after the specified duration. The TTL value can be provided as a Duration.

### Handling input rows

The `handleInputRows` method is used to process input rows belonging to a grouping key and emit output if needed. The method is invoked by the Spark query engine for each grouping key value received by the operator. If multiple rows belong to the same grouping key, the provided iterator will include all those rows.

### Handling expired timers

Within the `handleInputRows` or `handleExpiredTimer` methods, the stateful processor can register timers to be triggered at a later time. The `handleExpiredTimer` method is invoked by the Spark query engine when a timer set by the stateful processor has expired. This method is invoked once for each expired timer.
Here are a few timer properties that are supported:
- Multiple timers associated with the same grouping key can be registered
- The engine provides the ability to list/add/remove timers as needed
- Timers are also checkpointed as part of the query checkpoint and can be triggered on query restart as well.

### Handling initial state

The `handleInitialState` method is used to optionally handle the initial state batch dataframe. The initial state batch dataframe is used to pre-populate the state for the stateful processor. The method is invoked by the Spark query engine when the initial state batch dataframe is available.
This method is only called once in the lifetime of the query. This is invoked before any input rows are processed by the stateful processor.

### Putting it all together

Here is an example of a StatefulProcessor that implements a downtime detector. Each time a new value is seen for a given key, it updates the lastSeen state value, clears any existing timers, and resets a timer for the future.

When a timer expires, the application emits the elapsed time since the last observed event for the key. It then sets a new timer to emit an update 10 seconds later.

NOTE: `python_Pandas` tab guides the implementation of StatefulProcessor for `transformWithStateInPandas`, and `python_Row` tab guides the implementation of StatefulProcessor for `transformWithState`.

<div class="codetabs">

<div data-lang="python_Pandas"  markdown="1">

{% highlight python %}

class DownTimeDetector(StatefulProcessor):
    def init(self, handle: StatefulProcessorHandle) -> None:
        # Define schema for the state value (timestamp)
        state_schema = StructType([StructField("value", TimestampType(), True)])
        self.handle = handle
        # Initialize state to store the last seen timestamp for each key
        self.last_seen = handle.getValueState("last_seen", state_schema)

    def handleExpiredTimer(self, key, timerValues, expiredTimerInfo) -> Iterator[pd.DataFrame]:
        latest_from_existing = self.last_seen.get()
        # Calculate downtime duration
        downtime_duration = timerValues.getCurrentProcessingTimeInMs() - int(latest_from_existing.timestamp() * 1000)
        # Register a new timer for 10 seconds in the future
        self.handle.registerTimer(timerValues.getCurrentProcessingTimeInMs() + 10000)
        # Yield a DataFrame with the key and downtime duration
        yield pd.DataFrame(
            {
                "id": key,
                "timeValues": str(downtime_duration),
            }
        )

    def handleInputRows(self, key, rows, timerValues) -> Iterator[pd.DataFrame]:
        # Find the row with the maximum timestamp
        max_row = max((tuple(pdf.iloc[0]) for pdf in rows), key=lambda row: row[1])

        # Get the latest timestamp from existing state or use epoch start if not exists
        if self.last_seen.exists():
            latest_from_existing = self.last_seen.get()
        else:
            latest_from_existing = datetime.fromtimestamp(0)

        # If new data is more recent than existing state
        if latest_from_existing < max_row[1]:
            # Delete all existing timers
            for timer in self.handle.listTimers():
                self.handle.deleteTimer(timer)
            # Update the last seen timestamp
            self.last_seen.update((max_row[1],))

        # Register a new timer for 5 seconds in the future
        self.handle.registerTimer(timerValues.getCurrentProcessingTimeInMs() + 5000)

        # Yield an empty DataFrame
        yield pd.DataFrame()

    def close(self) -> None:
        # No cleanup needed
        pass

{% endhighlight %}

</div>

<div data-lang="python_Row"  markdown="1">

{% highlight python %}

class DownTimeDetector(StatefulProcessor):
    def init(self, handle: StatefulProcessorHandle) -> None:
        # Define schema for the state value (timestamp)
        state_schema = StructType([StructField("value", TimestampType(), True)])
        self.handle = handle
        # Initialize state to store the last seen timestamp for each key
        self.last_seen = handle.getValueState("last_seen", state_schema)

    def handleExpiredTimer(self, key, timerValues, expiredTimerInfo) -> Iterator[Row]:
        latest_from_existing = self.last_seen.get()
        # Calculate downtime duration
        downtime_duration = timerValues.getCurrentProcessingTimeInMs() - int(latest_from_existing.timestamp() * 1000)
        # Register a new timer for 10 seconds in the future
        self.handle.registerTimer(timerValues.getCurrentProcessingTimeInMs() + 10000)
        # Yield a DataFrame with the key and downtime duration
        yield Row(id=key[0], timeValues=str(downtime_duration))

    def handleInputRows(self, key, rows, timerValues) -> Iterator[Row]:
        # Find the maximum timestamp
        max_timestamp = max(map(lambda x: x.timestamp, rows))

        # Get the latest timestamp from existing state or use epoch start if not exists
        if self.last_seen.exists():
            latest_from_existing = self.last_seen.get()
        else:
            latest_from_existing = datetime.fromtimestamp(0)

        # If new data is more recent than existing state
        if latest_from_existing < max_timestamp:
            # Delete all existing timers
            for timer in self.handle.listTimers():
                self.handle.deleteTimer(timer)
            # Update the last seen timestamp
            self.last_seen.update((max_timestamp,))

        # Register a new timer for 5 seconds in the future
        self.handle.registerTimer(timerValues.getCurrentProcessingTimeInMs() + 5000)

        return iter([])

    def close(self) -> None:
        # No cleanup needed
        pass

{% endhighlight %}

</div>

<div data-lang="scala"  markdown="1">

{% highlight scala %}

// The (String, Timestamp) schema represents an (id, time). We want to do downtime
// detection on every single unique sensor, where each sensor has a sensor ID.
class DowntimeDetector(duration: Duration) extends
  StatefulProcessor[String, (String, Timestamp), (String, Duration)] {

  @transient private var _lastSeen: ValueState[Timestamp] = _

  override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = {
    _lastSeen = getHandle.getValueState[Timestamp]("lastSeen", Encoders.TIMESTAMP, TTLConfig.NONE)
  }

  // The logic here is as follows: find the largest timestamp seen so far. Set a timer for
  // the duration later.
  override def handleInputRows(
      key: String,
      inputRows: Iterator[(String, Timestamp)],
      timerValues: TimerValues): Iterator[(String, Duration)] = {
    val latestRecordFromNewRows = inputRows.maxBy(_._2.getTime)

    val latestTimestampFromExistingRows = if (_lastSeen.exists()) {
      _lastSeen.get()
    } else {
      new Timestamp(0)
    }

    val latestTimestampFromNewRows = latestRecordFromNewRows._2

    if (latestTimestampFromNewRows.after(latestTimestampFromExistingRows)) {
      // Cancel the one existing timer, since we have a new latest timestamp.
      // We call "listTimers()" just because we don't know ahead of time what
      // the timestamp of the existing timer is.
      getHandle.listTimers().foreach(timer => getHandle.deleteTimer(timer))

      _lastSeen.update(latestTimestampFromNewRows)
      // Use timerValues to schedule a timer using processing time.
      getHandle.registerTimer(timerValues.getCurrentProcessingTimeInMs() + duration.toMillis)
    } else {
      // No new latest timestamp, so no need to update state or set a timer.
    }

    Iterator.empty
  }

  override def handleExpiredTimer(
    key: String,
    timerValues: TimerValues,
    expiredTimerInfo: ExpiredTimerInfo): Iterator[(String, Duration)] = {
      val latestTimestamp = _lastSeen.get()
      val downtimeDuration = new Duration(
        timerValues.getCurrentProcessingTimeInMs() - latestTimestamp.getTime)

      // Register another timer that will fire in 10 seconds.
      // Timers can be registered anywhere but init()
      getHandle.registerTimer(timerValues.getCurrentProcessingTimeInMs() + 10000)

      Iterator((key, downtimeDuration))
  }
}

{% endhighlight %}

</div>

</div>

### Using the StatefulProcessor in a streaming query

Now that we have defined the `StatefulProcessor`, we can use it in a streaming query. The following code snippets show how to use the `StatefulProcessor` in a streaming query in Python and Scala.

<div class="codetabs">
<div data-lang="python_Pandas"  markdown="1">

{% highlight python %}

q = (df.groupBy("key")
  .transformWithStateInPandas(
    statefulProcessor=DownTimeDetector(),
    outputStructType=output_schema,
    outputMode="Update",
    timeMode="None",
  )
  .writeStream...

{% endhighlight %}
</div>

<div data-lang="python_Row"  markdown="1">

{% highlight python %}

q = (df.groupBy("key")
  .transformWithState(
    statefulProcessor=DownTimeDetector(),
    outputStructType=output_schema,
    outputMode="Update",
    timeMode="None",
  )
  .writeStream...
  
{% endhighlight %}
</div>

<div data-lang="scala"  markdown="1">

{% highlight scala %}
val query = df.groupBy("key")
  .transformWithState(
    statefulProcessor = new DownTimeDetector(),
    outputMode = OutputMode.Update,
    timeMode = TimeMode.None)
  .writeStream...
{% endhighlight %}
</div>
</div>

## State Schema Evolution

TransformWithState also allows for performing schema evolution of the managed state. There are 2 parts here:
- evolution across state variables
- evolution within a state variable

Note that schema evolution is only supported on the value side. Key side state schema evolution is not supported.

### Evolution across state variables

This operator allows for state variables to be added and removed across different runs of the same streaming query. In order to remove a variable, we also need to inform the engine so that the underlying state can be purged. Users can achieve this by invoking the `deleteIfExists` method for a given state variable within the `init` method of the StatefulProcessor.

### Evolution within a state variable

This operator also allows for the state schema of a specific state variable to also be evolved. For example, if you are using a case class to store the state within a `ValueState` variable, then it's possible for you to evolve this case class by adding/removing/widening fields.
We support such schema evolution only when the underlying encoding format is set to `Avro`. In order to enable this, please set the following Spark config as `spark.conf.set("spark.sql.streaming.stateStore.encodingFormat", "avro")`.

The following evolution operations are supported within Avro rules:
- Adding a new field
- Removing a field
- Type widening
- Reordering fields

The following evolution operations are not supported:
- Renaming a field
- Type narrowing

## Integration with State Data Source

TransformWithState is a stateful operator that allows users to maintain arbitrary state across batches. In order to read this state, the user needs to provide some additional options in the state data source reader query.
This operator allows for multiple state variables to be used within the same query. However, because they could be of different composite types and encoding formats, they need to be read within a batch query one variable at a time.
In order to allow this, the user needs to specify the `stateVarName` for the state variable they are interested in reading.

Timers can read by setting the option `readRegisteredTimers` to true. This will return all the registered timer across grouping keys.

We also allow for composite type variables to be read in 2 formats:
- Flattened: This is the default format where the composite types are flattened out into individual columns.
- Non-flattened: This is where the composite types are returned as a single column of Array or Map type in Spark SQL.

Depending on your memory requirements, you can choose the format that best suits your use case.
More information about source options can be found [here](./structured-streaming-state-data-source.html).

## Testing StatefulProcessor with TwsTester

It may be useful to test your implementation of `StatefulProcessor` without having to run a 
streaming query. For this, use a test helper called TwsTester (
[org.apache.spark.sql.streaming.TwsTester](https://github.com/apache/spark/blob/master/sql/core/src/main/scala/org/apache/spark/sql/streaming/TwsTester.scala)
for Scala/Java or
[pyspark.sql.streaming.tws_tester.TwsTester](https://github.com/apache/spark/blob/master/python/pyspark/sql/streaming/tws_tester.py) 
for Python).

TwsTester is a wrapper around your StatefulProcessor that allows you to supply input data and 
observe what rows would be produced by the TransformWithState operator. 

Additionally, TwsTester supports the following functionality:
* Passing initial state - to test your implementation of `handleInitialState`.
* Observing and updating state.
* Manually advancing time to test timers (in ProcessingTime and EventTime modes). This can be used
    to test your implementation of `handleExpiredTimer`.

### Limitations of TwsTester

TwsTester is designed to test your implementation of StatefulProcessor rather than Spark's 
implementation of TransformWithState. It's recommended for prototyping or writing unit 
tests, but not for end-to-end testing. These are features of real TransformWithState that TwsTester
does not model:
* TTL (Time-To-Live, evicting old states).
* Automatic watermark advancement. In EventTime mode, the watermark is computed for each batch based
  on the event time column. It is used to expire timers and is passed to the StatefulProcessor. With
  TwsTester, this is not happening and you must advance the watermark manually.


### TwsTester usage examples

The code below shows examples of using TwsTester in Scala and Python. It uses some example stateful
processors. You can find these processors 
[here](https://github.com/apache/spark/blob/master/sql/core/src/test/scala/org/apache/spark/sql/streaming/TestProcessors.scala) 
for Scala and 
[here](https://github.com/apache/spark/blob/master/python/pyspark/sql/tests/pandas/helper/helper_pandas_transform_with_state.py) 
for Python.


<div class="codetabs">
<div data-lang="python"  markdown="1">

{% highlight python %}

from pyspark.sql.streaming import TwsTester
from pyspark.sql.types import Row
import pandas as pd

# Example 1: Basic usage with Row mode - testing input and output.
processor = RunningCountStatefulProcessor(use_pandas=False)
tester = TwsTester(processor)
result = tester.test("key1", [Row(value="a"), Row(value="b")])
assert result == [Row(key="key1", count=2)]

# Example 2: Basic usage with Pandas mode - testing input and output.
processor = RunningCountStatefulProcessor(use_pandas=True)
tester = TwsTester(processor)
input_df = pd.DataFrame({"value": ["a", "b"]})
result_df = tester.testInPandas("key1", input_df)
assert result_df["key"].tolist() == ["key1"]
assert result_df["count"].tolist() == [2]

# Example 3: Testing with initial state (Row mode).
processor = RunningCountStatefulProcessor(use_pandas=False)
tester = TwsTester(processor, initialStateRow=[
    ("a", Row(initial_count=10)),
    ("b", Row(initial_count=20))
])
result = tester.test("a", [Row(value="x")])
assert result == [Row(key="a", count=11)]

# Example 4: Testing with initial state (Pandas mode).
processor = RunningCountStatefulProcessor(use_pandas=True)
tester = TwsTester(processor, initialStatePandas=[
    ("a", pd.DataFrame({"initial_count": [10]})),
    ("b", pd.DataFrame({"initial_count": [20]}))
])
result_df = tester.testInPandas("a", pd.DataFrame({"value": ["x"]}))
assert result_df.set_index("key")["count"]["a"] == 11

# Example 5: Inspecting ValueState.
processor = RunningCountStatefulProcessor(use_pandas=False)
tester = TwsTester(processor)
tester.test("key1", [Row(value="a"), Row(value="b")])
assert tester.peekValueState("count", "key1") == (2,)
assert tester.peekValueState("count", "key3") is None

# Example 6: Manipulating ValueState directly.
processor = RunningCountStatefulProcessor(use_pandas=False)
tester = TwsTester(processor)
tester.updateValueState("count", "foo", (100,))
tester.test("foo", [Row(value="a")])
assert tester.peekValueState("count", "foo") == (101,)

# Example 7: Inspecting and manipulating ListState.
processor = TopKProcessor(k=3, use_pandas=False)
tester = TwsTester(processor)
tester.updateListState("topK", "key1", [(10.0,), (5.0,)])
tester.test("key1", [Row(score=7.0)])
assert tester.peekListState("topK", "key1") == [(10.0,), (7.0,), (5.0,)]

# Example 8: Inspecting and manipulating MapState.
processor = RowWordFrequencyProcessor()
tester = TwsTester(processor)
tester.updateMapState("frequencies", "user1", {("hello",): (5,), ("world",): (3,)})
tester.test("user1", [Row(word="hello"), Row(word="spark")])
state = tester.peekMapState("frequencies", "user1")
assert state[("hello",)] == (6,)
assert state[("spark",)] == (1,)

{% endhighlight %}
</div>


<div data-lang="scala"  markdown="1">

{% highlight scala %}

import org.apache.spark.sql.streaming.{TwsTester, TimeMode}

// Example 1: Basic usage - testing input and output.
val processor = new RunningCountProcessor[String]()
val tester = new TwsTester[String, String, (String, Long)](processor)
val ans = tester.test("key1", List("a", "b"))
assert(ans == List(("key1", 2)))

// Example 2: Testing with initial state.
val processor = new RunningCountProcessor[String]()
val tester = new TwsTester(processor, initialState = List(("a", 10L), ("b", 20L)))
val ans = tester.test("a", List("x"))
assert(ans == List(("a", 11L)))

// Example 3: Inspecting ValueState.
val tester = new TwsTester(new RunningCountProcessor[String]())
tester.test("key1", List("a"))
assert(tester.peekValueState[Long]("count", "key1").get == 1L)
assert(tester.peekValueState[Long]("count", "key2").isEmpty)

// Example 4: Manipulating ValueState directly.
val tester = new TwsTester(new RunningCountProcessor[String]())
tester.updateValueState[Long]("count", "foo", 100)
tester.test("foo", List("a"))
assert(tester.peekValueState[Long]("count", "foo").get == 101L)

// Example 5: Inspecting and manipulating ListState.
val tester = new TwsTester(new TopKProcessor(3))
tester.updateListState("topK", "key1", List(10.0, 5.0))
val ans = tester.test("key1", List(("item", 7.0)))
assert(ans == List(("key1", 10.0), ("key1", 7.0), ("key1", 5.0)))
assert(tester.peekListState[Double]("topK", "key1") == List(10.0, 7.0, 5.0))

// Example 6: Inspecting and manipulating MapState.
val tester = new TwsTester(new WordFrequencyProcessor())
tester.updateMapState("frequencies", "user1", Map("hello" -> 5L, "world" -> 3L))
tester.test("user1", List(("", "hello"), ("", "spark")))
val state = tester.peekMapState[String, Long]("frequencies", "user1")
assert(state == Map("hello" -> 6L, "world" -> 3L, "spark" -> 1L))

{% endhighlight %}
</div>
</div>



### TwsTester example: step function

If you want to test how your StatefulProcessor updates state after processing a single row, you 
might write a simple helper for this:

<div class="codetabs">
<div data-lang="python"  markdown="1">

{% highlight python %}

processor = RunningCountStatefulProcessor(use_pandas=False)
tester = TwsTester(processor)

def step_function(key: str, input_row: str, state_in: int) -> int:
    tester.updateValueState("count", key, (state_in,))
    tester.test(key, [Row(value=input_row)])
    return tester.peekValueState("count", key)[0]

assert step_function("key1", "a", 10) == 11

{% endhighlight %}
</div>


<div data-lang="scala"  markdown="1">
{% highlight scala %}
val tester = new TwsTester(new RunningCountProcessor[String]())

def testStepFunction(key: String, input: String, stateIn: Long): Long = {
  tester.updateValueState[Long]("count", key, stateIn)
  tester.test(key, List(input))
  tester.peekValueState[Long]("count", key).get
}

assert(testStepFunction("k", "x", 10L) == 11L)
{% endhighlight %}
</div>
</div>



### TwsTester example: row-by-row processing

`TwsTester.test` takes multiple rows for a single key to match the API of `handleInputRows`.
If you have a list of rows and you want them to be processed one by one (so that `handleInputRows` 
always receives exactly one row), you can do that with a helper like this:


<div class="codetabs">
<div data-lang="python"  markdown="1">

{% highlight python %}

processor = RunningCountStatefulProcessor(use_pandas=False)
tester = TwsTester(processor)
def test_row_by_row(input_rows):
    return [output for row in input_rows for output in tester.test(row["key"], [row])]
output = test_row_by_row([Row(key="k1", value="a"), Row(key="k2", value="b"), Row(key="k1", value="c")])
assert output == [Row(key="k1", count=1), Row(key="k2", count=1), Row(key="k1", count=2)]

{% endhighlight %}
</div>


<div data-lang="scala"  markdown="1">

{% highlight scala %}

val tester = new TwsTester(new RunningCountProcessor[String]())
def testRowByRow(input: List[(String, String)]): List[(String, Long)] = {
  input.flatMap(row => tester.test(row._1, List(row._2)))
}
val output = testRowByRow(List(("k1", "a"), ("k2", "b"), ("k1", "c")))
assert(output == List(("k1", 1L), ("k2", 1L), ("k1", 2L)))

{% endhighlight %}
</div>
</div>


### TwsTester example: batch simulation

If you have a list of rows with different keys and you want to simulate how they will be grouped by 
key and then passed to your StatefulProcessor, you can write a helper for that like this:

<div class="codetabs">
<div data-lang="python"  markdown="1">

{% highlight python %}

from itertools import groupby
processor = RunningCountStatefulProcessor(use_pandas=False)
tester = TwsTester(processor)

def testBatch(input: list[Row], key_column_name: str = "key") -> list[Row]:
    result: list[Row] = []
    sorted_input = sorted(input, key=lambda row: row[key_column_name])
    for key, rows in groupby(sorted_input, key=lambda row: row[key_column_name]):
        result += tester.test(key, list(rows))
    return result

batch1 = testBatch([Row(key="key1", value="a"), Row(key="key2", value="b"), Row(key="key1", value="c")])
assert batch1 == [Row(key="key1", count=2), Row(key="key2", count=1)]
batch2 = testBatch([Row(key="key1", value="c"), Row(key="key1", value="d")])
assert batch2 == [Row(key="key1", count=4)]

{% endhighlight %}
</div>


<div data-lang="scala"  markdown="1">

{% highlight scala %}

val tester = new TwsTester(new RunningCountProcessor[String]())

def testBatch(input: List[(String, String)]): List[(String, Long)] =
  input.groupBy(_._1).toList.flatMap { case (key, pairs) => tester.test(key, pairs.map(_._2)) }

val batch1 = testBatch(List(("key1", "a"), ("key2", "b")))
assert(batch1.sorted == List(("key1", 1L), ("key2", 1L)))
val batch2 = testBatch(List(("key1", "c"), ("key1", "d")))
assert(batch2 == List(("key1", 3L)))

{% endhighlight %}
</div>
</div>


### TwsTester example: timers

To test timers, create the TwsTester with `timeMode` set to `ProcessingTime` or `EventTime`. Use 
`setProcessingTime` or `setWatermark` to advance time and fire expired timers.

<div class="codetabs">
<div data-lang="python"  markdown="1">

{% highlight python %}

processor = SessionTimeoutProcessor(use_pandas=False)
tester = TwsTester(processor, timeMode="ProcessingTime")

# Process input - registers a timer at t=10000.
result1 = tester.test("key1", [Row(value="hello")])
assert result1 == [Row(key="key1", result="received:hello")]

# Advance time to 5000 - timer should NOT fire yet.
expired1 = tester.setProcessingTime(5000)
assert expired1 == []

# Advance time to 11000 - timer fires, handleExpiredTimer is called.
expired2 = tester.setProcessingTime(11000)
assert expired2 == [Row(key="key1", result="session-expired")]

{% endhighlight %}
</div>


<div data-lang="scala"  markdown="1">
{% highlight scala %}
val tester = new TwsTester(
  new SessionTimeoutProcessor(),
  timeMode = TimeMode.ProcessingTime()
)

// Process input - registers a timer at t=10000.
val result1 = tester.test("key1", List("hello"))
assert(result1 == List(("key1", "received:hello")))

// Advance time to 5000 - timer should NOT fire yet.
val expired1 = tester.setProcessingTime(5000)
assert(expired1.isEmpty)

// Advance time to 11000 - timer fires, handleExpiredTimer is called.
val expired2 = tester.setProcessingTime(11000)
assert(expired2 == List(("key1", "session-expired@10000")))
{% endhighlight %}
</div>
</div>
