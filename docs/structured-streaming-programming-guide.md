---
layout: global
displayTitle: Structured Streaming Programming Guide [Alpha]
title: Structured Streaming Programming Guide
---

* This will become a table of contents (this text will be scraped).
{:toc}

# Overview
Structured Streaming is a scalable and fault-tolerant stream processing engine 
built on the Spark SQL engine. You can express your streaming computation by 
thinking you are running a batch computation on a static dataset, and the 
Spark SQL engine takes care of running it incrementally and continuously 
updating the final result as streaming data keeps arriving. You can use the 
[Dataset/DataFrame API](sql-programming-guide.html) in Scala, Java or Python to express streaming 
aggregations, event-time windows, stream-to-batch joins, etc. The computation 
is executed on the same optimized Spark SQL engine. Finally, the system 
ensures end-to-end exactly-once fault-tolerance guarantees through 
checkpointing and Write Ahead Logs. In short, *Stuctured Streaming provides 
fast, scalable, fault-tolerant, end-to-end exactly-once stream processing 
without the user having to reason about streaming.*

**Spark 2.0 is the ALPHA RELEASE of Structured Streaming** and the APIs are still experimental. In this guide, we are going to walk you through the programming model and the APIs. First, lets start with a simple example - a streaming word count. 

# Quick Example
Let’s say you want maintain a running word count of text data received from a data server listening on a TCP socket. Let’s see how you can express this using Structured Streaming. You can see the full code in Scala/Java/Python. And if you download Spark, you can directly run the example. In any case, let’s walk through the example step-by-step and understand how it is works. First, we have to import the names of the necessary classes and create a local SparkSession, the starting point of all functionalities related to Spark.

<div class="codetabs">
<div data-lang="scala"  markdown="1">

{% highlight scala %}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

val spark = SparkSession
  .builder
  .appName("StructuredNetworkWordCount")
  .getOrCreate()
{% endhighlight %}

</div>
<div data-lang="java"  markdown="1">

{% highlight java %}
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;

SparkSession spark = SparkSession
    .builder()
    .appName("JavaStructuredNetworkWordCount")
    .getOrCreate();
{% endhighlight %}

</div>
<div data-lang="python"  markdown="1">

{% highlight python %}
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split

spark = SparkSession\
    .builder()\
    .appName("StructuredNetworkWordCount")\
    .getOrCreate()
{% endhighlight %}

</div>
</div>

Next, let’s create a streaming DataFrame that represents text data received from a server listening on localhost:9999, and transform the DataFrame to calculate word counts.

<div class="codetabs">
<div data-lang="scala"  markdown="1">

{% highlight scala %}
val lines = spark.readStream
  .format("socket")
  .option("host", "localhost")
  .option("port", 9999)
  .load()

val words = lines.select(
  explode(
    split(lines.col("value"), " ")
  ).alias("word")
)

val wordCounts = words.groupBy("word").count()
{% endhighlight %}

</div>
<div data-lang="java"  markdown="1">

{% highlight java %}
Dataset<Row> lines = spark
 .readStream()
 .format("socket")
 .option("host", "localhost")
 .option("port", 9999)
 .load();

Dataset<Row> words = lines.select(
 functions.explode(
   functions.split(lines.col("value"), " ")
 ).alias("word")
);

Dataset<Row> wordCounts = words.groupBy("word").count();
{% endhighlight %}

</div>
<div data-lang="python"  markdown="1">

{% highlight python %}
lines = spark\
   .readStream\
   .format('socket')\
   .option('host', 'localhost')\
   .option('port', 9999)\
   .load()

words = lines.select(
   explode(
       split(lines.value, ' ')
   ).alias('word')
)

wordCounts = words.groupBy('word').count()
{% endhighlight %}

</div>
</div>

This `lines` DataFrame is like an unbounded table containing the streaming 
text data. This table contains one column of string named “value”, and each 
line in the streaming text data is like a row in this table. Note, that this 
is not currently receiving any data as we are just setting up the 
transformation, and have not yet started it. Next, we have used to built-in 
SQL functions - split and explode, to split each line into multiple rows with 
a word each. In addition, we use the function `alias` to name the new column 
as “word”. Finally, we have defined the running counts, by grouping the `words`
DataFrame by the column `word` and count on that grouping. 

We have now set up the query on the streaming data. All that is left is to 
actually start receiving data and computing the counts. To do this, we set it 
up to output the counts to the console every time they are updated. In 
addition we are also going to set up additional details like checkpoint 
location. Don’t worry about them for now, they are explained later in the guide. 

<div class="codetabs">
<div data-lang="scala"  markdown="1">

{% highlight java %}
val query = wordCounts
  .writeStream
  .outputMode("complete")
  .format("console")
  .option("checkpointLocation", checkpointDir)
  .start()

query.awaitTermination()
{% endhighlight %}

</div>
<div data-lang="java"  markdown="1">

{% highlight java %}
StreamingQuery query = wordCounts
    .writeStream()
    .outputMode("complete")
    .format("console")
    .option("checkpointLocation", checkpointDir)
    .start();

query.awaitTermination();
{% endhighlight %}

</div>
<div data-lang="python"  markdown="1">

{% highlight python %}
query = wordCounts\
    .writeStream\
    .outputMode('complete')\
    .format('console')\
    .option('checkpointLocation', checkpointDir)\
    .start()

query.awaitTermination()
{% endhighlight %}

</div>
</div>

Now the streaming computation has started in the background, and the `query` object is a handle to that active streaming query. Note that we are also waiting for the query to terminate, to prevent the process from finishing while the query is active.
To actually run this code, you can either compile your own Spark application, or simply run the example once you have downloaded Spark. We are showing the latter. You will first need to run Netcat (a small utility found in most Unix-like systems) as a data server by using

    $ nc -lk 9999

Then, in a different terminal, you can start the example by using

<div class="codetabs">
<div data-lang="scala"  markdown="1">

    $ ./bin/run-example org.apache.spark.examples.sql.streaming.StructuredNetworkWordCount

</div>
<div data-lang="java"  markdown="1">

    $ ./bin/run-example org.apache.spark.examples.sql.streaming.JavaStructuredNetworkWordCount 

</div>
<div data-lang="python"  markdown="1">
    
    $ ./bin/spark-submit examples/src/main/python/sql/streaming/structured_network_wordcount.py

</div>
</div>

Then, any lines typed in the terminal running the netcat server will be counted and printed on screen every second. It will look something like the following.

# Programming Model

The key idea is in Structured Streaming is to treat a live data stream as a 
table that is being continuously appended. This leads to a new stream 
processing model that is very similar to a batch processing model. You will 
express your streaming computation as standard batch-like query as on a static 
table, and Spark runs it as an *incremental* query on the *unbounded* input 
table. Let’s understand this model in more details.

## Basic Concepts
Consider the input data stream as the “Input Table”. Every data items that is 
arriving on the stream is like a new row being appended to the Input Table.

![Stream as a Table](img/structured-streaming-stream-as-a-table.png "Stream as a Table")

A query on the input will generate the “Result Table”. Every trigger interval (say, every 1 second), new rows gets appended to the Input Table, which eventually updates the Result Table. Whenever the result table gets updated, we would want write the changed result rows to an external sink. 

![Model](img/structured-streaming-model.png)

The “Output” is defined as what gets written out to the external storage. The output can be defined in different modes 

  - *Complete Mode* - The entire updated Result Table will be written to the external storage.

  - *Append Mode* - Only the new rows appended in the Result Table since the last trigger will be written to the external storage. This is applicable only on queries where existing rows in the Result Table is not expected to change.
  
  - *Update Mode* - Only the rows that were updated in the Result Table since the last trigger wil be written to the external storage (not available yet in Spark 2.0). Note that this is different from the Complete Mode in that this mode does not output the rows that are not changed.

Note that each mode is applicable on certain types of queries. This is discussed in detail later.

To illustrate the use of this model, let’s understand the model in context of 
the Quick Example above. The first `lines` DataFrame is the input table, and 
the final `wordCounts` DataFrame is the result table. Note that the query on 
streaming `lines` DataFrame to generate `wordCounts` is *exactly the same* as 
it would be a static DataFrame. However, when this query is started, Spark 
will continuously check for new data from the socket connection. If there is 
new data, Spark will run an “incremental” query that combines the previous 
running counts with the new data to compute updated counts, as shown below.

![Model](img/structured-streaming-example-model.png)

This model is significantly different from many other stream processing 
engines. Many streaming system require the user to maintain running 
aggregations themselves, thus having the reason about fault-tolerance, and 
data consistency (at-least-once, or at-most-once, or exactly-once). In this 
model, Spark is responsible for updating the Result Table when there is new 
data, thus relieving the users from reasoning about it. As an example, let’s 
see how this model handles event-time based processing and late arriving data.

## Handling Event-time and Late Data
Event-time is the time embedded in the data itself. For many applications, you may want to do operate using this event-time. For example, if you want to get the number of events generated by IoT devices every minute, then you probably want to use the time when the data was generated (that is, event-time in the data), rather than the time Spark receives them. This event-time is very naturally expressed in this model -- each event from the devices is a row in the table, and event-time a column value in the row. This allows window-based aggregations (e.g. number of event every minute) to be just a special type of grouping and aggregation on the even-time column -- each time window is a group and each row can belong to multiple windows/groups. Therefore, such event-time-window-based aggregation queries can be defined consistently on both a static dataset (e.g. from collected device events logs) as well as on streaming dataset, making the life of the user much easier.

Furthermore this model naturally handles data that has arrived later than expected based on its event-time. Since Spark is updating the Result Table, it has full control over updating/cleaning up the aggregates when there is late data. While not yet implemented in Spark 2.0 yet, event-time watermarking will be used to manage this data. These are explained in more details in the Window Operations section.

## Fault Tolerance Semantics
Delivering end-to-end exactly-once semantics was one of key goals behind the design of Structured Streaming. To achieve that, we have designed the Structured Streaming sources, the sinks and the execution engine to reliably track the exact progress of the processing so that it can handle any kind of failure by restarting and/or reprocessing. Every streaming source is assumed to have offsets (similar to Kafka offsets, or Kinesis sequence numbers)
for track the read position in the stream. The engine uses checkpointing and write ahead logs to record the offset range of the data being processed in each trigger. The streaming sinks are designed to be idempotent for handling reprocessing. Together, Structured Streaming can ensure **end-to-end exactly-once semantics** under any failure.

# API using Datasets and DataFrames
Since Spark 2.0, DataFrames and can represent of static, bounded data, as well as streaming, unbounded data. Similar to static Datasets/DataFrames, you can use the common entry point  SparkSession to create straming DataFrames/Datasets from streaming sources, and apply the same operations on them as static DataFrames/Datasets. If you are not familiar with Datasets/DataFrames, you are strongly advised to familiarize yourself with them using the Spark SQL Programming Guide.

## Creating streaming DataFrames and streaming Datasets
Streaming DataFrames can created through the DataStreamReader interface `SparkSession.readStream()`. Similar to the read interface for creating static DataFrame, you can specify the details of the source - data format, schema, options, etc. In Spark 2.0, there are a few built-in sources.

  - **File sources** - Reads files written in a directory as a stream of data. Supported file formats are text, csv, json, parquet. See the docs of the DataStreamReader interface for more up-to-date list, and supported options for each file format. Note that the files needs to be atomically put in the directory. In most file systems, this means that the files should be moved atomically into the directory.

  - **Socket source (for testing)** - Reads UTF8 text data from a socket connection. The connection is only on the server. Note that this should be used only for testing as this does not provide end-to-end fault-tolerance guarantees. 

Here are some examples.

<div class="codetabs">
<div data-lang="scala"  markdown="1">

{% highlight scala %}
val spark: SparkSession = … 

// Read text from socket 
val socketDF = spark
    .readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", 9999)
    .load()

socketDF.isStreaming    // Returns true for DataFrames that has a streaming source

socketDF.printSchema 

// Read all the csv files written atomically in a directory
val userSchema = new StructType().add("name", "string").add("age", "integer")
val csvDF = spark
    .readStream
    .option("sep", ";")
    .schema(userSchema)      // Specify schema of the parquet files
    .csv("/path/to/directory")    // Equivalent to format("cv").load("/path/to/directory")
{% endhighlight %}

</div>
<div data-lang="java"  markdown="1">

{% highlight java %}
SparkSession spark = ...

// Read text from socket 
Dataset[Row] socketDF = spark
    .readStream()
    .format("socket")
    .option("host", "localhost")
    .option("port", 9999)
    .load();

socketDF.isStreaming();    // Returns True for DataFrames that has a streaming source

socketDF.printSchema();

// Read all the csv files written atomically in a directory
StructType userSchema = new StructType().add("name", "string").add("age", "integer");
Dataset[Row] csvDF = spark
    .readStream()
    .option("sep", ";")
    .schema(userSchema)      // Specify schema of the parquet files
    .csv("/path/to/directory");    // Equivalent to format("cv").load("/path/to/directory")
{% endhighlight %}

</div>
<div data-lang="python"  markdown="1">

{% highlight python %}
spark = SparkSession. …. 

# Read text from socket 
socketDF = spark \
    .readStream()  \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

socketDF.isStreaming()    # Returns True for DataFrames that has a streaming source

socketDF.printSchema() 

# Read all the csv files written atomically in a directory
userSchema = StructType().add("name", "string").add("age", "integer")
csvDF = spark \
    .readStream() \
    .option("sep", ";") \
    .schema(userSchema) \
    .csv("/path/to/directory")    # Equivalent to format("cv").load("/path/to/directory")
{% endhighlight %}

</div>
</div>

These examples generate streaming DataFrames that are untyped, that is, the schema of the DataFrame is not checked at compile time, only checked in runtime when the query is submitted. Some operations like `map`, `flatMap`, etc. need the type to be known at compile time. To do those, you can convert these untyped streaming DataFrames to typed streaming Datasets using the same methods as static DataFrame. See the SQL Programming Guide for more details. Additionally, more details on the supported streaming sources are discussed later in the document.

## Operations on streaming DataFrames/Datasets
You can apply all kinds of streaming DataFrames/Datasets - ranging for untyped, SQL-like operations (e.g. select, where, groupBy), to typed RDD-like operations (e.g. map, filter, flatMap). See the SQL programming guide for more details. Let’s take a look at a few example operations that you can use.

### Basic Operations - Selection, Projection, Aggregation
Most of the common operations on DataFrame/Dataset operations are supported for streaming. The few operations that are not supported are discussed later in this section.

<div class="codetabs">
<div data-lang="scala"  markdown="1">

{% highlight scala %}
case class DeviceData(device: String, type: String, signal: Double, time: DateTime)

val df: DataFrame = … // streaming DataFrame with IOT device data with schema { device: string, type: string, signal: double, time: string }
val ds: Dataset[DeviceData] = df.as[DeviceData]    // streaming Dataset with IOT device data

// Select the devices which have signal more than 10
df.select("device").where("signal > 10")      // using untyped APIs   
ds.filter(_.signal > 10).map(_.device)         // using typed APIs

// Running count of the number of updates for each device type
df.groupBy("type").count()                          // using untyped API

// Running average signal for each device type
Import org.apache.spark.sql.expressions.scalalang.typed._
ds.groupByKey(_.type).agg(typed.avg(_.signal))    // using typed API
{% endhighlight %}

</div>
<div data-lang="java"  markdown="1">

</div>
<div data-lang="python"  markdown="1">

</div>
</div>

### Window Operations on Event Time
Aggregations over a sliding window is very easy. Applying a sliding window is simply a special type of grouping - each time window is a group and each row can belong to multiple windows/groups. Hence aggregating over sliding windows is same as aggregating over this special type of grouping. This is illustrated below. Imagine the quick example is modified and the stream contains lines along with the time when the line was generated. Lets say instead of running word counts, we want to count windows within 10 minute windows, updating every 5 minutes. The result tables would look like the following. Note the window notation 12:00 - 12:10 means data that arrived after 12:00 but before 12:10. The counts are indexed by the combined group (window, cat)

![Window Operations](img/structured-streaming-window.png)

In code, you can use windows as the following. 

<div class="codetabs">
<div data-lang="scala"  markdown="1">

{% highlight scala %}
// Number of events in every 1 minute time windows
df.groupBy(window(time, "1 minute"))
  .count()


// Average number of events for each device type in every 1 minute time windows
df.groupBy(
     "type",
     window(time, "1 minute"))
  .avg(df.col("signal"))
{% endhighlight %}

</div>
<div data-lang="java"  markdown="1">

</div>
<div data-lang="python"  markdown="1">

</div>
</div>

This windowing based on group automatically handles late arriving data. 

![Handling Late Data](img/structured-streaming-late-data.png)

### Joins Operations
Streaming DataFrames can be joined with static DataFrames to create new streaming DataFrames. Here are a few examples.

<div class="codetabs">
<div data-lang="scala"  markdown="1">

{% highlight scala %}
val staticDf = spark.read. … 
val streamingDf = spark.readStream. … 

streamingDf.join(staticDf, “type”)                     // inner equi-join with a static DF
streamingDf.join(staticDf, “type”, “right_join”)  // right outer join with a static DF  

{% endhighlight %}

</div>
<div data-lang="java"  markdown="1">

</div>
<div data-lang="python"  markdown="1">

</div>
</div>

### Unsupported Operations
However, note that all of the operations applicable on static DataFrames/Datasets are not supported in streaming DataFrames/Datasets yet. While some of these unsupported operations will be supported in future releases of Spark, there are others which are fundamentally hard to implement on streaming data efficiently. As of Spark 2.0, some of the unsupported operations are as follows

- Multiple aggregations (i.e. a chain of aggregations on a streaming DF) are not yet supported

- Limit and take first N rows are not supported

- Distinct and sorting operations are not supported

- Stream-batch outer joins are conditionally supported

    + Full outer join not allowed

    + Left outer join with a streaming DF on the left is not supported

    + Right outer join with a streaming DF on the right is not supported

- Stream-stream joins are not yet supported

In addition, there are some Dataset methods that will not work on streaming Datasets. They are actions that will immediately run queries and return results, which does not makes sense on a streaming Dataset. Rather those functionalities can be done by explicitly starting a streaming query (see the next section regarding that).

- `count()` - Cannot return a single count from a streaming Dataset. Instead, use `ds.groupBy.count()` which returns a streaming Dataset containing a running count. 

- `foreach()` - Instead use `ds.writeStream.foreach(...)` (see next section).

- `show()` - Instead use the console sink (see next section).

If you try any of these operations, you will see an AnalysisException like "operation XYZ is not supported with streaming DataFrames/Datasets".

## Starting Streaming Queries
Once you have defined the final result DataFrame/Dataset, all that is left is for you start the StreamingQuery. To do that, you have to use the `DataStreamWriter`, through you have to specify the following. 

- *Details of the output sink:* Data format, location, etc. 

- *Output mode:* Specify what gets written to the output sink.

- *Query name:* Optionally, specify a unique name of the query for identification.

- *Trigger interval:* Optionally, specify the trigger interval. If its not specified, the system will check for availability of new day as soon as the previous processing has completed. If a trigger time is missed because the previous processing has not completed, then the system will attempt to trigger at the next trigger point, not immediately after the processing has completed.

- *Checkpoint location:* For some output sinks where the end-to-end fault-tolerance can be guaranteed, specify the location where the system will write all the checkpoint information. This should be a directory in a HDFS-compatible fault-tolerant file system. The semantics of checkpointing is discussed in more detail in the next section.

#### Output Modes
There are two types of output mode currently implemented.

- **Append mode (default)** - This is the default mode, where only the new rows added to the result table since the last trigger will be outputted to the sink. This is only applicable to queries that *do not have any aggregations* (e.g. queries with only select, where, map, flatMap, filter, join, etc.).

- **Complete mode** - The whole result table will be outputted to the sink.This is only applicable to queries that *have aggregations*. 

#### Output Sinks
There are a few types of built-in output sinks.

- **File sink** - Stores the output to a directory. As of Spark 2.0, this only supports Parquet file format, and Append output mode. 

- **Foreach sink** - Runs arbitrary computation on the records in the output. See later in the section for more details.

- **Console sink (for debugging)** - Prints the output to the console/stdout every time there is a trigger. Both, Append and Complete output modes, are supported.

- **Memory sink (for debugging)** - The output is stored in memory as an in-memory table.  Both, Append and Complete output modes, are supported. Note that this requires all the data to be stored in the memory of the driver and does not scale well. Hence this should be used only for debugging.

Here is a table of all the sinks, and the corresponding settings.

<table class="table">
  <tr>
    <th>Sink</th>
    <th>Supported Output Modes</th>
    <th style="width:30%">Usage</th>
    <th>Fault-tolerant</th>
    <th>Notes</th>
  </tr>
  <tr>
    <td><b>File Sink</b><br/>(only parquet in Spark 2.0)</td>
    <td>Append</td>
    <td><pre>writeStream<br/>  .format(“parquet”)<br/>  .start()</pre></td>
    <td>Yes</td>
    <td>Supports writes to partitioned tables. Partitioning by time may be useful.</td>
  </tr>
  <tr>
    <td><b>Foreach Sink</b></td>
    <td>All modes</td>
    <td><pre>writeStream<br/>  .foreach(...)<br/>  .start()</pre></td>
    <td>Depends on ForeachWriter implementation</td>
    <td>More details in the next section</td>
  </tr>
  <tr>
    <td><b>Console Sink</b></td>
    <td>Append, Complete</td>
    <td><pre>writeStream<br/>  .format(“console”)<br/>  .start()</pre></td>
    <td>No</td>
    <td></td>
  </tr>
  <tr>
    <td><b>Memory Sink</b></td>
    <td>Append, Complete</td>
    <td><pre>writeStream<br/>  .format(“memory”)<br/>  .queryName(“table”)<br/>  .start()</pre></td>
    <td>No</td>
    <td>Saves the output data as a table, for interactive querying. Table name is the query name.</td>
  </tr> 
</table>

Finally, you have to call `start()` to actually to start the execution of the query. This returns a StreamingQuery object which is a handle to the continuously running execution. You can use this object to manage the query, which we will discuss in the next subsection. For now, let’s understand all this with a few examples.


<div class="codetabs">
<div data-lang="scala"  markdown="1">

{% highlight scala %}
// ========== DF with no aggregations ==========
val noAggDF = deviceDataDf.select("device").where("signal > 10")   

// Print new data to console
noAggDF
   .writeStream
   .format("console")
   .start()

// Write new data to Parquet files
noAggDF
   .writeStream
   .parquet("path/to/destination/directory")
   .start()
   
// ========== DF with aggregation ==========
val aggDF = df.groupBy(“device”).count()

// Print updated aggregations to console
aggDF
   .writeStream
   .outputMode("complete")
   .format("console")
   .start()

// Have all the aggregates in an in memory table 
aggDF
   .writeStream
   .queryName("aggregates")    // this query name will be the table name
   .outputMode("complete")
   .format("memory")
   .start()

spark.sql("select * from aggregates).show()   // interactively query in-memory table
{% endhighlight %}

</div>
<div data-lang="java"  markdown="1">

{% highlight java %}
// ========== DF with no aggregations ==========
Dataset[Row] noAggDF = deviceDataDf.select("device").where("signal > 10")   

// Print new data to console
noAggDF
   .writeStream()
   .format("console")
   .start();

// Write new data to Parquet files
noAggDF
   .writeStream()
   .parquet("path/to/destination/directory")
   .start();
   
// ========== DF with aggregation ==========
Dataset[Row] aggDF = df.groupBy(“device”).count();

// Print updated aggregations to console
aggDF
   .writeStream()
   .outputMode("complete")
   .format("console")
   .start();

// Have all the aggregates in an in memory table 
aggDF
   .writeStream()
   .queryName("aggregates")    // this query name will be the table name
   .outputMode("complete")
   .format("memory")
   .start();

spark.sql("select * from aggregates).show()   // interactively query in-memory table
{% endhighlight %}

</div>
<div data-lang="python"  markdown="1">

{% highlight python %}
# ========== DF with no aggregations ==========
Dataset[Row] noAggDF = deviceDataDf.select("device").where("signal > 10")   

# Print new data to console
noAggDF\
   .writeStream()\
   .format("console")\
   .start();

# Write new data to Parquet files
noAggDF\
   .writeStream()\
   .parquet("path/to/destination/directory")\
   .start();
   
# ========== DF with aggregation ==========
Dataset[Row] aggDF = df.groupBy(“device”).count();

# Print updated aggregations to console
aggDF\
   .writeStream()\
   .outputMode("complete")\
   .format("console")\
   .start();

# Have all the aggregates in an in memory table. The query name will be the table name
aggDF\
   .writeStream()\
   .queryName("aggregates")\
   .outputMode("complete")\
   .format("memory")\
   .start();

spark.sql("select * from aggregates).show()   # interactively query in-memory table
{% endhighlight %}

</div>
</div>

Using Foreach
The `foreach` operation allows arbitrary operations to be computer on the output data. As of Spark 2.0, this is available only for Scala and Java, not Python. To use this, you will have to implement the interface `ForeachWriter`, which has methods that gets called whenever there is a sequence of rows generated as output after a trigger. It is important note the following

- The writer must be serializable, as it will be serialized and sent to the executors for execution.

- All the three methods, `open`, `write` and `close` will be called on the executors.

- The writer must do all the initialization (e.g. opening connections, starting a transaction, etc.) only when the `open` method is called. Be aware that, if there is any initialization in the class as soon as the object is created, then that initialization will happen in the driver (because that is where the instance is being created), which may not be what you intend.

- `version` and `partition` are two parameter in the `open` that uniquely represents a set of rows that needs to be pushed out. `version` is monotonically increasing id that increases with every trigger. `partition` is an id that represents a partition of the output, since the output is distributed and will be processed on multiple executors.

- `open` can use the `version` and `partition` to choose whether it need to write the sequence of rows. Accordingly, it can return `true` (proceed with writing), or `false` (no need to write). If the `false` is returned, then `write` will not be called on any row. For example, after a partial failure, so partitions of the failed trigger may have already been committed to a database. Based on metadata stores in the database, the writer can identify partitions that have already been committed and 

- Whenever `open` is called, `close` will also be called (unless the JVM exits due to some error). This is true even if `open` returns false. If there is any error in processing and writing the data, `close` will be called with the error. It is the your responsibilty to clean up state (e.g. connections, transactions, etc.) that have been created in `open` such that there are no resource leaks.

## Managing Streaming Queries
The `StreamingQuery` object created when a query is started can be used to monitor and manage the query. 

<div class="codetabs">
<div data-lang="scala"  markdown="1">

{% highlight scala %}
val query = df.writeStream.format("console").start()   // get the query object

query.id          // get the unique identifier of the running query

query.name        // get the name of the auto-generated or user-specified name

query.explain()   // print detailed explanations of the query

query.stop()      // stop the query 

query.awaitTermination()   // block until query is terminated, with stop() or with error

query.exception()    // the exception if the query has been terminated with error

query.souceStatus()  // progress information about data has been read from the input sources

query.sinkStatus()   // progress information about data written to the output sink
{% endhighlight %}


</div>
<div data-lang="java"  markdown="1">

{% highlight java %}
StreamingQuery query = df.writeStream().format("console").start();   // get the query object

query.id()          // get the unique identifier of the running query

query.name()        // get the name of the auto-generated or user-specified name

query.explain()   // print detailed explanations of the query

query.stop()      // stop the query 

query.awaitTermination()   // block until query is terminated, with stop() or with error

query.exception()    // the exception if the query has been terminated with error

query.souceStatus()  // progress information about data has been read from the input sources

query.sinkStatus()   // progress information about data written to the output sink
{% endhighlight %}

</div>
<div data-lang="python"  markdown="1">

{% highlight python %}
query = df.writeStream().format("console").start()   # get the query object

query.id()          # get the unique identifier of the running query

query.name()        # get the name of the auto-generated or user-specified name

query.explain()   # print detailed explanations of the query

query.stop()      # stop the query 

query.awaitTermination()   # block until query is terminated, with stop() or with error

query.exception()    # the exception if the query has been terminated with error

query.souceStatus()  # progress information about data has been read from the input sources

query.sinkStatus()   # progress information about data written to the output sink
{% endhighlight %}

</div>
</div>

You can start any number of queries in a single SparkSession. They will all be running concurrently sharing the cluster resources. You can use `sparkSession.streams()` to get the StreamingQueryManager that can be used to manage the currently active queries.

<div class="codetabs">
<div data-lang="scala"  markdown="1">

{% highlight scala %}
val spark: SparkSession = …

spark.streams.active    // get the list of currently active streaming queries

spark.streams.get(id)   // get a query object by its unique id

spark.streams.awaitAnyTermination()   // block until any one of them terminates
{% endhighlight %}

</div>
<div data-lang="java"  markdown="1">

{% highlight java %}
SparkSession spark = …

spark.streams().active()    // get the list of currently active streaming queries

spark.streams().get(id)   // get a query object by its unique id

spark.streams().awaitAnyTermination()   // block until any one of them terminates
{% endhighlight %}

</div>
<div data-lang="python"  markdown="1">

{% highlight python %}
spark = …  # spark session

spark.streams().active()    # get the list of currently active streaming queries

spark.streams().get(id)   # get a query object by its unique id

spark.streams().awaitAnyTermination()   # block until any one of them terminates
{% endhighlight %}

</div>
</div>

Finally, for asynchronous monitoring of streaming queries, you can create and attach a StreamingQueryListener, which will give you regular callback-based updates when queries are started and terminated.

## Recovering from Failures with Checkpointing 
In case of a failure or intentional shutdown, you can recover the previous progress and state of a previous query, and continue where it left off. This is done using checkpointing and write ahead logs. You can configure query with a checkpoint location, and the query will save all the progress information (i.e. range of offsets processed in each trigger), and the running aggregates (e.g. word counts in the quick example) will be saved the checkpoint location. As of Spark 2.0, this checkpoint location has to be a path in a HDFS compatible file system, and can be set as an option in the DataStreamWriter when starting a query (see the previous section). 

<div class="codetabs">
<div data-lang="scala"  markdown="1">

{% highlight scala %}
aggDF
   .writeStream
   .outputMode("complete")
   .option(“checkpointLocation”, “path/to/HDFS/dir”)
   .format("memory")
   .start()
{% endhighlight %}

</div>
<div data-lang="java"  markdown="1">

{% highlight java %}
aggDF
   .writeStream()
   .outputMode("complete")
   .option(“checkpointLocation”, “path/to/HDFS/dir”)
   .format("memory")
   .start();
{% endhighlight %}

</div>
<div data-lang="python"  markdown="1">

{% highlight python %}
aggDF\
   .writeStream()\
   .outputMode("complete")\
   .option(“checkpointLocation”, “path/to/HDFS/dir”)\
   .format("memory")\
   .start()
{% endhighlight %}

</div>
</div>










