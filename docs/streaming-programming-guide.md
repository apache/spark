---
layout: global
title: Streaming (Alpha) Programming Guide
---
# Initializing Spark Streaming
The first thing a Spark Streaming program must do is create a `StreamingContext` object, which tells Spark how to access a cluster. A `StreamingContext` can be created from an existing `SparkContext`, or directly:

{% highlight scala %}
new StreamingContext(master, jobName, [sparkHome], [jars])
new StreamingContext(sparkContext)
{% endhighlight %}

Once a context is instantiated, the batch interval must be set:

{% highlight scala %}
context.setBatchDuration(Milliseconds(2000))
{% endhighlight %}


# DStreams - Discretized Streams
The primary abstraction in Spark Streaming is a DStream. A DStream represents distributed collection which is computed periodically according to a specified batch interval. DStream's can be chained together to create complex chains of transformation on streaming data. DStreams can be created by operating on existing DStreams or from an input source. To creating DStreams from an input source, use the StreamingContext:

{% highlight scala %}
context.neworkStream(host, port) // A stream that reads from a socket
context.flumeStream(hosts, ports) // A stream populated by a Flume flow
{% endhighlight %}

# DStream Operators
Once an input stream has been created, you can transform it using _stream operators_. Most of these operators return new DStreams which you can further transform. Eventually, you'll need to call an _output operator_, which forces evaluation of the stream by writing data out to an external source.

## Transformations

DStreams support many of the transformations available on normal Spark RDD's:

<table class="table">
<tr><th style="width:25%">Transformation</th><th>Meaning</th></tr>
<tr>
  <td> <b>map</b>(<i>func</i>) </td>
  <td> Return a new stream formed by passing each element of the source through a function <i>func</i>. </td>
</tr>
<tr>
  <td> <b>filter</b>(<i>func</i>) </td>
  <td> Return a new stream formed by selecting those elements of the source on which <i>func</i> returns true. </td>
</tr>
<tr>
  <td> <b>flatMap</b>(<i>func</i>) </td>
  <td> Similar to map, but each input item can be mapped to 0 or more output items (so <i>func</i> should return a Seq rather than a single item). </td>
</tr>
<tr>
  <td> <b>mapPartitions</b>(<i>func</i>) </td>
  <td> Similar to map, but runs separately on each partition (block) of the DStream, so <i>func</i> must be of type
    Iterator[T] => Iterator[U] when running on an DStream of type T. </td>
</tr>
<tr>
  <td> <b>union</b>(<i>otherStream</i>) </td>
  <td> Return a new stream that contains the union of the elements in the source stream and the argument. </td>
</tr>
<tr>
  <td> <b>groupByKey</b>([<i>numTasks</i>]) </td>
  <td> When called on a stream of (K, V) pairs, returns a stream of (K, Seq[V]) pairs. <br />
<b>Note:</b> By default, this uses only 8 parallel tasks to do the grouping. You can pass an optional <code>numTasks</code> argument to set a different number of tasks.
</td>
</tr>
<tr>
  <td> <b>reduceByKey</b>(<i>func</i>, [<i>numTasks</i>]) </td>
  <td> When called on a stream of (K, V) pairs, returns a stream of (K, V) pairs where the values for each key are aggregated using the given reduce function. Like in <code>groupByKey</code>, the number of reduce tasks is configurable through an optional second argument. </td>
</tr>
<tr>
  <td> <b>join</b>(<i>otherStream</i>, [<i>numTasks</i>]) </td>
  <td> When called on streams of type (K, V) and (K, W), returns a stream of (K, (V, W)) pairs with all pairs of elements for each key. </td>
</tr>
<tr>
  <td> <b>cogroup</b>(<i>otherStream</i>, [<i>numTasks</i>]) </td>
  <td> When called on streams of type (K, V) and (K, W), returns a stream of (K, Seq[V], Seq[W]) tuples. This operation is also called <code>groupWith</code>. </td>
</tr>
</table>

DStreams also support the following additional transformations:

<table class="table">
<tr>
  <td> <b>reduce</b>(<i>func</i>) </td>
  <td> Create a new single-element stream by aggregating the elements of the stream using a function func (which takes two arguments and returns one). The function should be associative so that it can be computed correctly in parallel. </td>
</tr>
</table>


## Windowed Transformations
Spark streaming features windowed computations, which allow you to report statistics over a sliding window of data. All window functions take a <i>windowTime</i>, which represents the width of the window and a <i>slideTime</i>, which represents the frequency during which the window is calculated. 

<table class="table">
<tr><th style="width:25%">Transformation</th><th>Meaning</th></tr>
<tr>
  <td> <b>window</b>(<i>windowTime</i>, </i>slideTime</i>) </td>
  <td> Return a new stream which is computed based on windowed batches of the source stream. <i>windowTime</i> is the width of the window and <i>slideTime</i> is the frequency during which the window is calculated. Both times must be multiples of the batch interval.
  </td>
</tr>
<tr>
  <td> <b>countByWindow</b>(<i>windowTime</i>, </i>slideTime</i>) </td>
  <td> Return a sliding count of elements in the stream. <i>windowTime</i> and <i>slideTime</i> are exactly as defined in <code>window()</code>.
  </td>
</tr>
<tr>
  <td> <b>reduceByWindow</b>(<i>func</i>, <i>windowTime</i>, </i>slideTime</i>) </td>
  <td> Return a new single-element stream, created by aggregating elements in the stream over a sliding interval using <i>func</i>. The function should be associative so that it can be computed correctly in parallel. <i>windowTime</i> and <i>slideTime</i> are exactly as defined in <code>window()</code>.
  </td>
</tr>
<tr>
  <td> <b>groupByKeyAndWindow</b>(windowTime, slideTime, [<i>numTasks</i>]) 
  </td>
  <td> When called on a stream of (K, V) pairs, returns a stream of (K, Seq[V]) pairs over a sliding window. <br />
<b>Note:</b> By default, this uses only 8 parallel tasks to do the grouping. You can pass an optional <code>numTasks</code> argument to set a different number of tasks. <i>windowTime</i> and <i>slideTime</i> are exactly as defined in <code>window()</code>.
</td>
</tr>
<tr>
  <td> <b>reduceByKeyAndWindow</b>(<i>func</i>, [<i>numTasks</i>]) </td>
  <td> When called on a stream of (K, V) pairs, returns a stream of (K, V) pairs where the values for each key are aggregated using the given reduce function over batches within a sliding window. Like in <code>groupByKeyAndWindow</code>, the number of reduce tasks is configurable through an optional second argument. 
 <i>windowTime</i> and <i>slideTime</i> are exactly as defined in <code>window()</code>.
</td> 
</tr>
<tr>
  <td> <b>countByKeyAndWindow</b>([<i>numTasks</i>]) </td>
  <td> When called on a stream of (K, V) pairs, returns a stream of (K, Int) pairs where the values for each key are the count within a sliding window. Like in <code>countByKeyAndWindow</code>, the number of reduce tasks is configurable through an optional second argument. 
 <i>windowTime</i> and <i>slideTime</i> are exactly as defined in <code>window()</code>.
</td> 
</tr>

</table>


## Output Operators
When an output operator is called, it triggers the computation of a stream. Currently the following output operators are defined:

<table class="table">
<tr><th style="width:25%">Operator</th><th>Meaning</th></tr>
<tr>
  <td> <b>foreachRDD</b>(<i>func</i>) </td>
  <td> The fundamental output operator. Applies a function, <i>func</i>, to each RDD generated from the stream. This function should have side effects, such as printing output, saving the RDD to external files, or writing it over the network to an external system. </td>
</tr>

<tr>
  <td> <b>print</b>() </td>
  <td> Prints the contents of this DStream on the driver. At each interval, this will take at most ten elements from the DStream's RDD and print them. </td>
</tr>

<tr>
  <td> <b>saveAsObjectFile</b>(<i>prefix</i>, [<i>suffix</i>]) </td>
  <td> Save this DStream's contents as a <code>SequenceFile</code> of serialized objects. The file name at each batch interval is calculated based on <i>prefix</i> and <i>suffix</i>: <i>"prefix-TIME_IN_MS[.suffix]"</i>.
  </td>
</tr>

<tr>
  <td> <b>saveAsTextFile</b>(<i>prefix</i>, <i>suffix</i>) </td>
  <td> Save this DStream's contents as a text files. The file name at each batch interval is calculated based on <i>prefix</i> and <i>suffix</i>: <i>"prefix-TIME_IN_MS[.suffix]"</i>. </td>
</tr>

<tr>
  <td> <b>saveAsHadoopFiles</b>(<i>prefix</i>, <i>suffix</i>) </td>
  <td> Save this DStream's contents as a Hadoop file. The file name at each batch interval is calculated based on <i>prefix</i> and <i>suffix</i>: <i>"prefix-TIME_IN_MS[.suffix]"</i>. </td>
</tr>

</table>

