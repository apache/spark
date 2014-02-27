---
layout: global
title: Bagel Programming Guide
---

**Bagel will soon be superseded by [GraphX](graphx-programming-guide.html); we recommend that new users try GraphX instead.**

Bagel is a Spark implementation of Google's [Pregel](http://portal.acm.org/citation.cfm?id=1807184) graph processing framework. Bagel currently supports basic graph computation, combiners, and aggregators.

In the Pregel programming model, jobs run as a sequence of iterations called _supersteps_. In each superstep, each vertex in the graph runs a user-specified function that can update state associated with the vertex and send messages to other vertices for use in the *next* iteration.

This guide shows the programming model and features of Bagel by walking through an example implementation of PageRank on Bagel.

# Linking with Bagel

To use Bagel in your program, add the following SBT or Maven dependency:

    groupId = org.apache.spark
    artifactId = spark-bagel_{{site.SCALA_BINARY_VERSION}}
    version = {{site.SPARK_VERSION}}

# Programming Model

Bagel operates on a graph represented as a [distributed dataset](scala-programming-guide.html) of (K, V) pairs, where keys are vertex IDs and values are vertices plus their associated state. In each superstep, Bagel runs a user-specified compute function on each vertex that takes as input the current vertex state and a list of messages sent to that vertex during the previous superstep, and returns the new vertex state and a list of outgoing messages.

For example, we can use Bagel to implement PageRank. Here, vertices represent pages, edges represent links between pages, and messages represent shares of PageRank sent to the pages that a particular page links to.

We first extend the default `Vertex` class to store a `Double`
representing the current PageRank of the vertex, and similarly extend
the `Message` and `Edge` classes. Note that these need to be marked `@serializable` to allow Spark to transfer them across machines. We also import the Bagel types and implicit conversions.

{% highlight scala %}
import org.apache.spark.bagel._
import org.apache.spark.bagel.Bagel._

@serializable class PREdge(val targetId: String) extends Edge

@serializable class PRVertex(
  val id: String, val rank: Double, val outEdges: Seq[Edge],
  val active: Boolean) extends Vertex

@serializable class PRMessage(
  val targetId: String, val rankShare: Double) extends Message
{% endhighlight %}

Next, we load a sample graph from a text file as a distributed dataset and package it into `PRVertex` objects. We also cache the distributed dataset because Bagel will use it multiple times and we'd like to avoid recomputing it.

{% highlight scala %}
val input = sc.textFile("pagerank_data.txt")

val numVerts = input.count()

val verts = input.map(line => {
  val fields = line.split('\t')
  val (id, linksStr) = (fields(0), fields(1))
    val links = linksStr.split(',').map(new PREdge(_))
  (id, new PRVertex(id, 1.0 / numVerts, links, true))
}).cache
{% endhighlight %}

We run the Bagel job, passing in `verts`, an empty distributed dataset of messages, and a custom compute function that runs PageRank for 10 iterations.

{% highlight scala %}
val emptyMsgs = sc.parallelize(List[(String, PRMessage)]())

def compute(self: PRVertex, msgs: Option[Seq[PRMessage]], superstep: Int)
: (PRVertex, Iterable[PRMessage]) = {
  val msgSum = msgs.getOrElse(List()).map(_.rankShare).sum
    val newRank =
      if (msgSum != 0)
        0.15 / numVerts + 0.85 * msgSum
      else
        self.rank
    val halt = superstep >= 10
    val msgsOut =
      if (!halt)
        self.outEdges.map(edge =>
          new PRMessage(edge.targetId, newRank / self.outEdges.size))
      else
        List()
    (new PRVertex(self.id, newRank, self.outEdges, !halt), msgsOut)
}
{% endhighlight %}

val result = Bagel.run(sc, verts, emptyMsgs)()(compute)

Finally, we print the results.

{% highlight scala %}
println(result.map(v => "%s\t%s\n".format(v.id, v.rank)).collect.mkString)
{% endhighlight %}

## Combiners

Sending a message to another vertex generally involves expensive communication over the network. For certain algorithms, it's possible to reduce the amount of communication using _combiners_. For example, if the compute function receives integer messages and only uses their sum, it's possible for Bagel to combine multiple messages to the same vertex by summing them.

For combiner support, Bagel can optionally take a set of combiner functions that convert messages to their combined form.

_Example: PageRank with combiners_

## Aggregators

Aggregators perform a reduce across all vertices after each superstep, and provide the result to each vertex in the next superstep.

For aggregator support, Bagel can optionally take an aggregator function that reduces across each vertex.

_Example_

## Operations

Here are the actions and types in the Bagel API. See [Bagel.scala](https://github.com/apache/incubator-spark/blob/master/bagel/src/main/scala/org/apache/spark/bagel/Bagel.scala) for details.

### Actions

{% highlight scala %}
/*** Full form ***/

Bagel.run(sc, vertices, messages, combiner, aggregator, partitioner, numSplits)(compute)
// where compute takes (vertex: V, combinedMessages: Option[C], aggregated: Option[A], superstep: Int)
// and returns (newVertex: V, outMessages: Array[M])

/*** Abbreviated forms ***/

Bagel.run(sc, vertices, messages, combiner, partitioner, numSplits)(compute)
// where compute takes (vertex: V, combinedMessages: Option[C], superstep: Int)
// and returns (newVertex: V, outMessages: Array[M])

Bagel.run(sc, vertices, messages, combiner, numSplits)(compute)
// where compute takes (vertex: V, combinedMessages: Option[C], superstep: Int)
// and returns (newVertex: V, outMessages: Array[M])

Bagel.run(sc, vertices, messages, numSplits)(compute)
// where compute takes (vertex: V, messages: Option[Array[M]], superstep: Int)
// and returns (newVertex: V, outMessages: Array[M])
{% endhighlight %}

### Types

{% highlight scala %}
trait Combiner[M, C] {
  def createCombiner(msg: M): C
  def mergeMsg(combiner: C, msg: M): C
  def mergeCombiners(a: C, b: C): C
}

trait Aggregator[V, A] {
  def createAggregator(vert: V): A
  def mergeAggregators(a: A, b: A): A
}

trait Vertex {
  def active: Boolean
}

trait Message[K] {
  def targetId: K
}
{% endhighlight %}

# Where to Go from Here

Two example jobs, PageRank and shortest path, are included in `examples/src/main/scala/org/apache/spark/examples/bagel`. You can run them by passing the class name to the `bin/run-example` script included in Spark; e.g.:

    ./bin/run-example org.apache.spark.examples.bagel.WikipediaPageRank

Each example program prints usage help when run without any arguments.
