---
layout: global
title: GraphX Programming Guide
---

* This will become a table of contents (this text will be scraped).
{:toc}

<p style="text-align: center;">
  <img src="img/graphx_logo.png"
       title="GraphX Logo"
       alt="GraphX"
       width="65%" />
</p>

# Overview

GraphX is the new (alpha) Spark API for graphs and graph-parallel
computation. At a high-level GraphX, extends the Spark
[RDD](api/core/index.html#org.apache.spark.rdd.RDD) by
introducing the [Resilient Distributed property Graph (RDG)](#property_graph):
a directed graph with properties attached to each vertex and edge.
To support graph computation, GraphX exposes a set of functions
(e.g., [mapReduceTriplets](#mrTriplets)) as well as optimized variants of the
[Pregel](http://giraph.apache.org) and [GraphLab](http://graphlab.org)
APIs. In addition, GraphX includes a growing collection of graph
[algorithms](#graph_algorithms) and [builders](#graph_builders) to simplify
graph analytics tasks.

## Background on Graph-Parallel Computation

From social networks to language modeling, the growing scale and importance of
graph data has driven the development of numerous new *graph-parallel* systems
(e.g., [Giraph](http://http://giraph.apache.org) and
[GraphLab](http://graphlab.org)).  By restricting the types of computation that can be
expressed and introducing new techniques to partition and distribute graphs,
these systems can efficiently execute sophisticated graph algorithms orders of
magnitude faster than more general *data-parallel* systems.

<p style="text-align: center;">
  <img src="img/data_parallel_vs_graph_parallel.png"
       title="Data-Parallel vs. Graph-Parallel"
       alt="Data-Parallel vs. Graph-Parallel"
       width="50%" />
</p>

However, the same restrictions that enable these substantial performance gains
also make it difficult to express many of the important stages in a typical graph-analytics pipeline:
constructing the graph, modifying its structure, or expressing computation that
spans multiple graphs.  As a consequence, existing graph analytics pipelines
compose graph-parallel and data-parallel systems, leading to extensive data
movement and duplication and a complicated programming model.

<p style="text-align: center;">
  <img src="img/graph_analytics_pipeline.png"
       title="Graph Analytics Pipeline"
       alt="Graph Analytics Pipeline"
       width="50%" />
</p>

The goal of the GraphX project is to unify graph-parallel and data-parallel
computation in one system with a single composable API. This goal is achieved
through an API that enables users to view data both as a graph and as
collections (i.e., RDDs) without data movement or duplication and by
incorporating advances in graph-parallel systems to optimize the execution of
operations on the graph view.  In preliminary experiments we find that the GraphX
system is able to achieve performance comparable to state-of-the-art
graph-parallel systems while easily expressing the entire analytics pipelines.

<p style="text-align: center;">
  <img src="img/graphx_performance_comparison.png"
       title="GraphX Performance Comparison"
       alt="GraphX Performance Comparison"
       width="50%" />
</p>

## GraphX Replaces the Spark Bagel API

Prior to the release of GraphX, graph computation in Spark was expressed using
Bagel, an implementation of the Pregel API.  GraphX improves upon Bagel by exposing
a richer property graph API, a more streamlined version of the Pregel abstraction,
and system optimizations to improve performance and reduce memory
overhead.  While we plan to eventually deprecate the Bagel, we will continue to
support the API and [Bagel programming guide](bagel-programming-guide.html). However,
we encourage Bagel to explore the new GraphX API and comment on issues that may
complicate the transition from Bagel.

# The Property Graph
<a name="property_graph"></a>

<p style="text-align: center;">
  <img src="img/edge_cut_vs_vertex_cut.png"
       title="Edge Cut vs. Vertex Cut"
       alt="Edge Cut vs. Vertex Cut"
       width="50%" />
</p>

<p style="text-align: center;">
  <img src="img/property_graph.png"
       title="The Property Graph"
       alt="The Property Graph"
       width="50%" />
</p>

<p style="text-align: center;">
  <img src="img/vertex_routing_edge_tables.png"
       title="RDD Graph Representation"
       alt="RDD Graph Representation"
       width="50%" />
</p>


# Graph Operators

## Map Reduce Triplets (mapReduceTriplets)
<a name="mrTriplets"></a>

# Graph Algorithms
<a name="graph_algorithms"></a>

# Graph Builders
<a name="graph_builders"></a>

<p style="text-align: center;">
  <img src="img/tables_and_graphs.png"
       title="Tables and Graphs"
       alt="Tables and Graphs"
       width="50%" />
</p>

# Examples

Suppose I want to build a graph from some text files, restrict the graph
to important relationships and users, run page-rank on the sub-graph, and
then finally return attributes associated with the top users.  I can do
all of this in just a few lines with GraphX:

{% highlight scala %}
// Connect to the Spark cluster
val sc = new SparkContext("spark://master.amplab.org", "research")

// Load my user data and prase into tuples of user id and attribute list
val users = sc.textFile("hdfs://user_attributes.tsv")
  .map(line => line.split).map( parts => (parts.head, parts.tail) )

// Parse the edge data which is already in userId -> userId format
val followerGraph = Graph.textFile(sc, "hdfs://followers.tsv")

// Attach the user attributes
val graph = followerGraph.outerJoinVertices(users){
  case (uid, deg, Some(attrList)) => attrList
  // Some users may not have attributes so we set them as empty
  case (uid, deg, None) => Array.empty[String]
  }

// Restrict the graph to users which have exactly two attributes
val subgraph = graph.subgraph((vid, attr) => attr.size == 2)

// Compute the PageRank
val pagerankGraph = Analytics.pagerank(subgraph)

// Get the attributes of the top pagerank users
val userInfoWithPageRank = subgraph.outerJoinVertices(pagerankGraph.vertices){
  case (uid, attrList, Some(pr)) => (pr, attrList)
  case (uid, attrList, None) => (pr, attrList)
  }

println(userInfoWithPageRank.top(5))

{% endhighlight %}

