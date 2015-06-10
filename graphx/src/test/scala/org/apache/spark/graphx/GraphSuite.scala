/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.graphx

import org.apache.spark.{SparkContext, SparkFunSuite}
import org.apache.spark.graphx.Graph._
import org.apache.spark.graphx.PartitionStrategy._
import org.apache.spark.rdd._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.Utils

class GraphSuite extends SparkFunSuite with LocalSparkContext {

  def starGraph(sc: SparkContext, n: Int): Graph[String, Int] = {
    Graph.fromEdgeTuples(sc.parallelize((1 to n).map(x => (0: VertexId, x: VertexId)), 3), "v")
  }

  test("Graph.fromEdgeTuples") {
    withSpark { sc =>
      val ring = (0L to 100L).zip((1L to 99L) :+ 0L)
      val doubleRing = ring ++ ring
      val graph = Graph.fromEdgeTuples(sc.parallelize(doubleRing), 1)
      assert(graph.edges.count() === doubleRing.size)
      assert(graph.edges.collect().forall(e => e.attr == 1))

      // uniqueEdges option should uniquify edges and store duplicate count in edge attributes
      val uniqueGraph = Graph.fromEdgeTuples(sc.parallelize(doubleRing), 1, Some(RandomVertexCut))
      assert(uniqueGraph.edges.count() === ring.size)
      assert(uniqueGraph.edges.collect().forall(e => e.attr == 2))
    }
  }

  test("Graph.fromEdges") {
    withSpark { sc =>
      val ring = (0L to 100L).zip((1L to 99L) :+ 0L).map { case (a, b) => Edge(a, b, 1) }
      val graph = Graph.fromEdges(sc.parallelize(ring), 1.0F)
      assert(graph.edges.count() === ring.size)
    }
  }

  test("Graph.apply") {
    withSpark { sc =>
      val rawEdges = (0L to 98L).zip((1L to 99L) :+ 0L)
      val edges: RDD[Edge[Int]] = sc.parallelize(rawEdges).map { case (s, t) => Edge(s, t, 1) }
      val vertices: RDD[(VertexId, Boolean)] = sc.parallelize((0L until 10L).map(id => (id, true)))
      val graph = Graph(vertices, edges, false)
      assert( graph.edges.count() === rawEdges.size )
      // Vertices not explicitly provided but referenced by edges should be created automatically
      assert( graph.vertices.count() === 100)
      graph.triplets.collect().map { et =>
        assert((et.srcId < 10 && et.srcAttr) || (et.srcId >= 10 && !et.srcAttr))
        assert((et.dstId < 10 && et.dstAttr) || (et.dstId >= 10 && !et.dstAttr))
      }
    }
  }

  test("triplets") {
    withSpark { sc =>
      val n = 5
      val star = starGraph(sc, n)
      assert(star.triplets.map(et => (et.srcId, et.dstId, et.srcAttr, et.dstAttr)).collect().toSet
        === (1 to n).map(x => (0: VertexId, x: VertexId, "v", "v")).toSet)
    }
  }

  test("partitionBy") {
    withSpark { sc =>
      def mkGraph(edges: List[(Long, Long)]): Graph[Int, Int] = {
        Graph.fromEdgeTuples(sc.parallelize(edges, 2), 0)
      }
      def nonemptyParts(graph: Graph[Int, Int]): RDD[List[Edge[Int]]] = {
        graph.edges.partitionsRDD.mapPartitions { iter =>
          Iterator(iter.next()._2.iterator.toList)
        }.filter(_.nonEmpty)
      }
      val identicalEdges = List((0L, 1L), (0L, 1L))
      val canonicalEdges = List((0L, 1L), (1L, 0L))
      val sameSrcEdges = List((0L, 1L), (0L, 2L))

      // The two edges start out in different partitions
      for (edges <- List(identicalEdges, canonicalEdges, sameSrcEdges)) {
        assert(nonemptyParts(mkGraph(edges)).count === 2)
      }
      // partitionBy(RandomVertexCut) puts identical edges in the same partition
      assert(nonemptyParts(mkGraph(identicalEdges).partitionBy(RandomVertexCut)).count === 1)
      // partitionBy(EdgePartition1D) puts same-source edges in the same partition
      assert(nonemptyParts(mkGraph(sameSrcEdges).partitionBy(EdgePartition1D)).count === 1)
      // partitionBy(CanonicalRandomVertexCut) puts edges that are identical modulo direction into
      // the same partition
      assert(
        nonemptyParts(mkGraph(canonicalEdges).partitionBy(CanonicalRandomVertexCut)).count === 1)
      // partitionBy(EdgePartition2D) puts identical edges in the same partition
      assert(nonemptyParts(mkGraph(identicalEdges).partitionBy(EdgePartition2D)).count === 1)

      // partitionBy(EdgePartition2D) ensures that vertices need only be replicated to 2 * sqrt(p)
      // partitions
      val n = 100
      val p = 100
      val verts = 1 to n
      val graph = Graph.fromEdgeTuples(sc.parallelize(verts.flatMap(x =>
        verts.withFilter(y => y % x == 0).map(y => (x: VertexId, y: VertexId))), p), 0)
      assert(graph.edges.partitions.length === p)
      val partitionedGraph = graph.partitionBy(EdgePartition2D)
      assert(graph.edges.partitions.length === p)
      val bound = 2 * math.sqrt(p)
      // Each vertex should be replicated to at most 2 * sqrt(p) partitions
      val partitionSets = partitionedGraph.edges.partitionsRDD.mapPartitions { iter =>
        val part = iter.next()._2
        Iterator((part.iterator.flatMap(e => Iterator(e.srcId, e.dstId))).toSet)
      }.collect
      if (!verts.forall(id => partitionSets.count(_.contains(id)) <= bound)) {
        val numFailures = verts.count(id => partitionSets.count(_.contains(id)) > bound)
        val failure = verts.maxBy(id => partitionSets.count(_.contains(id)))
        fail(("Replication bound test failed for %d/%d vertices. " +
          "Example: vertex %d replicated to %d (> %f) partitions.").format(
          numFailures, n, failure, partitionSets.count(_.contains(failure)), bound))
      }
      // This should not be true for the default hash partitioning
      val partitionSetsUnpartitioned = graph.edges.partitionsRDD.mapPartitions { iter =>
        val part = iter.next()._2
        Iterator((part.iterator.flatMap(e => Iterator(e.srcId, e.dstId))).toSet)
      }.collect
      assert(verts.exists(id => partitionSetsUnpartitioned.count(_.contains(id)) > bound))

      // Forming triplets view
      val g = Graph(
        sc.parallelize(List((0L, "a"), (1L, "b"), (2L, "c"))),
        sc.parallelize(List(Edge(0L, 1L, 1), Edge(0L, 2L, 1)), 2))
      assert(g.triplets.collect().map(_.toTuple).toSet ===
        Set(((0L, "a"), (1L, "b"), 1), ((0L, "a"), (2L, "c"), 1)))
      val gPart = g.partitionBy(EdgePartition2D)
      assert(gPart.triplets.collect().map(_.toTuple).toSet ===
        Set(((0L, "a"), (1L, "b"), 1), ((0L, "a"), (2L, "c"), 1)))
    }
  }

  test("mapVertices") {
    withSpark { sc =>
      val n = 5
      val star = starGraph(sc, n)
      // mapVertices preserving type
      val mappedVAttrs = star.mapVertices((vid, attr) => attr + "2")
      assert(mappedVAttrs.vertices.collect().toSet === (0 to n).map(x => (x: VertexId, "v2")).toSet)
      // mapVertices changing type
      val mappedVAttrs2 = star.mapVertices((vid, attr) => attr.length)
      assert(mappedVAttrs2.vertices.collect().toSet === (0 to n).map(x => (x: VertexId, 1)).toSet)
    }
  }

  test("mapVertices changing type with same erased type") {
    withSpark { sc =>
      val vertices = sc.parallelize(Array[(Long, Option[java.lang.Integer])](
        (1L, Some(1)),
        (2L, Some(2)),
        (3L, Some(3))
      ))
      val edges = sc.parallelize(Array(
        Edge(1L, 2L, 0),
        Edge(2L, 3L, 0),
        Edge(3L, 1L, 0)
      ))
      val graph0 = Graph(vertices, edges)
      // Trigger initial vertex replication
      graph0.triplets.foreach(x => {})
      // Change type of replicated vertices, but preserve erased type
      val graph1 = graph0.mapVertices { case (vid, integerOpt) =>
        integerOpt.map((x: java.lang.Integer) => x.toDouble: java.lang.Double)
      }
      // Access replicated vertices, exposing the erased type
      val graph2 = graph1.mapTriplets(t => t.srcAttr.get)
      assert(graph2.edges.map(_.attr).collect().toSet === Set[java.lang.Double](1.0, 2.0, 3.0))
    }
  }

  test("mapEdges") {
    withSpark { sc =>
      val n = 3
      val star = starGraph(sc, n)
      val starWithEdgeAttrs = star.mapEdges(e => e.dstId)

      val edges = starWithEdgeAttrs.edges.collect()
      assert(edges.size === n)
      assert(edges.toSet === (1 to n).map(x => Edge(0, x, x)).toSet)
    }
  }

  test("mapTriplets") {
    withSpark { sc =>
      val n = 5
      val star = starGraph(sc, n)
      assert(star.mapTriplets(et => et.srcAttr + et.dstAttr).edges.collect().toSet ===
        (1L to n).map(x => Edge(0, x, "vv")).toSet)
    }
  }

  test("reverse") {
    withSpark { sc =>
      val n = 5
      val star = starGraph(sc, n)
      assert(star.reverse.outDegrees.collect().toSet === (1 to n).map(x => (x: VertexId, 1)).toSet)
    }
  }

  test("reverse with join elimination") {
    withSpark { sc =>
      val vertices: RDD[(VertexId, Int)] = sc.parallelize(Array((1L, 1), (2L, 2)))
      val edges: RDD[Edge[Int]] = sc.parallelize(Array(Edge(1L, 2L, 0)))
      val graph = Graph(vertices, edges).reverse
      val result = graph.mapReduceTriplets[Int](et => Iterator((et.dstId, et.srcAttr)), _ + _)
      assert(result.collect().toSet === Set((1L, 2)))
    }
  }

  test("subgraph") {
    withSpark { sc =>
      // Create a star graph of 10 veritces.
      val n = 10
      val star = starGraph(sc, n)
      // Take only vertices whose vids are even
      val subgraph = star.subgraph(vpred = (vid, attr) => vid % 2 == 0)

      // We should have 5 vertices.
      assert(subgraph.vertices.collect().toSet === (0 to n by 2).map(x => (x, "v")).toSet)

      // And 4 edges.
      assert(subgraph.edges.map(_.copy()).collect().toSet ===
        (2 to n by 2).map(x => Edge(0, x, 1)).toSet)
    }
  }

  test("mask") {
    withSpark { sc =>
      val n = 5
      val vertices = sc.parallelize((0 to n).map(x => (x: VertexId, x)))
      val edges = sc.parallelize((1 to n).map(x => Edge(0, x, x)))
      val graph: Graph[Int, Int] = Graph(vertices, edges).cache()

      val subgraph = graph.subgraph(
        e => e.dstId != 4L,
        (vid, vdata) => vid != 3L
      ).mapVertices((vid, vdata) => -1).mapEdges(e => -1)

      val projectedGraph = graph.mask(subgraph)

      val v = projectedGraph.vertices.collect().toSet
      assert(v === Set((0, 0), (1, 1), (2, 2), (4, 4), (5, 5)))

      // the map is necessary because of object-reuse in the edge iterator
      val e = projectedGraph.edges.map(e => Edge(e.srcId, e.dstId, e.attr)).collect().toSet
      assert(e === Set(Edge(0, 1, 1), Edge(0, 2, 2), Edge(0, 5, 5)))

    }
  }

  test("groupEdges") {
    withSpark { sc =>
      val n = 5
      val star = starGraph(sc, n)
      val doubleStar = Graph.fromEdgeTuples(
        sc.parallelize((1 to n).flatMap(x =>
          List((0: VertexId, x: VertexId), (0: VertexId, x: VertexId))), 1), "v")
      val star2 = doubleStar.groupEdges { (a, b) => a}
      assert(star2.edges.collect().toArray.sorted(Edge.lexicographicOrdering[Int]) ===
        star.edges.collect().toArray.sorted(Edge.lexicographicOrdering[Int]))
      assert(star2.vertices.collect().toSet === star.vertices.collect().toSet)
    }
  }

  test("mapReduceTriplets") {
    withSpark { sc =>
      val n = 5
      val star = starGraph(sc, n).mapVertices { (_, _) => 0 }.cache()
      val starDeg = star.joinVertices(star.degrees){ (vid, oldV, deg) => deg }
      val neighborDegreeSums = starDeg.mapReduceTriplets(
        edge => Iterator((edge.srcId, edge.dstAttr), (edge.dstId, edge.srcAttr)),
        (a: Int, b: Int) => a + b)
      assert(neighborDegreeSums.collect().toSet === (0 to n).map(x => (x, n)).toSet)

      // activeSetOpt
      val allPairs = for (x <- 1 to n; y <- 1 to n) yield (x: VertexId, y: VertexId)
      val complete = Graph.fromEdgeTuples(sc.parallelize(allPairs, 3), 0)
      val vids = complete.mapVertices((vid, attr) => vid).cache()
      val active = vids.vertices.filter { case (vid, attr) => attr % 2 == 0 }
      val numEvenNeighbors = vids.mapReduceTriplets(et => {
        // Map function should only run on edges with destination in the active set
        if (et.dstId % 2 != 0) {
          throw new Exception("map ran on edge with dst vid %d, which is odd".format(et.dstId))
        }
        Iterator((et.srcId, 1))
      }, (a: Int, b: Int) => a + b, Some((active, EdgeDirection.In))).collect().toSet
      assert(numEvenNeighbors === (1 to n).map(x => (x: VertexId, n / 2)).toSet)

      // outerJoinVertices followed by mapReduceTriplets(activeSetOpt)
      val ringEdges = sc.parallelize((0 until n).map(x => (x: VertexId, (x + 1) % n: VertexId)), 3)
      val ring = Graph.fromEdgeTuples(ringEdges, 0) .mapVertices((vid, attr) => vid).cache()
      val changed = ring.vertices.filter { case (vid, attr) => attr % 2 == 1 }.mapValues(-_).cache()
      val changedGraph = ring.outerJoinVertices(changed) { (vid, old, newOpt) =>
        newOpt.getOrElse(old)
      }
      val numOddNeighbors = changedGraph.mapReduceTriplets(et => {
        // Map function should only run on edges with source in the active set
        if (et.srcId % 2 != 1) {
          throw new Exception("map ran on edge with src vid %d, which is even".format(et.dstId))
        }
        Iterator((et.dstId, 1))
      }, (a: Int, b: Int) => a + b, Some(changed, EdgeDirection.Out)).collect().toSet
      assert(numOddNeighbors === (2 to n by 2).map(x => (x: VertexId, 1)).toSet)

    }
  }

  test("aggregateMessages") {
    withSpark { sc =>
      val n = 5
      val agg = starGraph(sc, n).aggregateMessages[String](
        ctx => {
          if (ctx.dstAttr != null) {
            throw new Exception(
              "expected ctx.dstAttr to be null due to TripletFields, but it was " + ctx.dstAttr)
          }
          ctx.sendToDst(ctx.srcAttr)
        }, _ + _, TripletFields.Src)
      assert(agg.collect().toSet === (1 to n).map(x => (x: VertexId, "v")).toSet)
    }
  }

  test("outerJoinVertices") {
    withSpark { sc =>
      val n = 5
      val reverseStar = starGraph(sc, n).reverse.cache()
      // outerJoinVertices changing type
      val reverseStarDegrees = reverseStar.outerJoinVertices(reverseStar.outDegrees) {
        (vid, a, bOpt) => bOpt.getOrElse(0)
      }
      val neighborDegreeSums = reverseStarDegrees.mapReduceTriplets(
        et => Iterator((et.srcId, et.dstAttr), (et.dstId, et.srcAttr)),
        (a: Int, b: Int) => a + b).collect().toSet
      assert(neighborDegreeSums === Set((0: VertexId, n)) ++ (1 to n).map(x => (x: VertexId, 0)))
      // outerJoinVertices preserving type
      val messages = reverseStar.vertices.mapValues { (vid, attr) => vid.toString }
      val newReverseStar =
        reverseStar.outerJoinVertices(messages) { (vid, a, bOpt) => a + bOpt.getOrElse("") }
      assert(newReverseStar.vertices.map(_._2).collect().toSet ===
        (0 to n).map(x => "v%d".format(x)).toSet)
    }
  }

  test("more edge partitions than vertex partitions") {
    withSpark { sc =>
      val verts = sc.parallelize(List((1: VertexId, "a"), (2: VertexId, "b")), 1)
      val edges = sc.parallelize(List(Edge(1, 2, 0), Edge(2, 1, 0)), 2)
      val graph = Graph(verts, edges)
      val triplets = graph.triplets.map(et => (et.srcId, et.dstId, et.srcAttr, et.dstAttr))
        .collect().toSet
      assert(triplets ===
        Set((1: VertexId, 2: VertexId, "a", "b"), (2: VertexId, 1: VertexId, "b", "a")))
    }
  }

  test("checkpoint") {
    val checkpointDir = Utils.createTempDir()
    withSpark { sc =>
      sc.setCheckpointDir(checkpointDir.getAbsolutePath)
      val ring = (0L to 100L).zip((1L to 99L) :+ 0L).map { case (a, b) => Edge(a, b, 1)}
      val rdd = sc.parallelize(ring)
      val graph = Graph.fromEdges(rdd, 1.0F)
      assert(!graph.isCheckpointed)
      assert(graph.getCheckpointFiles.size === 0)
      graph.checkpoint()
      graph.edges.map(_.attr).count()
      graph.vertices.map(_._2).count()

      val edgesDependencies = graph.edges.partitionsRDD.dependencies
      val verticesDependencies = graph.vertices.partitionsRDD.dependencies
      assert(edgesDependencies.forall(_.rdd.isInstanceOf[CheckpointRDD[_]]))
      assert(verticesDependencies.forall(_.rdd.isInstanceOf[CheckpointRDD[_]]))
      assert(graph.isCheckpointed)
      assert(graph.getCheckpointFiles.size === 2)
    }
  }

  test("cache, getStorageLevel") {
    // test to see if getStorageLevel returns correct value
    withSpark { sc =>
      val verts = sc.parallelize(List((1: VertexId, "a"), (2: VertexId, "b")), 1)
      val edges = sc.parallelize(List(Edge(1, 2, 0), Edge(2, 1, 0)), 2)
      val graph = Graph(verts, edges, "", StorageLevel.MEMORY_ONLY, StorageLevel.MEMORY_ONLY)
      // Note: Before caching, graph.vertices is cached, but graph.edges is not (but graph.edges'
      //       parent RDD is cached).
      graph.cache()
      assert(graph.vertices.getStorageLevel == StorageLevel.MEMORY_ONLY)
      assert(graph.edges.getStorageLevel == StorageLevel.MEMORY_ONLY)
    }
  }

  test("non-default number of edge partitions") {
    val n = 10
    val defaultParallelism = 3
    val numEdgePartitions = 4
    assert(defaultParallelism != numEdgePartitions)
    val conf = new org.apache.spark.SparkConf()
      .set("spark.default.parallelism", defaultParallelism.toString)
    val sc = new SparkContext("local", "test", conf)
    try {
      val edges = sc.parallelize((1 to n).map(x => (x: VertexId, 0: VertexId)),
        numEdgePartitions)
      val graph = Graph.fromEdgeTuples(edges, 1)
      val neighborAttrSums = graph.mapReduceTriplets[Int](
        et => Iterator((et.dstId, et.srcAttr)), _ + _)
      assert(neighborAttrSums.collect().toSet === Set((0: VertexId, n)))
    } finally {
      sc.stop()
    }
  }

}
