package org.apache.spark.graph

import org.scalatest.FunSuite

import org.apache.spark.SparkContext
import org.apache.spark.graph.LocalSparkContext._
import org.apache.spark.rdd._

class GraphSuite extends FunSuite with LocalSparkContext {

  System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  System.setProperty("spark.kryo.registrator", "org.apache.spark.graph.GraphKryoRegistrator")

  test("Graph Creation") {
    withSpark(new SparkContext("local", "test")) { sc =>
      val rawEdges = (0L to 100L).zip((1L to 99L) :+ 0L)
      val edges = sc.parallelize(rawEdges)
      val graph = Graph.fromEdgeTuples(edges, 1.0F)
      assert(graph.edges.count() === rawEdges.size)
    }
  }

  test("Graph Creation with invalid vertices") {
    withSpark(new SparkContext("local", "test")) { sc =>
      val rawEdges = (0L to 98L).zip((1L to 99L) :+ 0L)
      val edges: RDD[Edge[Int]] = sc.parallelize(rawEdges).map { case (s, t) => Edge(s, t, 1) }
      val vertices: RDD[(Vid, Boolean)] = sc.parallelize((0L until 10L).map(id => (id, true)))
      val graph = Graph(vertices, edges, false)
      assert( graph.edges.count() === rawEdges.size )
      assert( graph.vertices.count() === 100)
      graph.triplets.map { et =>
        assert((et.srcId < 10 && et.srcAttr) || (et.srcId >= 10 && !et.srcAttr))
        assert((et.dstId < 10 && et.dstAttr) || (et.dstId >= 10 && !et.dstAttr))
      }
    }
  }

  test("mapEdges") {
    withSpark(new SparkContext("local", "test")) { sc =>
      val n = 3
      val star = Graph.fromEdgeTuples(
        sc.parallelize((1 to n).map(x => (0: Vid, x: Vid))),
        "defaultValue")
      val starWithEdgeAttrs = star.mapEdges(e => e.dstId)

      // map(_.copy()) is a workaround for https://github.com/amplab/graphx/issues/25
      val edges = starWithEdgeAttrs.edges.map(_.copy()).collect()
      assert(edges.size === n)
      assert(edges.toSet === (1 to n).map(x => Edge(0, x, x)).toSet)
    }
  }

  test("mapReduceTriplets") {
    withSpark(new SparkContext("local", "test")) { sc =>
      val n = 3
      val star = Graph.fromEdgeTuples(sc.parallelize((1 to n).map(x => (0: Vid, x: Vid))), 0)
      val starDeg = star.joinVertices(star.degrees){ (vid, oldV, deg) => deg }
      val neighborDegreeSums = starDeg.mapReduceTriplets(
        edge => Iterator((edge.srcId, edge.dstAttr), (edge.dstId, edge.srcAttr)),
        (a: Int, b: Int) => a + b)
      assert(neighborDegreeSums.collect().toSet === (0 to n).map(x => (x, n)).toSet)
    }
  }

  test("aggregateNeighbors") {
    withSpark(new SparkContext("local", "test")) { sc =>
      val n = 3
      val star = Graph.fromEdgeTuples(sc.parallelize((1 to n).map(x => (0: Vid, x: Vid))), 1)

      val indegrees = star.aggregateNeighbors(
        (vid, edge) => Some(1),
        (a: Int, b: Int) => a + b,
        EdgeDirection.In)
      assert(indegrees.collect().toSet === (1 to n).map(x => (x, 1)).toSet)

      val outdegrees = star.aggregateNeighbors(
        (vid, edge) => Some(1),
        (a: Int, b: Int) => a + b,
        EdgeDirection.Out)
      assert(outdegrees.collect().toSet === Set((0, n)))

      val noVertexValues = star.aggregateNeighbors[Int](
        (vid: Vid, edge: EdgeTriplet[Int, Int]) => None,
        (a: Int, b: Int) => throw new Exception("reduceFunc called unexpectedly"),
        EdgeDirection.In)
      assert(noVertexValues.collect().toSet === Set.empty[(Vid, Int)])
    }
  }

  test("joinVertices") {
    withSpark(new SparkContext("local", "test")) { sc =>
      val vertices = sc.parallelize(Seq[(Vid, String)]((1, "one"), (2, "two"), (3, "three")), 2)
      val edges = sc.parallelize((Seq(Edge(1, 2, "onetwo"))))
      val g: Graph[String, String] = Graph(vertices, edges)

      val tbl = sc.parallelize(Seq[(Vid, Int)]((1, 10), (2, 20)))
      val g1 = g.joinVertices(tbl) { (vid: Vid, attr: String, u: Int) => attr + u }

      val v = g1.vertices.collect().toSet
      assert(v === Set((1, "one10"), (2, "two20"), (3, "three")))
    }
  }

  test("collectNeighborIds") {
    withSpark(new SparkContext("local", "test")) { sc =>
      val chain = (0 until 100).map(x => (x, (x+1)%100) )
      val rawEdges = sc.parallelize(chain, 3).map { case (s,d) => (s.toLong, d.toLong) }
      val graph = Graph.fromEdgeTuples(rawEdges, 1.0)
      val nbrs = graph.collectNeighborIds(EdgeDirection.Both)
      assert(nbrs.count === chain.size)
      assert(graph.numVertices === nbrs.count)
      nbrs.collect.foreach { case (vid, nbrs) => assert(nbrs.size === 2) }
      nbrs.collect.foreach { case (vid, nbrs) =>
        val s = nbrs.toSet
        assert(s.contains((vid + 1) % 100))
        assert(s.contains(if (vid > 0) vid - 1 else 99 ))
      }
    }
  }

  test("VertexSetRDD") {
    withSpark(new SparkContext("local", "test")) { sc =>
      val n = 100
      val a = sc.parallelize((0 to n).map(x => (x.toLong, x.toLong)), 5)
      val b = VertexRDD(a).mapValues(x => -x).cache() // Allow joining b with a derived RDD of b
      assert(b.count === n + 1)
      assert(b.leftJoin(a){ (id, a, bOpt) => a + bOpt.get }.map(x=> x._2).reduce(_+_) === 0)
      val c = b.aggregateUsingIndex[Long](a, (x, y) => x)
      assert(b.leftJoin(c){ (id, b, cOpt) => b + cOpt.get }.map(x=> x._2).reduce(_+_) === 0)
      val d = c.filter(q => ((q._2 % 2) == 0))
      val e = a.filter(q => ((q._2 % 2) == 0))
      assert(d.count === e.count)
      assert(b.zipJoin(c)((id, b, c) => b + c).map(x => x._2).reduce(_+_) === 0)
      val f = b.mapValues(x => if (x % 2 == 0) -x else x)
      assert(b.diff(f).collect().toSet === (2 to n by 2).map(x => (x.toLong, x.toLong)).toSet)
    }
  }

  test("subgraph") {
    withSpark(new SparkContext("local", "test")) { sc =>
      // Create a star graph of 10 veritces.
      val n = 10
      val star = Graph.fromEdgeTuples(sc.parallelize((1 to n).map(x => (0: Vid, x: Vid))), "v")
      // Take only vertices whose vids are even
      val subgraph = star.subgraph(vpred = (vid, attr) => vid % 2 == 0)

      // We should have 5 vertices.
      assert(subgraph.vertices.collect().toSet === (0 to n by 2).map(x => (x, "v")).toSet)

      // And 4 edges.
      assert(subgraph.edges.map(_.copy()).collect().toSet === (2 to n by 2).map(x => Edge(0, x, 1)).toSet)
    }
  }

  test("deltaJoinVertices") {
    withSpark(new SparkContext("local", "test")) { sc =>
      // Create a star graph of 10 vertices
      val n = 10
      val star = Graph.fromEdgeTuples(sc.parallelize((1 to n).map(x => (0: Vid, x: Vid))), "v1").cache()

      // Modify only vertices whose vids are even
      val newVerts = star.vertices.mapValues((vid, attr) => if (vid % 2 == 0) "v2" else attr)
      val changedVerts = star.vertices.diff(newVerts)

      // Apply the modification to the graph
      val changedStar = star.deltaJoinVertices(newVerts, changedVerts)

      // The graph's vertices should be correct
      assert(changedStar.vertices.collect().toSet === newVerts.collect().toSet)

      // Send the leaf attributes to the center
      val sums = changedStar.mapReduceTriplets(
        edge => Iterator((edge.srcId, Set(edge.dstAttr))),
        (a: Set[String], b: Set[String]) => a ++ b)
      assert(sums.collect().toSet === Set((0, Set("v1", "v2"))))
    }
  }
}
