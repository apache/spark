package org.apache.spark.graph

import org.scalatest.FunSuite

import org.apache.spark.SparkContext
import org.apache.spark.graph.LocalSparkContext._


class GraphSuite extends FunSuite with LocalSparkContext {

//  val sc = new SparkContext("local[4]", "test")

  test("Graph Creation") {
    withSpark(new SparkContext("local", "test")) { sc =>
      val rawEdges = (0L to 100L).zip((1L to 99L) :+ 0L)
      val edges = sc.parallelize(rawEdges)
      val graph = Graph(edges)
      assert( graph.edges.count() === rawEdges.size )
    }
  }

  test("aggregateNeighbors") {
    withSpark(new SparkContext("local", "test")) { sc =>
      val star = Graph(sc.parallelize(List((0, 1), (0, 2), (0, 3))))

      val indegrees = star.aggregateNeighbors(
        (vid, edge) => Some(1),
        (a: Int, b: Int) => a + b,
        EdgeDirection.In).vertices.map(v => (v.id, v.data._2.getOrElse(0)))
      assert(indegrees.collect().toSet === Set((0, 0), (1, 1), (2, 1), (3, 1)))

      val outdegrees = star.aggregateNeighbors(
        (vid, edge) => Some(1),
        (a: Int, b: Int) => a + b,
        EdgeDirection.Out).vertices.map(v => (v.id, v.data._2.getOrElse(0)))
      assert(outdegrees.collect().toSet === Set((0, 3), (1, 0), (2, 0), (3, 0)))

      val noVertexValues = star.aggregateNeighbors[Int](
        (vid: Vid, edge: EdgeTriplet[Int, Int]) => None,
        (a: Int, b: Int) => throw new Exception("reduceFunc called unexpectedly"),
        EdgeDirection.In).vertices.map(v => (v.id, v.data._2))
      assert(noVertexValues.collect().toSet === Set((0, None), (1, None), (2, None), (3, None)))
    }
  }

  test("groupEdges") {
    withSpark(new SparkContext("local", "test")) { sc =>
      val vertices = sc.parallelize(List(Vertex(6, 1),Vertex(7, 1), Vertex(8,1)))
      val edges = sc.parallelize(List(
        Edge(6, 7, 0.4),
        Edge(6, 7, 0.9),
        Edge(6, 7, 0.7),
        Edge(7, 6, 25.0),
        Edge(7, 6, 300.0),
        Edge(7, 6, 600.0),
        Edge(8, 7, 11.0),
        Edge(7, 8, 89.0)))

      val original = Graph(vertices, edges)
      for (e <- original.edges) {
        println("(" + e.src + ", " + e.dst + ", " + e.data + ")")
      }
      //assert(original.edges.count() === 6)
      val grouped = original.groupEdgeTriplets { iter => 
        println("----------------------------------------")
        iter.map(_.data).sum }

      for (e <- grouped.edges) {
        println("******************************(" + e.src + ", " + e.dst + ", " + e.data + ")")
      }

      //val groups: Map[(Vid, Vid), List[Edge[Double]]] = original.edges.collect.toList.groupBy { e => (e.src, e.dst) }
      //for (k <- groups.keys) {
      //  println("#################  " + k + "  #################")
      //}
      //assert(grouped.edges.count() === 2)
      //assert(grouped.edges.collect().toSet === Set(Edge(0, 1, 2.0), Edge(1, 0, 6.0)))
    }
  }

 /* test("joinVertices") {
    sc = new SparkContext("local", "test")
    val vertices = sc.parallelize(Seq(Vertex(1, "one"), Vertex(2, "two"), Vertex(3, "three")), 2)
    val edges = sc.parallelize((Seq(Edge(1, 2, "onetwo"))))
    val g: Graph[String, String] = new GraphImpl(vertices, edges)

    val tbl = sc.parallelize(Seq((1, 10), (2, 20)))
    val g1 = g.joinVertices(tbl, (v: Vertex[String], u: Int) => v.data + u)

    val v = g1.vertices.collect().sortBy(_.id)
    assert(v(0).data === "one10")
    assert(v(1).data === "two20")
    assert(v(2).data === "three")

    val e = g1.edges.collect()
    assert(e(0).data === "onetwo")
  }
  */

//  test("graph partitioner") {
//    sc = new SparkContext("local", "test")
//    val vertices = sc.parallelize(Seq(Vertex(1, "one"), Vertex(2, "two")))
//    val edges = sc.parallelize(Seq(Edge(1, 2, "onlyedge")))
//    var g = Graph(vertices, edges)
//
//    g = g.withPartitioner(4, 7)
//    assert(g.numVertexPartitions === 4)
//    assert(g.numEdgePartitions === 7)
//
//    g = g.withVertexPartitioner(5)
//    assert(g.numVertexPartitions === 5)
//
//    g = g.withEdgePartitioner(8)
//    assert(g.numEdgePartitions === 8)
//
//    g = g.mapVertices(x => x)
//    assert(g.numVertexPartitions === 5)
//    assert(g.numEdgePartitions === 8)
//
//    g = g.mapEdges(x => x)
//    assert(g.numVertexPartitions === 5)
//    assert(g.numEdgePartitions === 8)
//
//    val updates = sc.parallelize(Seq((1, " more")))
//    g = g.updateVertices(
//      updates,
//      (v, u: Option[String]) => if (u.isDefined) v.data + u.get else v.data)
//    assert(g.numVertexPartitions === 5)
//    assert(g.numEdgePartitions === 8)
//
//    g = g.reverse
//    assert(g.numVertexPartitions === 5)
//    assert(g.numEdgePartitions === 8)
//
//  }
}
