package org.apache.spark.graph.algorithms

import org.scalatest.FunSuite

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.graph._
import org.apache.spark.graph.util.GraphGenerators
import org.apache.spark.rdd._


class StronglyConnectedComponentsSuite extends FunSuite with LocalSparkContext {

  test("Island Strongly Connected Components") {
    withSpark { sc =>
      val vertices = sc.parallelize((1L to 5L).map(x => (x, -1)))
      val edges = sc.parallelize(Seq.empty[Edge[Int]])
      val graph = Graph(vertices, edges)
      val sccGraph = StronglyConnectedComponents.run(graph, 5)
      for ((id, scc) <- sccGraph.vertices.collect) {
        assert(id == scc)
      }
    }
  }

  test("Cycle Strongly Connected Components") {
    withSpark { sc =>
      val rawEdges = sc.parallelize((0L to 6L).map(x => (x, (x + 1) % 7)))
      val graph = Graph.fromEdgeTuples(rawEdges, -1)
      val sccGraph = StronglyConnectedComponents.run(graph, 20)
      for ((id, scc) <- sccGraph.vertices.collect) {
        assert(0L == scc)
      }
    }
  }

  test("2 Cycle Strongly Connected Components") {
    withSpark { sc =>
      val edges =
        Array(0L -> 1L, 1L -> 2L, 2L -> 0L) ++
        Array(3L -> 4L, 4L -> 5L, 5L -> 3L) ++
        Array(6L -> 0L, 5L -> 7L)
      val rawEdges = sc.parallelize(edges)
      val graph = Graph.fromEdgeTuples(rawEdges, -1)
      val sccGraph = StronglyConnectedComponents.run(graph, 20)
      for ((id, scc) <- sccGraph.vertices.collect) {
        if (id < 3)
          assert(0L == scc)
        else if (id < 6)
          assert(3L == scc)
        else
          assert(id == scc)
      }
    }
  }

}
