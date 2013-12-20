package org.apache.spark.graph.algorithms

import org.scalatest.FunSuite

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.graph._
import org.apache.spark.graph.util.GraphGenerators
import org.apache.spark.rdd._


class TriangleCountSuite extends FunSuite with LocalSparkContext {

  test("Count a single triangle") {
    withSpark { sc =>
      val rawEdges = sc.parallelize(Array( 0L->1L, 1L->2L, 2L->0L ), 2)
      val graph = Graph.fromEdgeTuples(rawEdges, true).cache()
      val triangleCount = TriangleCount.run(graph)
      val verts = triangleCount.vertices
      verts.collect.foreach { case (vid, count) => assert(count === 1) }
    }
  }

  test("Count two triangles") {
    withSpark { sc =>
      val triangles = Array(0L -> 1L, 1L -> 2L, 2L -> 0L) ++
        Array(0L -> -1L, -1L -> -2L, -2L -> 0L)
      val rawEdges = sc.parallelize(triangles, 2)
      val graph = Graph.fromEdgeTuples(rawEdges, true).cache()
      val triangleCount = TriangleCount.run(graph)
      val verts = triangleCount.vertices
      verts.collect().foreach { case (vid, count) =>
        if (vid == 0) {
          assert(count === 2)
        } else {
          assert(count === 1)
        }
      }
    }
  }

  test("Count two triangles with bi-directed edges") {
    withSpark { sc =>
      val triangles =
        Array(0L -> 1L, 1L -> 2L, 2L -> 0L) ++
        Array(0L -> -1L, -1L -> -2L, -2L -> 0L)
      val revTriangles = triangles.map { case (a,b) => (b,a) }
      val rawEdges = sc.parallelize(triangles ++ revTriangles, 2)
      val graph = Graph.fromEdgeTuples(rawEdges, true).cache()
      val triangleCount = TriangleCount.run(graph)
      val verts = triangleCount.vertices
      verts.collect().foreach { case (vid, count) =>
        if (vid == 0) {
          assert(count === 4)
        } else {
          assert(count === 2)
        }
      }
    }
  }

  test("Count a single triangle with duplicate edges") {
    withSpark { sc =>
      val rawEdges = sc.parallelize(Array(0L -> 1L, 1L -> 2L, 2L -> 0L) ++
        Array(0L -> 1L, 1L -> 2L, 2L -> 0L), 2)
      val graph = Graph.fromEdgeTuples(rawEdges, true, uniqueEdges = Some(RandomVertexCut)).cache()
      val triangleCount = TriangleCount.run(graph)
      val verts = triangleCount.vertices
      verts.collect.foreach { case (vid, count) => assert(count === 1) }
    }
  }

}
