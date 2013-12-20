package org.apache.spark.graph.algorithms

import org.scalatest.FunSuite

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.graph._
import org.apache.spark.graph.util.GraphGenerators
import org.apache.spark.rdd._


class ConnectedComponentsSuite extends FunSuite with LocalSparkContext {

  test("Grid Connected Components") {
    withSpark { sc =>
      val gridGraph = GraphGenerators.gridGraph(sc, 10, 10).cache()
      val ccGraph = ConnectedComponents.run(gridGraph).cache()
      val maxCCid = ccGraph.vertices.map { case (vid, ccId) => ccId }.sum
      assert(maxCCid === 0)
    }
  } // end of Grid connected components


  test("Reverse Grid Connected Components") {
    withSpark { sc =>
      val gridGraph = GraphGenerators.gridGraph(sc, 10, 10).reverse.cache()
      val ccGraph = ConnectedComponents.run(gridGraph).cache()
      val maxCCid = ccGraph.vertices.map { case (vid, ccId) => ccId }.sum
      assert(maxCCid === 0)
    }
  } // end of Grid connected components


  test("Chain Connected Components") {
    withSpark { sc =>
      val chain1 = (0 until 9).map(x => (x, x+1) )
      val chain2 = (10 until 20).map(x => (x, x+1) )
      val rawEdges = sc.parallelize(chain1 ++ chain2, 3).map { case (s,d) => (s.toLong, d.toLong) }
      val twoChains = Graph.fromEdgeTuples(rawEdges, 1.0).cache()
      val ccGraph = ConnectedComponents.run(twoChains).cache()
      val vertices = ccGraph.vertices.collect()
      for ( (id, cc) <- vertices ) {
        if(id < 10) { assert(cc === 0) }
        else { assert(cc === 10) }
      }
      val ccMap = vertices.toMap
      for (id <- 0 until 20) {
        if (id < 10) {
          assert(ccMap(id) === 0)
        } else {
          assert(ccMap(id) === 10)
        }
      }
    }
  } // end of chain connected components

  test("Reverse Chain Connected Components") {
    withSpark { sc =>
      val chain1 = (0 until 9).map(x => (x, x+1) )
      val chain2 = (10 until 20).map(x => (x, x+1) )
      val rawEdges = sc.parallelize(chain1 ++ chain2, 3).map { case (s,d) => (s.toLong, d.toLong) }
      val twoChains = Graph.fromEdgeTuples(rawEdges, true).reverse.cache()
      val ccGraph = ConnectedComponents.run(twoChains).cache()
      val vertices = ccGraph.vertices.collect
      for ( (id, cc) <- vertices ) {
        if (id < 10) {
          assert(cc === 0)
        } else {
          assert(cc === 10)
        }
      }
      val ccMap = vertices.toMap
      for ( id <- 0 until 20 ) {
        if (id < 10) {
          assert(ccMap(id) === 0)
        } else {
          assert(ccMap(id) === 10)
        }
      }
    }
  } // end of reverse chain connected components

}
