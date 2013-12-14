package org.apache.spark.graph

import org.scalatest.FunSuite

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.graph.algorithms._
import org.apache.spark.rdd._

import org.apache.spark.graph.LocalSparkContext._

import org.apache.spark.graph.util.GraphGenerators


object GridPageRank {
  def apply(nRows: Int, nCols: Int, nIter: Int, resetProb: Double) = {
    val inNbrs = Array.fill(nRows * nCols)(collection.mutable.MutableList.empty[Int])
    val outDegree = Array.fill(nRows * nCols)(0)
    // Convert row column address into vertex ids (row major order)
    def sub2ind(r: Int, c: Int): Int = r * nCols + c
    // Make the grid graph
    for (r <- 0 until nRows; c <- 0 until nCols) {
      val ind = sub2ind(r,c)
      if (r+1 < nRows) {
        outDegree(ind) += 1
        inNbrs(sub2ind(r+1,c)) += ind
      }
      if (c+1 < nCols) {
        outDegree(ind) += 1
        inNbrs(sub2ind(r,c+1)) += ind
      }
    }
    // compute the pagerank
    var pr = Array.fill(nRows * nCols)(resetProb)
    for (iter <- 0 until nIter) {
      val oldPr = pr
      pr = new Array[Double](nRows * nCols)
      for (ind <- 0 until (nRows * nCols)) {
        pr(ind) = resetProb + (1.0 - resetProb) *
          inNbrs(ind).map( nbr => oldPr(nbr) / outDegree(nbr)).sum
      }
    }
    (0L until (nRows * nCols)).zip(pr)
  }

}


class AnalyticsSuite extends FunSuite with LocalSparkContext {

  System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  System.setProperty("spark.kryo.registrator", "org.apache.spark.graph.GraphKryoRegistrator")

  def compareRanks(a: VertexRDD[Double], b: VertexRDD[Double]): Double = {
    a.leftJoin(b) { case (id, a, bOpt) => (a - bOpt.getOrElse(0.0)) * (a - bOpt.getOrElse(0.0)) }
      .map { case (id, error) => error }.sum
  }

  test("Star PageRank") {
    withSpark(new SparkContext("local", "test")) { sc =>
      val nVertices = 100
      val starGraph = GraphGenerators.starGraph(sc, nVertices).cache()
      val resetProb = 0.15
      val errorTol = 1.0e-5

      val staticRanks1 = PageRank.run(starGraph, numIter = 1, resetProb).vertices.cache()
      val staticRanks2 = PageRank.run(starGraph, numIter = 2, resetProb).vertices.cache()

      // Static PageRank should only take 2 iterations to converge
      val notMatching = staticRanks1.zipJoin(staticRanks2) { (vid, pr1, pr2) =>
        if (pr1 != pr2) 1 else 0
      }.map { case (vid, test) => test }.sum
      assert(notMatching === 0)

      val staticErrors = staticRanks2.map { case (vid, pr) =>
        val correct = (vid > 0 && pr == resetProb) ||
          (vid == 0 && math.abs(pr - (resetProb + (1.0 - resetProb) * (resetProb * (nVertices - 1)) )) < 1.0E-5)
        if (!correct) 1 else 0
      }
      assert(staticErrors.sum === 0)

      val dynamicRanks = PageRank.runUntillConvergence(starGraph, 0, resetProb).vertices.cache()
      val standaloneRanks = PageRank.runStandalone(starGraph, 0, resetProb).cache()
      assert(compareRanks(staticRanks2, dynamicRanks) < errorTol)
      assert(compareRanks(staticRanks2, standaloneRanks) < errorTol)
    }
  } // end of test Star PageRank



  test("Grid PageRank") {
    withSpark(new SparkContext("local", "test")) { sc =>
      val rows = 10
      val cols = 10
      val resetProb = 0.15
      val tol = 0.0001
      val numIter = 50
      val errorTol = 1.0e-5
      val gridGraph = GraphGenerators.gridGraph(sc, rows, cols).cache()

      val staticRanks = PageRank.run(gridGraph, numIter, resetProb).vertices.cache()
      val dynamicRanks = PageRank.runUntillConvergence(gridGraph, tol, resetProb).vertices.cache()
      val standaloneRanks = PageRank.runStandalone(gridGraph, tol, resetProb).cache()
      val referenceRanks = VertexRDD(sc.parallelize(GridPageRank(rows, cols, numIter, resetProb)))

      assert(compareRanks(staticRanks, referenceRanks) < errorTol)
      assert(compareRanks(dynamicRanks, referenceRanks) < errorTol)
      assert(compareRanks(standaloneRanks, referenceRanks) < errorTol)
    }
  } // end of Grid PageRank


  test("Chain PageRank") {
    withSpark(new SparkContext("local", "test")) { sc =>
      val chain1 = (0 until 9).map(x => (x, x+1) )
      val rawEdges = sc.parallelize(chain1, 1).map { case (s,d) => (s.toLong, d.toLong) }
      val chain = Graph.fromEdgeTuples(rawEdges, 1.0).cache()
      val resetProb = 0.15
      val tol = 0.0001
      val numIter = 10
      val errorTol = 1.0e-5

      val staticRanks = PageRank.run(chain, numIter, resetProb).vertices.cache()
      val dynamicRanks = PageRank.runUntillConvergence(chain, tol, resetProb).vertices.cache()
      val standaloneRanks = PageRank.runStandalone(chain, tol, resetProb).cache()

      assert(compareRanks(staticRanks, dynamicRanks) < errorTol)
      assert(compareRanks(dynamicRanks, standaloneRanks) < errorTol)
    }
  }


  test("Grid Connected Components") {
    withSpark(new SparkContext("local", "test")) { sc =>
      val gridGraph = GraphGenerators.gridGraph(sc, 10, 10).cache()
      val ccGraph = ConnectedComponents.run(gridGraph).cache()
      val maxCCid = ccGraph.vertices.map { case (vid, ccId) => ccId }.sum
      assert(maxCCid === 0)
    }
  } // end of Grid connected components


  test("Reverse Grid Connected Components") {
    withSpark(new SparkContext("local", "test")) { sc =>
      val gridGraph = GraphGenerators.gridGraph(sc, 10, 10).reverse.cache()
      val ccGraph = ConnectedComponents.run(gridGraph).cache()
      val maxCCid = ccGraph.vertices.map { case (vid, ccId) => ccId }.sum
      assert(maxCCid === 0)
    }
  } // end of Grid connected components


  test("Chain Connected Components") {
    withSpark(new SparkContext("local", "test")) { sc =>
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
    withSpark(new SparkContext("local", "test")) { sc =>
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

  test("Count a single triangle") {
    withSpark(new SparkContext("local", "test")) { sc =>
      val rawEdges = sc.parallelize(Array( 0L->1L, 1L->2L, 2L->0L ), 2)
      val graph = Graph.fromEdgeTuples(rawEdges, true).cache()
      val triangleCount = TriangleCount.run(graph)
      val verts = triangleCount.vertices
      verts.collect.foreach { case (vid, count) => assert(count === 1) }
    }
  }

  test("Count two triangles") {
    withSpark(new SparkContext("local", "test")) { sc =>
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
    withSpark(new SparkContext("local", "test")) { sc =>
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
    withSpark(new SparkContext("local", "test")) { sc =>
      val rawEdges = sc.parallelize(Array(0L -> 1L, 1L -> 2L, 2L -> 0L) ++
        Array(0L -> 1L, 1L -> 2L, 2L -> 0L), 2)
      val graph = Graph.fromEdgeTuples(rawEdges, true).cache()
      val triangleCount = TriangleCount.run(graph)
      val verts = triangleCount.vertices
      verts.collect.foreach { case (vid, count) => assert(count === 1) }
    }
  }
} // end of AnalyticsSuite
