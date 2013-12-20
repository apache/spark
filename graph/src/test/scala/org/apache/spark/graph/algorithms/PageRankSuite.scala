package org.apache.spark.graph.algorithms

import org.scalatest.FunSuite

import org.apache.spark.graph._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._

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


class PageRankSuite extends FunSuite with LocalSparkContext {

  def compareRanks(a: VertexRDD[Double], b: VertexRDD[Double]): Double = {
    a.leftJoin(b) { case (id, a, bOpt) => (a - bOpt.getOrElse(0.0)) * (a - bOpt.getOrElse(0.0)) }
      .map { case (id, error) => error }.sum
  }

  test("Star PageRank") {
    withSpark { sc =>
      val nVertices = 100
      val starGraph = GraphGenerators.starGraph(sc, nVertices).cache()
      val resetProb = 0.15
      val errorTol = 1.0e-5

      val staticRanks1 = PageRank.run(starGraph, numIter = 1, resetProb).vertices.cache()
      val staticRanks2 = PageRank.run(starGraph, numIter = 2, resetProb).vertices.cache()

      // Static PageRank should only take 2 iterations to converge
      val notMatching = staticRanks1.innerZipJoin(staticRanks2) { (vid, pr1, pr2) =>
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
    withSpark { sc =>
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
    withSpark { sc =>
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
}
