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

package org.apache.spark.graphx.lib


import org.scalatest.{Matchers, FunSuite}

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib._
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.rdd._

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
    val normalizer = pr.sum
    (0L until (nRows * nCols)).zip(pr.map(p => p / normalizer))
  }

}


class PageRankSuite extends FunSuite with LocalSparkContext with Matchers {

  def compareRanks(a: VertexRDD[Double], b: VertexRDD[Double]): Double = {
    a.leftJoin(b) { case (id, a, bOpt) => (a - bOpt.getOrElse(0.0)) * (a - bOpt.getOrElse(0.0)) }
      .map { case (id, error) => error }.sum
  }
/*

  test("Static Star PageRank") {
    withSpark { sc =>
      val nVertices = 10
      val starGraph = GraphGenerators.starGraph(sc, nVertices).cache()
      val resetProb = 0.15
      val staticRanks: VertexRDD[Double] = starGraph.staticPageRank(numIter = 2, resetProb).vertices
      // Check the static pagerank
      val pageranks: Map[VertexId, Double] = staticRanks.collect().toMap
      val perimeter = (nVertices - 1.0) * resetProb
      val center = resetProb + (1.0 - resetProb) * perimeter
      val normalizer = perimeter + center
      pageranks(0) should equal((center / normalizer) +- 1.0e-7)
      for (i <- 1 until nVertices) {
        pageranks(i) should equal((resetProb / normalizer) +- 1.0e-7)
      }
    }
  }


  test("Dynamic Star PageRank") {
    withSpark { sc =>
      val nVertices = 10
      val starGraph = GraphGenerators.starGraph(sc, nVertices).cache()
      val resetProb = 0.15
      val dynamicRanks: VertexRDD[Double] = starGraph.pageRank(1.0e-10, resetProb).vertices
      // Check the pagerank values
      val pageranks: Map[VertexId, Double] = dynamicRanks.collect().toMap
      val perimeter = (nVertices - 1.0) * resetProb
      val center = resetProb + (1.0 - resetProb) * perimeter
      val normalizer = perimeter + center
      pageranks(0) should equal ((center / normalizer) +- 1.0e-7)
      for(i <- 1 until nVertices) {
        pageranks(i) should equal ((resetProb / normalizer) +- 1.0e-7)
      }
    }
  } // end of test Star PageRank


  test("Static Cycle PageRank") {
    withSpark { sc =>
      val nVertices = 10
      val starGraph = GraphGenerators.cycleGraph(sc, nVertices)
      val resetProb = 0.15
      val staticRanks: VertexRDD[Double] = starGraph.staticPageRank(numIter = 10, resetProb).vertices
      // Check the static pagerank
      val pageranks: Map[VertexId, Double] = staticRanks.collect().toMap
      for (i <- 0 until nVertices) {
        pageranks(i) should equal ( (1.0/nVertices) +- 1.0e-7)
      }
    }
  }


  test("Dynamic Cycle PageRank") {
    withSpark { sc =>
      val nVertices = 3
      val starGraph = GraphGenerators.cycleGraph(sc, nVertices)
      val resetProb = 0.15
      val staticRanks: VertexRDD[Double] = starGraph.pageRank(1.0e-3, resetProb).vertices
      // Check the static pagerank
      val pageranks: Map[VertexId, Double] = staticRanks.collect().toMap
      println(pageranks)
      for (i <- 0 until nVertices) {
        pageranks(i) should equal ( (1.0/nVertices) +- 1.0e-7)
      }
    }
  }
*/

  test("Grid Static PageRank") {
    withSpark { sc =>
      val rows = 5
      val cols = 5
      val resetProb = 0.15
      val tol = 0.0001
      val numIter = 20
      val errorTol = 1.0e-5
      val truePr = GridPageRank(rows, cols, numIter, resetProb).toMap
      val gridGraph = GraphGenerators.gridGraph(sc, rows, cols).cache()
      val vertices = gridGraph.staticPageRank(numIter, resetProb).vertices.cache()
      val pageranks: Map[VertexId, Double] = vertices.collect().toMap
      for ((k,pr) <- truePr) {
        pageranks(k) should equal ( pr +- 1.0e-5)
      }
    }
  } // end of Grid PageRank

/*
  test("Grid Dynamic PageRank") {
    withSpark { sc =>
      val rows = 5
      val cols = 5
      val resetProb = 0.15
      val tol = 0.0001
      val numIter = 20
      val errorTol = 1.0e-5
      val truePr = GridPageRank(rows, cols, numIter, resetProb).toMap
      val gridGraph = GraphGenerators.gridGraph(sc, rows, cols).cache()
      val vertices = gridGraph.pageRank(1.0e-10, resetProb).vertices.cache()
      val pageranks: Map[VertexId, Double] = vertices.collect().toMap
      //      vertices.collect.foreach(println(_))
      for ((k,pr) <- truePr) {
        pageranks(k) should equal ( pr +- 1.0e-5)
      }
    }
  } // end of Grid PageRank
*/

}
