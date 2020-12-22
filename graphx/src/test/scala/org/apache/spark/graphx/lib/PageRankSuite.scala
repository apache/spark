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

import org.apache.spark.SparkFunSuite
import org.apache.spark.graphx._
import org.apache.spark.graphx.util.GraphGenerators


object GridPageRank {
  def apply(nRows: Int, nCols: Int, nIter: Int, resetProb: Double): Seq[(VertexId, Double)] = {
    val inNbrs = Array.fill(nRows * nCols)(collection.mutable.ArrayBuffer.empty[Int])
    val outDegree = Array.fill(nRows * nCols)(0)
    // Convert row column address into vertex ids (row major order)
    def sub2ind(r: Int, c: Int): Int = r * nCols + c
    // Make the grid graph
    for (r <- 0 until nRows; c <- 0 until nCols) {
      val ind = sub2ind(r, c)
      if (r + 1 < nRows) {
        outDegree(ind) += 1
        inNbrs(sub2ind(r + 1, c)) += ind
      }
      if (c + 1 < nCols) {
        outDegree(ind) += 1
        inNbrs(sub2ind(r, c + 1)) += ind
      }
    }
    // compute the pagerank
    var pr = Array.fill(nRows * nCols)(1.0)
    for (iter <- 0 until nIter) {
      val oldPr = pr
      pr = new Array[Double](nRows * nCols)
      for (ind <- 0 until (nRows * nCols)) {
        pr(ind) = resetProb + (1.0 - resetProb) *
          inNbrs(ind).map( nbr => oldPr(nbr) / outDegree(nbr)).sum
      }
    }
    val prSum = pr.sum
    (0L until (nRows * nCols)).zip(pr.map(_ * pr.length / prSum))
  }

}


class PageRankSuite extends SparkFunSuite with LocalSparkContext {

  def compareRanks(a: VertexRDD[Double], b: VertexRDD[Double]): Double = {
    a.leftJoin(b) { case (id, a, bOpt) => (a - bOpt.getOrElse(0.0)) * (a - bOpt.getOrElse(0.0)) }
      .map { case (id, error) => error }.sum()
  }

  def convergenceIterations[VD, ED](graph: Graph[VD, ED], resetProb: Double,
                                    tol: Double, errorTol: Double): (Int, Int) = {
    val dynamicRanks = graph.ops.pageRank(tol, resetProb).vertices.cache()

    // Compute how many iterations it takes to converge
    var iter = 1
    var staticGraphRank = graph.ops.staticPageRank(iter, resetProb).vertices.cache()
    while (!(compareRanks(staticGraphRank, dynamicRanks) < errorTol)) {
      iter += 1
      staticGraphRank = graph.ops.staticPageRank(iter, resetProb).vertices.cache()
    }
    val convergenceIter = iter
    val checkPointIter = convergenceIter / 2

    // CheckPoint the graph computed at half of these iterations
    val staticGraphRankPartial = graph.ops.staticPageRank(checkPointIter, resetProb)

    // Compute how many iterations it takes to converge when a checkPoint is provided
    var iterWithCheckPoint = 1
    var staticGraphRankWithCheckPoint = graph.ops.staticPageRank(iterWithCheckPoint,
      resetProb, staticGraphRankPartial).vertices.cache()
    while (compareRanks(staticGraphRankWithCheckPoint, dynamicRanks) >= errorTol) {
      iterWithCheckPoint += 1
      staticGraphRankWithCheckPoint = graph.ops.staticPageRank(iterWithCheckPoint,
        resetProb, staticGraphRankPartial).vertices.cache()
    }

    val convergenceIterWithCheckPoint = iterWithCheckPoint

    (convergenceIterWithCheckPoint, convergenceIter)
  }

  test("Star PageRank") {
    withSpark { sc =>
      val nVertices = 100
      val starGraph = GraphGenerators.starGraph(sc, nVertices).cache()
      val resetProb = 0.15
      val tol = 0.0001
      val numIter = 2
      val errorTol = 1.0e-5

      val staticRanks = starGraph.staticPageRank(numIter, resetProb).vertices.cache()
      val staticRanks2 = starGraph.staticPageRank(numIter + 1, resetProb).vertices

      // Static PageRank should only take 2 iterations to converge
      val notMatching = staticRanks.innerZipJoin(staticRanks2) { (vid, pr1, pr2) =>
        if (pr1 != pr2) 1 else 0
      }.map { case (vid, test) => test }.sum()
      assert(notMatching === 0)

      val dynamicRanks = starGraph.pageRank(tol, resetProb).vertices.cache()
      assert(compareRanks(staticRanks, dynamicRanks) < errorTol)

      // Computed in igraph 1.0 w/ R bindings:
      // > page_rank(make_star(100, mode = "in"))
      // Alternatively in NetworkX 1.11:
      // > nx.pagerank(nx.DiGraph([(x, 0) for x in range(1,100)]))
      // We multiply by the number of vertices to account for difference in normalization
      val centerRank = 0.462394787 * nVertices
      val othersRank = 0.005430356 * nVertices
      val igraphPR = centerRank +: Seq.fill(nVertices - 1)(othersRank)
      val ranks = VertexRDD(sc.parallelize(0L until nVertices zip igraphPR))
      assert(compareRanks(staticRanks, ranks) < errorTol)
      assert(compareRanks(dynamicRanks, ranks) < errorTol)

    }
  } // end of test Star PageRank

  test("Star PersonalPageRank") {
    withSpark { sc =>
      val nVertices = 100
      val starGraph = GraphGenerators.starGraph(sc, nVertices).cache()
      val resetProb = 0.15
      val tol = 0.0001
      val numIter = 2
      val errorTol = 1.0e-5

      val staticRanks = starGraph.staticPersonalizedPageRank(0, numIter, resetProb).vertices.cache()

      val dynamicRanks = starGraph.personalizedPageRank(0, tol, resetProb).vertices.cache()
      assert(compareRanks(staticRanks, dynamicRanks) < errorTol)

      val parallelStaticRanks = starGraph
        .staticParallelPersonalizedPageRank(Array(0), numIter, resetProb).mapVertices {
          case (vertexId, vector) => vector(0)
        }.vertices.cache()
      assert(compareRanks(staticRanks, parallelStaticRanks) < errorTol)

      // Computed in igraph 1.0 w/ R bindings:
      // > page_rank(make_star(100, mode = "in"),  personalized = c(1, rep(0, 99)), algo = "arpack")
      // NOTE: We use the arpack algorithm as prpack (the default) redistributes rank to all
      // vertices uniformly instead of just to the personalization source.
      // Alternatively in NetworkX 1.11:
      // > nx.pagerank(nx.DiGraph([(x, 0) for x in range(1,100)]),
      //   personalization=dict([(x, 1 if x == 0 else 0) for x in range(0,100)]))
      // We multiply by the number of vertices to account for difference in normalization
      val igraphPR0 = 1.0 +: Seq.fill(nVertices - 1)(0.0)
      val ranks0 = VertexRDD(sc.parallelize(0L until nVertices zip igraphPR0))
      assert(compareRanks(staticRanks, ranks0) < errorTol)
      assert(compareRanks(dynamicRanks, ranks0) < errorTol)


      // We have one outbound edge from 1 to 0
      val otherStaticRanks = starGraph.staticPersonalizedPageRank(1, numIter, resetProb)
        .vertices.cache()
      val otherDynamicRanks = starGraph.personalizedPageRank(1, tol, resetProb).vertices.cache()
      val otherParallelStaticRanks = starGraph
        .staticParallelPersonalizedPageRank(Array(0, 1), numIter, resetProb).mapVertices {
          case (vertexId, vector) => vector(1)
        }.vertices.cache()
      assert(compareRanks(otherDynamicRanks, otherStaticRanks) < errorTol)
      assert(compareRanks(otherStaticRanks, otherParallelStaticRanks) < errorTol)
      assert(compareRanks(otherDynamicRanks, otherParallelStaticRanks) < errorTol)

      // Computed in igraph 1.0 w/ R bindings:
      // > page_rank(make_star(100, mode = "in"),
      //   personalized = c(0, 1, rep(0, 98)), algo = "arpack")
      // NOTE: We use the arpack algorithm as prpack (the default) redistributes rank to all
      // vertices uniformly instead of just to the personalization source.
      // Alternatively in NetworkX 1.11:
      // > nx.pagerank(nx.DiGraph([(x, 0) for x in range(1,100)]),
      //   personalization=dict([(x, 1 if x == 1 else 0) for x in range(0,100)]))
      val centerRank = 0.4594595
      val sourceRank = 0.5405405
      val igraphPR1 = centerRank +: sourceRank +: Seq.fill(nVertices - 2)(0.0)
      val ranks1 = VertexRDD(sc.parallelize(0L until nVertices zip igraphPR1))
      assert(compareRanks(otherStaticRanks, ranks1) < errorTol)
      assert(compareRanks(otherDynamicRanks, ranks1) < errorTol)
      assert(compareRanks(otherParallelStaticRanks, ranks1) < errorTol)
    }
  } // end of test Star PersonalPageRank

  test("Grid PageRank") {
    withSpark { sc =>
      val rows = 10
      val cols = 10
      val resetProb = 0.15
      val tol = 0.0001
      val numIter = 50
      val errorTol = 1.0e-5
      val gridGraph = GraphGenerators.gridGraph(sc, rows, cols).cache()

      val staticRanks = gridGraph.staticPageRank(numIter, resetProb).vertices.cache()
      val dynamicRanks = gridGraph.pageRank(tol, resetProb).vertices.cache()
      val referenceRanks = VertexRDD(
        sc.parallelize(GridPageRank(rows, cols, numIter, resetProb))).cache()

      assert(compareRanks(staticRanks, referenceRanks) < errorTol)
      assert(compareRanks(dynamicRanks, referenceRanks) < errorTol)
    }
  } // end of Grid PageRank

  test("Grid PageRank with checkpoint") {
    withSpark { sc =>
      // Check that checkPointing helps the static PageRank to converge in less iterations
      val rows = 10
      val cols = 10
      val resetProb = 0.15
      val errorTol = 1.0e-5
      val tol = 0.0001
      val gridGraph = GraphGenerators.gridGraph(sc, rows, cols).cache()

      val (iterAfterHalfCheckPoint, totalIters) =
        convergenceIterations(gridGraph, resetProb, tol, errorTol)

      // In this case checkPoint does not help much
      assert(totalIters == 19)
      assert(iterAfterHalfCheckPoint == 18)
    }
  } // end of Grid PageRank

  test("Chain PageRank") {
    withSpark { sc =>
      val chain1 = (0 until 9).map(x => (x, x + 1))
      val rawEdges = sc.parallelize(chain1, 1).map { case (s, d) => (s.toLong, d.toLong) }
      val chain = Graph.fromEdgeTuples(rawEdges, 1.0).cache()
      val resetProb = 0.15
      val tol = 0.0001
      val numIter = 10
      val errorTol = 1.0e-5

      val staticRanks = chain.staticPageRank(numIter, resetProb).vertices
      val dynamicRanks = chain.pageRank(tol, resetProb).vertices

      assert(compareRanks(staticRanks, dynamicRanks) < errorTol)
    }
  }

  test("Chain PageRank with checkpoint") {
    withSpark { sc =>
      val chain1 = (0 until 9).map(x => (x, x + 1))
      val rawEdges = sc.parallelize(chain1, 1).map { case (s, d) => (s.toLong, d.toLong) }
      val chain = Graph.fromEdgeTuples(rawEdges, 1.0).cache()
      val resetProb = 0.15
      val errorTol = 1.0e-5
      val tol = 0.0001

      val (iterAfterHalfCheckPoint, totalIters) =
        convergenceIterations(chain, resetProb, tol, errorTol)

      // In this case checkPoint does not help but it does not take more iterations
      assert(totalIters == 10)
      assert(iterAfterHalfCheckPoint == 10)
    }
  } // end of Grid PageRank

  test("Chain PersonalizedPageRank") {
    withSpark { sc =>
      // Check that implementation can handle large vertexIds, SPARK-25149
      val vertexIdOffset = Int.MaxValue.toLong + 1
      val sourceOffset = 4
      val source = vertexIdOffset + sourceOffset
      val numIter = 10
      val vertices = vertexIdOffset until vertexIdOffset + numIter
      val chain1 = vertices.zip(vertices.tail)
      val rawEdges = sc.parallelize(chain1, 1).map { case (s, d) => (s.toLong, d.toLong) }
      val chain = Graph.fromEdgeTuples(rawEdges, 1.0).cache()
      val resetProb = 0.15
      val tol = 0.0001
      val errorTol = 1.0e-1

      val a = resetProb / (1 - Math.pow(1 - resetProb, numIter - sourceOffset))
      // We expect the rank to decay as (1 - resetProb) ^ distance
      val expectedRanks = sc.parallelize(vertices).map { vid =>
        val rank = if (vid < source) {
          0.0
        } else {
          a * Math.pow(1 - resetProb, vid - source)
        }
        vid -> rank
      }
      val expected = VertexRDD(expectedRanks)

      val staticRanks = chain.staticPersonalizedPageRank(source, numIter, resetProb).vertices
      assert(compareRanks(staticRanks, expected) < errorTol)

      val dynamicRanks = chain.personalizedPageRank(source, tol, resetProb).vertices
      assert(compareRanks(dynamicRanks, expected) < errorTol)

      val parallelStaticRanks = chain
        .staticParallelPersonalizedPageRank(Array(source), numIter, resetProb).mapVertices {
          case (vertexId, vector) => vector(0)
        }.vertices.cache()
      assert(compareRanks(parallelStaticRanks, expected) < errorTol)
    }
  }

  test("Loop with source PageRank") {
    withSpark { sc =>
      val edges = sc.parallelize((1L, 2L) :: (2L, 3L) :: (3L, 4L) :: (4L, 2L) :: Nil)
      val g = Graph.fromEdgeTuples(edges, 1)
      val resetProb = 0.15
      val tol = 0.0001
      val numIter = 50
      val errorTol = 1.0e-5

      val staticRanks = g.staticPageRank(numIter, resetProb).vertices
      val dynamicRanks = g.pageRank(tol, resetProb).vertices
      assert(compareRanks(staticRanks, dynamicRanks) < errorTol)

      // Computed in igraph 1.0 w/ R bindings:
      // > page_rank(graph_from_literal( A -+ B -+ C -+ D -+ B))
      // Alternatively in NetworkX 1.11:
      // > nx.pagerank(nx.DiGraph([(1,2),(2,3),(3,4),(4,2)]))
      // We multiply by the number of vertices to account for difference in normalization
      val igraphPR = Seq(0.0375000, 0.3326045, 0.3202138, 0.3096817).map(_ * 4)
      val ranks = VertexRDD(sc.parallelize(1L to 4L zip igraphPR))
      assert(compareRanks(staticRanks, ranks) < errorTol)
      assert(compareRanks(dynamicRanks, ranks) < errorTol)
    }
  }

  test("Loop with source PageRank with checkpoint") {
    withSpark { sc =>
      val edges = sc.parallelize((1L, 2L) :: (2L, 3L) :: (3L, 4L) :: (4L, 2L) :: Nil)
      val g = Graph.fromEdgeTuples(edges, 1)
      val resetProb = 0.15
      val tol = 0.0001
      val errorTol = 1.0e-5

      val (iterAfterHalfCheckPoint, totalIters) =
        convergenceIterations(g, resetProb, tol, errorTol)

      // In this case checkPoint helps a lot
      assert(totalIters == 34)
      assert(iterAfterHalfCheckPoint == 17)
    }
  }

  test("Loop with sink PageRank") {
    withSpark { sc =>
      val edges = sc.parallelize((1L, 2L) :: (2L, 3L) :: (3L, 1L) :: (1L, 4L) :: Nil)
      val g = Graph.fromEdgeTuples(edges, 1)
      val resetProb = 0.15
      val tol = 0.0001
      val numIter = 20
      val errorTol = 1.0e-5

      val staticRanks = g.staticPageRank(numIter, resetProb).vertices.cache()
      val dynamicRanks = g.pageRank(tol, resetProb).vertices.cache()

      assert(compareRanks(staticRanks, dynamicRanks) < errorTol)

      // Computed in igraph 1.0 w/ R bindings:
      // > page_rank(graph_from_literal( A -+ B -+ C -+ A -+ D))
      // Alternatively in NetworkX 1.11:
      // > nx.pagerank(nx.DiGraph([(1,2),(2,3),(3,1),(1,4)]))
      // We multiply by the number of vertices to account for difference in normalization
      val igraphPR = Seq(0.3078534, 0.2137622, 0.2646223, 0.2137622).map(_ * 4)
      val ranks = VertexRDD(sc.parallelize(1L to 4L zip igraphPR))
      assert(compareRanks(staticRanks, ranks) < errorTol)
      assert(compareRanks(dynamicRanks, ranks) < errorTol)

      val p1staticRanks = g.staticPersonalizedPageRank(1, numIter, resetProb).vertices.cache()
      val p1dynamicRanks = g.personalizedPageRank(1, tol, resetProb).vertices.cache()
      val p1parallelDynamicRanks =
        g.staticParallelPersonalizedPageRank(Array(1, 2, 3, 4), numIter, resetProb)
        .vertices.mapValues(v => v(0)).cache()

      // Computed in igraph 1.0 w/ R bindings:
      // > page_rank(graph_from_literal( A -+ B -+ C -+ A -+ D), personalized = c(1, 0, 0, 0),
      //   algo = "arpack")
      // NOTE: We use the arpack algorithm as prpack (the default) redistributes rank to all
      // vertices uniformly instead of just to the personalization source.
      // Alternatively in NetworkX 1.11:
      // > nx.pagerank(nx.DiGraph([(1,2),(2,3),(3,1),(1,4)]), personalization={1:1, 2:0, 3:0, 4:0})
      val igraphPR2 = Seq(0.4522329, 0.1921990, 0.1633691, 0.1921990)
      val ranks2 = VertexRDD(sc.parallelize(1L to 4L zip igraphPR2))
      assert(compareRanks(p1staticRanks, ranks2) < errorTol)
      assert(compareRanks(p1dynamicRanks, ranks2) < errorTol)
      assert(compareRanks(p1parallelDynamicRanks, ranks2) < errorTol)

    }
  }

  test("Loop with sink PageRank with checkpoint") {
    withSpark { sc =>
      val edges = sc.parallelize((1L, 2L) :: (2L, 3L) :: (3L, 1L) :: (1L, 4L) :: Nil)
      val g = Graph.fromEdgeTuples(edges, 1)
      val resetProb = 0.15
      val tol = 0.0001
      val errorTol = 1.0e-5

      val (iterAfterHalfCheckPoint, totalIters) =
        convergenceIterations(g, resetProb, tol, errorTol)

      // In this case checkPoint helps a lot
      assert(totalIters == 15)
      assert(iterAfterHalfCheckPoint == 9)
    }
  }
}
