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

package org.apache.spark.graphx.util

import org.scalatest.FunSuite

import org.apache.spark.graphx.LocalSparkContext

class GraphGeneratorsSuite extends FunSuite with LocalSparkContext {

  test("GraphGenerators.generateRandomEdges") {
    val src = 5
    val numEdges10 = 10
    val numEdges20 = 20
    val maxVertexId = 100

    val edges10 = GraphGenerators.generateRandomEdges(src, numEdges10, maxVertexId)
    assert(edges10.length == numEdges10)

    val correctSrc = edges10.forall(e => e.srcId == src)
    assert(correctSrc)

    val correctWeight = edges10.forall(e => e.attr == 1)
    assert(correctWeight)

    val correctRange = edges10.forall(e => e.dstId >= 0 && e.dstId <= maxVertexId)
    assert(correctRange)

    val edges20 = GraphGenerators.generateRandomEdges(src, numEdges20, maxVertexId)
    assert(edges20.length == numEdges20)

    val edges10_round1 =
      GraphGenerators.generateRandomEdges(src, numEdges10, maxVertexId, seed = 12345)
    val edges10_round2 =
      GraphGenerators.generateRandomEdges(src, numEdges10, maxVertexId, seed = 12345)
    assert(edges10_round1.zip(edges10_round2).forall { case (e1, e2) =>
      e1.srcId == e2.srcId && e1.dstId == e2.dstId && e1.attr == e2.attr
    })

    val edges10_round3 =
      GraphGenerators.generateRandomEdges(src, numEdges10, maxVertexId, seed = 3467)
    assert(!edges10_round1.zip(edges10_round3).forall { case (e1, e2) =>
      e1.srcId == e2.srcId && e1.dstId == e2.dstId && e1.attr == e2.attr
    })
  }

  test("GraphGenerators.sampleLogNormal") {
    val mu = 4.0
    val sigma = 1.3
    val maxVal = 100

    val trials = 1000
    for (i <- 1 to trials) {
      val dstId = GraphGenerators.sampleLogNormal(mu, sigma, maxVal)
      assert(dstId < maxVal)
    }

    val dstId_round1 = GraphGenerators.sampleLogNormal(mu, sigma, maxVal, 12345)
    val dstId_round2 = GraphGenerators.sampleLogNormal(mu, sigma, maxVal, 12345)
    assert(dstId_round1 == dstId_round2)

    val dstId_round3 = GraphGenerators.sampleLogNormal(mu, sigma, maxVal, 789)
    assert(dstId_round1 != dstId_round3)
  }

  test("GraphGenerators.logNormalGraph") {
    withSpark { sc =>
      val mu = 4.0
      val sigma = 1.3
      val numVertices100 = 100

      val graph = GraphGenerators.logNormalGraph(sc, numVertices100, mu = mu, sigma = sigma)
      assert(graph.vertices.count() == numVertices100)

      val graph_round1 =
        GraphGenerators.logNormalGraph(sc, numVertices100, mu = mu, sigma = sigma, seed = 12345)
      val graph_round2 =
        GraphGenerators.logNormalGraph(sc, numVertices100, mu = mu, sigma = sigma, seed = 12345)

      val graph_round1_edges = graph_round1.edges.collect()
      val graph_round2_edges = graph_round2.edges.collect()

      assert(graph_round1_edges.zip(graph_round2_edges).forall { case (e1, e2) =>
        e1.srcId == e2.srcId && e1.dstId == e2.dstId && e1.attr == e2.attr
      })

      val graph_round3 =
        GraphGenerators.logNormalGraph(sc, numVertices100, mu = mu, sigma = sigma, seed = 567)

      val graph_round3_edges = graph_round3.edges.collect()

      assert(!graph_round1_edges.zip(graph_round3_edges).forall { case (e1, e2) =>
        e1.srcId == e2.srcId && e1.dstId == e2.dstId && e1.attr == e2.attr
      })
    }
  }

  test("SPARK-5064 GraphGenerators.rmatGraph numEdges upper bound") {
    withSpark { sc =>
      val g1 = GraphGenerators.rmatGraph(sc, 4, 4)
      assert(g1.edges.count() === 4)
      intercept[IllegalArgumentException] {
        val g2 = GraphGenerators.rmatGraph(sc, 4, 8)
      }
    }
  }

}
