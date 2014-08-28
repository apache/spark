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

package org.apache.spark.graphx

import org.scalatest.FunSuite

import org.apache.spark.SparkContext
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.rdd._

class GraphGeneratorsSuite extends FunSuite with LocalSparkContext {

  test("GraphGenerators.generateRandomEdges") {
    val src = 5
    val numEdges10 = 10
    val numEdges20 = 20
    val maxVertexId = 100

    val edges10 = GraphGenerators.generateRandomEdges(src, numEdges10, maxVertexId)
    assert(edges10.length == numEdges10)

    val correctSrc = edges10.forall( e => e.srcId == src )
    assert(correctSrc)

    val correctWeight = edges10.forall( e => e.attr == 1 )
    assert(correctWeight)

    val correctRange = edges10.forall( e => e.dstId >= 0 && e.dstId <= maxVertexId )
    assert(correctRange)

    val edges20 = GraphGenerators.generateRandomEdges(src, numEdges20, maxVertexId)
    assert(edges20.length == numEdges20)

    val edges10_round1 = GraphGenerators.generateRandomEdges(src, numEdges10, maxVertexId, seed=12345)
    val edges10_round2 = GraphGenerators.generateRandomEdges(src, numEdges10, maxVertexId, seed=12345)
    assert(edges10_round1 == edges10_round2)

  }

}
