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

import org.apache.spark.{SparkContext, SparkFunSuite}
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib._
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.rdd._

class TwoVertexShortestPathSuite extends SparkFunSuite with LocalSparkContext {

  test("Shortest Path Computations") {
    withSpark { sc =>

	val shortestPaths = Set(
	(5,Map(0 -> (2,List(5, 0)))),
	(2,Map()),
	(4,Map(0 -> (4,List(4, 5, 0)))),
	(1,Map()), (3,Map()),
	(0,Map(0 -> (0,List(0)))),
	(6,Map())
	)

      val vertexArray = Array(
	(0L, 0),
	(1L,1),
	(2L,2),
	(3L,3),
	(4L,4),
	(5L,5),
	(6L,6)
	)

      val edgeArray = Array(
	Edge(0L,  2L, 2L),
	Edge(0L,  4L, 2L),
	Edge(2L,  3L, 0L),
	Edge(3L,  6L, 0L),
	Edge(4L,  2L, 0L),
	Edge(4L,  5L, 2L),
	Edge(5L,  3L, 2L),
	Edge(5L,  6L, 2L),
	Edge(5L,  0L, 2L),
	Edge(5L,  6L, 0L)
	)

      val vertexRDD: RDD[(Long, Int)] = sc.parallelize(vertexArray)
      val edgeRDD: RDD[Edge[Long]] = sc.parallelize(edgeArray)
      val graph: Graph[Int, Long] = Graph(vertexRDD, edgeRDD)
      val toVid: VertexId = 0
      val fromVid: VertexId = 4
      val results = TwoVertexShortestPath.run(graph, fromVid, toVid).vertices.collect.map {
        case (v, spMap) => (v, spMap.mapValues(i => i))
      }
      assert(results.toSet === shortestPaths)
    }
  }

}
