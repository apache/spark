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
import org.apache.spark.rdd._


class KCoreSuite extends SparkFunSuite with LocalSparkContext {

  test("KCore on a Toy Connected Directional Graph") {
    withSpark { sc =>
      val edges =
        Array(2L -> 5L, 5L -> 3L, 5L -> 0L) ++
          Array(3L -> 7L, 4L -> 0L, 7L -> 0L) ++
          Array(7L -> 8L, 8L -> 9L, 5L -> 7L)

      val rawEdges = sc.parallelize(edges)
      val graph = Graph.fromEdgeTuples(rawEdges, -1L)
      // Edges are:
      //   2 ---> 5 ---> 3
      //          | \    |
      //          V    \ |
      //   4 ---> 0 <--- 7 ---> 8 ---> 9
      //
      val ccGraph = graph.kCore(2)
      val verticesOutput = ccGraph.vertices.collect()

      val expected = Array((4,false), (0,true), (3,true), (7,true), (9,false), (8,false), (5,true), (2,false))
      assert(verticesOutput.deep == expected.deep)

    }
  } // end of toy KCore Directional Graph

  test("KCore on a Toy Connected Bi-Directional Graph") {
    withSpark { sc =>
      val edges1 =
        Array(2L -> 5L, 5L -> 3L, 5L -> 0L) ++
          Array(3L -> 7L, 4L -> 0L, 7L -> 0L) ++
          Array(7L -> 8L, 8L -> 9L, 5L -> 7L)

      val edges = edges1 ++ edges1.map(_.swap)

      val rawEdges = sc.parallelize(edges)
      val graph = Graph.fromEdgeTuples(rawEdges, -1L)
      // Edges are:
      //   2 ---- 5 ---- 3
      //          | \    |
      //          |    \ |
      //   4 ---- 0 ---- 7 ---- 8 ---- 9
      //
      val ccGraph = graph.kCore(3)
      val verticesOutput = ccGraph.vertices.collect()

      val expected = Array((4,false), (0,true), (7,true), (3,true), (9,false), (8,false), (5,true), (2,false))
      assert(verticesOutput.deep == expected.deep)

    }
  } // end of toy KCore Bi-Directional Graph

}
