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

import org.apache.spark.graphx._
import org.apache.spark.util.Utils
import org.scalatest.FunSuite

class AllPairsShortestPathsSuite extends FunSuite with LocalSparkContext {

  test("3-cycle") {
    withSpark { sc =>
      val edges = sc.parallelize(List(Edge(0L, 1L, 1), Edge(1L, 2L, 1), Edge(2L, 0L, 1)))
      val graph = Graph.fromEdges(edges, 1)
      val dists = AllPairsShortestPaths.run(graph)
      assert(dists.collect.toSet ===
        (for (src <- 0L to 2L; dst <- 0L to 2L) yield
          if (src == dst) (src, (dst, 0)) else (src, (dst, 1))).toSet)
    }
  }

  test("long chain") {
    withSpark { sc =>
      sc.setCheckpointDir(Utils.getLocalDir(sc.getConf))
      val n = 100
      val edges = sc.parallelize((0 until n).map(x => Edge(x, x + 1, 1)))
      val graph = Graph.fromEdges(edges, 1)
      val dists = AllPairsShortestPaths.run(graph)
      assert(dists.collect.toSet ===
        (for (src <- 0L to n; dst <- 0L to n) yield (src, (dst, math.abs(src - dst)))).toSet)
    }
  }

}
