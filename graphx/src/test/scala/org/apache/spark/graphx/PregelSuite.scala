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

import org.apache.spark.SparkFunSuite

class PregelSuite extends SparkFunSuite with LocalSparkContext {

  test("1 iteration") {
    withSpark { sc =>
      val n = 5
      val starEdges = (1 to n).map(x => (0: VertexId, x: VertexId))
      val star = Graph.fromEdgeTuples(sc.parallelize(starEdges, 3), "v").cache()
      val result = Pregel(star, 0)(
        (vid, attr, msg) => attr,
        et => Iterator.empty,
        (a: Int, b: Int) => throw new Exception("mergeMsg run unexpectedly"))
      assert(result.vertices.collect().toSet === star.vertices.collect().toSet)
    }
  }

  test("chain propagation") {
    withSpark { sc =>
      val n = 5
      val chain = Graph.fromEdgeTuples(
        sc.parallelize((1 until n).map(x => (x: VertexId, x + 1: VertexId)), 3),
        0).cache()
      assert(chain.vertices.collect().toSet === (1 to n).map(x => (x: VertexId, 0)).toSet)
      val chainWithSeed = chain.mapVertices { (vid, attr) => if (vid == 1) 1 else 0 }.cache()
      assert(chainWithSeed.vertices.collect().toSet ===
        Set((1: VertexId, 1)) ++ (2 to n).map(x => (x: VertexId, 0)).toSet)
      val result = Pregel(chainWithSeed, 0)(
        (vid, attr, msg) => math.max(msg, attr),
        et => if (et.dstAttr != et.srcAttr) Iterator((et.dstId, et.srcAttr)) else Iterator.empty,
        (a: Int, b: Int) => math.max(a, b))
      assert(result.vertices.collect().toSet ===
        chain.vertices.mapValues { (vid, attr) => attr + 1 }.collect().toSet)
    }
  }
}
