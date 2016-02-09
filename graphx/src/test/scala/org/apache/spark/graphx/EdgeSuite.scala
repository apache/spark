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

class EdgeSuite extends SparkFunSuite {
  test ("compare") {
    // decending order
    val testEdges: Array[Edge[Int]] = Array(
      Edge(0x7FEDCBA987654321L, -0x7FEDCBA987654321L, 1),
      Edge(0x2345L, 0x1234L, 1),
      Edge(0x1234L, 0x5678L, 1),
      Edge(0x1234L, 0x2345L, 1),
      Edge(-0x7FEDCBA987654321L, 0x7FEDCBA987654321L, 1)
    )
    // to ascending order
    val sortedEdges = testEdges.sorted(Edge.lexicographicOrdering[Int])

    for (i <- 0 until testEdges.length) {
      assert(sortedEdges(i) == testEdges(testEdges.length - i - 1))
    }
  }
}
