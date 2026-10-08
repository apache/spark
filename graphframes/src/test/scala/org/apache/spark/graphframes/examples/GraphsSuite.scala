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

package org.apache.spark.graphframes.examples

import org.apache.spark.graphframes.GraphFrameTestSparkContext
import org.apache.spark.graphframes.SparkFunSuite

class GraphsSuite extends SparkFunSuite with GraphFrameTestSparkContext {

  test("empty graph") {
    for (empty <- Seq(Graphs.empty[Int], Graphs.empty[Long], Graphs.empty[String])) {
      assert(empty.vertices.count() === 0L)
      assert(empty.edges.count() === 0L)
    }
  }

  test("chain graph") {
    val spark = this.spark
    import spark.implicits._

    val chain0 = Graphs.chain(0L)
    assert(chain0.vertices.count() === 0L)
    assert(chain0.edges.count() === 0L)

    val chain1 = Graphs.chain(1L)
    assert(chain1.vertices.as[Long].collect() === Array(0L))
    assert(chain1.edges.count() === 0L)

    val chain2 = Graphs.chain(2L)
    assert(chain2.vertices.as[Long].collect().toSet === Set(0L, 1L))
    assert(chain2.edges.as[(Long, Long)].collect() === Array((0L, 1L)))

    val chain3 = Graphs.chain(3L)
    assert(chain3.vertices.as[Long].collect().toSet === Set(0L, 1L, 2L))
    assert(chain3.edges.as[(Long, Long)].collect().toSet === Set((0L, 1L), (1L, 2L)))

    withClue("Constructing a large chain graph shouldn't OOM the driver.") {
      Graphs.chain(1e10.toLong)
    }
  }
}
