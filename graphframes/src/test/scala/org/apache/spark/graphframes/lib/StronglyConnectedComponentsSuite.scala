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

package org.apache.spark.graphframes.lib

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.graphframes.GraphFrame
import org.apache.spark.graphframes.GraphFrameTestSparkContext
import org.apache.spark.graphframes.SparkFunSuite
import org.apache.spark.graphframes.TestUtils

class StronglyConnectedComponentsSuite extends SparkFunSuite with GraphFrameTestSparkContext {
  test("Island Strongly Connected Components") {
    val vertices = spark
      .createDataFrame(Seq((1L, "a"), (2L, "b"), (3L, "c"), (4L, "d"), (5L, "e")))
      .toDF("id", "value")
    val edges = spark.createDataFrame(Seq.empty[(Long, Long)]).toDF("src", "dst")
    val graph = GraphFrame(vertices, edges)
    val c = graph.stronglyConnectedComponents.maxIter(5).run()
    TestUtils.testSchemaInvariants(graph, c)
    TestUtils.checkColumnType(c.schema, "component", DataTypes.LongType)
    for (Row(id: Long, component: Long, _) <- c.select("id", "component", "value").collect()) {
      assert(id === component)
    }
    c.unpersist()
  }
}
