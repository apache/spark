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
package org.apache.spark.sql.execution.ui

import org.apache.spark.SparkFunSuite

class SparkPlanGraphSuite extends SparkFunSuite {
  test("SPARK-47503: name of a node should be escaped even if there is no metrics") {
    val planGraphNode = new SparkPlanGraphNode(
      id = 24,
      name = "Scan JDBCRelation(\"test-schema\".tickets) [numPartitions=1]",
      desc = "Scan JDBCRelation(\"test-schema\".tickets) [numPartitions=1] " +
        "[ticket_no#0] PushedFilters: [], ReadSchema: struct<ticket_no:string>",
      metrics = List(
        SQLPlanMetric(
          name = "number of output rows",
          accumulatorId = 75,
          metricType = "sum"
        ),
        SQLPlanMetric(
          name = "JDBC query execution time",
          accumulatorId = 35,
          metricType = "nsTiming")))
    val dotNode = planGraphNode.makeDotNode(Map.empty[Long, String])
    val expectedDotNode = "  24 [id=\"node24\" labelType=\"html\" label=\"" +
      "<br><b>Scan JDBCRelation(\\\"test-schema\\\".tickets) [numPartitions=1]</b><br><br>\" " +
      "tooltip=\"Scan JDBCRelation(\\\"test-schema\\\".tickets) [numPartitions=1] [ticket_no#0] " +
      "PushedFilters: [], ReadSchema: struct<ticket_no:string>\"];"

    assertResult(expectedDotNode)(dotNode)
  }
}
