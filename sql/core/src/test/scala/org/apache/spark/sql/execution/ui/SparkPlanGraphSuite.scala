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
    val expectedDotNode = "  24 [id=\"node24\" label=\"" +
      "Scan JDBCRelation(\\\"test-schema\\\".tickets) [numPartitions=1]\" " +
      "tooltip=\"Scan JDBCRelation(\\\"test-schema\\\".tickets) [numPartitions=1] [ticket_no#0] " +
      "PushedFilters: [], ReadSchema: struct<ticket_no:string>\"];"

    assertResult(expectedDotNode)(dotNode)
  }

  test("SPARK-55786: makeNodeDetailsJson uses cluster prefix and includes children") {
    import scala.collection.mutable
    val childNode = new SparkPlanGraphNode(
      id = 1, name = "HashAggregate", desc = "HashAggregate(keys=[])",
      metrics = Seq(SQLPlanMetric("peak memory", 10, "size")))
    val cluster = new SparkPlanGraphCluster(
      id = 2, name = "WholeStageCodegen (1)", desc = "WholeStageCodegen (1)",
      nodes = mutable.ArrayBuffer(childNode),
      metrics = Seq(SQLPlanMetric("duration", 20, "timing")))
    val graph = SparkPlanGraph(Seq(cluster), Seq.empty)
    val json = graph.makeNodeDetailsJson(Map(10L -> "256.0 KiB", 20L -> "5 ms"))
    assert(json.contains("\"cluster2\""), "cluster should use cluster prefix")
    assert(json.contains("\"node1\""), "child should use node prefix")
    assert(json.contains("\"children\":[\"node1\"]"),
      "cluster should list children")
    assert(!json.contains("\"node2\""),
      "cluster should not use node prefix")
    assert(json.contains("\"desc\":\"HashAggregate(keys=[])\""),
      "node should include desc field")
    assert(json.contains("\"desc\":\"WholeStageCodegen (1)\""),
      "cluster should include desc field")
  }

  test("SPARK-56331: truncateLabel returns short labels unchanged") {
    assert(SparkPlanGraph.truncateLabel("Sort") === "Sort")
    assert(SparkPlanGraph.truncateLabel("Filter") === "Filter")
    assert(SparkPlanGraph.truncateLabel("HashAggregate") === "HashAggregate")
    assert(SparkPlanGraph.truncateLabel("SortMergeJoin Inner") === "SortMergeJoin Inner")
    assert(SparkPlanGraph.truncateLabel(null) === null)
    assert(SparkPlanGraph.truncateLabel("") === "")
  }

  test("SPARK-56331: truncateLabel does not truncate long operator names") {
    // Long operator names without qualified paths should NOT be truncated
    val insertCmd = "Execute InsertIntoHadoopFsRelationCommand"
    assert(SparkPlanGraph.truncateLabel(insertCmd) === insertCmd)
    val longOp = "BroadcastHashJoin Inner BuildRight with very long hint"
    assert(SparkPlanGraph.truncateLabel(longOp) === longOp)
  }

  test("SPARK-56331: truncateLabel truncates qualified catalog paths") {
    val exactly36 = "Scan spark_catalog.default.store_sal"
    assert(SparkPlanGraph.truncateLabel(exactly36) === exactly36)

    val longScan =
      "Scan parquet spark_catalog.very_long_schema_name.catalog_sales"
    val truncated = SparkPlanGraph.truncateLabel(longScan)
    assert(truncated.length <= 36,
      s"Truncated label should be <= 36 chars but was ${truncated.length}")
    assert(truncated.startsWith("Scan parquet spa"),
      "Should preserve operator prefix")
    assert(truncated.endsWith("catalog_sales"),
      "Should preserve table name at the end")
    assert(truncated.contains("..."),
      "Should contain ellipsis in the middle")
  }

  test("SPARK-56331: DOT label truncated, tooltip keeps full name") {
    val longName = "ScanTransformer parquet " +
      "spark_catalog.abcdefghijklmnopqrstuvwxyz.store_sales"
    val node = new SparkPlanGraphNode(
      id = 1, name = longName, desc = longName,
      metrics = Seq.empty)
    val dot = node.makeDotNode(Map.empty)
    assert(dot.contains("..."), "DOT label should contain ellipsis")
    assert(dot.contains("tooltip=\"" + longName + "\""),
      "Tooltip should contain the full name")
  }

  test("SPARK-56331: JSON details keep full name") {
    val longName = "ScanTransformer parquet " +
      "spark_catalog.abcdefghijklmnopqrstuvwxyz.store_sales"
    val node = new SparkPlanGraphNode(
      id = 1, name = longName, desc = longName,
      metrics = Seq.empty)
    val graph = SparkPlanGraph(Seq(node), Seq.empty)
    val json = graph.makeNodeDetailsJson(Map.empty)
    assert(json.contains(longName),
      "JSON details should contain the full untruncated name")
  }
}
