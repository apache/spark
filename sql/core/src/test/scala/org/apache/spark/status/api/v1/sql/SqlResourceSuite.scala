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

package org.apache.spark.status.api.v1.sql

import java.util.Date

import scala.collection.mutable.ArrayBuffer

import org.scalatest.PrivateMethodTester

import org.apache.spark.{JobExecutionStatus, SparkFunSuite}
import org.apache.spark.sql.execution.ui.{SparkPlanGraph, SparkPlanGraphCluster, SparkPlanGraphEdge, SparkPlanGraphNode, SQLExecutionUIData, SQLPlanMetric}

object SqlResourceSuite {

  val SCAN_TEXT = "Scan text"
  val FILTER = "Filter"
  val WHOLE_STAGE_CODEGEN_1 = "WholeStageCodegen (1)"
  val DURATION = "duration"
  val NUMBER_OF_OUTPUT_ROWS = "number of output rows"
  val METADATA_TIME = "metadata time"
  val NUMBER_OF_FILES_READ = "number of files read"
  val SIZE_OF_FILES_READ = "size of files read"
  val PLAN_DESCRIPTION = "== Physical Plan ==\nCollectLimit (3)\n+- * Filter (2)\n +- Scan text..."
  val DESCRIPTION = "csv at MyDataFrames.scala:57"

  val nodeIdAndWSCGIdMap: Map[Long, Option[Long]] = Map(1L -> Some(1L))

  val filterNode = new SparkPlanGraphNode(1, FILTER, "",
    metrics = Seq(SQLPlanMetric(NUMBER_OF_OUTPUT_ROWS, 1, "")))
  val nodes: Seq[SparkPlanGraphNode] = Seq(
    new SparkPlanGraphCluster(0, WHOLE_STAGE_CODEGEN_1, "",
      nodes = ArrayBuffer(filterNode),
      metrics = Seq(SQLPlanMetric(DURATION, 0, ""))),
    new SparkPlanGraphNode(2, SCAN_TEXT, "",
      metrics = Seq(
      SQLPlanMetric(METADATA_TIME, 2, ""),
      SQLPlanMetric(NUMBER_OF_FILES_READ, 3, ""),
      SQLPlanMetric(NUMBER_OF_OUTPUT_ROWS, 4, ""),
      SQLPlanMetric(SIZE_OF_FILES_READ, 5, ""))))

  val edges: Seq[SparkPlanGraphEdge] = Seq(SparkPlanGraphEdge(3, 2))

  val nodesWhenCodegenIsOff: Seq[SparkPlanGraphNode] =
    SparkPlanGraph(nodes, edges).allNodes.filterNot(_.name == WHOLE_STAGE_CODEGEN_1)

  val metrics: Seq[SQLPlanMetric] = {
    Seq(SQLPlanMetric(DURATION, 0, ""),
      SQLPlanMetric(NUMBER_OF_OUTPUT_ROWS, 1, ""),
      SQLPlanMetric(METADATA_TIME, 2, ""),
      SQLPlanMetric(NUMBER_OF_FILES_READ, 3, ""),
      SQLPlanMetric(NUMBER_OF_OUTPUT_ROWS, 4, ""),
      SQLPlanMetric(SIZE_OF_FILES_READ, 5, ""))
  }

  val sqlExecutionUIData: SQLExecutionUIData = {
    def getMetricValues() = {
      Map[Long, String](
        0L -> "0 ms",
        1L -> "1",
        2L -> "2 ms",
        3L -> "1",
        4L -> "1",
        5L -> "330.0 B"
      )
    }

    new SQLExecutionUIData(
      executionId = 0,
      description = DESCRIPTION,
      details = "",
      physicalPlanDescription = PLAN_DESCRIPTION,
      metrics = metrics,
      submissionTime = 1586768888233L,
      completionTime = Some(new Date(1586768888999L)),
      jobs = Map[Int, JobExecutionStatus](
        0 -> JobExecutionStatus.SUCCEEDED,
        1 -> JobExecutionStatus.SUCCEEDED),
      stages = Set[Int](),
      metricValues = getMetricValues()
    )
  }

  private def getNodes(): Seq[Node] = {
    val node = Node(0, WHOLE_STAGE_CODEGEN_1,
      wholeStageCodegenId = None, metrics = Seq(Metric(DURATION, "0 ms")))
    val node2 = Node(1, FILTER,
      wholeStageCodegenId = Some(1), metrics = Seq(Metric(NUMBER_OF_OUTPUT_ROWS, "1")))
    val node3 = Node(2, SCAN_TEXT, wholeStageCodegenId = None,
      metrics = Seq(Metric(METADATA_TIME, "2 ms"),
        Metric(NUMBER_OF_FILES_READ, "1"),
        Metric(NUMBER_OF_OUTPUT_ROWS, "1"),
        Metric(SIZE_OF_FILES_READ, "330.0 B")))

    // reverse order because of supporting execution order by aligning with Spark-UI
    Seq(node3, node2, node)
  }

  private def getExpectedNodesWhenWholeStageCodegenIsOff(): Seq[Node] = {
    val node = Node(1, FILTER, metrics = Seq(Metric(NUMBER_OF_OUTPUT_ROWS, "1")))
    val node2 = Node(2, SCAN_TEXT,
      metrics = Seq(Metric(METADATA_TIME, "2 ms"),
        Metric(NUMBER_OF_FILES_READ, "1"),
        Metric(NUMBER_OF_OUTPUT_ROWS, "1"),
        Metric(SIZE_OF_FILES_READ, "330.0 B")))

    // reverse order because of supporting execution order by aligning with Spark-UI
    Seq(node2, node)
  }

  private def verifyExpectedExecutionData(executionData: ExecutionData,
    nodes: Seq[Node],
    edges: Seq[SparkPlanGraphEdge],
    planDescription: String): Unit = {

    assert(executionData.id == 0)
    assert(executionData.status == "COMPLETED")
    assert(executionData.description == DESCRIPTION)
    assert(executionData.planDescription == planDescription)
    assert(executionData.submissionTime == new Date(1586768888233L))
    assert(executionData.duration == 766L)
    assert(executionData.successJobIds == Seq[Int](0, 1))
    assert(executionData.runningJobIds == Seq[Int]())
    assert(executionData.failedJobIds == Seq.empty)
    assert(executionData.nodes == nodes)
    assert(executionData.edges == edges)
  }

}

/**
 * Sql Resource Public API Unit Tests.
 */
class SqlResourceSuite extends SparkFunSuite with PrivateMethodTester {

  import SqlResourceSuite._

  val sqlResource = new SqlResource()
  val prepareExecutionData = PrivateMethod[ExecutionData]('prepareExecutionData)

  test("Prepare ExecutionData when details = false and planDescription = false") {
    val executionData =
      sqlResource invokePrivate prepareExecutionData(
        sqlExecutionUIData, SparkPlanGraph(Seq.empty, Seq.empty), false, false)
    verifyExpectedExecutionData(executionData, edges = Seq.empty,
      nodes = Seq.empty, planDescription = "")
  }

  test("Prepare ExecutionData when details = true and planDescription = false") {
    val executionData =
      sqlResource invokePrivate prepareExecutionData(
        sqlExecutionUIData, SparkPlanGraph(nodes, edges), true, false)
    verifyExpectedExecutionData(
      executionData,
      nodes = getNodes(),
      edges,
      planDescription = "")
  }

  test("Prepare ExecutionData when details = true and planDescription = true") {
    val executionData =
      sqlResource invokePrivate prepareExecutionData(
        sqlExecutionUIData, SparkPlanGraph(nodes, edges), true, true)
    verifyExpectedExecutionData(
      executionData,
      nodes = getNodes(),
      edges = edges,
      planDescription = PLAN_DESCRIPTION)
  }

  test("Prepare ExecutionData when details = true and planDescription = false and WSCG = off") {
    val executionData =
      sqlResource invokePrivate prepareExecutionData(
        sqlExecutionUIData, SparkPlanGraph(nodesWhenCodegenIsOff, edges), true, false)
    verifyExpectedExecutionData(
      executionData,
      nodes = getExpectedNodesWhenWholeStageCodegenIsOff(),
      edges = edges,
      planDescription = "")
  }

  test("Parse wholeStageCodegenId from nodeName") {
    val getWholeStageCodegenId = PrivateMethod[Option[Long]]('getWholeStageCodegenId)
    val wholeStageCodegenId =
      sqlResource invokePrivate getWholeStageCodegenId(WHOLE_STAGE_CODEGEN_1)
    assert(wholeStageCodegenId == Some(1))
  }

}
