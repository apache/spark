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

import java.net.URL
import java.text.SimpleDateFormat
import java.util.Date

import scala.collection.mutable.ArrayBuffer

import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods
import org.scalatest.PrivateMethodTester

import org.apache.spark.{JobExecutionStatus, SparkConf, SparkFunSuite}
import org.apache.spark.deploy.history.HistoryServerSuite.getContentAndCode
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.metric.SQLMetricsTestUtils
import org.apache.spark.sql.execution.ui.{SQLExecutionUIData, SQLPlanMetric, SparkPlanGraph, SparkPlanGraphCluster, SparkPlanGraphEdge, SparkPlanGraphNode}
import org.apache.spark.sql.test.SharedSparkSession

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

case class Person(id: Int, name: String, age: Int)
case class Salary(personId: Int, salary: Double)

/**
  * Sql Resource Public API Unit Tests running query and extracting the metrics.
  */
class SqlResourceWithActualMetricsSuite extends SharedSparkSession with SQLMetricsTestUtils {

  import testImplicits._

  // Exclude nodes which may not have the metrics
  val excludedNodes = List("WholeStageCodegen", "Project", "SerializeFromObject")

  implicit val formats = new DefaultFormats {
    override def dateFormatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")
  }

  override def sparkConf = {
    new SparkConf()
      .set("spark.ui.enabled", "true")
  }

  test("Check Sql Rest Api Endpoints") {
    // Materalize result DataFrame
    val count = getDF().count()
    assert(count == 2, s"Expected Query Count is 2 but received: $count")

    // Spark apps launched by local-mode seems not having `attemptId` as default
    // so UT is just added for existing endpoints.
    val executionId = callSqlRestEndpointAndVerifyResult()
    callSqlRestEndpointByExecutionIdAndVerifyResult(executionId)
  }

  private def callSqlRestEndpointAndVerifyResult(): Long = {
    val url = new URL(spark.sparkContext.ui.get.webUrl
      + s"/api/v1/applications/${spark.sparkContext.applicationId}/sql")
    val jsonResult = verifyAndGetSqlRestResult(url)
    val executionDatas = JsonMethods.parse(jsonResult).extract[Seq[ExecutionData]]
    assert(executionDatas.size > 0,
      s"Expected Query Result Size is higher than 0 but received: ${executionDatas.size}")
    val executionData = executionDatas.head
    verifySqlRestContent(executionData)
    executionData.id
  }

  private def callSqlRestEndpointByExecutionIdAndVerifyResult(executionId: Long): Unit = {
    val url = new URL(spark.sparkContext.ui.get.webUrl
      + s"/api/v1/applications/${spark.sparkContext.applicationId}/sql/${executionId}")
    val jsonResult = verifyAndGetSqlRestResult(url)
    val executionData = JsonMethods.parse(jsonResult).extract[ExecutionData]
    verifySqlRestContent(executionData)
  }

  private def verifySqlRestContent(executionData: ExecutionData): Unit = {
    assert(executionData.status == "COMPLETED",
      s"Expected status is COMPLETED but actual: ${executionData.status}")
    assert(executionData.successJobIds.nonEmpty,
      s"Expected successJobIds should not be empty")
    assert(executionData.runningJobIds.isEmpty,
      s"Expected runningJobIds should be empty but actual: ${executionData.runningJobIds}")
    assert(executionData.failedJobIds.isEmpty,
      s"Expected failedJobIds should be empty but actual: ${executionData.failedJobIds}")
    assert(executionData.nodes.nonEmpty, "Expected nodes should not be empty}")
    executionData.nodes.filterNot(node => excludedNodes.contains(node.nodeName)).foreach { node =>
      assert(node.metrics.nonEmpty, "Expected metrics of nodes should not be empty")
    }
  }

  private def verifyAndGetSqlRestResult(url: URL): String = {
    val (code, resultOpt, error) = getContentAndCode(url)
    assert(code == 200, s"Expected Http Response Code is 200 but received: $code for url: $url")
    assert(resultOpt.nonEmpty, s"Rest result should not be empty for url: $url")
    assert(error.isEmpty, s"Error message should be empty for url: $url")
    resultOpt.get
  }

  private def getDF(): DataFrame = {
    val person: DataFrame =
      spark.sparkContext.parallelize(
        Person(0, "mike", 30) ::
          Person(1, "jim", 20) :: Nil).toDF()

    val salary: DataFrame =
      spark.sparkContext.parallelize(
        Salary(0, 2000.0) ::
          Salary(1, 1000.0) :: Nil).toDF()

    val salaryDF = salary.withColumnRenamed("personId", "id")
    val ds = person.join(salaryDF, "id")
      .groupBy("name", "age", "salary").avg("age", "salary")
      .filter(_.getAs[Int]("age") <= 30)
      .sort()

    ds.toDF
  }

}
