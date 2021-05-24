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

import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods

import org.apache.spark.SparkConf
import org.apache.spark.deploy.history.HistoryServerSuite.getContentAndCode
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.execution.metric.SQLMetricsTestUtils
import org.apache.spark.sql.internal.SQLConf.ADAPTIVE_EXECUTION_ENABLED
import org.apache.spark.sql.test.SharedSparkSession

case class Person(id: Int, name: String, age: Int)
case class Salary(personId: Int, salary: Double)

/**
 * Sql Resource Public API Unit Tests running query and extracting the metrics.
 */
class SqlResourceWithActualMetricsSuite
  extends SharedSparkSession with SQLMetricsTestUtils with SQLHelper {

  import testImplicits._

  // Exclude nodes which may not have the metrics
  val excludedNodes = List("WholeStageCodegen", "Project", "SerializeFromObject")

  implicit val formats = new DefaultFormats {
    override def dateFormatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")
  }

  override def sparkConf: SparkConf = {
    super.sparkConf.set("spark.ui.enabled", "true")
  }

  test("Check Sql Rest Api Endpoints") {
    // Materalize result DataFrame
    withSQLConf(ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      val count = getDF().count()
      assert(count == 2, s"Expected Query Count is 2 but received: $count")
    }

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
