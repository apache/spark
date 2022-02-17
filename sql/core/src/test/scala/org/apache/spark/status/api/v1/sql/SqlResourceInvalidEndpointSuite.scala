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

import org.apache.spark.SparkConf
import org.apache.spark.deploy.history.HistoryServerSuite.getContentAndCode
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.execution.metric.SQLMetricsTestUtils
import org.apache.spark.sql.internal.SQLConf.ADAPTIVE_EXECUTION_ENABLED
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Sql Resource Public API Unit Tests running query testing negative cases.
 */
class SqlResourceInvalidEndpointSuite
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

  test("Check streaming query REST API endpoints for 404") {
    withSQLConf(ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      val count = getDF().count()
    }
    val url = new URL(spark.sparkContext.ui.get.webUrl
      + s"/api/v1/applications/${spark.sparkContext.applicationId}" +
      s"/sql/streamingqueries")
    verifyAndGet404SqlRestResult(url)
  }

  test("Check streaming query statistics REST API endpoints with invalid RunId") {
    val runId = "bae7e04f-1376-4723-9864-be9fb994f4eb"

    val url = new URL(spark.sparkContext.ui.get.webUrl
      + s"/api/v1/applications/${spark.sparkContext.applicationId}" +
      s"/sql/streamingqueries/${runId}/progress")
    val (code, resultOpt, error) = getContentAndCode(url)
    assert(code == 404, s"Expected Http Response Code is 404 but received: $code for url: $url")
    assert(error.nonEmpty, s"Error message should be non empty for url: $url")
    assert(error.contains(s"Failed to find streaming query $runId"))
  }

  private def verifyAndGet404SqlRestResult(url: URL) = {
    val (code, resultOpt, error) = getContentAndCode(url)
    assert(code == 404, s"Expected Http Response Code is 404 but received: $code for url: $url")
    assert(error.nonEmpty, s"Error message should be non empty for url: $url")
    assert(error.get.contains("No streaming queries exist."))
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
