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
import org.scalatest.PrivateMethodTester

import org.apache.spark.SparkConf
import org.apache.spark.deploy.history.HistoryServerSuite.getContentAndCode
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.execution.metric.SQLMetricsTestUtils
import org.apache.spark.sql.internal.SQLConf.ADAPTIVE_EXECUTION_ENABLED
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Sql Resource Public API Unit Tests running query and extracting the metrics.
 */
class SqlCompileResourceSuite
  extends SharedSparkSession with SQLMetricsTestUtils with SQLHelper with PrivateMethodTester {

  import testImplicits._
  implicit val formats = new DefaultFormats {
    override def dateFormatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")
  }
  override def sparkConf: SparkConf = {
    super.sparkConf.set("spark.ui.enabled", "true")
  }

  test("Check Compile Stat Rest Api Endpoints") {
    // Materalize result DataFrame
    withSQLConf(ADAPTIVE_EXECUTION_ENABLED.key -> "true") {
      val count = getDF().count()
      assert(count == 2, s"Expected Query Count is 2 but received: $count")
    }

    // Spark apps launched by local-mode seems not having `attemptId` as default
    // so UT is just added for existing endpoints.
    val executionId = callCompilerStatRestEndpointAndVerifyResult()
    callCompilerStatRestEndpointByExecutionIdAndVerifyResult(executionId)
  }

  private def callCompilerStatRestEndpointAndVerifyResult(): Long = {
    val url = new URL(spark.sparkContext.ui.get.webUrl
      + s"/api/v1/applications/${spark.sparkContext.applicationId}/compiler")
    val jsonResult = verifyAndGetCompileStatRestResult(url)
    val compilerStats = JsonMethods.parse(jsonResult).extract[Seq[CompileData]]
    assert(compilerStats.size > 0,
      s"Expected Query Result Size is higher than 0 but received: ${compilerStats.size}")
    val compilerStatData = compilerStats.head
    verifyCompilerStatRestContent(compilerStatData)
    compilerStatData.id
  }

  private def callCompilerStatRestEndpointByExecutionIdAndVerifyResult(executionId: Long): Unit = {
    val url = new URL(spark.sparkContext.ui.get.webUrl
      + s"/api/v1/applications/${spark.sparkContext.applicationId}/compiler/${executionId}")
    val jsonResult = verifyAndGetCompileStatRestResult(url)
    val compilerStatData = JsonMethods.parse(jsonResult).extract[CompileData]
    verifyCompilerStatRestContent(compilerStatData)
  }

  private def verifyCompilerStatRestContent(compileStats: CompileData): Unit = {
    assert(compileStats.appID != "", "Should have valid application ID")
    assert(compileStats.id > -1,
      s"Expected execution ID is valid, found ${compileStats.id}")
    assert(compileStats.ruleInfo.length == 50,
      s"Expected number of Spark rules is 50, found ${compileStats.ruleInfo.length}")
    assert(compileStats.phaseInfo.length == 3,
      s"Expected number of phase info is: 3 found ${compileStats.phaseInfo.length}")
  }

  private def verifyAndGetCompileStatRestResult(url: URL): String = {
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
