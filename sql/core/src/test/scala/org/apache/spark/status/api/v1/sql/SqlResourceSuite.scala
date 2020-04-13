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

import org.scalatest.PrivateMethodTester

import org.apache.spark.{JobExecutionStatus, SparkFunSuite}
import org.apache.spark.sql.execution.ui.{SQLExecutionUIData, SQLPlanMetric}

object SqlResourceSuite {

  val SCAN_TEXT = "Scan text"
  val FILTER = "Filter"
  val WHOLE_STAGE_CODEGEN_1 = "WholeStageCodegen (1)"
  val DURATION = "duration"
  val NUMBER_OF_OUTPUT_ROWS = "number of output rows"
  val METADATA_TIME = "metadata time"
  val NUMBER_OF_FILES_READ = "number of files read"
  val SIZE_OF_FILES_READ = "size of files read"
  val PLAN_DESCRIPTION = "== Physical Plan ==\nCollectLimit (3)\n+- * Filter (2)\n +- Scan text ..."
  val DESCRIPTION = "csv at MyDataFrames.scala:57"

  private def getSQLExecutionUIData(): SQLExecutionUIData = {
    def getMetrics(): Seq[SQLPlanMetric] = {
      Seq(SQLPlanMetric(DURATION, 0, "", Some(1), Some(WHOLE_STAGE_CODEGEN_1)),
        SQLPlanMetric(NUMBER_OF_OUTPUT_ROWS, 1, "", Some(2), Some(FILTER)),
        SQLPlanMetric(METADATA_TIME, 2, "", Some(3), Some(SCAN_TEXT)),
        SQLPlanMetric(NUMBER_OF_FILES_READ, 3, "", Some(3), Some(SCAN_TEXT)),
        SQLPlanMetric(NUMBER_OF_OUTPUT_ROWS, 4, "", Some(3), Some(SCAN_TEXT)),
        SQLPlanMetric(SIZE_OF_FILES_READ, 5, "", Some(3), Some(SCAN_TEXT)))
    }

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
      metrics = getMetrics(),
      submissionTime = 1586768888233L,
      completionTime = Some(new Date(1586768888999L)),
      jobs = Map[Int, JobExecutionStatus](
        0 -> JobExecutionStatus.SUCCEEDED,
        1 -> JobExecutionStatus.SUCCEEDED),
      stages = Set[Int](),
      metricValues = getMetricValues()
    )
  }

  private def getExpectedMetricDetails(): Seq[MetricDetails] = {
    val metricDetails =
      MetricDetails(1, WHOLE_STAGE_CODEGEN_1, metrics = Seq(Metric(DURATION, "0 ms")))
    val metricDetails2 = MetricDetails(2, FILTER, metrics = Seq(Metric(NUMBER_OF_OUTPUT_ROWS, "1")))
    val metricDetails3 = MetricDetails(3, SCAN_TEXT,
      metrics = Seq(Metric(METADATA_TIME, "2 ms"),
        Metric(NUMBER_OF_FILES_READ, "1"),
        Metric(NUMBER_OF_OUTPUT_ROWS, "1"),
        Metric(SIZE_OF_FILES_READ, "330.0 B")))

    // reverse order because of supporting execution order by aligning with Spark-UI
    Seq(metricDetails3, metricDetails2, metricDetails)
  }

  private def verifyExpectedExecutionData(executionData: ExecutionData,
                                          metricDetails: Seq[MetricDetails],
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
    assert(executionData.metricDetails == metricDetails)
  }
}

/**
 * Sql Resource Public API Unit Tests.
 */
class SqlResourceSuite extends SparkFunSuite with PrivateMethodTester {

  import SqlResourceSuite._

  val sqlResource = new SqlResource()
  val sqlExecutionUIData = getSQLExecutionUIData()
  val prepareExecutionData = PrivateMethod[ExecutionData]('prepareExecutionData)

  test("Prepare ExecutionData when details = false and planDescription = false") {
    val executionData =
      sqlResource invokePrivate prepareExecutionData(sqlExecutionUIData, false, false)
    verifyExpectedExecutionData(executionData, metricDetails = Seq.empty, planDescription = "")
  }

  test("Prepare ExecutionData when details = true and planDescription = false") {
    val executionData =
      sqlResource invokePrivate prepareExecutionData(sqlExecutionUIData, true, false)
    verifyExpectedExecutionData(
      executionData,
      metricDetails = getExpectedMetricDetails(),
      planDescription = "")
  }

  test("Prepare ExecutionData when details = true and planDescription = true") {
    val executionData =
      sqlResource invokePrivate prepareExecutionData(sqlExecutionUIData, true, true)
    verifyExpectedExecutionData(
      executionData,
      metricDetails = getExpectedMetricDetails(),
      planDescription = PLAN_DESCRIPTION)
  }

}
