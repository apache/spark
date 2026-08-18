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

import scala.concurrent.duration._

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.SparkPlanInfo
import org.apache.spark.sql.test.SharedSparkSession

class SparkPlanInfoSuite extends SharedSparkSession {

  import testImplicits._

  private def collectSparkPlanInfo(sparkPlanInfo: SparkPlanInfo): Seq[SparkPlanInfo] = {
    sparkPlanInfo +: sparkPlanInfo.children.flatMap(collectSparkPlanInfo)
  }

  private def findSparkPlanInfo(sparkPlanInfo: SparkPlanInfo, nodeName: String): SparkPlanInfo = {
    collectSparkPlanInfo(sparkPlanInfo)
      .find(_.nodeName == nodeName)
      .getOrElse(fail(s"Could not find $nodeName in ${sparkPlanInfo.simpleString}"))
  }

  private def collectSparkPlanGraphMetrics(
      df: DataFrame): (SparkPlanGraph, Map[Long, String]) = {
    val statusStore = spark.sharedState.statusStore
    spark.sparkContext.listenerBus.waitUntilEmpty(10000)
    val previousExecutionIds = statusStore.executionsList().map(_.executionId).toSet

    df.collect()
    spark.sparkContext.listenerBus.waitUntilEmpty(10000)

    eventually(timeout(10.seconds), interval(10.milliseconds)) {
      assert(statusStore.executionsList().map(_.executionId).toSet
        .diff(previousExecutionIds).size === 1)
    }
    val executionIds = statusStore.executionsList().map(_.executionId).toSet
      .diff(previousExecutionIds)
    val executionId = executionIds.head
    eventually(timeout(10.seconds), interval(10.milliseconds)) {
      assert(statusStore.execution(executionId).exists(_.metricValues != null))
    }

    (statusStore.planGraph(executionId), statusStore.executionMetrics(executionId))
  }

  def validateSparkPlanInfo(sparkPlanInfo: SparkPlanInfo): Unit = {
    sparkPlanInfo.nodeName match {
      case "InMemoryTableScan" => assert(sparkPlanInfo.children.length == 1)
      case _ => sparkPlanInfo.children.foreach(validateSparkPlanInfo)
    }
  }

  test("SparkPlanInfo creation from SparkPlan with InMemoryTableScan node") {
    val dfWithCache = Seq(
      (1, 1),
      (2, 2)
    ).toDF().filter("_1 > 1").cache().repartition(10)

    val planInfoResult = SparkPlanInfo.fromSparkPlan(dfWithCache.queryExecution.executedPlan)

    validateSparkPlanInfo(planInfoResult)
  }

  test("SPARK-47017: SparkPlanInfo and SQL UI include SQL plan inside RDDScanExec") {
    val source = spark.range(10).where($"id" > 3).select($"id".as("age"))
    val recreated = spark.createDataFrame(source.rdd, source.schema)

    val planInfo = SparkPlanInfo.fromSparkPlan(recreated.queryExecution.executedPlan)
    val rddScanInfo = findSparkPlanInfo(planInfo, "Scan ExistingRDD")
    val internalRDDPlanInfos = rddScanInfo.children.flatMap(collectSparkPlanInfo)
    val filterInfo = internalRDDPlanInfos
      .find(_.nodeName == "Filter")
      .getOrElse(fail(s"Could not find Filter under Scan ExistingRDD in ${planInfo.simpleString}"))

    assert(rddScanInfo.children.nonEmpty)
    assert(filterInfo.metrics.exists(_.name == "number of output rows"))

    val unionRDD = spark.sparkContext.union(source.rdd, source.rdd)
    val unionPlanInfo = SparkPlanInfo.fromSparkPlan(
      spark.createDataFrame(unionRDD, source.schema).queryExecution.executedPlan)
    val unionRDDScanInfo = findSparkPlanInfo(unionPlanInfo, "Scan ExistingRDD")

    assert(unionRDDScanInfo.children.size === 1)

    val nested = spark.createDataFrame(recreated.rdd, recreated.schema)
    val nestedPlanInfo = SparkPlanInfo.fromSparkPlan(nested.queryExecution.executedPlan)
    val nestedRDDScanInfo = findSparkPlanInfo(nestedPlanInfo, "Scan ExistingRDD")

    assert(nestedRDDScanInfo.children.size === 1)
    assert(collectSparkPlanInfo(nestedRDDScanInfo.children.head).exists(_.nodeName == "Filter"))

    val (planGraph, metricValues) = collectSparkPlanGraphMetrics(recreated)
    val filterNode = planGraph.allNodes
      .find(_.name == "Filter")
      .getOrElse(fail(s"Could not find Filter in ${recreated.queryExecution.executedPlan}"))
    val filterMetric = filterNode.metrics
      .find(_.name == "number of output rows")
      .getOrElse(fail("Could not find number of output rows metric for Filter"))
    val filterMetricValue = metricValues
      .getOrElse(filterMetric.accumulatorId, fail("Could not find Filter metric value"))
    val outputRows = "\\d+".r.findFirstIn(filterMetricValue.replace(",", ""))
      .map(_.toLong)
      .getOrElse(fail(s"Could not parse Filter metric value $filterMetricValue"))

    assert(outputRows === 6L)
  }
}
