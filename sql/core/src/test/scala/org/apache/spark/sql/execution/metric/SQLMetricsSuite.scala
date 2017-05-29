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

package org.apache.spark.sql.execution.metric

import java.io.File

import scala.collection.mutable.HashMap

import org.apache.spark.SparkFunSuite
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.execution.SparkPlanInfo
import org.apache.spark.sql.execution.ui.SparkPlanGraph
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.util.{AccumulatorContext, JsonProtocol}

class SQLMetricsSuite extends SparkFunSuite with SharedSQLContext {
  import testImplicits._

  /**
   * Call `df.collect()` and verify if the collected metrics are same as "expectedMetrics".
   *
   * @param df `DataFrame` to run
   * @param expectedNumOfJobs number of jobs that will run
   * @param expectedMetrics the expected metrics. The format is
   *                        `nodeId -> (operatorName, metric name -> metric value)`.
   */
  private def testSparkPlanMetrics(
      df: DataFrame,
      expectedNumOfJobs: Int,
      expectedMetrics: Map[Long, (String, Map[String, Any])]): Unit = {
    val previousExecutionIds = spark.sharedState.listener.executionIdToData.keySet
    withSQLConf("spark.sql.codegen.wholeStage" -> "false") {
      df.collect()
    }
    sparkContext.listenerBus.waitUntilEmpty(10000)
    val executionIds =
      spark.sharedState.listener.executionIdToData.keySet.diff(previousExecutionIds)
    assert(executionIds.size === 1)
    val executionId = executionIds.head
    val jobs = spark.sharedState.listener.getExecution(executionId).get.jobs
    // Use "<=" because there is a race condition that we may miss some jobs
    // TODO Change it to "=" once we fix the race condition that missing the JobStarted event.
    assert(jobs.size <= expectedNumOfJobs)
    if (jobs.size == expectedNumOfJobs) {
      // If we can track all jobs, check the metric values
      val metricValues = spark.sharedState.listener.getExecutionMetrics(executionId)
      val actualMetrics = SparkPlanGraph(SparkPlanInfo.fromSparkPlan(
        df.queryExecution.executedPlan)).allNodes.filter { node =>
        expectedMetrics.contains(node.id)
      }.map { node =>
        val nodeMetrics = node.metrics.map { metric =>
          val metricValue = metricValues(metric.accumulatorId)
          (metric.name, metricValue)
        }.toMap
        (node.id, node.name -> nodeMetrics)
      }.toMap

      assert(expectedMetrics.keySet === actualMetrics.keySet)
      for (nodeId <- expectedMetrics.keySet) {
        val (expectedNodeName, expectedMetricsMap) = expectedMetrics(nodeId)
        val (actualNodeName, actualMetricsMap) = actualMetrics(nodeId)
        assert(expectedNodeName === actualNodeName)
        for (metricName <- expectedMetricsMap.keySet) {
          assert(expectedMetricsMap(metricName).toString === actualMetricsMap(metricName))
        }
      }
    } else {
      // TODO Remove this "else" once we fix the race condition that missing the JobStarted event.
      // Since we cannot track all jobs, the metric values could be wrong and we should not check
      // them.
      logWarning("Due to a race condition, we miss some jobs and cannot verify the metric values")
    }
  }

  test("LocalTableScanExec computes metrics in collect and take") {
    val df1 = spark.createDataset(Seq(1, 2, 3))
    val logical = df1.queryExecution.logical
    require(logical.isInstanceOf[LocalRelation])
    df1.collect()
    val metrics1 = df1.queryExecution.executedPlan.collectLeaves().head.metrics
    assert(metrics1.contains("numOutputRows"))
    assert(metrics1("numOutputRows").value === 3)

    val df2 = spark.createDataset(Seq(1, 2, 3)).limit(2)
    df2.collect()
    val metrics2 = df2.queryExecution.executedPlan.collectLeaves().head.metrics
    assert(metrics2.contains("numOutputRows"))
    assert(metrics2("numOutputRows").value === 2)
  }

  test("Filter metrics") {
    // Assume the execution plan is
    // PhysicalRDD(nodeId = 1) -> Filter(nodeId = 0)
    val df = person.filter('age < 25)
    testSparkPlanMetrics(df, 1, Map(
      0L -> ("Filter", Map(
        "number of output rows" -> 1L)))
    )
  }

  test("WholeStageCodegen metrics") {
    // Assume the execution plan is
    // WholeStageCodegen(nodeId = 0, Range(nodeId = 2) -> Filter(nodeId = 1))
    // TODO: update metrics in generated operators
    val ds = spark.range(10).filter('id < 5)
    testSparkPlanMetrics(ds.toDF(), 1, Map.empty)
  }

  test("Aggregate metrics") {
    // Assume the execution plan is
    // ... -> HashAggregate(nodeId = 2) -> Exchange(nodeId = 1)
    // -> HashAggregate(nodeId = 0)
    val df = testData2.groupBy().count() // 2 partitions
    testSparkPlanMetrics(df, 1, Map(
      2L -> ("HashAggregate", Map("number of output rows" -> 2L)),
      0L -> ("HashAggregate", Map("number of output rows" -> 1L)))
    )

    // 2 partitions and each partition contains 2 keys
    val df2 = testData2.groupBy('a).count()
    testSparkPlanMetrics(df2, 1, Map(
      2L -> ("HashAggregate", Map("number of output rows" -> 4L)),
      0L -> ("HashAggregate", Map("number of output rows" -> 3L)))
    )
  }

  test("ObjectHashAggregate metrics") {
    // Assume the execution plan is
    // ... -> ObjectHashAggregate(nodeId = 2) -> Exchange(nodeId = 1)
    // -> ObjectHashAggregate(nodeId = 0)
    val df = testData2.groupBy().agg(collect_set('a)) // 2 partitions
    testSparkPlanMetrics(df, 1, Map(
      2L -> ("ObjectHashAggregate", Map("number of output rows" -> 2L)),
      0L -> ("ObjectHashAggregate", Map("number of output rows" -> 1L)))
    )

    // 2 partitions and each partition contains 2 keys
    val df2 = testData2.groupBy('a).agg(collect_set('a))
    testSparkPlanMetrics(df2, 1, Map(
      2L -> ("ObjectHashAggregate", Map("number of output rows" -> 4L)),
      0L -> ("ObjectHashAggregate", Map("number of output rows" -> 3L)))
    )
  }

  test("Sort metrics") {
    // Assume the execution plan is
    // WholeStageCodegen(nodeId = 0, Range(nodeId = 2) -> Sort(nodeId = 1))
    val ds = spark.range(10).sort('id)
    testSparkPlanMetrics(ds.toDF(), 2, Map.empty)
  }

  test("SortMergeJoin metrics") {
    // Because SortMergeJoin may skip different rows if the number of partitions is different, this
    // test should use the deterministic number of partitions.
    val testDataForJoin = testData2.filter('a < 2) // TestData2(1, 1) :: TestData2(1, 2)
    testDataForJoin.createOrReplaceTempView("testDataForJoin")
    withTempView("testDataForJoin") {
      // Assume the execution plan is
      // ... -> SortMergeJoin(nodeId = 1) -> TungstenProject(nodeId = 0)
      val df = spark.sql(
        "SELECT * FROM testData2 JOIN testDataForJoin ON testData2.a = testDataForJoin.a")
      testSparkPlanMetrics(df, 1, Map(
        0L -> ("SortMergeJoin", Map(
          // It's 4 because we only read 3 rows in the first partition and 1 row in the second one
          "number of output rows" -> 4L)))
      )
    }
  }

  test("SortMergeJoin(outer) metrics") {
    // Because SortMergeJoin may skip different rows if the number of partitions is different,
    // this test should use the deterministic number of partitions.
    val testDataForJoin = testData2.filter('a < 2) // TestData2(1, 1) :: TestData2(1, 2)
    testDataForJoin.createOrReplaceTempView("testDataForJoin")
    withTempView("testDataForJoin") {
      // Assume the execution plan is
      // ... -> SortMergeJoin(nodeId = 1) -> TungstenProject(nodeId = 0)
      val df = spark.sql(
        "SELECT * FROM testData2 left JOIN testDataForJoin ON testData2.a = testDataForJoin.a")
      testSparkPlanMetrics(df, 1, Map(
        0L -> ("SortMergeJoin", Map(
          // It's 4 because we only read 3 rows in the first partition and 1 row in the second one
          "number of output rows" -> 8L)))
      )

      val df2 = spark.sql(
        "SELECT * FROM testDataForJoin right JOIN testData2 ON testData2.a = testDataForJoin.a")
      testSparkPlanMetrics(df2, 1, Map(
        0L -> ("SortMergeJoin", Map(
          // It's 4 because we only read 3 rows in the first partition and 1 row in the second one
          "number of output rows" -> 8L)))
      )
    }
  }

  test("BroadcastHashJoin metrics") {
    val df1 = Seq((1, "1"), (2, "2")).toDF("key", "value")
    val df2 = Seq((1, "1"), (2, "2"), (3, "3"), (4, "4")).toDF("key", "value")
    // Assume the execution plan is
    // ... -> BroadcastHashJoin(nodeId = 1) -> TungstenProject(nodeId = 0)
    val df = df1.join(broadcast(df2), "key")
    testSparkPlanMetrics(df, 2, Map(
      1L -> ("BroadcastHashJoin", Map(
        "number of output rows" -> 2L)))
    )
  }

  test("BroadcastHashJoin(outer) metrics") {
    val df1 = Seq((1, "a"), (1, "b"), (4, "c")).toDF("key", "value")
    val df2 = Seq((1, "a"), (1, "b"), (2, "c"), (3, "d")).toDF("key2", "value")
    // Assume the execution plan is
    // ... -> BroadcastHashJoin(nodeId = 0)
    val df = df1.join(broadcast(df2), $"key" === $"key2", "left_outer")
    testSparkPlanMetrics(df, 2, Map(
      0L -> ("BroadcastHashJoin", Map(
        "number of output rows" -> 5L)))
    )

    val df3 = df1.join(broadcast(df2), $"key" === $"key2", "right_outer")
    testSparkPlanMetrics(df3, 2, Map(
      0L -> ("BroadcastHashJoin", Map(
        "number of output rows" -> 6L)))
    )
  }

  test("BroadcastNestedLoopJoin metrics") {
    val testDataForJoin = testData2.filter('a < 2) // TestData2(1, 1) :: TestData2(1, 2)
    testDataForJoin.createOrReplaceTempView("testDataForJoin")
    withSQLConf(SQLConf.CROSS_JOINS_ENABLED.key -> "true") {
      withTempView("testDataForJoin") {
        // Assume the execution plan is
        // ... -> BroadcastNestedLoopJoin(nodeId = 1) -> TungstenProject(nodeId = 0)
        val df = spark.sql(
          "SELECT * FROM testData2 left JOIN testDataForJoin ON " +
            "testData2.a * testDataForJoin.a != testData2.a + testDataForJoin.a")
        testSparkPlanMetrics(df, 3, Map(
          1L -> ("BroadcastNestedLoopJoin", Map(
            "number of output rows" -> 12L)))
        )
      }
    }
  }

  test("BroadcastLeftSemiJoinHash metrics") {
    val df1 = Seq((1, "1"), (2, "2")).toDF("key", "value")
    val df2 = Seq((1, "1"), (2, "2"), (3, "3"), (4, "4")).toDF("key2", "value")
    // Assume the execution plan is
    // ... -> BroadcastHashJoin(nodeId = 0)
    val df = df1.join(broadcast(df2), $"key" === $"key2", "leftsemi")
    testSparkPlanMetrics(df, 2, Map(
      0L -> ("BroadcastHashJoin", Map(
        "number of output rows" -> 2L)))
    )
  }

  test("CartesianProduct metrics") {
    withSQLConf(SQLConf.CROSS_JOINS_ENABLED.key -> "true") {
      val testDataForJoin = testData2.filter('a < 2) // TestData2(1, 1) :: TestData2(1, 2)
      testDataForJoin.createOrReplaceTempView("testDataForJoin")
      withTempView("testDataForJoin") {
        // Assume the execution plan is
        // ... -> CartesianProduct(nodeId = 1) -> TungstenProject(nodeId = 0)
        val df = spark.sql(
          "SELECT * FROM testData2 JOIN testDataForJoin")
        testSparkPlanMetrics(df, 1, Map(
          0L -> ("CartesianProduct", Map("number of output rows" -> 12L)))
        )
      }
    }
  }

  test("save metrics") {
    withTempPath { file =>
      val previousExecutionIds = spark.sharedState.listener.executionIdToData.keySet
      // Assume the execution plan is
      // PhysicalRDD(nodeId = 0)
      person.select('name).write.format("json").save(file.getAbsolutePath)
      sparkContext.listenerBus.waitUntilEmpty(10000)
      val executionIds =
        spark.sharedState.listener.executionIdToData.keySet.diff(previousExecutionIds)
      assert(executionIds.size === 1)
      val executionId = executionIds.head
      val jobs = spark.sharedState.listener.getExecution(executionId).get.jobs
      // Use "<=" because there is a race condition that we may miss some jobs
      // TODO Change "<=" to "=" once we fix the race condition that missing the JobStarted event.
      assert(jobs.size <= 1)
      val metricValues = spark.sharedState.listener.getExecutionMetrics(executionId)
      // Because "save" will create a new DataFrame internally, we cannot get the real metric id.
      // However, we still can check the value.
      assert(metricValues.values.toSeq.exists(_ === "2"))
    }
  }

  test("metrics can be loaded by history server") {
    val metric = SQLMetrics.createMetric(sparkContext, "zanzibar")
    metric += 10L
    val metricInfo = metric.toInfo(Some(metric.value), None)
    metricInfo.update match {
      case Some(v: Long) => assert(v === 10L)
      case Some(v) => fail(s"metric value was not a Long: ${v.getClass.getName}")
      case _ => fail("metric update is missing")
    }
    assert(metricInfo.metadata === Some(AccumulatorContext.SQL_ACCUM_IDENTIFIER))
    // After serializing to JSON, the original value type is lost, but we can still
    // identify that it's a SQL metric from the metadata
    val metricInfoJson = JsonProtocol.accumulableInfoToJson(metricInfo)
    val metricInfoDeser = JsonProtocol.accumulableInfoFromJson(metricInfoJson)
    metricInfoDeser.update match {
      case Some(v: String) => assert(v.toLong === 10L)
      case Some(v) => fail(s"deserialized metric value was not a string: ${v.getClass.getName}")
      case _ => fail("deserialized metric update is missing")
    }
    assert(metricInfoDeser.metadata === Some(AccumulatorContext.SQL_ACCUM_IDENTIFIER))
  }

  test("range metrics") {
    val res1 = InputOutputMetricsHelper.run(
      spark.range(30).filter(x => x % 3 == 0).toDF()
    )
    assert(res1 === (30L, 0L, 30L) :: Nil)

    val res2 = InputOutputMetricsHelper.run(
      spark.range(150).repartition(4).filter(x => x < 10).toDF()
    )
    assert(res2 === (150L, 0L, 150L) :: (0L, 150L, 10L) :: Nil)

    withTempDir { tempDir =>
      val dir = new File(tempDir, "pqS").getCanonicalPath

      spark.range(10).write.parquet(dir)
      spark.read.parquet(dir).createOrReplaceTempView("pqS")

      val res3 = InputOutputMetricsHelper.run(
        spark.range(30).repartition(3).crossJoin(sql("select * from pqS")).repartition(2).toDF()
      )
      // The query above is executed in the following stages:
      //   1. sql("select * from pqS")    => (10, 0, 10)
      //   2. range(30)                   => (30, 0, 30)
      //   3. crossJoin(...) of 1. and 2. => (0, 30, 300)
      //   4. shuffle & return results    => (0, 300, 0)
      assert(res3 === (10L, 0L, 10L) :: (30L, 0L, 30L) :: (0L, 30L, 300L) :: (0L, 300L, 0L) :: Nil)
    }
  }
}

object InputOutputMetricsHelper {
  private class InputOutputMetricsListener extends SparkListener {
    private case class MetricsResult(
        var recordsRead: Long = 0L,
        var shuffleRecordsRead: Long = 0L,
        var sumMaxOutputRows: Long = 0L)

    private[this] val stageIdToMetricsResult = HashMap.empty[Int, MetricsResult]

    def reset(): Unit = {
      stageIdToMetricsResult.clear()
    }

    /**
     * Return a list of recorded metrics aggregated per stage.
     *
     * The list is sorted in the ascending order on the stageId.
     * For each recorded stage, the following tuple is returned:
     *  - sum of inputMetrics.recordsRead for all the tasks in the stage
     *  - sum of shuffleReadMetrics.recordsRead for all the tasks in the stage
     *  - sum of the highest values of "number of output rows" metric for all the tasks in the stage
     */
    def getResults(): List[(Long, Long, Long)] = {
      stageIdToMetricsResult.keySet.toList.sorted.map { stageId =>
        val res = stageIdToMetricsResult(stageId)
        (res.recordsRead, res.shuffleRecordsRead, res.sumMaxOutputRows)
      }
    }

    override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = synchronized {
      val res = stageIdToMetricsResult.getOrElseUpdate(taskEnd.stageId, MetricsResult())

      res.recordsRead += taskEnd.taskMetrics.inputMetrics.recordsRead
      res.shuffleRecordsRead += taskEnd.taskMetrics.shuffleReadMetrics.recordsRead

      var maxOutputRows = 0L
      for (accum <- taskEnd.taskMetrics.externalAccums) {
        val info = accum.toInfo(Some(accum.value), None)
        if (info.name.toString.contains("number of output rows")) {
          info.update match {
            case Some(n: Number) =>
              if (n.longValue() > maxOutputRows) {
                maxOutputRows = n.longValue()
              }
            case _ => // Ignore.
          }
        }
      }
      res.sumMaxOutputRows += maxOutputRows
    }
  }

  // Run df.collect() and return aggregated metrics for each stage.
  def run(df: DataFrame): List[(Long, Long, Long)] = {
    val spark = df.sparkSession
    val sparkContext = spark.sparkContext
    val listener = new InputOutputMetricsListener()
    sparkContext.addSparkListener(listener)

    try {
      sparkContext.listenerBus.waitUntilEmpty(5000)
      listener.reset()
      df.collect()
      sparkContext.listenerBus.waitUntilEmpty(5000)
    } finally {
      sparkContext.removeSparkListener(listener)
    }
    listener.getResults()
  }
}
