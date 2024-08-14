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

import org.apache.spark.TestUtils
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.{SparkPlan, SparkPlanInfo}
import org.apache.spark.sql.execution.ui.{SparkPlanGraph, SQLAppStatusStore}
import org.apache.spark.sql.internal.SQLConf.WHOLESTAGE_CODEGEN_ENABLED
import org.apache.spark.sql.test.SQLTestUtils


trait SQLMetricsTestUtils extends SQLTestUtils {
  import testImplicits._

  protected def currentExecutionIds(): Set[Long] = {
    spark.sparkContext.listenerBus.waitUntilEmpty(10000)
    statusStore.executionsList.map(_.executionId).toSet
  }

  protected def statusStore: SQLAppStatusStore = spark.sharedState.statusStore

  // Pattern of size SQLMetric value, e.g. "\n96.2 MiB (32.1 MiB, 32.1 MiB, 32.1 MiB (stage 0.0:
  // task 4))" OR "\n96.2 MiB (32.1 MiB, 32.1 MiB, 32.1 MiB)"
  protected val sizeMetricPattern = {
    val bytes = "([0-9]+(\\.[0-9]+)?) (EiB|PiB|TiB|GiB|MiB|KiB|B)"
    val maxMetrics = "\\(stage ([0-9])+\\.([0-9])+\\: task ([0-9])+\\)"
    s"(.*\\n$bytes \\($bytes, $bytes, $bytes( $maxMetrics)?\\))|($bytes)"
  }

  // Pattern of timing SQLMetric value, e.g. "\n2.0 ms (1.0 ms, 1.0 ms, 1.0 ms (stage 3.0):
  // task 217))" OR "\n2.0 ms (1.0 ms, 1.0 ms, 1.0 ms)" OR "1.0 ms"
  protected val timingMetricPattern = {
    val duration = "([0-9]+(\\.[0-9]+)?) (ms|s|m|h)"
    val maxMetrics = "\\(stage ([0-9])+\\.([0-9])+\\: task ([0-9])+\\)"
    s"(.*\\n$duration \\($duration, $duration, $duration( $maxMetrics)?\\))|($duration)"
  }

  // Pattern of size SQLMetric value for Aggregate tests.
  // e.g "\n(1, 1, 0.9 (stage 1.0: task 8)) OR "\n(1, 1, 0.9 )" OR "1"
  protected val aggregateMetricsPattern = {
    val iters = "([0-9]+(\\.[0-9]+)?)"
    val maxMetrics = "\\(stage ([0-9])+\\.([0-9])+\\: task ([0-9])+\\)"
    s"(.*\\n\\($iters, $iters, $iters( $maxMetrics)?\\))|($iters)"
  }

  /**
   * Get execution metrics for the SQL execution and verify metrics values.
   *
   * @param metricsValues the expected metric values (numFiles, numPartitions, numOutputRows).
   * @param func the function can produce execution id after running.
   */
  private def verifyWriteDataMetrics(metricsValues: Seq[Int])(func: => Unit): Unit = {
    val previousExecutionIds = currentExecutionIds()
    // Run the given function to trigger query execution.
    func
    spark.sparkContext.listenerBus.waitUntilEmpty(10000)
    val executionIds = currentExecutionIds().diff(previousExecutionIds)
    assert(executionIds.size == 1)
    val executionId = executionIds.head

    val executedNode = statusStore.planGraph(executionId).nodes.head

    val metricsNames = Seq(
      "number of written files",
      "number of dynamic part",
      "number of output rows")

    val metrics = statusStore.executionMetrics(executionId)

    metricsNames.zip(metricsValues).foreach { case (metricsName, expected) =>
      val sqlMetric = executedNode.metrics.find(_.name == metricsName)
      assert(sqlMetric.isDefined)
      val accumulatorId = sqlMetric.get.accumulatorId
      val metricValue = metrics(accumulatorId).replaceAll(",", "").toInt
      assert(metricValue == expected)
    }

    val totalNumBytesMetric = executedNode.metrics.find(
      _.name == "written output").get
    val totalNumBytes = metrics(totalNumBytesMetric.accumulatorId).replaceAll(",", "")
      .split(" ").head.trim.toDouble
    assert(totalNumBytes > 0)
  }

  protected def testMetricsNonDynamicPartition(
      dataFormat: String,
      tableName: String): Unit = {
    withTable(tableName) {
      Seq((1, 2)).toDF("i", "j")
        .write.format(dataFormat).mode("overwrite").saveAsTable(tableName)

      val tableLocation =
        new File(spark.sessionState.catalog.getTableMetadata(TableIdentifier(tableName)).location)

      // 2 files, 100 rows, 0 dynamic partition.
      verifyWriteDataMetrics(Seq(2, 0, 100)) {
        (0 until 100).map(i => (i, i + 1)).toDF("i", "j").repartition(2)
          .write.format(dataFormat).mode("overwrite").insertInto(tableName)
      }
      assert(TestUtils.recursiveList(tableLocation).count(_.getName.startsWith("part-")) == 2)
    }
  }

  protected def testMetricsDynamicPartition(
      provider: String,
      dataFormat: String,
      tableName: String): Unit = {
    withTable(tableName) {
      withTempPath { dir =>
        spark.sql(
          s"""
             |CREATE TABLE $tableName(a int, b int)
             |USING $provider
             |PARTITIONED BY(a)
             |LOCATION '${dir.toURI}'
           """.stripMargin)
        val table = spark.sessionState.catalog.getTableMetadata(TableIdentifier(tableName))
        assert(table.location == makeQualifiedPath(dir.getAbsolutePath))

        val df = spark.range(start = 0, end = 40, step = 1, numPartitions = 1)
          .selectExpr("id a", "id b")

        // 40 files, 80 rows, 40 dynamic partitions.
        verifyWriteDataMetrics(Seq(40, 40, 80)) {
          df.union(df).repartition(2, $"a")
            .write
            .format(dataFormat)
            .mode("overwrite")
            .insertInto(tableName)
        }
        assert(TestUtils.recursiveList(dir).count(_.getName.startsWith("part-")) == 40)
      }
    }
  }

  /**
   * Call `df.collect()` and collect necessary metrics from execution data.
   *
   * @param df `DataFrame` to run
   * @param expectedNumOfJobs number of jobs that will run
   * @param expectedNodeIds the node ids of the metrics to collect from execution data.
   * @param enableWholeStage enable whole-stage code generation or not.
   */
  protected def getSparkPlanMetrics(
       df: DataFrame,
       expectedNumOfJobs: Int,
       expectedNodeIds: Set[Long],
       enableWholeStage: Boolean = false): Option[Map[Long, (String, Map[String, Any])]] = {
    val previousExecutionIds = currentExecutionIds()
    withSQLConf(WHOLESTAGE_CODEGEN_ENABLED.key -> enableWholeStage.toString) {
      df.collect()
    }
    sparkContext.listenerBus.waitUntilEmpty(10000)
    val executionIds = currentExecutionIds().diff(previousExecutionIds)
    assert(executionIds.size === 1)
    val executionId = executionIds.head
    val jobs = statusStore.execution(executionId).get.jobs
    // Use "<=" because there is a race condition that we may miss some jobs
    // TODO Change it to "=" once we fix the race condition that missing the JobStarted event.
    assert(jobs.size <= expectedNumOfJobs)
    if (jobs.size == expectedNumOfJobs) {
      // If we can track all jobs, check the metric values
      val metricValues = statusStore.executionMetrics(executionId)
      val metrics = SparkPlanGraph(SparkPlanInfo.fromSparkPlan(
        df.queryExecution.executedPlan)).allNodes.filter { node =>
        expectedNodeIds.contains(node.id)
      }.map { node =>
        val nodeMetrics = node.metrics.map { metric =>
          val metricValue = metricValues(metric.accumulatorId)
          (metric.name, metricValue)
        }.toMap
        (node.id, node.name -> nodeMetrics)
      }.toMap
      Some(metrics)
    } else {
      // TODO Remove this "else" once we fix the race condition that missing the JobStarted event.
      // Since we cannot track all jobs, the metric values could be wrong and we should not check
      // them.
      logWarning("Due to a race condition, we miss some jobs and cannot verify the metric values")
      None
    }
  }

  /**
   * Call `df.collect()` and verify if the collected metrics are same as "expectedMetrics".
   *
   * @param df `DataFrame` to run
   * @param expectedNumOfJobs number of jobs that will run
   * @param expectedMetrics the expected metrics. The format is
   *                        `nodeId -> (operatorName, metric name -> metric value)`.
   */
  protected def testSparkPlanMetrics(
      df: DataFrame,
      expectedNumOfJobs: Int,
      expectedMetrics: Map[Long, (String, Map[String, Any])],
      enableWholeStage: Boolean = false): Unit = {
    val expectedMetricsPredicates = expectedMetrics.mapValues { case (nodeName, nodeMetrics) =>
      (nodeName, nodeMetrics.mapValues(expectedMetricValue =>
        (actualMetricValue: Any) => {
          actualMetricValue.toString.matches(expectedMetricValue.toString)
        }).toMap)
    }
    testSparkPlanMetricsWithPredicates(df, expectedNumOfJobs, expectedMetricsPredicates.toMap,
      enableWholeStage)
  }

  /**
   * Call `df.collect()` and verify if the collected metrics satisfy the specified predicates.
   * @param df `DataFrame` to run
   * @param expectedNumOfJobs number of jobs that will run
   * @param expectedMetricsPredicates the expected metrics predicates. The format is
   *                                  `nodeId -> (operatorName, metric name -> metric predicate)`.
   * @param enableWholeStage enable whole-stage code generation or not.
   */
  protected def testSparkPlanMetricsWithPredicates(
      df: DataFrame,
      expectedNumOfJobs: Int,
      expectedMetricsPredicates: Map[Long, (String, Map[String, Any => Boolean])],
      enableWholeStage: Boolean = false): Unit = {
    val optActualMetrics =
      getSparkPlanMetrics(df, expectedNumOfJobs, expectedMetricsPredicates.keySet, enableWholeStage)
    optActualMetrics.foreach { actualMetrics =>
      assert(expectedMetricsPredicates.keySet === actualMetrics.keySet)
      for ((nodeId, (expectedNodeName, expectedMetricsPredicatesMap))
          <- expectedMetricsPredicates) {
        val (actualNodeName, actualMetricsMap) = actualMetrics(nodeId)
        assert(expectedNodeName === actualNodeName)
        for ((metricName, metricPredicate) <- expectedMetricsPredicatesMap) {
          assert(metricPredicate(actualMetricsMap(metricName)),
            s"$nodeId / '$metricName' (= ${actualMetricsMap(metricName)}) did not match predicate.")
        }
      }
    }
  }

  /**
   * Verify if the metrics in `SparkPlan` operator are same as expected metrics.
   *
   * @param plan `SparkPlan` operator to check metrics
   * @param expectedMetrics the expected metrics. The format is `metric name -> metric value`.
   */
  protected def testMetricsInSparkPlanOperator(
      plan: SparkPlan,
      expectedMetrics: Map[String, Long]): Unit = {
    expectedMetrics.foreach { case (metricName: String, metricValue: Long) =>
      assert(plan.metrics.contains(metricName), s"The query plan should have metric $metricName")
      val actualMetric = plan.metrics(metricName)
      assert(actualMetric.value == metricValue,
        s"The query plan metric $metricName did not match, " +
          s"expected:$metricValue, actual:${actualMetric.value}")
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
      taskEnd.taskMetrics.withExternalAccums(_.foreach { accum =>
        val info = accum.toInfoUpdate
        if (info.name.toString.contains("number of output rows")) {
          info.update match {
            case Some(n: Number) =>
              if (n.longValue() > maxOutputRows) {
                maxOutputRows = n.longValue()
              }
            case _ => // Ignore.
          }
        }
      })
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
