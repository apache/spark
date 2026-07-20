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

import java.util.{Map => JMap}

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.internal.config.SHUFFLE_IO_PLUGIN_CLASS
import org.apache.spark.internal.config.UI.UI_ENABLED
import org.apache.spark.shuffle.api.{ShuffleDataIO, ShuffleDriverComponents, ShuffleExecutorComponents, ShuffleMapOutputWriter, ShufflePartitionWriter, SingleSpillShuffleMapOutputWriter}
import org.apache.spark.shuffle.api.metadata.MapOutputCommitMessage
import org.apache.spark.shuffle.api.metric.{CustomShuffleMetric, CustomShuffleTaskMetric}
import org.apache.spark.shuffle.sort.io.LocalDiskShuffleDataIO
import org.apache.spark.sql.{LocalSparkSession, SparkSession}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.metric.SQLShuffleWriteMetricsReporter.SHUFFLE_RECORDS_WRITTEN
import org.apache.spark.util.MetricUtils

/**
 * Integration coverage for the shuffle custom-metrics SPI: a toy [[ShuffleDataIO]] declares a
 * custom metric on its driver components and reports a fixed per-task value from every map output
 * writer, and we assert the value surfaces on the [[ShuffleExchangeExec]] operator node after a
 * real shuffle. This exercises the full path (driver declaration, task reporting, accumulator
 * propagation, and the SQL-side bridge) without a `ThreadLocal` side-channel.
 */
class CustomShuffleMetricsIntegrationSuite
  extends SparkFunSuite with LocalSparkSession with AdaptiveSparkPlanHelper {

  private def withPluginSession[T](pluginClass: Option[String])(f: SparkSession => T): T = {
    val conf = new SparkConf(false)
      .setMaster("local[2]")
      .setAppName("custom-shuffle-metrics")
      .set(UI_ENABLED, false)
    pluginClass.foreach(conf.set(SHUFFLE_IO_PLUGIN_CLASS, _))
    spark = SparkSession.builder().config(conf).getOrCreate()
    LocalSparkSession.withSparkSession(spark)(f)
  }

  private def runShuffleAndGetExchange(spark: SparkSession): ShuffleExchangeExec = {
    import spark.implicits._
    val df = spark.range(0, 100, 1, 4).map(id => (id % 8, id)).toDF("key", "value")
      .groupBy("key").count()
    df.collect()
    collectFirst(df.queryExecution.executedPlan) {
      case e: ShuffleExchangeExec => e
    }.getOrElse(fail("query plan had no ShuffleExchangeExec"))
  }

  test("a declared custom shuffle metric surfaces on the exchange node after a shuffle") {
    withPluginSession(Some(classOf[TestCustomMetricShuffleDataIO].getName)) { spark =>
      val exchange = runShuffleAndGetExchange(spark)
      assert(exchange.metrics.contains(TestCustomMetricShuffleDataIO.MetricName))
      // Four map tasks each report a fixed value; the metric aggregates as a plain sum.
      assert(exchange.metrics(TestCustomMetricShuffleDataIO.MetricName).value ===
        4 * TestCustomMetricShuffleDataIO.PerTaskValue)
    }
  }

  test("the built-in local-disk plugin declares no custom metrics (regression guard)") {
    withPluginSession(None) { spark =>
      val exchange = runShuffleAndGetExchange(spark)
      assert(!exchange.metrics.contains(TestCustomMetricShuffleDataIO.MetricName))
      // Only the built-in exchange metrics are present; no custom key leaks in.
      assert(exchange.metrics.keySet.forall(!_.startsWith("test")))
    }
  }

  test("a custom metric colliding with a Spark-owned name is dropped and cannot shadow it") {
    withPluginSession(Some(classOf[TestCollidingMetricShuffleDataIO].getName)) { spark =>
      val exchange = runShuffleAndGetExchange(spark)
      assert(exchange.metrics(SHUFFLE_RECORDS_WRITTEN).value > 0)
    }
  }
}

object TestCustomMetricShuffleDataIO {
  val MetricName: String = "testShuffleBytesUploaded"
  val PerTaskValue: Long = 1024L
}

/**
 * A [[ShuffleDataIO]] that delegates all real writing to the local-disk implementation but declares
 * one custom metric on the driver and reports a fixed per-task value from every map output writer.
 */
class TestCustomMetricShuffleDataIO(sparkConf: SparkConf) extends ShuffleDataIO {
  private val delegate = new LocalDiskShuffleDataIO(sparkConf)

  override def driver(): ShuffleDriverComponents =
    new TestCustomMetricDriverComponents(delegate.driver())

  override def executor(): ShuffleExecutorComponents =
    new TestCustomMetricExecutorComponents(delegate.executor())
}

class TestCustomMetricDriverComponents(delegate: ShuffleDriverComponents)
  extends ShuffleDriverComponents {

  override def initializeApplication(): JMap[String, String] = delegate.initializeApplication()

  override def cleanupApplication(): Unit = delegate.cleanupApplication()

  override def supportedCustomMetrics(): Array[CustomShuffleMetric] = Array(
    new CustomShuffleMetric {
      override def name(): String = TestCustomMetricShuffleDataIO.MetricName
      override def description(): String = "test shuffle bytes uploaded"
      override def metricType(): String = MetricUtils.SIZE_METRIC
    })
}

class TestCustomMetricExecutorComponents(delegate: ShuffleExecutorComponents)
  extends ShuffleExecutorComponents {

  override def initializeExecutor(
      appId: String, execId: String, extraConfigs: JMap[String, String]): Unit =
    delegate.initializeExecutor(appId, execId, extraConfigs)

  override def createMapOutputWriter(
      shuffleId: Int, mapTaskId: Long, numPartitions: Int): ShuffleMapOutputWriter =
    new TestCustomMetricMapOutputWriter(
      delegate.createMapOutputWriter(shuffleId, mapTaskId, numPartitions))

  override def createSingleFileMapOutputWriter(
      shuffleId: Int, mapId: Long): java.util.Optional[SingleSpillShuffleMapOutputWriter] =
    delegate.createSingleFileMapOutputWriter(shuffleId, mapId)
}

class TestCustomMetricMapOutputWriter(delegate: ShuffleMapOutputWriter)
  extends ShuffleMapOutputWriter {

  override def getPartitionWriter(reducePartitionId: Int): ShufflePartitionWriter =
    delegate.getPartitionWriter(reducePartitionId)

  override def commitAllPartitions(checksums: Array[Long]): MapOutputCommitMessage =
    delegate.commitAllPartitions(checksums)

  override def abort(error: Throwable): Unit = delegate.abort(error)

  override def currentMetricsValues(): Array[CustomShuffleTaskMetric] = Array(
    new CustomShuffleTaskMetric {
      override def name(): String = TestCustomMetricShuffleDataIO.MetricName
      override def value(): Long = TestCustomMetricShuffleDataIO.PerTaskValue
    })
}

/**
 * A [[ShuffleDataIO]] that delegates all real work to the local-disk implementation but declares a
 * custom driver metric using a Spark-owned name (`shuffleRecordsWritten`).
 */
class TestCollidingMetricShuffleDataIO(sparkConf: SparkConf) extends ShuffleDataIO {
  private val delegate = new LocalDiskShuffleDataIO(sparkConf)

  override def driver(): ShuffleDriverComponents =
    new TestCollidingMetricDriverComponents(delegate.driver())

  override def executor(): ShuffleExecutorComponents = delegate.executor()
}

class TestCollidingMetricDriverComponents(delegate: ShuffleDriverComponents)
  extends ShuffleDriverComponents {

  override def initializeApplication(): JMap[String, String] = delegate.initializeApplication()

  override def cleanupApplication(): Unit = delegate.cleanupApplication()

  override def supportedCustomMetrics(): Array[CustomShuffleMetric] = Array(
    new CustomShuffleMetric {
      override def name(): String = SHUFFLE_RECORDS_WRITTEN
      override def description(): String = "colliding shuffle records written"
      override def metricType(): String = MetricUtils.SUM_METRIC
    })
}
