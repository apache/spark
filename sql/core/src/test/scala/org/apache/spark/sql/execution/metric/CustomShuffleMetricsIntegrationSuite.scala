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
import org.apache.spark.shuffle.api.{ShuffleDataIO, ShuffleDriverComponents, ShuffleExecutorComponents}
import org.apache.spark.shuffle.api.metric.CustomShuffleMetric
import org.apache.spark.shuffle.sort.{CustomMetricReportingExecutorComponents, TestCustomShuffleTaskMetric}
import org.apache.spark.shuffle.sort.io.LocalDiskShuffleDataIO
import org.apache.spark.sql.{LocalSparkSession, SparkSession}
import org.apache.spark.sql.execution.CollectLimitExec
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.metric.SQLShuffleWriteMetricsReporter.SHUFFLE_RECORDS_WRITTEN
import org.apache.spark.util.MetricUtils

/**
 * Integration coverage for the shuffle custom-metrics SPI: a toy [[ShuffleDataIO]] declares a
 * custom metric and reports a fixed per-task value, and we assert it surfaces on the operator node.
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

  test("no custom shuffle metrics surface when no plugin declares any") {
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

  test("a declared custom shuffle metric surfaces on a multi-partition limit shuffle") {
    withPluginSession(Some(classOf[TestCustomMetricShuffleDataIO].getName)) { spark =>
      import spark.implicits._
      val df = spark.range(0, 100, 1, 4).map(id => (id, id)).toDF("key", "value").limit(50)
      val limitExec = collectFirst(df.queryExecution.executedPlan) {
        case c: CollectLimitExec => c
      }.getOrElse(fail("query plan had no CollectLimitExec"))
      limitExec.execute().count()
      assert(limitExec.metrics.contains(TestCustomMetricShuffleDataIO.MetricName),
        "CollectLimitExec did not expose the declared custom shuffle metric")
      assert(limitExec.metrics(TestCustomMetricShuffleDataIO.MetricName).value ===
        4 * TestCustomMetricShuffleDataIO.PerTaskValue)
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
    new CustomMetricReportingExecutorComponents(
      delegate.executor(),
      Array(TestCustomShuffleTaskMetric(
        TestCustomMetricShuffleDataIO.MetricName, TestCustomMetricShuffleDataIO.PerTaskValue)))
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
