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

import org.apache.spark.SharedSparkContext
import org.apache.spark.SparkFunSuite
import org.apache.spark.shuffle.api.metric.{CustomShuffleMetric, CustomShuffleTaskMetric}

class CustomShuffleMetricsSuite extends SparkFunSuite with SharedSparkContext {

  test("buildMetricTypeName and parseMetricType round-trip the declaring class name") {
    val metric = shuffleMetric("s3BytesUploaded", "s3 bytes uploaded")
    val tag = CustomShuffleMetrics.buildMetricTypeName(metric)
    assert(tag === s"${CustomShuffleMetrics.SHUFFLE_CUSTOM}_${metric.getClass.getName}")
    assert(CustomShuffleMetrics.parseMetricType(tag).contains(metric.getClass.getName))
    // A built-in SQLMetric type is not a custom shuffle metric.
    assert(CustomShuffleMetrics.parseMetricType(MetricUtilsSumTag).isEmpty)
  }

  test("createMetrics tags each SQLMetric so the UI delegates to the plugin's aggregation") {
    val metrics = CustomShuffleMetrics.createMetrics(sc, Array(
      shuffleMetric("s3BytesUploaded", "s3 bytes uploaded"),
      shuffleMetric("s3BlockUploads", "s3 block uploads")))

    assert(metrics.keySet === Set("s3BytesUploaded", "s3BlockUploads"))
    metrics.values.foreach { m =>
      assert(CustomShuffleMetrics.parseMetricType(m.metricType).isDefined)
    }
  }

  test("updateMetrics folds reported values into the matching SQLMetrics by name") {
    val metrics = CustomShuffleMetrics.createMetrics(sc, Array(
      shuffleMetric("s3BytesUploaded", "s3 bytes uploaded"),
      shuffleMetric("s3BlockUploads", "s3 block uploads")))

    CustomShuffleMetrics.updateMetrics(
      Array(taskMetric("s3BytesUploaded", 1024L), taskMetric("s3BlockUploads", 3L)), metrics)

    assert(metrics("s3BytesUploaded").value === 1024L)
    assert(metrics("s3BlockUploads").value === 3L)
  }

  test("updateMetrics ignores reported values with no matching declaration") {
    val metrics = CustomShuffleMetrics.createMetrics(sc, Array(
      shuffleMetric("s3BytesUploaded", "s3 bytes uploaded")))

    CustomShuffleMetrics.updateMetrics(
      Array(taskMetric("s3BytesUploaded", 512L), taskMetric("undeclared", 99L)), metrics)

    assert(metrics("s3BytesUploaded").value === 512L)
    assert(!metrics.contains("undeclared"))
  }

  private val MetricUtilsSumTag = "sum"

  private def shuffleMetric(name: String, description: String): CustomShuffleMetric =
    new TestShuffleMetric(name, description)

  private def taskMetric(name: String, value: Long): CustomShuffleTaskMetric = {
    val metricName = name
    val metricValue = value
    new CustomShuffleTaskMetric {
      override def name(): String = metricName
      override def value(): Long = metricValue
    }
  }
}

private class TestShuffleMetric(metricName: String, metricDescription: String)
  extends CustomShuffleMetric {
  override def name(): String = metricName
  override def description(): String = metricDescription
  override def aggregateTaskMetrics(taskMetrics: Array[Long]): String = taskMetrics.sum.toString
}
