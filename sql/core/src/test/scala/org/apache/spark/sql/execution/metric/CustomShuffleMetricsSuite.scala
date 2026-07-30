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
import org.apache.spark.util.MetricUtils

class CustomShuffleMetricsSuite extends SparkFunSuite with SharedSparkContext {

  test("each metric type maps to a SQLMetric that renders the summed value accordingly") {
    val metrics = CustomShuffleMetrics.createMetrics(sc, Array(
      shuffleMetric("s3BytesUploaded", "s3 bytes uploaded", CustomShuffleMetric.MetricType.SIZE),
      shuffleMetric("s3BlockUploads", "s3 block uploads", CustomShuffleMetric.MetricType.SUM),
      shuffleMetric("s3Latency", "s3 first byte latency", CustomShuffleMetric.MetricType.TIMING),
      shuffleMetric(
        "s3LatencyNs", "s3 first byte latency ns", CustomShuffleMetric.MetricType.NS_TIMING)))

    assert(metrics.keySet ===
      Set("s3BytesUploaded", "s3BlockUploads", "s3Latency", "s3LatencyNs"))
    assert(metrics("s3BytesUploaded").metricType === MetricUtils.SIZE_METRIC)
    assert(metrics("s3BlockUploads").metricType === MetricUtils.SUM_METRIC)
    assert(metrics("s3Latency").metricType === MetricUtils.TIMING_METRIC)
    assert(metrics("s3LatencyNs").metricType === MetricUtils.NS_TIMING_METRIC)
  }

  test("updateMetrics folds reported values into the matching SQLMetrics by name") {
    val metrics = CustomShuffleMetrics.createMetrics(sc, Array(
      shuffleMetric("s3BytesUploaded", "s3 bytes uploaded", CustomShuffleMetric.MetricType.SIZE),
      shuffleMetric("s3BlockUploads", "s3 block uploads", CustomShuffleMetric.MetricType.SUM)))

    CustomShuffleMetrics.updateMetrics(
      Array(taskMetric("s3BytesUploaded", 1024L), taskMetric("s3BlockUploads", 3L)), metrics)

    assert(metrics("s3BytesUploaded").value === 1024L)
    assert(metrics("s3BlockUploads").value === 3L)
  }

  test("updateMetrics ignores reported values with no matching declaration") {
    val metrics = CustomShuffleMetrics.createMetrics(sc, Array(
      shuffleMetric("s3BytesUploaded", "s3 bytes uploaded", CustomShuffleMetric.MetricType.SIZE)))

    CustomShuffleMetrics.updateMetrics(
      Array(taskMetric("s3BytesUploaded", 512L), taskMetric("undeclared", 99L)), metrics)

    assert(metrics("s3BytesUploaded").value === 512L)
    assert(!metrics.contains("undeclared"))
  }

  private def shuffleMetric(
      name: String,
      description: String,
      metricType: CustomShuffleMetric.MetricType): CustomShuffleMetric = {
    val metricName = name
    val metricDescription = description
    val metricTypeValue = metricType
    new CustomShuffleMetric {
      override def name(): String = metricName
      override def description(): String = metricDescription
      override def metricType(): CustomShuffleMetric.MetricType = metricTypeValue
    }
  }

  private def taskMetric(name: String, value: Long): CustomShuffleTaskMetric = {
    val metricName = name
    val metricValue = value
    new CustomShuffleTaskMetric {
      override def name(): String = metricName
      override def value(): Long = metricValue
    }
  }
}
