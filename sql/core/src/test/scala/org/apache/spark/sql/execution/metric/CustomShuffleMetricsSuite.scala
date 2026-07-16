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

  test("createMetrics maps each declared metric to a SQLMetric of the declared type") {
    val metrics = CustomShuffleMetrics.createMetrics(sc, Array(
      shuffleMetric("s3BytesUploaded", "s3 bytes uploaded", MetricUtils.SIZE_METRIC),
      shuffleMetric("s3BlockUploads", "s3 block uploads", MetricUtils.SUM_METRIC),
      shuffleMetric("s3FirstByteLatency", "s3 first byte latency", MetricUtils.TIMING_METRIC)))

    assert(metrics.keySet === Set("s3BytesUploaded", "s3BlockUploads", "s3FirstByteLatency"))
    assert(metrics("s3BytesUploaded").metricType === MetricUtils.SIZE_METRIC)
    assert(metrics("s3BlockUploads").metricType === MetricUtils.SUM_METRIC)
    assert(metrics("s3FirstByteLatency").metricType === MetricUtils.TIMING_METRIC)
  }

  test("createMetrics rejects an unsupported metric type") {
    val e = intercept[IllegalArgumentException] {
      CustomShuffleMetrics.createMetrics(sc, Array(
        shuffleMetric("s3Avg", "s3 avg", MetricUtils.AVERAGE_METRIC)))
    }
    assert(e.getMessage.contains("s3Avg"))
    assert(e.getMessage.contains(MetricUtils.AVERAGE_METRIC))
  }

  test("updateMetrics folds reported values into the matching SQLMetrics by name") {
    val metrics = CustomShuffleMetrics.createMetrics(sc, Array(
      shuffleMetric("s3BytesUploaded", "s3 bytes uploaded", MetricUtils.SIZE_METRIC),
      shuffleMetric("s3BlockUploads", "s3 block uploads", MetricUtils.SUM_METRIC)))

    CustomShuffleMetrics.updateMetrics(
      Array(taskMetric("s3BytesUploaded", 1024L), taskMetric("s3BlockUploads", 3L)), metrics)

    assert(metrics("s3BytesUploaded").value === 1024L)
    assert(metrics("s3BlockUploads").value === 3L)
  }

  test("updateMetrics ignores reported values with no matching declaration") {
    val metrics = CustomShuffleMetrics.createMetrics(sc, Array(
      shuffleMetric("s3BytesUploaded", "s3 bytes uploaded", MetricUtils.SIZE_METRIC)))

    CustomShuffleMetrics.updateMetrics(
      Array(taskMetric("s3BytesUploaded", 512L), taskMetric("undeclared", 99L)), metrics)

    assert(metrics("s3BytesUploaded").value === 512L)
    assert(!metrics.contains("undeclared"))
  }

  private def shuffleMetric(
      metricName: String, desc: String, mType: String): CustomShuffleMetric = {
    new CustomShuffleMetric {
      override def name(): String = metricName
      override def description(): String = desc
      override def metricType(): String = mType
    }
  }

  private def taskMetric(metricName: String, v: Long): CustomShuffleTaskMetric = {
    new CustomShuffleTaskMetric {
      override def name(): String = metricName
      override def value(): Long = v
    }
  }
}
