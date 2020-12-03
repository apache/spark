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
package org.apache.spark.executor

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind.JsonSerializer
import com.fasterxml.jackson.databind.SerializerProvider
import com.fasterxml.jackson.databind.annotation.JsonSerialize

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.metrics.ExecutorMetricType

/**
 * :: DeveloperApi ::
 * Metrics Distributions tracked for executors and the driver.
 */
@DeveloperApi
@JsonSerialize(using = classOf[ExecutorMetricsDistributionJsonSerializer])
class ExecutorMetricsDistributions private[spark](
    val quantiles: IndexedSeq[Double],
    val executorMetrics: IndexedSeq[ExecutorMetrics]) {
  private lazy val count = executorMetrics.length
  private lazy val indices = quantiles.map { q => math.min((q * count).toLong, count - 1) }

  /** Returns the distributions for the specified metric. */
  def getMetricDistribution(metricName: String): IndexedSeq[Double] = {
    val sorted = executorMetrics.map(_.getMetricValue(metricName)).sorted
    indices.map(i => sorted(i.toInt).toDouble).toIndexedSeq
  }
}

/**
 * Serializer for ExecutorMetricsDistributions
 * Convert to map with metric name as key
 */
private[spark] class ExecutorMetricsDistributionJsonSerializer
  extends JsonSerializer[ExecutorMetricsDistributions] {
  override def serialize(
      metrics: ExecutorMetricsDistributions,
      jsonGenerator: JsonGenerator,
      serializerProvider: SerializerProvider): Unit = {
    val metricsMap = ExecutorMetricType.metricToOffset.map { case (metric, _) =>
      metric -> metrics.getMetricDistribution(metric)
    }
    jsonGenerator.writeObject(metricsMap)
  }
}
