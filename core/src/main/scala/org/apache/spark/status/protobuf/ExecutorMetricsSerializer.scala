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

package org.apache.spark.status.protobuf

import org.apache.spark.executor.ExecutorMetrics
import org.apache.spark.metrics.ExecutorMetricType

private[protobuf] object ExecutorMetricsSerializer {
  def serialize(e: ExecutorMetrics): StoreTypes.ExecutorMetrics = {
    val builder = StoreTypes.ExecutorMetrics.newBuilder()
    ExecutorMetricType.metricToOffset.foreach { case (metric, _) =>
      builder.putMetrics(metric, e.getMetricValue(metric))
    }
    builder.build()
  }

  def deserialize(binary: StoreTypes.ExecutorMetrics): ExecutorMetrics = {
    val array = ExecutorMetricType.metricToOffset.map { case (name, idx) =>
      binary.getMetricsOrDefault(name, 0L)
    }.toArray
    new ExecutorMetrics(array)
  }
}
