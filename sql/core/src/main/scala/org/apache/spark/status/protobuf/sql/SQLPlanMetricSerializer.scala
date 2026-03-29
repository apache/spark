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

package org.apache.spark.status.protobuf.sql

import org.apache.spark.sql.execution.ui.SQLPlanMetric
import org.apache.spark.status.protobuf.StoreTypes
import org.apache.spark.status.protobuf.Utils._
import org.apache.spark.util.Utils.weakIntern

private[protobuf] object SQLPlanMetricSerializer {

  def serialize(metric: SQLPlanMetric): StoreTypes.SQLPlanMetric = {
    val builder = StoreTypes.SQLPlanMetric.newBuilder()
    setStringField(metric.name, builder.setName)
    builder.setAccumulatorId(metric.accumulatorId)
    setStringField(metric.metricType, builder.setMetricType)
    builder.build()
  }

  def deserialize(metrics: StoreTypes.SQLPlanMetric): SQLPlanMetric = {
    SQLPlanMetric(
      name = getStringField(metrics.hasName, () => weakIntern(metrics.getName)),
      accumulatorId = metrics.getAccumulatorId,
      metricType = getStringField(metrics.hasMetricType, () => weakIntern(metrics.getMetricType))
    )
  }
}
