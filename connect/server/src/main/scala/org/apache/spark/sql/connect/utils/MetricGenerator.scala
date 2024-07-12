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

package org.apache.spark.sql.connect.utils

import scala.jdk.CollectionConverters._

import org.apache.spark.connect.proto.ExecutePlanResponse
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.connect.service.SessionHolder
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper

/**
 * Helper object for generating responses with metrics from queries.
 */
private[connect] object MetricGenerator extends AdaptiveSparkPlanHelper {

  def createMetricsResponse(
      sessionHolder: SessionHolder,
      rows: DataFrame): ExecutePlanResponse = {
    ExecutePlanResponse
      .newBuilder()
      .setSessionId(sessionHolder.sessionId)
      .setServerSideSessionId(sessionHolder.serverSessionId)
      .setMetrics(MetricGenerator.buildMetrics(rows.queryExecution.executedPlan))
      .build()
  }

  private def buildMetrics(p: SparkPlan): ExecutePlanResponse.Metrics = {
    val b = ExecutePlanResponse.Metrics.newBuilder
    b.addAllMetrics(transformPlan(p, p.id).asJava)
    b.build()
  }

  private def transformChildren(p: SparkPlan): Seq[ExecutePlanResponse.Metrics.MetricObject] = {
    allChildren(p).flatMap(c => transformPlan(c, p.id))
  }

  private[connect] def transformPlan(
      rows: DataFrame): Seq[ExecutePlanResponse.Metrics.MetricObject] = {
    val executedPlan = rows.queryExecution.executedPlan
    transformPlan(executedPlan, executedPlan.id)
  }

  private def transformPlan(
      p: SparkPlan,
      parentId: Int): Seq[ExecutePlanResponse.Metrics.MetricObject] = {
    val mv = p.metrics.map(m =>
      m._1 -> ExecutePlanResponse.Metrics.MetricValue.newBuilder
        .setName(m._2.name.getOrElse(""))
        .setValue(m._2.value)
        .setMetricType(m._2.metricType)
        .build())
    val mo = ExecutePlanResponse.Metrics.MetricObject
      .newBuilder()
      .setName(p.nodeName)
      .setPlanId(p.id)
      .setParent(parentId)
      .putAllExecutionMetrics(mv.asJava)
      .build()
    Seq(mo) ++ transformChildren(p)
  }
}
