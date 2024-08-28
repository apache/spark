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
package org.apache.spark.sql.internal

import org.apache.spark.sql.{Observation, SparkSession}
import org.apache.spark.sql.catalyst.plans.logical.CollectMetrics
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.util.QueryExecutionListener

/**
 * Listener that awaits the completion of an Observation.
 */
private[sql] class ObservationListener(
    observation: Observation,
    session: SparkSession,
    dataFrameId: Long)
  extends QueryExecutionListener {

  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit =
    tryComplete(qe)

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit =
    tryComplete(qe)

  private def tryComplete(qe: QueryExecution): Unit = {
    if (isCorrectQueryExecution(qe)) {
      val maybeMetrics = qe.observedMetrics.get(observation.name)
      if (maybeMetrics.isDefined) {
        val metrics = maybeMetrics.get
        observation.setMetricsAndNotify(metrics)
        session.listenerManager.unregister(this)
      }
    }
  }

  private def isCorrectQueryExecution(qe: QueryExecution): Boolean = {
    qe.logical.exists {
      case c: CollectMetrics =>
        c.name == observation.name && c.dataframeId == dataFrameId
    }
  }
}
