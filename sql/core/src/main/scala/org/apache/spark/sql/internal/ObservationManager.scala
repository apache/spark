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

import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.sql.{Dataset, Observation, SparkSession}
import org.apache.spark.sql.catalyst.plans.logical.CollectMetrics
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.util.QueryExecutionListener

/**
 * This class keeps track of registered Observations that await query completion.
 */
private[sql] class ObservationManager(session: SparkSession) {
  private val observations = new ConcurrentHashMap[(String, Long), Observation]
  session.listenerManager.register(Listener)

  def register(observation: Observation, ds: Dataset[_]): Unit = {
    if (ds.isStreaming) {
      throw new IllegalArgumentException("Observation does not support streaming Datasets." +
        "This is because there will be multiple observed metrics as microbatches are constructed" +
        ". Please register a StreamingQueryListener and get the metric for each microbatch in " +
        "QueryProgressEvent.progress, or use query.lastProgress or query.recentProgress.")
    }
    register(observation, ds.id)
  }

  def register(observation: Observation, dataFrameId: Long): Unit = {
    observation.markRegistered()
    observations.putIfAbsent((observation.name, dataFrameId), observation)
  }

  private def tryComplete(qe: QueryExecution): Unit = {
    val allMetrics = qe.observedMetrics
    qe.logical.foreach {
      case c: CollectMetrics =>
        allMetrics.get(c.name).foreach { metrics =>
          val observation = observations.remove((c.name, c.dataframeId))
          if (observation != null) {
            observation.setMetricsAndNotify(metrics)
          }
        }
      case _ =>
    }
  }

  private object Listener extends QueryExecutionListener {
    override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit =
      tryComplete(qe)

    override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit =
      tryComplete(qe)
  }
}
