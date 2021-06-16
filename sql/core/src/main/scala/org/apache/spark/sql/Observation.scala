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

package org.apache.spark.sql

import java.util.UUID
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.{Condition, Lock, ReentrantLock}

import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.util.QueryExecutionListener

/**
 * Not thread-safe.
 * @param name
 * @param sparkSession
 */
class Observation(name: String) {

  private val lock: Lock = new ReentrantLock()
  private val completed: Condition = lock.newCondition()
  private val listener: ObservationListener = ObservationListener(this)

  private var sparkSession: Option[SparkSession] = None

  @transient private var row: Option[Row] = None

  /**
   * Attach this observation to the given Dataset.
   * Remember to call `close()` when the observation is done.
   *
   * @param ds dataset
   * @tparam T dataset type
   * @return observed dataset
   */
  def on[T](ds: Dataset[T])(expr: Column, exprs: Column*): Dataset[T] = {
    if (ds.isStreaming) {
      throw new IllegalArgumentException("Observation does not support streaming Datasets")
    }
    register(ds.sparkSession)
    ds.observe(name, expr, exprs: _*)
  }

  /**
   * Wait for the first action on the observed dataset to complete and returns true.
   * This method times out after the given amount of time and returns false.
   *
   * @param time timeout
   * @param unit timeout time unit
   * @return true if action complete within timeout, false on timeout
   */
  def waitCompleted(time: Long, unit: TimeUnit): Boolean = waitCompleted(Some(time), unit)

  /**
   * Get the observation results. This waits until the observed dataset finishes its first action.
   * If you want to wait for the result and provide a timeout, use waitCompleted.
   * Only the result of the first action is available. Subsequent actions do not modify the result.
   */
  def get: Row = {
    waitCompleted(None, TimeUnit.SECONDS)
    row.get
  }

  private def waitCompleted(time: Option[Long], unit: TimeUnit): Boolean = {
    lock.lock()
    try {
      if (row.isEmpty) {
        if (time.isDefined) {
          completed.await(time.get, unit)
        } else {
          completed.await()
        }
      }
      row.isDefined
    } finally {
      lock.unlock()
    }
  }

  private def getMetricRow(metrics: Map[String, Row]): Option[Row] =
    metrics
      .find { case (metricName, _) => metricName.equals(name) }
      .map { case (_, row) => row }

  private def register(sparkSession: SparkSession): Unit = {
    if (this.sparkSession.isDefined) {
      throw new IllegalStateException("An Observation can be used with a Dataset only once")
    }
    this.sparkSession = Some(sparkSession)
    sparkSession.listenerManager.register(listener)
  }

  private def unregister(): Unit = {
    this.sparkSession.foreach(_.listenerManager.unregister(listener))
  }

  private[spark] def onFinish(funcName: String, qe: QueryExecution): Unit = {
    lock.lock()
    try {
      this.row = getMetricRow(qe.observedMetrics)
      if (this.row.isDefined) completed.signalAll()
    } finally {
      lock.unlock()
    }
    unregister()
  }

}

private[sql] case class ObservationListener(observation: Observation)
  extends QueryExecutionListener {

  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit =
    observation.onFinish(funcName, qe)

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit =
    observation.onFinish(funcName, qe)

}

object Observation {

  /**
   * Observation constructor for creating an anonymous observation.
   */
  def apply(): Observation = new Observation(UUID.randomUUID().toString)

  /**
   * Observation constructor for creating a named observation.
   */
  def apply(name: String): Observation = new Observation(name)

}
