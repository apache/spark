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

import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.util.QueryExecutionListener

/**
 * Helper class to simplify usage of [[Dataset.observe(String, Column, Column*)]]:
 *
 * {{{
 *   // Observe row count (rows) and highest id (maxid) in the Dataset while writing it
 *   val observation = Observation("my_metrics")
 *   val observed_ds = ds.observe(observation, count(lit(1)).as("rows"), max($"id").as("maxid"))
 *   observed_ds.write.parquet("ds.parquet")
 *   val metrics = observation.get
 * }}}
 *
 * This collects the metrics while the first action is executed on the obseerved dataset. Subsequent
 * actions do not modify the metrics returned by [[org.apache.spark.sql.Observation.get]]. Retrieval
 * of the metric via [[org.apache.spark.sql.Observation.get]] blocks until the first action has
 * finished and metrics become available. You can add a timeout to that blocking via
 * [[org.apache.spark.sql.Observation.waitCompleted]]:
 *
 * {{{
 *   if (observation.waitCompleted(100, TimeUnit.MILLISECONDS)) {
 *     observation.get
 *   }
 * }}}
 *
 * This class does not support streaming datasets.
 *
 * @param name name of the metric
 * @since 3.2.0
 */
class Observation(name: String) {

  private val listener: ObservationListener = ObservationListener(this)

  @volatile private var sparkSession: Option[SparkSession] = None

  @volatile private var row: Option[Row] = None

  /**
   * Attaches this observation to the given [[Dataset]] to observe aggregation expressions.
   *
   * @param ds dataset
   * @param expr first aggregation expression
   * @param exprs more aggregation expressions
   * @tparam T dataset type
   * @return observed dataset
   * @throws IllegalArgumentException If this is a streaming Dataset (ds.isStreaming == true)
   */
  private[spark] def on[T](ds: Dataset[T], expr: Column, exprs: Column*): Dataset[T] = {
    if (ds.isStreaming) {
      throw new IllegalArgumentException("Observation does not support streaming Datasets")
    }
    register(ds.sparkSession)
    ds.observe(name, expr, exprs: _*)
  }

  /**
   * Waits for the first action on the observed dataset to complete and returns true.
   * The result is then available through the get method.
   * This method times out after the given amount of time returning false.
   *
   * @param time timeout
   * @param unit timeout time unit
   * @return true if action completed within timeout, false otherwise
   * @throws InterruptedException interrupted while waiting
   */
  def waitCompleted(time: Long, unit: TimeUnit): Boolean = waitCompleted(Some(unit.toMillis(time)))

  /**
   * Get the observed metrics. This waits until the observed dataset finishes its first action.
   * If you want to wait for the result and provide a timeout, use [[waitCompleted]]. Only the
   * result of the first action is available. Subsequent actions do not modify the result.
   *
   * @return the observed metrics as a [[Row]]
   * @throws InterruptedException interrupted while waiting
   */
  def get: Row = {
    assert(waitCompleted(None), "waitCompleted without timeout returned false")
    row.get
  }

  private def waitCompleted(millis: Option[Long]): Boolean = {
    synchronized {
      // millis might be 0 or negative, calling this.wait(0) waits forever
      // while we may want to wait 0 ms if millis is set to 0
      if (millis.forall(_ > 0)) {
        if (row.isEmpty) {
          // if millis is None, we want to wait forever, hence wait(0)
          this.wait(millis.getOrElse(0))
        }
      }
      row.isDefined
    }
  }

  private def register(sparkSession: SparkSession): Unit = {
    // makes this class thread-safe:
    // only the first thread entering this block can set sparkSession
    // all other threads will see the exception, because it is only allowed to do this once
    synchronized {
      if (this.sparkSession.isDefined) {
        throw new IllegalStateException("An Observation can be used with a Dataset only once")
      }
      this.sparkSession = Some(sparkSession)
    }

    sparkSession.listenerManager.register(listener)
  }

  private def unregister(): Unit = {
    this.sparkSession.foreach(_.listenerManager.unregister(listener))
  }

  private[spark] def onFinish(qe: QueryExecution): Unit = {
    synchronized {
      if (this.row.isEmpty) {
        this.row = qe.observedMetrics.get(name)
        assert(this.row.isDefined, "No metric provided by QueryExecutionListener")
      }
      this.notifyAll()
    }
    unregister()
  }

}

private[sql] case class ObservationListener(observation: Observation)
  extends QueryExecutionListener {

  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit =
    observation.onFinish(qe)

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit =
    observation.onFinish(qe)

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
