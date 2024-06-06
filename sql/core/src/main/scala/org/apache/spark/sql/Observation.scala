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

import org.apache.spark.sql.catalyst.plans.logical.CollectMetrics
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.util.QueryExecutionListener
import org.apache.spark.util.ArrayImplicits._


/**
 * Helper class to simplify usage of `Dataset.observe(String, Column, Column*)`:
 *
 * {{{
 *   // Observe row count (rows) and highest id (maxid) in the Dataset while writing it
 *   val observation = Observation("my metrics")
 *   val observed_ds = ds.observe(observation, count(lit(1)).as("rows"), max($"id").as("maxid"))
 *   observed_ds.write.parquet("ds.parquet")
 *   val metrics = observation.get
 * }}}
 *
 * This collects the metrics while the first action is executed on the observed dataset. Subsequent
 * actions do not modify the metrics returned by [[get]]. Retrieval of the metric via [[get]]
 * blocks until the first action has finished and metrics become available.
 *
 * This class does not support streaming datasets.
 *
 * @param name name of the metric
 * @since 3.3.0
 */
class Observation(name: String) extends ObservationBase(name) {

  /**
   * Create an Observation instance without providing a name. This generates a random name.
   */
  def this() = this(UUID.randomUUID().toString)

  private val listener: ObservationListener = ObservationListener(this)

  @volatile private var dataframeId: Option[(SparkSession, Long)] = None

  /**
   * Attach this observation to the given [[Dataset]] to observe aggregation expressions.
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
      throw new IllegalArgumentException("Observation does not support streaming Datasets." +
        "This is because there will be multiple observed metrics as microbatches are constructed" +
        ". Please register a StreamingQueryListener and get the metric for each microbatch in " +
        "QueryProgressEvent.progress, or use query.lastProgress or query.recentProgress.")
    }
    register(ds.sparkSession, ds.id)
    ds.observe(name, expr, exprs: _*)
  }

  private[sql] def register(sparkSession: SparkSession, dataframeId: Long): Unit = {
    // makes this class thread-safe:
    // only the first thread entering this block can set sparkSession
    // all other threads will see the exception, as it is only allowed to do this once
    synchronized {
      if (this.dataframeId.isDefined) {
        throw new IllegalArgumentException("An Observation can be used with a Dataset only once")
      }
      this.dataframeId = Some((sparkSession, dataframeId))
    }

    sparkSession.listenerManager.register(this.listener)
  }

  private def unregister(): Unit = {
    this.dataframeId.foreach(_._1.listenerManager.unregister(this.listener))
  }

  private[spark] def onFinish(qe: QueryExecution): Unit = {
    synchronized {
      if (this.metrics.isEmpty && qe.logical.exists {
        case CollectMetrics(name, _, _, dataframeId) =>
          name == this.name && dataframeId == this.dataframeId.get._2
        case _ => false
      }) {
        val row = qe.observedMetrics.get(name)
        val metrics = row.map(r => r.getValuesMap[Any](r.schema.fieldNames.toImmutableArraySeq))
        if (setMetricsAndNotify(metrics)) {
          unregister()
        }
      }
    }
  }

}

private[sql] case class ObservationListener(observation: Observation)
  extends QueryExecutionListener {

  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit =
    observation.onFinish(qe)

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit =
    observation.onFinish(qe)

}

/**
 * (Scala-specific) Create instances of Observation via Scala `apply`.
 * @since 3.3.0
 */
object Observation {

  /**
   * Observation constructor for creating an anonymous observation.
   */
  def apply(): Observation = new Observation()

  /**
   * Observation constructor for creating a named observation.
   */
  def apply(name: String): Observation = new Observation(name)

}
