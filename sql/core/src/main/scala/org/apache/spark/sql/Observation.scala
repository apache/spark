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

import scala.collection.JavaConverters
import scala.concurrent.{Future, Promise}
import scala.concurrent.duration.Duration

import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.util.QueryExecutionListener
import org.apache.spark.util.SparkThreadUtils


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
class Observation(val name: String) {

  if (name.isEmpty) throw new IllegalArgumentException("Name must not be empty")

  /**
   * Create an Observation instance without providing a name. This generates a random name.
   */
  def this() = this(UUID.randomUUID().toString)

  private val listener: ObservationListener = ObservationListener(this)

  @volatile private var sparkSession: Option[SparkSession] = None

  private val promise = Promise[Row]()

  /**
   * Future holding the (yet to be completed) observation.
   */
  val future: Future[Row] = promise.future

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
      throw new IllegalArgumentException("Observation does not support streaming Datasets")
    }
    register(ds.sparkSession)
    ds.observe(name, expr, exprs: _*)
  }

  /**
   * (Scala-specific) Get the observed metrics. This waits for the observed dataset to finish
   * its first action. Only the result of the first action is available. Subsequent actions do not
   * modify the result.
   *
   * @return the observed metrics as a `Map[String, Any]`
   * @throws InterruptedException interrupted while waiting
   */
  @throws[InterruptedException]
  def get: Map[String, _] = {
    val row = getRow
    row.getValuesMap(row.schema.map(_.name))
  }

  /**
   * (Java-specific) Get the observed metrics. This waits for the observed dataset to finish
   * its first action. Only the result of the first action is available. Subsequent actions do not
   * modify the result.
   *
   * @return the observed metrics as a `java.util.Map[String, Object]`
   * @throws InterruptedException interrupted while waiting
   */
  @throws[InterruptedException]
  def getAsJava: java.util.Map[String, AnyRef] = {
    JavaConverters.mapAsJavaMap(
      get.map { case (key, value) => (key, value.asInstanceOf[Object])}
    )
  }

  private def register(sparkSession: SparkSession): Unit = {
    // makes this class thread-safe:
    // only the first thread entering this block can set sparkSession
    // all other threads will see the exception, as it is only allowed to do this once
    synchronized {
      if (this.sparkSession.isDefined) {
        throw new IllegalArgumentException("An Observation can be used with a Dataset only once")
      }
      this.sparkSession = Some(sparkSession)
    }

    sparkSession.listenerManager.register(this.listener)
  }

  private def unregister(): Unit = {
    this.sparkSession.foreach(_.listenerManager.unregister(this.listener))
  }

  private[spark] def onFinish(qe: QueryExecution): Unit = {
    qe.observedMetrics.get(name).foreach { metrics =>
      promise.trySuccess(metrics)
      unregister()
    }
  }

  /**
   * Get the observed metrics as a Row.
   *
   * @return
   *   the observed metrics as a `Row`.
   */
  private[sql] def getRow: Row = {
    SparkThreadUtils.awaitResult(future, Duration.Inf)
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
