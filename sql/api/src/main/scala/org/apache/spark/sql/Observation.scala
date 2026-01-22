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
import java.util.concurrent.atomic.AtomicBoolean

import scala.concurrent.{Future, Promise}
import scala.concurrent.duration.{Duration, DurationInt}
import scala.jdk.CollectionConverters.MapHasAsJava
import scala.util.Try

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
 * This collects the metrics while the first action is executed on the observed dataset.
 * Subsequent actions do not modify the metrics returned by [[get]]. Retrieval of the metric via
 * [[get]] blocks until the first action has finished and metrics become available.
 *
 * This class does not support streaming datasets.
 *
 * @param name
 *   name of the metric
 * @since 3.3.0
 */
class Observation(val name: String) {
  require(name.nonEmpty, "Name must not be empty")

  /**
   * Create an Observation with a random name.
   */
  def this() = this(UUID.randomUUID().toString)

  private val isRegistered = new AtomicBoolean()

  private val promise = Promise[Row]()

  /**
   * Future holding the (yet to be completed) observation.
   */
  val future: Future[Row] = promise.future

  /**
   * (Scala-specific) Get the observed metrics. This waits for the observed dataset to finish its
   * first action. Only the result of the first action is available. Subsequent actions do not
   * modify the result.
   *
   * Note that if no metrics were recorded, an empty map is probably returned. It possibly happens
   * when the operators used for observation are optimized away.
   *
   * @return
   *   the observed metrics as a `Map[String, Any]`
   * @throws InterruptedException
   *   interrupted while waiting
   */
  @throws[InterruptedException]
  def get: Map[String, Any] = {
    val row = getRow
    if (row == null || row.schema == null) {
      Map.empty
    } else {
      row.getValuesMap(row.schema.map(_.name))
    }
  }

  /**
   * (Java-specific) Get the observed metrics. This waits for the observed dataset to finish its
   * first action. Only the result of the first action is available. Subsequent actions do not
   * modify the result.
   *
   * Note that if no metrics were recorded, an empty map is probably returned. It possibly happens
   * when the operators used for observation are optimized away.
   *
   * @return
   *   the observed metrics as a `java.util.Map[String, Object]`
   * @throws InterruptedException
   *   interrupted while waiting
   */
  @throws[InterruptedException]
  def getAsJava: java.util.Map[String, Any] = get.asJava

  /**
   * Mark this Observation as registered.
   */
  private[sql] def markRegistered(): Unit = {
    if (!isRegistered.compareAndSet(false, true)) {
      throw new IllegalArgumentException("An Observation can be used with a Dataset only once")
    }
  }

  /**
   * Set the observed metrics and notify all waiting threads to resume.
   *
   * @return
   *   `true` if all waiting threads were notified, `false` if otherwise.
   */
  private[sql] def setMetricsAndNotify(metrics: Row): Boolean = {
    promise.trySuccess(metrics)
  }

  /**
   * Get the observed metrics as a Row.
   *
   * @return
   *   the observed metrics as a `Row`, or None if the metrics are not available.
   */
  private[sql] def getRowOrEmpty: Option[Row] = {
    Try(SparkThreadUtils.awaitResult(future, 100.millis)).toOption
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
