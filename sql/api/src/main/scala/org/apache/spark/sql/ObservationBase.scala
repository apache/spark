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

import scala.jdk.CollectionConverters.MapHasAsJava

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
abstract class ObservationBase(val name: String) {

  if (name.isEmpty) throw new IllegalArgumentException("Name must not be empty")

  @volatile protected var metrics: Option[Map[String, Any]] = None

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
    synchronized {
      // we need to loop as wait might return without us calling notify
      // https://en.wikipedia.org/w/index.php?title=Spurious_wakeup&oldid=992601610
      while (this.metrics.isEmpty) {
        wait()
      }
    }

    this.metrics.get
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
    get.map { case (key, value) => (key, value.asInstanceOf[Object]) }.asJava
  }

  /**
   * Get the observed metrics. This returns the metrics if they are available, otherwise an empty.
   *
   * @return the observed metrics as a `Map[String, Any]`
   */
  @throws[InterruptedException]
  private[sql] def getOrEmpty: Map[String, _] = {
    synchronized {
      if (metrics.isEmpty) {
        wait(100) // Wait for 100ms to see if metrics are available
      }
      metrics.getOrElse(Map.empty)
    }
  }

  /**
   * Set the observed metrics and notify all waiting threads to resume.
   *
   * @return `true` if all waiting threads were notified, `false` if otherwise.
   */
  private[spark] def setMetricsAndNotify(metrics: Option[Map[String, Any]]): Boolean = {
    synchronized {
      this.metrics = metrics
      if(metrics.isDefined) {
        notifyAll()
        true
      } else {
        false
      }
    }
  }
}
