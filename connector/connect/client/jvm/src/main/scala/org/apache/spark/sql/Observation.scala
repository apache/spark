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

class Observation(name: String) extends ObservationBase(name) {

  /**
   * Create an Observation instance without providing a name. This generates a random name.
   */
  def this() = this(UUID.randomUUID().toString)

  @volatile private var planId: Option[(SparkSession, Long)] = None

  private[sql] def register(sparkSession: SparkSession, planId: Long): Unit = {
    // makes this class thread-safe:
    // only the first thread entering this block can set sparkSession
    // all other threads will see the exception, as it is only allowed to do this once
    synchronized {
      if (this.planId.isDefined) {
        throw new IllegalArgumentException("An Observation can be used with a Dataset only once")
      }
      this.planId = Some((sparkSession, planId))
    }

    sparkSession.observationRegistry.put(planId, this)
  }

  private def unregister(): Unit = {
    this.planId.map { case (sparkSession, planId) =>
      sparkSession.observationRegistry.remove(planId)
    }
  }

  override private[spark] def setMetricsAndNotify(metrics: Option[Map[String, Any]]): Boolean = {
    this.unregister()
    super.setMetricsAndNotify(metrics)
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
