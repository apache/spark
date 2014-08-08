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

package org.apache.spark.scheduler.cluster

import akka.actor.ActorSystem

import org.apache.spark.SparkContext
import org.apache.spark.scheduler.TaskSchedulerImpl

/**
 * Subclass of CoarseGrainedSchedulerBackend that handles waiting until sufficient resources
 * are registered before beginning to schedule tasks (needed by both Yarn scheduler backends).
  */
private[spark] class YarnSchedulerBackend(
    scheduler: TaskSchedulerImpl,
    actorSystem: ActorSystem)
  extends CoarseGrainedSchedulerBackend(scheduler, actorSystem) {

  // Submit tasks only after (registered executors / total expected executors)
  // is equal to at least this value (expected to be a double between 0 and 1, inclusive).
  var minRegisteredRatio = conf.getDouble("spark.scheduler.minRegisteredExecutorsRatio", 0.8)
  if (minRegisteredRatio > 1) minRegisteredRatio = 1
  // Regardless of whether the required number of executors have registered, return true from
  // isReady() after this amount of time has elapsed.
  val maxRegisteredWaitingTime =
    conf.getInt("spark.scheduler.maxRegisteredExecutorsWaitingTime", 30000)
  private val createTime = System.currentTimeMillis()

  var totalExpectedExecutors: Int = _

  override def isReady(): Boolean = {
    val registeredExecutors = totalRegisteredExecutors.get()
    if (registeredExecutors >= totalExpectedExecutors * minRegisteredRatio) {
      logInfo("Sufficient resources registered with YarnSchedulerBackend to begin scheduling " +
        s"tasks: $registeredExecutors registered executors out of $totalExpectedExecutors " +
        s"total expected executors (min registered executors ratio of ${minRegisteredRatio})"
      true
    } else if ((System.currentTimeMillis() - createTime) >= maxRegisteredWaitingTime) {
      logInfo("YarnSchedulerBackend is ready to begin scheduling, even though insufficient" +
        "resources have been registered, because the maximum waiting time of " +
        s"$maxRegisteredWaitingTime milliseconds has elapsed ($registeredExecutors of " +
        s"${minRegisteredRatio * totalExpectedExecutors} required executors have registered)")
      true
    } else {
      false
    }
  }
}
