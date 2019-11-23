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

package org.apache.spark.scheduler.cluster.mesos

import org.apache.spark.SparkContext
import org.apache.spark.deploy.mesos.config._
import org.apache.spark.internal.config._
import org.apache.spark.scheduler.{ExternalClusterManager, SchedulerBackend, TaskScheduler, TaskSchedulerImpl}

/**
 * Cluster Manager for creation of Mesos scheduler and backend
 */
private[spark] class MesosClusterManager extends ExternalClusterManager {
  private val MESOS_REGEX = """mesos://(.*)""".r

  override def canCreate(masterURL: String): Boolean = {
    masterURL.startsWith("mesos")
  }

  override def createTaskScheduler(sc: SparkContext, masterURL: String): TaskScheduler = {
    new TaskSchedulerImpl(sc)
  }

  override def createSchedulerBackend(sc: SparkContext,
      masterURL: String,
      scheduler: TaskScheduler): SchedulerBackend = {
    require(!sc.conf.get(IO_ENCRYPTION_ENABLED),
      "I/O encryption is currently not supported in Mesos.")

    val mesosUrl = MESOS_REGEX.findFirstMatchIn(masterURL).get.group(1)
    val coarse = sc.conf.get(COARSE_MODE)
    if (coarse) {
      new MesosCoarseGrainedSchedulerBackend(
        scheduler.asInstanceOf[TaskSchedulerImpl],
        sc,
        mesosUrl,
        sc.env.securityManager)
    } else {
      new MesosFineGrainedSchedulerBackend(
        scheduler.asInstanceOf[TaskSchedulerImpl],
        sc,
        mesosUrl)
    }
  }

  override def initialize(scheduler: TaskScheduler, backend: SchedulerBackend): Unit = {
    scheduler.asInstanceOf[TaskSchedulerImpl].initialize(backend)
  }
}

