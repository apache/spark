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

package org.apache.spark.scheduler.cluster.nomad

import com.hashicorp.nomad.scalasdk.NomadScalaApi

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{ExternalClusterManager, SchedulerBackend, TaskScheduler, TaskSchedulerImpl}

/**
 * A cluster manager that enables Spark applications to spawn executors on a Nomad cluster.
 *
 * The main implementation is in [[NomadClusterSchedulerBackend]].
 */
private[spark] class NomadClusterManager extends ExternalClusterManager with Logging {

  override def canCreate(masterURL: String): Boolean = {
    masterURL.startsWith("nomad")
  }

  override def createTaskScheduler(sc: SparkContext, masterURL: String): TaskScheduler = {
    new TaskSchedulerImpl(sc)
  }

  override def createSchedulerBackend(
      sc: SparkContext,
      masterURL: String,
      scheduler: TaskScheduler): SchedulerBackend = {

    val clusterManagerConf = NomadClusterManagerConf(sc.conf, None)

    logInfo(s"Will access Nomad API at ${clusterManagerConf.nomadApi.getAddress}")
    val api = NomadScalaApi(clusterManagerConf.nomadApi)

    val jobController = SparkNomadJobController.initialize(clusterManagerConf, api)

    new NomadClusterSchedulerBackend(
      scheduler.asInstanceOf[TaskSchedulerImpl],
      sc,
      jobController,
      clusterManagerConf.staticExecutorInstances)
  }

  override def initialize(scheduler: TaskScheduler, backend: SchedulerBackend): Unit = {
    scheduler.asInstanceOf[TaskSchedulerImpl].initialize(backend)
  }

}
