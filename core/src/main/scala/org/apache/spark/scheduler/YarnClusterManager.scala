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

package org.apache.spark.scheduler

import org.apache.spark.{SparkContext, SparkException}
import org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend
import org.apache.spark.util.Utils

/**
 * Cluster Manager for creation of Yarn scheduler and backend
 */
class YarnClusterManager extends ExternalClusterManager {

  def canCreate(masterURL: String): Boolean = {
    masterURL == "yarn"
  }

  def createTaskScheduler(sc: SparkContext, masterURL: String): TaskScheduler = {

    val className = if (sc.deployMode == "cluster") {
      "org.apache.spark.scheduler.cluster.YarnClusterScheduler"
    } else if (sc.deployMode == "client") {
      "org.apache.spark.scheduler.cluster.YarnScheduler"
    } else {
      throw new SparkException(s"Unknown deploy mode '${sc.deployMode}' for Yarn")
    }
    try {
      val clazz = Utils.classForName(className)
      val cons = clazz.getConstructor(classOf[SparkContext])
      cons.newInstance(sc).asInstanceOf[TaskSchedulerImpl]
    } catch {
      // TODO: Enumerate the exact reasons why it can fail
      // But irrespective of it, it means we cannot proceed !
      case e: Exception =>
        throw new SparkException("YARN mode not available ?", e)
    }

  }

  def createSchedulerBackend(sc: SparkContext,
      masterURL: String,
      scheduler: TaskScheduler): SchedulerBackend = {

    val className = if (sc.deployMode == "cluster") {
      "org.apache.spark.scheduler.cluster.YarnClusterSchedulerBackend"
    } else if (sc.deployMode == "client") {
      "org.apache.spark.scheduler.cluster.YarnClientSchedulerBackend"
    } else {
      throw new SparkException(s"Unknown deploy mode '${sc.deployMode}' for Yarn")
    }
    try {
      val clazz =
        Utils.classForName(className)
      val cons = clazz.getConstructor(classOf[TaskSchedulerImpl], classOf[SparkContext])
      cons.newInstance(scheduler, sc).asInstanceOf[CoarseGrainedSchedulerBackend]
    } catch {
      case e: Exception =>
        throw new SparkException("YARN mode not available ?", e)
    }
  }

  def initialize(scheduler: TaskScheduler, backend: SchedulerBackend): Unit = {
    scheduler.asInstanceOf[TaskSchedulerImpl].initialize(backend)
  }
}
