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

import org.apache.spark.{SparkContext, SparkException}
import org.apache.spark.scheduler.{ExternalClusterManager, SchedulerBackend, TaskScheduler, TaskSchedulerImpl}

/**
 * Cluster Manager for creation of Yarn scheduler and backend
 */
private[spark] class YarnClusterManager extends ExternalClusterManager {

  override def canCreate(masterURL: String): Boolean = {
    masterURL == "yarn"
  }

  override def createTaskScheduler(sc: SparkContext, masterURL: String): TaskScheduler = {
    sc.deployMode match {
      case "cluster" => new YarnClusterScheduler(sc)
      case "client" => new YarnScheduler(sc)
      case _ => throw new SparkException(s"Unknown deploy mode '${sc.deployMode}' for Yarn")
    }
  }

  override def createSchedulerBackend(sc: SparkContext,
      masterURL: String,
      scheduler: TaskScheduler): SchedulerBackend = {
    sc.deployMode match {
      case "cluster" =>
        new YarnClusterSchedulerBackend(scheduler.asInstanceOf[TaskSchedulerImpl], sc)
      case "client" =>
        new YarnClientSchedulerBackend(scheduler.asInstanceOf[TaskSchedulerImpl], sc)
      case  _ =>
        throw new SparkException(s"Unknown deploy mode '${sc.deployMode}' for Yarn")
    }
  }

  override def initialize(scheduler: TaskScheduler, backend: SchedulerBackend): Unit = {
    scheduler.asInstanceOf[TaskSchedulerImpl].initialize(backend)
  }
}
