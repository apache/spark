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

import org.apache.spark.SparkContext
import org.apache.spark.deploy.yarn.ApplicationMaster
import org.apache.spark.scheduler.TaskSchedulerImpl
import org.apache.spark.util.YarnContainerInfoHelper

private[spark] class YarnClusterSchedulerBackend(
    scheduler: TaskSchedulerImpl,
    sc: SparkContext)
  extends YarnSchedulerBackend(scheduler, sc) {

  override def start(): Unit = {
    val attemptId = ApplicationMaster.getAttemptId
    bindToYarn(attemptId.getApplicationId(), Some(attemptId))
    super.start()
    totalExpectedExecutors = SchedulerBackendUtils.getInitialTargetExecutorNumber(sc.conf)
    startBindings()
  }

  override def stop(exitCode: Int): Unit = {
    yarnSchedulerEndpoint.signalDriverStop(exitCode)
    super.stop()
  }

  override def getDriverLogUrls: Option[Map[String, String]] = {
    YarnContainerInfoHelper.getLogUrls(sc.hadoopConfiguration, container = None)
  }

  override def getDriverAttributes: Option[Map[String, String]] = {
    YarnContainerInfoHelper.getAttributes(sc.hadoopConfiguration, container = None)
  }
}
