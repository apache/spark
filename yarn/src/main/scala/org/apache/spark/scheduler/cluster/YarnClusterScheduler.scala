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

import org.apache.hadoop.yarn.util.RackResolver

import org.apache.spark._
import org.apache.spark.deploy.yarn.ApplicationMaster
import org.apache.spark.scheduler.TaskSchedulerImpl
import org.apache.spark.util.Utils

/**
 * This is a simple extension to ClusterScheduler - to ensure that appropriate initialization of
 * ApplicationMaster, etc is done
 */
private[spark] class YarnClusterScheduler(sc: SparkContext) extends TaskSchedulerImpl(sc) {

  logInfo("Created YarnClusterScheduler")

  // Nothing else for now ... initialize application master : which needs a SparkContext to
  // determine how to allocate.
  // Note that only the first creation of a SparkContext influences (and ideally, there must be
  // only one SparkContext, right ?). Subsequent creations are ignored since executors are already
  // allocated by then.

  // By default, rack is unknown
  override def getRackForHost(hostPort: String): Option[String] = {
    val host = Utils.parseHostPort(hostPort)._1
    Option(RackResolver.resolve(sc.hadoopConfiguration, host).getNetworkLocation)
  }

  override def postStartHook() {
    ApplicationMaster.sparkContextInitialized(sc)
    super.postStartHook()
    logInfo("YarnClusterScheduler.postStartHook done")
  }

  override def stop() {
    super.stop()
    ApplicationMaster.sparkContextStopped(sc)
  }

}
