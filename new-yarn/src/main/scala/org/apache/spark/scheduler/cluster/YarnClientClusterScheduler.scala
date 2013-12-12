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

import org.apache.spark._
import org.apache.hadoop.conf.Configuration
import org.apache.spark.deploy.yarn.YarnAllocationHandler
import org.apache.spark.util.Utils

/**
 *
 * This scheduler launch worker through Yarn - by call into Client to launch WorkerLauncher as AM.
 */
private[spark] class YarnClientClusterScheduler(sc: SparkContext, conf: Configuration) extends ClusterScheduler(sc) {

  def this(sc: SparkContext) = this(sc, new Configuration())

  // By default, rack is unknown
  override def getRackForHost(hostPort: String): Option[String] = {
    val host = Utils.parseHostPort(hostPort)._1
    val retval = YarnAllocationHandler.lookupRack(conf, host)
    if (retval != null) Some(retval) else None
  }

  override def postStartHook() {

    // The yarn application is running, but the worker might not yet ready
    // Wait for a few seconds for the slaves to bootstrap and register with master - best case attempt
    Thread.sleep(2000L)
    logInfo("YarnClientClusterScheduler.postStartHook done")
  }
}
