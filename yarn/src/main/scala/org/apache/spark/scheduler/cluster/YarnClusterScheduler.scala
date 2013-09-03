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
import org.apache.spark.deploy.yarn.{ApplicationMaster, YarnAllocationHandler}
import org.apache.spark.util.Utils
import org.apache.hadoop.conf.Configuration

/**
 *
 * This is a simple extension to ClusterScheduler - to ensure that appropriate initialization of ApplicationMaster, etc is done
 */
private[spark] class YarnClusterScheduler(sc: SparkContext, conf: Configuration) extends ClusterScheduler(sc) {

  logInfo("Created YarnClusterScheduler")

  def this(sc: SparkContext) = this(sc, new Configuration())

  // Nothing else for now ... initialize application master : which needs sparkContext to determine how to allocate
  // Note that only the first creation of SparkContext influences (and ideally, there must be only one SparkContext, right ?)
  // Subsequent creations are ignored - since nodes are already allocated by then.


  // By default, rack is unknown
  override def getRackForHost(hostPort: String): Option[String] = {
    val host = Utils.parseHostPort(hostPort)._1
    val retval = YarnAllocationHandler.lookupRack(conf, host)
    if (retval != null) Some(retval) else None
  }

  override def postStartHook() {
    val sparkContextInitialized = ApplicationMaster.sparkContextInitialized(sc)
    if (sparkContextInitialized){
      // Wait for a few seconds for the slaves to bootstrap and register with master - best case attempt
      Thread.sleep(3000L)
    }
    logInfo("YarnClusterScheduler.postStartHook done")
  }
}
