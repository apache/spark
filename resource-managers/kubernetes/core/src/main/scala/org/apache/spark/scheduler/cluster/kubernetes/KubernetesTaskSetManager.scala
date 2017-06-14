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
package org.apache.spark.scheduler.cluster.kubernetes

import java.net.InetAddress

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.scheduler.{TaskSchedulerImpl, TaskSet, TaskSetManager}

private[spark] class KubernetesTaskSetManager(
    sched: TaskSchedulerImpl,
    taskSet: TaskSet,
    maxTaskFailures: Int,
    inetAddressUtil: InetAddressUtil = new InetAddressUtil)
  extends TaskSetManager(sched, taskSet, maxTaskFailures) {

  /**
   * Overrides the lookup to use not only the executor pod IP, but also the cluster node
   * name and host IP address that the pod is running on. The base class may have populated
   * the lookup target map with HDFS datanode locations if this task set reads HDFS data.
   * Those datanode locations are based on cluster node names or host IP addresses. Using
   * only executor pod IPs may not match them.
   */
  override def getPendingTasksForHost(executorIP: String): ArrayBuffer[Int] = {
    val pendingTasksExecutorIP = super.getPendingTasksForHost(executorIP)
    if (pendingTasksExecutorIP.nonEmpty) {
      pendingTasksExecutorIP
    } else {
      val backend = sched.backend.asInstanceOf[KubernetesClusterSchedulerBackend]
      val pod = backend.getExecutorPodByIP(executorIP)
      if (pod.nonEmpty) {
        val clusterNodeName = pod.get.getSpec.getNodeName
        val pendingTasksClusterNodeName = super.getPendingTasksForHost(clusterNodeName)
        if (pendingTasksClusterNodeName.nonEmpty) {
          logDebug(s"Got preferred task list $pendingTasksClusterNodeName for executor host " +
            s"$executorIP using cluster node name $clusterNodeName")
          pendingTasksClusterNodeName
        } else {
          val clusterNodeIP = pod.get.getStatus.getHostIP
          val pendingTasksClusterNodeIP = super.getPendingTasksForHost(clusterNodeIP)
          if (pendingTasksClusterNodeIP.nonEmpty) {
            logDebug(s"Got preferred task list $pendingTasksClusterNodeIP for executor host " +
              s"$executorIP using cluster node IP $clusterNodeIP")
            pendingTasksClusterNodeIP
          } else {
            val clusterNodeFullName = inetAddressUtil.getFullHostName(clusterNodeIP)
            val pendingTasksClusterNodeFullName = super.getPendingTasksForHost(clusterNodeFullName)
            if (pendingTasksClusterNodeFullName.nonEmpty) {
              logDebug(s"Got preferred task list $pendingTasksClusterNodeFullName " +
                s"for executor host $executorIP using cluster node full name $clusterNodeFullName")
            }
            pendingTasksClusterNodeFullName
          }
        }
      } else {
        pendingTasksExecutorIP  // Empty
      }
    }
  }
}

// To support mocks in unit tests.
private[kubernetes] class InetAddressUtil {

  // NOTE: This does issue a network call to DNS. Caching is done internally by the InetAddress
  // class for both hits and misses.
  def getFullHostName(ipAddress: String): String = {
    InetAddress.getByName(ipAddress).getCanonicalHostName
  }
}
