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

import org.apache.spark.deploy.kubernetes.config._
import org.apache.spark.scheduler.{SchedulerBackend, TaskSchedulerImpl, TaskSet, TaskSetManager}
import org.apache.spark.util.Utils
import org.apache.spark.SparkContext

private[spark] class KubernetesTaskSchedulerImpl(
    sc: SparkContext,
    rackResolverUtil: RackResolverUtil,
    inetAddressUtil: InetAddressUtil = InetAddressUtilImpl) extends TaskSchedulerImpl(sc) {

  var kubernetesSchedulerBackend: KubernetesClusterSchedulerBackend = null

  def this(sc: SparkContext) = this(sc, new RackResolverUtilImpl(sc.hadoopConfiguration))

  override def initialize(backend: SchedulerBackend): Unit = {
    super.initialize(backend)
    kubernetesSchedulerBackend = this.backend.asInstanceOf[KubernetesClusterSchedulerBackend]
  }
  override def createTaskSetManager(taskSet: TaskSet, maxTaskFailures: Int): TaskSetManager = {
    new KubernetesTaskSetManager(this, taskSet, maxTaskFailures)
  }

  override def getRackForHost(hostPort: String): Option[String] = {
    if (!rackResolverUtil.isConfigured) {
      // Only calls resolver when it is configured to avoid sending DNS queries for cluster nodes.
      // See InetAddressUtil for details.
      None
    } else {
      getRackForDatanodeOrExecutor(hostPort)
    }
  }

  private def getRackForDatanodeOrExecutor(hostPort: String): Option[String] = {
    val host = Utils.parseHostPort(hostPort)._1
    val executorPod = kubernetesSchedulerBackend.getExecutorPodByIP(host)
    val hadoopConfiguration = sc.hadoopConfiguration
    executorPod.map(
        pod => {
          val clusterNodeName = pod.getSpec.getNodeName
          val rackByNodeName = rackResolverUtil.resolveRack(hadoopConfiguration, clusterNodeName)
          rackByNodeName.orElse({
            val clusterNodeIP = pod.getStatus.getHostIP
            val rackByNodeIP = rackResolverUtil.resolveRack(hadoopConfiguration, clusterNodeIP)
            rackByNodeIP.orElse({
              if (conf.get(KUBERNETES_DRIVER_CLUSTER_NODENAME_DNS_LOOKUP_ENABLED)) {
                val clusterNodeFullName = inetAddressUtil.getFullHostName(clusterNodeIP)
                rackResolverUtil.resolveRack(hadoopConfiguration, clusterNodeFullName)
              } else {
                Option.empty
              }
            })
          })
        }
      ).getOrElse(rackResolverUtil.resolveRack(hadoopConfiguration, host))
  }
}
