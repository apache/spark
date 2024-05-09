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
package org.apache.spark.deploy.yarn

import scala.collection.mutable.HashMap
import scala.jdk.CollectionConverters._

import org.apache.hadoop.yarn.client.api.AMRMClient
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest

import org.apache.spark.SparkConf
import org.apache.spark.deploy.ExecutorFailureTracker
import org.apache.spark.deploy.yarn.config._
import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKeys.{FAILURES, HOST, NODES}
import org.apache.spark.internal.config._
import org.apache.spark.scheduler.HealthTracker

/**
 * YarnAllocatorNodeHealthTracker is responsible for tracking the health of nodes
 * and synchronizing the node list to YARN as to which nodes are excluded.
 *
 * Excluding nodes are coming from two different sources:
 *
 * <ul>
 *   <li> from the scheduler as task level excluded nodes
 *   <li> from this class (tracked here) as YARN resource allocation problems
 * </ul>
 *
 * The reason to realize this logic here (and not in the driver) is to avoid possible delays
 * between synchronizing the excluded nodes with YARN and resource allocations.
 */
private[spark] class YarnAllocatorNodeHealthTracker(
    sparkConf: SparkConf,
    amClient: AMRMClient[ContainerRequest],
    failureTracker: ExecutorFailureTracker)
  extends Logging {

  private val excludeOnFailureTimeoutMillis = HealthTracker.getExcludeOnFailureTimeout(sparkConf)

  private val launchExcludeOnFailureEnabled =
    sparkConf.get(YARN_EXECUTOR_LAUNCH_EXCLUDE_ON_FAILURE_ENABLED)

  private val maxFailuresPerHost = sparkConf.get(MAX_FAILED_EXEC_PER_NODE)

  private val excludeNodes = sparkConf.get(YARN_EXCLUDE_NODES).toSet

  private val allocatorExcludedNodeList = new HashMap[String, Long]()

  private var currentExcludededYarnNodes = Set.empty[String]

  private var schedulerExcludedNodeList = Set.empty[String]

  private var numClusterNodes = Int.MaxValue

  def setNumClusterNodes(numClusterNodes: Int): Unit = {
    this.numClusterNodes = numClusterNodes
  }

  def handleResourceAllocationFailure(hostOpt: Option[String]): Unit = {
    hostOpt match {
      case Some(hostname) if launchExcludeOnFailureEnabled =>
        // failures on an already excluded node are not even tracked.
        // otherwise, such failures could shutdown the application
        // as resource requests are asynchronous
        // and a late failure response could exceed MAX_EXECUTOR_FAILURES
        if (!schedulerExcludedNodeList.contains(hostname) &&
            !allocatorExcludedNodeList.contains(hostname)) {
          failureTracker.registerFailureOnHost(hostname)
          updateAllocationExcludedNodes(hostname)
        }
      case _ =>
        failureTracker.registerExecutorFailure()
    }
  }

  private def updateAllocationExcludedNodes(hostname: String): Unit = {
    val failuresOnHost = failureTracker.numFailuresOnHost(hostname)
    if (failuresOnHost > maxFailuresPerHost) {
      logInfo(log"excluding ${MDC(HOST, hostname)} as YARN allocation failed " +
        log"${MDC(FAILURES, failuresOnHost)} times")
      allocatorExcludedNodeList.put(
        hostname,
        failureTracker.clock.getTimeMillis() + excludeOnFailureTimeoutMillis)
      refreshExcludedNodes()
    }
  }

  def setSchedulerExcludedNodes(schedulerExcludedNodesWithExpiry: Set[String]): Unit = {
    this.schedulerExcludedNodeList = schedulerExcludedNodesWithExpiry
    refreshExcludedNodes()
  }

  def isAllNodeExcluded: Boolean = {
    if (numClusterNodes <= 0) {
      logWarning("No available nodes reported, please check Resource Manager.")
      false
    } else {
      currentExcludededYarnNodes.size >= numClusterNodes
    }
  }

  private def refreshExcludedNodes(): Unit = {
    removeExpiredYarnExcludedNodes()
    val allExcludedNodes =
      excludeNodes ++ schedulerExcludedNodeList ++ allocatorExcludedNodeList.keySet
    synchronizeExcludedNodesWithYarn(allExcludedNodes)
  }

  private def synchronizeExcludedNodesWithYarn(nodesToExclude: Set[String]): Unit = {
    // Update YARN with the nodes that are excluded for this application,
    // in order to avoid allocating new Containers on the problematic nodes.
    val additions = (nodesToExclude -- currentExcludededYarnNodes).toList.sorted
    val removals = (currentExcludededYarnNodes -- nodesToExclude).toList.sorted
    if (additions.nonEmpty) {
      logInfo(log"adding nodes to YARN application master's " +
        log"excluded node list: ${MDC(NODES, additions)}")
    }
    if (removals.nonEmpty) {
      logInfo(log"removing nodes from YARN application master's " +
        log"excluded node list: ${MDC(NODES, removals)}")
    }
    if (additions.nonEmpty || removals.nonEmpty) {
      // Note YARNs api for excluding nodes is updateBlacklist.
      // TODO - We need to update once Hadoop changes -
      // https://issues.apache.org/jira/browse/HADOOP-17169
      amClient.updateBlacklist(additions.asJava, removals.asJava)
    }
    currentExcludededYarnNodes = nodesToExclude
  }

  private def removeExpiredYarnExcludedNodes(): Unit = {
    val now = failureTracker.clock.getTimeMillis()
    allocatorExcludedNodeList.filterInPlace { (_, expiryTime) => expiryTime > now }
  }

  refreshExcludedNodes()
}
