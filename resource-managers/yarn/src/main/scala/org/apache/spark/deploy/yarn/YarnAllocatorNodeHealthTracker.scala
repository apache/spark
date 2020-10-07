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

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.HashMap

import org.apache.hadoop.yarn.client.api.AMRMClient
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest

import org.apache.spark.SparkConf
import org.apache.spark.deploy.yarn.config._
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.scheduler.HealthTracker
import org.apache.spark.util.{Clock, SystemClock}

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
    failureTracker: FailureTracker)
  extends Logging {

  private val excludeOnFailureTimeoutMillis = HealthTracker.getExludeOnFailureTimeout(sparkConf)

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
      logInfo(s"excluding $hostname as YARN allocation failed $failuresOnHost times")
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
      logInfo(s"adding nodes to YARN application master's excluded node list: $additions")
    }
    if (removals.nonEmpty) {
      logInfo(s"removing nodes from YARN application master's excluded node list: $removals")
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
    allocatorExcludedNodeList.retain { (_, expiryTime) => expiryTime > now }
  }
}

/**
 * FailureTracker is responsible for tracking executor failures both for each host separately
 * and for all hosts altogether.
 */
private[spark] class FailureTracker(
    sparkConf: SparkConf,
    val clock: Clock = new SystemClock) extends Logging {

  private val executorFailuresValidityInterval =
    sparkConf.get(config.EXECUTOR_ATTEMPT_FAILURE_VALIDITY_INTERVAL_MS).getOrElse(-1L)

  // Queue to store the timestamp of failed executors for each host
  private val failedExecutorsTimeStampsPerHost = mutable.Map[String, mutable.Queue[Long]]()

  private val failedExecutorsTimeStamps = new mutable.Queue[Long]()

  private def updateAndCountFailures(failedExecutorsWithTimeStamps: mutable.Queue[Long]): Int = {
    val endTime = clock.getTimeMillis()
    while (executorFailuresValidityInterval > 0 &&
        failedExecutorsWithTimeStamps.nonEmpty &&
        failedExecutorsWithTimeStamps.head < endTime - executorFailuresValidityInterval) {
      failedExecutorsWithTimeStamps.dequeue()
    }
    failedExecutorsWithTimeStamps.size
  }

  def numFailedExecutors: Int = synchronized {
    updateAndCountFailures(failedExecutorsTimeStamps)
  }

  def registerFailureOnHost(hostname: String): Unit = synchronized {
    val timeMillis = clock.getTimeMillis()
    failedExecutorsTimeStamps.enqueue(timeMillis)
    val failedExecutorsOnHost =
      failedExecutorsTimeStampsPerHost.getOrElse(hostname, {
        val failureOnHost = mutable.Queue[Long]()
        failedExecutorsTimeStampsPerHost.put(hostname, failureOnHost)
        failureOnHost
      })
    failedExecutorsOnHost.enqueue(timeMillis)
  }

  def registerExecutorFailure(): Unit = synchronized {
    val timeMillis = clock.getTimeMillis()
    failedExecutorsTimeStamps.enqueue(timeMillis)
  }

  def numFailuresOnHost(hostname: String): Int = {
    failedExecutorsTimeStampsPerHost.get(hostname).map { failedExecutorsOnHost =>
      updateAndCountFailures(failedExecutorsOnHost)
    }.getOrElse(0)
  }

}

