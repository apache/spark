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
import scala.collection.mutable.HashMap

import org.apache.hadoop.yarn.client.api.AMRMClient
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest

import org.apache.spark.SparkConf
import org.apache.spark.deploy.yarn.config._
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.util.{Clock, SystemClock, Utils}

private[spark] class YarnAllocatorBlacklistTracker(
    sparkConf: SparkConf,
    amClient: AMRMClient[ContainerRequest],
    failureWithinTimeIntervalTracker: FailureWithinTimeIntervalTracker)
  extends Logging {

  private val DEFAULT_TIMEOUT = "1h"

  private val BLACKLIST_TIMEOUT_MILLIS =
    sparkConf.get(BLACKLIST_TIMEOUT_CONF).getOrElse(Utils.timeStringAsMs(DEFAULT_TIMEOUT))

  private val IS_YARN_EXECUTOR_LAUNCH_BLACKLIST_ENABLED =
    sparkConf.get(YARN_EXECUTOR_LAUNCH_BLACKLIST_ENABLED).getOrElse(false)

  private val BLACKLIST_MAX_FAILED_EXEC_PER_NODE = sparkConf.get(MAX_FAILED_EXEC_PER_NODE)

  private val BLACKLIST_MAX_NODE_BLACKLIST_RATIO =
    sparkConf.get(YARN_BLACKLIST_MAX_NODE_BLACKLIST_RATIO)

  private var clock: Clock = new SystemClock

  private val allocationBlacklistedNodesWithExpiry = new HashMap[String, Long]()

  private var currentBlacklistedYarnNodes = Set.empty[String]

  private var schedulerBlacklistedNodesWithExpiry = Map.empty[String, Long]

  private var numClusterNodes = (Int.MaxValue / BLACKLIST_MAX_NODE_BLACKLIST_RATIO).toInt

  def setNumClusterNodes(numClusterNodes: Int): Unit = {
    this.numClusterNodes = numClusterNodes
  }

  /**
   * Use a different clock. This is mainly used for testing.
   */
  def setClock(newClock: Clock): Unit = {
    clock = newClock
  }

  def handleResourceAllocationFailure(hostOpt: Option[String]): Unit = {
    hostOpt match {
      case Some(hostname) =>
        // failures on a already blacklisted nodes are not even tracked
        // otherwise such failures could shutdown the application
        // as resource requests are asynchronous
        // and a late failure response could exceed MAX_EXECUTOR_FAILURES
        if (!schedulerBlacklistedNodesWithExpiry.contains(hostname) &&
          !allocationBlacklistedNodesWithExpiry.contains(hostname)) {
          failureWithinTimeIntervalTracker.registerFailureOnHost(hostname)
          updateAllocationBlacklistedNodes(hostname)
        }
      case None =>
        failureWithinTimeIntervalTracker.registerExecutorFailure()
    }
  }

  private def updateAllocationBlacklistedNodes(hostname: String): Unit = {
    if (IS_YARN_EXECUTOR_LAUNCH_BLACKLIST_ENABLED) {
      val failuresOnHost = failureWithinTimeIntervalTracker.getNumExecutorFailuresOnHost(hostname)
      if (failuresOnHost > BLACKLIST_MAX_FAILED_EXEC_PER_NODE) {
        logInfo(s"blacklisting $hostname as YARN allocation failed $failuresOnHost times")
        allocationBlacklistedNodesWithExpiry.put(
          hostname,
          clock.getTimeMillis() + BLACKLIST_TIMEOUT_MILLIS)
        refreshBlacklistedNodes()
      }
    }
  }

  def setSchedulerBlacklistedNodes(schedulerBlacklistedNodesWithExpiry: Map[String, Long]): Unit = {
    this.schedulerBlacklistedNodesWithExpiry = schedulerBlacklistedNodesWithExpiry
    refreshBlacklistedNodes()
  }

  private def refreshBlacklistedNodes(): Unit = {
    removeExpiredYarnBlacklistedNodes()
    val limit = (numClusterNodes * BLACKLIST_MAX_NODE_BLACKLIST_RATIO).toInt
    val nodesToBlacklist =
      if (schedulerBlacklistedNodesWithExpiry.size +
          allocationBlacklistedNodesWithExpiry.size > limit) {
        mostRelevantSubsetOfBlacklistedNodes(limit)
      } else {
        schedulerBlacklistedNodesWithExpiry.keySet ++ allocationBlacklistedNodesWithExpiry.keySet
      }

    synchronizeBlacklistedNodeWithYarn(nodesToBlacklist)
  }

  private def mostRelevantSubsetOfBlacklistedNodes(limit: Int) = {
    val relevant =
      (schedulerBlacklistedNodesWithExpiry ++ allocationBlacklistedNodesWithExpiry).toSeq
      .sortBy(_._2)(Ordering[Long].reverse)
      .take(limit)
      .map(_._1)
      .toSet
    logInfo(s"blacklist size limit ($limit) is reached, the most relevant subset is: $relevant")
    relevant
  }

  private def synchronizeBlacklistedNodeWithYarn(nodesToBlacklist: Set[String]): Unit = {
    // Update blacklist information to YARN ResourceManager for this application,
    // in order to avoid allocating new Containers on the problematic nodes.
    val blacklistAdditions = (nodesToBlacklist -- currentBlacklistedYarnNodes).toList.sorted
    val blacklistRemovals = (currentBlacklistedYarnNodes -- nodesToBlacklist).toList.sorted
    if (blacklistAdditions.nonEmpty) {
      logInfo(s"adding nodes to YARN application master's blacklist: $blacklistAdditions")
    }
    if (blacklistRemovals.nonEmpty) {
      logInfo(s"removing nodes from YARN application master's blacklist: $blacklistRemovals")
    }
    amClient.updateBlacklist(blacklistAdditions.asJava, blacklistRemovals.asJava)
    currentBlacklistedYarnNodes = nodesToBlacklist
  }

  private def removeExpiredYarnBlacklistedNodes() = {
    val now = clock.getTimeMillis()
    allocationBlacklistedNodesWithExpiry.retain {
      (_: String, expiryTime: Long) => expiryTime > now
    }
  }
}

