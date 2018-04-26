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

/**
 * YarnAllocatorBlacklistTracker is responsible for tracking the blacklisted nodes
 * and synchronizing these node list to YARN.
 *
 */
private[spark] class YarnAllocatorBlacklistTracker(
    sparkConf: SparkConf,
    amClient: AMRMClient[ContainerRequest],
    failureTracker: FailureTracker)
  extends Logging {

  private val defaultTimeout = "1h"

  private val blacklistTimeoutMillis =
    sparkConf.get(BLACKLIST_TIMEOUT_CONF).getOrElse(Utils.timeStringAsMs(defaultTimeout))

  private val launchBlacklistEnabled =
    sparkConf.get(YARN_EXECUTOR_LAUNCH_BLACKLIST_ENABLED).getOrElse(false)

  private val maxFailuresPerHost = sparkConf.get(MAX_FAILED_EXEC_PER_NODE)

  private val blacklistMaxNodeRatio =
    sparkConf.get(YARN_BLACKLIST_MAX_NODE_BLACKLIST_RATIO)

  private val allocatorBlacklist = new HashMap[String, Long]()

  private var currentBlacklistedYarnNodes = Set.empty[String]

  private var schedulerBlacklist = Map.empty[String, Long]

  private var numClusterNodes = (Int.MaxValue / blacklistMaxNodeRatio).toInt

  def setNumClusterNodes(numClusterNodes: Int): Unit = {
    this.numClusterNodes = numClusterNodes
  }

  def handleResourceAllocationFailure(hostOpt: Option[String]): Unit = {
    hostOpt match {
      case Some(hostname) =>
        // failures on a already blacklisted nodes are not even tracked
        // otherwise such failures could shutdown the application
        // as resource requests are asynchronous
        // and a late failure response could exceed MAX_EXECUTOR_FAILURES
        if (!schedulerBlacklist.contains(hostname) &&
            !allocatorBlacklist.contains(hostname)) {
          failureTracker.registerFailureOnHost(hostname)
          updateAllocationBlacklistedNodes(hostname)
        }
      case None =>
        failureTracker.registerExecutorFailure()
    }
  }

  private def updateAllocationBlacklistedNodes(hostname: String): Unit = {
    if (launchBlacklistEnabled) {
      val failuresOnHost = failureTracker.numFailuresOnHost(hostname)
      if (failuresOnHost > maxFailuresPerHost) {
        logInfo(s"blacklisting $hostname as YARN allocation failed $failuresOnHost times")
        allocatorBlacklist.put(
          hostname,
          failureTracker.clock.getTimeMillis() + blacklistTimeoutMillis)
        refreshBlacklistedNodes()
      }
    }
  }

  def setSchedulerBlacklistedNodes(schedulerBlacklistedNodesWithExpiry: Map[String, Long]): Unit = {
    this.schedulerBlacklist = schedulerBlacklistedNodesWithExpiry
    refreshBlacklistedNodes()
  }

  private def refreshBlacklistedNodes(): Unit = {
    removeExpiredYarnBlacklistedNodes()
    val limit = (numClusterNodes * blacklistMaxNodeRatio).toInt
    val nodesToBlacklist =
      if (schedulerBlacklist.size +
          allocatorBlacklist.size > limit) {
        mostRelevantSubsetOfBlacklistedNodes(limit)
      } else {
        schedulerBlacklist.keySet ++ allocatorBlacklist.keySet
      }

    synchronizeBlacklistedNodeWithYarn(nodesToBlacklist)
  }

  private def mostRelevantSubsetOfBlacklistedNodes(limit: Int) = {
    val allBlacklist = schedulerBlacklist ++ allocatorBlacklist
    val relevant =
      allBlacklist.toSeq
      .sortBy(_._2)(Ordering[Long].reverse)
      .take(limit)
      .map(_._1)
      .toSet
    logInfo(s"blacklist size limit ($limit) is reached, total count: ${allBlacklist.size}")
    relevant
  }

  private def synchronizeBlacklistedNodeWithYarn(nodesToBlacklist: Set[String]): Unit = {
    // Update blacklist information to YARN ResourceManager for this application,
    // in order to avoid allocating new Containers on the problematic nodes.
    val additions = (nodesToBlacklist -- currentBlacklistedYarnNodes).toList.sorted
    val removals = (currentBlacklistedYarnNodes -- nodesToBlacklist).toList.sorted
    if (additions.nonEmpty) {
      logInfo(s"adding nodes to YARN application master's blacklist: $additions")
    }
    if (removals.nonEmpty) {
      logInfo(s"removing nodes from YARN application master's blacklist: $removals")
    }
    amClient.updateBlacklist(additions.asJava, removals.asJava)
    currentBlacklistedYarnNodes = nodesToBlacklist
  }

  private def removeExpiredYarnBlacklistedNodes() = {
    val now = failureTracker.clock.getTimeMillis()
    allocatorBlacklist.retain {
      (_, expiryTime) => expiryTime > now
    }
  }
}

