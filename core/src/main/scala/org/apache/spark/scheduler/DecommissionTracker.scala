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

package org.apache.spark.scheduler

import java.text.SimpleDateFormat
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

import scala.collection.mutable.HashMap

import org.apache.spark.{ExecutorAllocationClient, SparkConf, SparkContext}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config
import org.apache.spark.util.{Clock, SystemClock, ThreadUtils, Utils}

/**
 * DecommissionTracker tracks the list of decommissioned nodes.
 * This decommission trackers maintains the decommissioned nodes state.
 * Decommission tracker schedules the executor decommission and shuffle
 * decommission for that node.
 */
private[scheduler] class DecommissionTracker (
    conf: SparkConf,
    executorAllocClient: Option[ExecutorAllocationClient],
    dagScheduler: DAGScheduler,
    clock: Clock = new SystemClock()) extends Logging {

  def this(sc: SparkContext,
      client: Option[ExecutorAllocationClient],
      dagScheduler: DAGScheduler) = {
    this(sc.conf, client, dagScheduler)
  }

  // Decommission thread of node decommissioning!!
  private val decommissionThread =
    ThreadUtils.newDaemonThreadPoolScheduledExecutor("node-decommissioning-thread", 20)

  // Contains workers hostname which are decommissioning. Added when
  // decommissioning event arrives from the AM. And is removed when the
  // last node (identified by nodeId) is running again.
  private val decommissionHostNameMap = new HashMap[String, NodeDecommissionInfo]

  private val minDecommissionTimeMs =
    conf.get(config.GRACEFUL_DECOMMISSION_MIN_TERMINATION_TIME_IN_SEC)*1000

  private val executorDecommissionLeasePct =
    conf.get(config.GRACEFUL_DECOMMISSION_EXECUTOR_LEASETIME_PCT)

  private val shuffleDataDecommissionLeasePct =
    conf.get(config.GRACEFUL_DECOMMISSION_SHUFFLEDATA_LEASETIME_PCT)

  /*
   * Is the node decommissioned i.e from driver point of
   * view the node is considered decommissioned.
   */
  def isNodeDecommissioned(hostname: String): Boolean = synchronized {
    decommissionHostNameMap.get(hostname) match {
      case None => false
      case Some(info) =>
        return info.state == NodeDecommissionState.SHUFFLEDATA_DECOMMISSIONED ||
          info.state == NodeDecommissionState.TERMINATED
    }
  }

  /*
   * Is the node present in decommission Tracker
   * This is used by driver for not registering any new executor
   * on that node which is present in decommission Tracker and
   * neither to assign any new task to that existing executor on that node.
   */
  def isNodePresentInDecommissionTracker(hostname: String): Boolean = synchronized {
    decommissionHostNameMap.contains(hostname)
  }

  /**
   * Used for Unit Test
   */
  def getDecommissionedNodeState(hostname: String): Option[NodeDecommissionState.Value] =
    synchronized {
    decommissionHostNameMap.get(hostname) match {
      case Some(info) => Some(info.state)
      case _ => None
    }
  }

  /**
   * @param delayTime - time after which the node will be terminated
   * @param currentTimeMs - Current time in milliseconds
   * @return executorDecommissionTimeMs and shuffleDataDecommissionTimeMs
   */
  private def getDecommissionTimeOut(
      delayTime: Long,
      currentTimeMs: Long): (Long, Long) = {
    val executorDecommissionTimeMs =
      if (executorDecommissionLeasePct > shuffleDataDecommissionLeasePct) {
        // if executorDecommissionLeasePct is greater than
        // shuffleDataDecommissionLeasePct. In that scenario calculate
        // executorDecommissionTimeMs using shuffleDataDecommissionLeasePct
        (delayTime * shuffleDataDecommissionLeasePct) / 100 + currentTimeMs
      } else {
        (delayTime * executorDecommissionLeasePct) / 100 + currentTimeMs
      }
    val shuffleDataDecommissionTimeMs =
      if (executorDecommissionLeasePct >= shuffleDataDecommissionLeasePct) {
      // Add a  delay of one second in shuffleDataDecommissionTimeMs if
      // executorDecommissionLeasePct greater than equals to shuffleDataDecommissionLeasePct
      // Since we want executor to be decommissioned first
      // than after that shuffleDataDecommission
      (delayTime * shuffleDataDecommissionLeasePct) / 100 + currentTimeMs + 1000
      } else {
        (delayTime * shuffleDataDecommissionLeasePct) / 100 + currentTimeMs
      }
    (executorDecommissionTimeMs, shuffleDataDecommissionTimeMs)
  }

  def addNodeToDecommission(
      hostname: String,
      terminationTimeMs: Long,
      reason: NodeDecommissionReason): Unit = synchronized {

    val df: SimpleDateFormat = new SimpleDateFormat("yy/MM/dd HH:mm:ss")
    val tDateTime = df.format(terminationTimeMs)
    val curTimeMs = clock.getTimeMillis()

    if (terminationTimeMs <= curTimeMs) {
      // Ignoring the decommission request if termination
      // time is less than or same as the current time
      logWarning(s"Ignoring decommissioning request for host $hostname as" +
        s" terminationTimeMs ${terminationTimeMs} is less" +
        s" than current time ${curTimeMs}")
      return
    }

    // Override decommissionHostNameMap in case termination time is less than
    // existing the terminationTime in decommissionHostNameMap.
    if (decommissionHostNameMap.contains(hostname)) {
      val nodeDecommissionInfo = decommissionHostNameMap(hostname)
      // There will be no duplicate entry of terminationTimeMs in decommissionHostNameMap
      // since the terminationTime is updated only when it is less than the existing termination
      // time in decommissionHostNameMap
      if (decommissionHostNameMap(hostname).terminationTime <= terminationTimeMs) {
        logDebug(
          s"""Ignoring decommissioning """ +
            s""" request : {"node":"$hostname","reason":"${reason.message}",terminationTime"""" +
            s""":"$tDateTime"} current : {"node":"$hostname",$nodeDecommissionInfo}""")
        return
      } else {
        logInfo(s"Updating the termination time to :${terminationTimeMs} in " +
          s"decommission tracker for the hostname ${hostname}")
      }
    }

    val delay = terminationTimeMs - curTimeMs

    var executorDecommissionTimeMs = terminationTimeMs
    var shuffleDataDecommissionTimeMs = terminationTimeMs

    // if delay is less than a minDecommissionTime than decommission immediately
    if (delay < minDecommissionTimeMs) {
      executorDecommissionTimeMs = curTimeMs
      // Added the delay of 1 second in case of delay is less than minDecommissionTime
      // Since we want executor to be decommissioned first
      // than after that shuffleDataDecommission
      shuffleDataDecommissionTimeMs = curTimeMs + 1000
    } else {
      reason match {
        case NodeLoss =>
          // In Nodeloss case adjust termination time so that enough
          // buffer to real termination is available for job to finish
          // consuming shuffle data.
          var (executorDecommissionTime, shuffleDataDecommissionTime) = getDecommissionTimeOut(
            delay, curTimeMs)
          executorDecommissionTimeMs = executorDecommissionTime
          shuffleDataDecommissionTimeMs = shuffleDataDecommissionTime
        case _ =>
        // No action
      }
    }

    // Count of executors/worker which went to decommissioning
    // state over lifetime of application
    DecommissionTracker.incrNodeDecommissionCnt()
    val nodeDecommissionInfo = new NodeDecommissionInfo(
      executorDecommissionTime = executorDecommissionTimeMs,
      shuffleDataDecommissionTime = shuffleDataDecommissionTimeMs,
      terminationTime = terminationTimeMs,
      reason = reason,
      state = NodeDecommissionState.DECOMMISSIONING)

    logInfo(s"""Adding decommissioning""" +
      s""" request : {"node":"$hostname",$nodeDecommissionInfo} """)

    // Add node to the list of decommissioning nodes.
    decommissionHostNameMap.put(hostname, nodeDecommissionInfo)

    // Schedule executor decommission
    decommissionThread.schedule(new Runnable {
      override def run(): Unit = Utils.tryLogNonFatalError {
        executorDecommission(hostname, nodeDecommissionInfo)
      }
    }, executorDecommissionTimeMs - curTimeMs, TimeUnit.MILLISECONDS)

    // Schedule shuffle decommission
    decommissionThread.schedule(new Runnable {
      override def run(): Unit = Utils.tryLogNonFatalError {
        removeShuffleData(hostname, nodeDecommissionInfo)
      }
    }, shuffleDataDecommissionTimeMs - curTimeMs, TimeUnit.MILLISECONDS)
  }

  def removeNodeToDecommission(hostname: String): Unit = synchronized {
    if (!decommissionHostNameMap.contains(hostname)) {
      return
    }

    val nodeDecommissionInfo = decommissionHostNameMap(hostname)
    logInfo(s"""Removing decommissioning""" +
      s""" request : {"node":"$hostname",$nodeDecommissionInfo}""")
    decommissionHostNameMap -= hostname
  }

  def updateNodeToDecommissionSetTerminate(hostname: String): Unit = synchronized {
    terminate(hostname)
  }

  private def executorDecommission(hostname: String,
      nodeDecommissionInfo: NodeDecommissionInfo): Unit = {
    // Not found, only valid scenario is the nodes
    // has moved back to running state
    // Scenario where nodeLoss terminated the node
    // for the Graceful Decommission node.
    // If the node is already terminated and hostname is re-used in that scenario
    // no need to kill the executor on that host
    if (! decommissionHostNameMap.contains(hostname)) {
      logInfo(s"""Node $hostname not found in decommisssionTrackerList while""" +
        """performing executor decommission""")
      return
    }
    // if the terminationTime in the thread is not equal to
    // terminationTime in decommissionHostNameMap for that
    // host than Ignore the ExecutorDecommission
    if (decommissionHostNameMap(hostname).terminationTime
      != nodeDecommissionInfo.terminationTime) {
      logInfo(s"Ignoring ExecutorDecommission for hostname ${hostname}," +
        s" since node is already terminated")
      return
    }

    // Kill executor if there still are some running. This call is
    // async does not wait for response. Otherwise it may cause
    // deadlock between schedulerBacked (ExecutorAllocationManager)
    // and this.
    executorAllocClient.map(_.killExecutorsOnHost(hostname))
    decommissionHostNameMap(hostname).state = NodeDecommissionState.EXECUTOR_DECOMMISSIONED
    logInfo(s"Node $hostname decommissioned")
    return
  }

  private def removeShuffleData(hostname: String,
      nodeDecommissionInfo: NodeDecommissionInfo): Unit = {
    // Not found, only valid scenario is the nodes
    // has moved back to running state
    // This for scenario where about_to_be_lost terminated the node
    // for the Graceful Decommission node.
    // If the node is already terminated and hostname is reused in that scenario
    // no need to remove the shuffle entry from map-output tracker
    if (! decommissionHostNameMap.contains(hostname)) {
      logInfo(s"""Node $hostname not found in decommisssionTrackerList while """ +
        """performing shuffle data decommission""")
      return
    }
    // if the terminationTime in the thread is not equal to
    // terminationTime in decommissionHostNameMap for that
    // host than Ignore the removeShuffleData
    if (decommissionHostNameMap(hostname).terminationTime
      != nodeDecommissionInfo.terminationTime) {
      logInfo(s"Ignoring removeShuffleData for hostname ${hostname}," +
        s" since node is already terminated")
      return
    }

    // Unregister shuffle data.
    dagScheduler.nodeDecommissioned(hostname)

    decommissionHostNameMap(hostname).state = NodeDecommissionState.SHUFFLEDATA_DECOMMISSIONED

    logInfo(s"Node $hostname Shuffle data decommissioned")

    return
  }

  private def terminate(hostname: String): Unit = {
    // Not found, only valid scenario is the nodes
    // has moved back to running state
    if (!decommissionHostNameMap.contains(hostname)) {
      logWarning(s"Node $hostname not found in decommisssionTrackerList")
      return
    }

    // Remove all the shuffle data of all the executors for the terminated node
    dagScheduler.nodeDecommissioned(hostname)

    decommissionHostNameMap(hostname).state = NodeDecommissionState.TERMINATED

    logInfo(s"Node $hostname terminated")
  }

  def stop (): Unit = {
    val decommissionNodeCnt = DecommissionTracker.getNodeDecommissionCnt()
    val fetchFailIgnoreCnt = DecommissionTracker.getFetchFailIgnoreCnt()
    val fetchFailIgnoreThresholdExceed = DecommissionTracker.getFetchFailIgnoreCntThresholdFlag()
    val fetchFailedIgnoreThreshold =
      conf.get(config.GRACEFUL_DECOMMISSION_FETCHFAILED_IGNORE_THRESHOLD)
    decommissionThread.shutdown()
    var message = s"Decommission metrics: ${decommissionNodeCnt} nodes decommissioned" +
      s" (either due to node loss or node block rotation) while running ${conf.getAppId}." +
      s" Ignored ${fetchFailIgnoreCnt} fetch failed exception caused by decommissioned node."

    if (fetchFailIgnoreThresholdExceed) {
      message += s"Fetch fail ignore exceeded threshold ${fetchFailedIgnoreThreshold}" +
        s" causing stage abort"
    } else {
      message += s"Fetch fail ignored under allowed threshold ${fetchFailedIgnoreThreshold}"
    }
    // logging the metrics related to graceful decommission from decommission tracker
    logDebug(message)
  }

}

private[spark] object DecommissionTracker extends Logging {
  val infiniteTime = Long.MaxValue

  // Stats
  val decommissionNodeCnt = new AtomicInteger(0)
  val fetchFailIgnoreCnt = new AtomicInteger(0)
  val fetchFailIgnoreCntThresholdExceeded = new AtomicBoolean(false)
  val abortStage = new AtomicBoolean(false)

  def incrNodeDecommissionCnt(): Unit = {
    decommissionNodeCnt.getAndIncrement()
  }

  def incrFetchFailIgnoreCnt(): Unit = {
    fetchFailIgnoreCnt.getAndIncrement()
  }

  def setFetchFailIgnoreCntThresholdFlag(thresholdExceedFlag: Boolean): Unit = {
    fetchFailIgnoreCntThresholdExceeded.set(thresholdExceedFlag)
  }

  def setAbortStageFlag(AbortStageFlag: Boolean): Unit = {
    abortStage.set(AbortStageFlag)
  }

  def getNodeDecommissionCnt(): Int = {
    decommissionNodeCnt.get()
  }

  def getFetchFailIgnoreCnt(): Int = {
    fetchFailIgnoreCnt.get()
  }

  def getFetchFailIgnoreCntThresholdFlag(): Boolean = {
    fetchFailIgnoreCntThresholdExceeded.get()
  }

  def getAbortStageFlag(): Boolean = {
    abortStage.get()
  }

  def isDecommissionEnabled(conf: SparkConf): Boolean = {
    conf.get(config.GRACEFUL_DECOMMISSION_ENABLE)
  }
}

private class NodeDecommissionInfo(
    var terminationTime: Long,
    var executorDecommissionTime: Long,
    var shuffleDataDecommissionTime: Long,
    var state: NodeDecommissionState.Value,
    var reason: NodeDecommissionReason) {
  override def toString(): String = {
    val df: SimpleDateFormat = new SimpleDateFormat("YY/MM/dd HH:mm:ss")
    val tDateTime = df.format(terminationTime)
    val edDateTime = df.format(executorDecommissionTime)
    val sdDateTime = df.format(shuffleDataDecommissionTime)
    s""""terminationTime":"$tDateTime","reason":"${reason.message}","executorDecommissionTime"""" +
      s""":"$edDateTime","shuffleDataDecommissionTime":"$sdDateTime","state":"$state""""
  }
}

/*
 * NB: exposed for testing
 */
private[scheduler] object NodeDecommissionState extends Enumeration {
  val DECOMMISSIONING, EXECUTOR_DECOMMISSIONED, SHUFFLEDATA_DECOMMISSIONED, TERMINATED = Value
  type NodeDecommissionState = Value
}

/**
 * Represents an explanation for a Node being decommissioned.
 * NB: exposed for testing
  */
@DeveloperApi
private[spark] sealed trait NodeDecommissionReason extends Serializable {
  def message: String
}

@DeveloperApi
private[spark] case object NodeLoss extends NodeDecommissionReason {
  override def message: String = "nodeLoss"
}
