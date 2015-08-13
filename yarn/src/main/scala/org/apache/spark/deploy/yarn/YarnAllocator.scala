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

import java.util.Collections
import java.util.concurrent._
import java.util.regex.Pattern

import org.apache.spark.util.Utils

import scala.collection.JavaConversions._
import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}

import com.google.common.util.concurrent.ThreadFactoryBuilder

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.client.api.AMRMClient
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest
import org.apache.hadoop.yarn.util.RackResolver

import org.apache.log4j.{Level, Logger}

import org.apache.spark.{Logging, SecurityManager, SparkConf}
import org.apache.spark.deploy.yarn.YarnSparkHadoopUtil._
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages._

/**
 * YarnAllocator is charged with requesting containers from the YARN ResourceManager and deciding
 * what to do with containers when YARN fulfills these requests.
 *
 * This class makes use of YARN's AMRMClient APIs. We interact with the AMRMClient in three ways:
 * * Making our resource needs known, which updates local bookkeeping about containers requested.
 * * Calling "allocate", which syncs our local container requests with the RM, and returns any
 *   containers that YARN has granted to us.  This also functions as a heartbeat.
 * * Processing the containers granted to us to possibly launch executors inside of them.
 *
 * The public methods of this class are thread-safe.  All methods that mutate state are
 * synchronized.
 */
private[yarn] class YarnAllocator(
    driverUrl: String,
    driverRef: RpcEndpointRef,
    conf: Configuration,
    sparkConf: SparkConf,
    amClient: AMRMClient[ContainerRequest],
    appAttemptId: ApplicationAttemptId,
    args: ApplicationMasterArguments,
    securityMgr: SecurityManager)
  extends Logging {

  import YarnAllocator._

  // RackResolver logs an INFO message whenever it resolves a rack, which is way too often.
  if (Logger.getLogger(classOf[RackResolver]).getLevel == null) {
    Logger.getLogger(classOf[RackResolver]).setLevel(Level.WARN)
  }

  // Visible for testing.
  val allocatedHostToContainersMap = new HashMap[String, collection.mutable.Set[ContainerId]]
  val allocatedContainerToHostMap = new HashMap[ContainerId, String]

  // Containers that we no longer care about. We've either already told the RM to release them or
  // will on the next heartbeat. Containers get removed from this map after the RM tells us they've
  // completed.
  private val releasedContainers = Collections.newSetFromMap[ContainerId](
    new ConcurrentHashMap[ContainerId, java.lang.Boolean])

  @volatile private var numExecutorsRunning = 0
  // Used to generate a unique ID per executor
  private var executorIdCounter = 0
  @volatile private var numExecutorsFailed = 0

  @volatile private var targetNumExecutors =
    if (Utils.isDynamicAllocationEnabled(sparkConf)) {
      sparkConf.getInt("spark.dynamicAllocation.initialExecutors", 0)
    } else {
      sparkConf.getInt("spark.executor.instances", YarnSparkHadoopUtil.DEFAULT_NUMBER_EXECUTORS)
    }

  // Keep track of which container is running which executor to remove the executors later
  // Visible for testing.
  private[yarn] val executorIdToContainer = new HashMap[String, Container]

  private var numUnexpectedContainerRelease = 0L
  private val containerIdToExecutorId = new HashMap[ContainerId, String]

  // Executor memory in MB.
  protected val executorMemory = args.executorMemory
  // Additional memory overhead.
  protected val memoryOverhead: Int = sparkConf.getInt("spark.yarn.executor.memoryOverhead",
    math.max((MEMORY_OVERHEAD_FACTOR * executorMemory).toInt, MEMORY_OVERHEAD_MIN))
  // Number of cores per executor.
  protected val executorCores = args.executorCores
  // Resource capability requested for each executors
  private[yarn] val resource = Resource.newInstance(executorMemory + memoryOverhead, executorCores)

  private val launcherPool = new ThreadPoolExecutor(
    // max pool size of Integer.MAX_VALUE is ignored because we use an unbounded queue
    sparkConf.getInt("spark.yarn.containerLauncherMaxThreads", 25), Integer.MAX_VALUE,
    1, TimeUnit.MINUTES,
    new LinkedBlockingQueue[Runnable](),
    new ThreadFactoryBuilder().setNameFormat("ContainerLauncher #%d").setDaemon(true).build())
  launcherPool.allowCoreThreadTimeOut(true)

  // For testing
  private val launchContainers = sparkConf.getBoolean("spark.yarn.launchContainers", true)

  private val labelExpression = sparkConf.getOption("spark.yarn.executor.nodeLabelExpression")

  // ContainerRequest constructor that can take a node label expression. We grab it through
  // reflection because it's only available in later versions of YARN.
  private val nodeLabelConstructor = labelExpression.flatMap { expr =>
    try {
      Some(classOf[ContainerRequest].getConstructor(classOf[Resource],
        classOf[Array[String]], classOf[Array[String]], classOf[Priority], classOf[Boolean],
        classOf[String]))
    } catch {
      case e: NoSuchMethodException => {
        logWarning(s"Node label expression $expr will be ignored because YARN version on" +
          " classpath does not support it.")
        None
      }
    }
  }

  // A map to store preferred hostname and possible task numbers running on it.
  private var hostToLocalTaskCounts: Map[String, Int] = Map.empty

  // Number of tasks that have locality preferences in active stages
  private var numLocalityAwareTasks: Int = 0

  // A container placement strategy based on pending tasks' locality preference
  private[yarn] val containerPlacementStrategy =
    new LocalityPreferredContainerPlacementStrategy(sparkConf, conf, resource)

  def getNumExecutorsRunning: Int = numExecutorsRunning

  def getNumExecutorsFailed: Int = numExecutorsFailed

  /**
   * Number of container requests that have not yet been fulfilled.
   */
  def getNumPendingAllocate: Int = getNumPendingAtLocation(ANY_HOST)

  /**
   * Number of container requests at the given location that have not yet been fulfilled.
   */
  private def getNumPendingAtLocation(location: String): Int =
    amClient.getMatchingRequests(RM_REQUEST_PRIORITY, location, resource).map(_.size).sum

  /**
   * Request as many executors from the ResourceManager as needed to reach the desired total. If
   * the requested total is smaller than the current number of running executors, no executors will
   * be killed.
   * @param requestedTotal total number of containers requested
   * @param localityAwareTasks number of locality aware tasks to be used as container placement hint
   * @param hostToLocalTaskCount a map of preferred hostname to possible task counts to be used as
   *                             container placement hint.
   * @return Whether the new requested total is different than the old value.
   */
  def requestTotalExecutorsWithPreferredLocalities(
      requestedTotal: Int,
      localityAwareTasks: Int,
      hostToLocalTaskCount: Map[String, Int]): Boolean = synchronized {
    this.numLocalityAwareTasks = localityAwareTasks
    this.hostToLocalTaskCounts = hostToLocalTaskCount

    if (requestedTotal != targetNumExecutors) {
      logInfo(s"Driver requested a total number of $requestedTotal executor(s).")
      targetNumExecutors = requestedTotal
      true
    } else {
      false
    }
  }

  /**
   * Request that the ResourceManager release the container running the specified executor.
   */
  def killExecutor(executorId: String): Unit = synchronized {
    if (executorIdToContainer.contains(executorId)) {
      val container = executorIdToContainer.remove(executorId).get
      containerIdToExecutorId.remove(container.getId)
      internalReleaseContainer(container)
      numExecutorsRunning -= 1
    } else {
      logWarning(s"Attempted to kill unknown executor $executorId!")
    }
  }

  /**
   * Request resources such that, if YARN gives us all we ask for, we'll have a number of containers
   * equal to maxExecutors.
   *
   * Deal with any containers YARN has granted to us by possibly launching executors in them.
   *
   * This must be synchronized because variables read in this method are mutated by other methods.
   */
  def allocateResources(): Unit = synchronized {
    updateResourceRequests()

    val progressIndicator = 0.1f
    // Poll the ResourceManager. This doubles as a heartbeat if there are no pending container
    // requests.
    val allocateResponse = amClient.allocate(progressIndicator)

    val allocatedContainers = allocateResponse.getAllocatedContainers()

    if (allocatedContainers.size > 0) {
      logDebug("Allocated containers: %d. Current executor count: %d. Cluster resources: %s."
        .format(
          allocatedContainers.size,
          numExecutorsRunning,
          allocateResponse.getAvailableResources))

      handleAllocatedContainers(allocatedContainers)
    }

    val completedContainers = allocateResponse.getCompletedContainersStatuses()
    if (completedContainers.size > 0) {
      logDebug("Completed %d containers".format(completedContainers.size))

      processCompletedContainers(completedContainers)

      logDebug("Finished processing %d completed containers. Current running executor count: %d."
        .format(completedContainers.size, numExecutorsRunning))
    }
  }

  /**
   * Update the set of container requests that we will sync with the RM based on the number of
   * executors we have currently running and our target number of executors.
   *
   * Visible for testing.
   */
  def updateResourceRequests(): Unit = {
    val numPendingAllocate = getNumPendingAllocate
    val missing = targetNumExecutors - numPendingAllocate - numExecutorsRunning

    // TODO. Consider locality preferences of pending container requests.
    // Since the last time we made container requests, stages have completed and been submitted,
    // and that the localities at which we requested our pending executors
    // no longer apply to our current needs. We should consider to remove all outstanding
    // container requests and add requests anew each time to avoid this.
    if (missing > 0) {
      logInfo(s"Will request $missing executor containers, each with ${resource.getVirtualCores} " +
        s"cores and ${resource.getMemory} MB memory including $memoryOverhead MB overhead")

      val containerLocalityPreferences = containerPlacementStrategy.localityOfRequestedContainers(
        missing, numLocalityAwareTasks, hostToLocalTaskCounts, allocatedHostToContainersMap)

      for (locality <- containerLocalityPreferences) {
        val request = createContainerRequest(resource, locality.nodes, locality.racks)
        amClient.addContainerRequest(request)
        val nodes = request.getNodes
        val hostStr = if (nodes == null || nodes.isEmpty) "Any" else nodes.last
        logInfo(s"Container request (host: $hostStr, capability: $resource)")
      }
    } else if (missing < 0) {
      val numToCancel = math.min(numPendingAllocate, -missing)
      logInfo(s"Canceling requests for $numToCancel executor containers")

      val matchingRequests = amClient.getMatchingRequests(RM_REQUEST_PRIORITY, ANY_HOST, resource)
      if (!matchingRequests.isEmpty) {
        matchingRequests.head.take(numToCancel).foreach(amClient.removeContainerRequest)
      } else {
        logWarning("Expected to find pending requests, but found none.")
      }
    }
  }

  /**
   * Creates a container request, handling the reflection required to use YARN features that were
   * added in recent versions.
   */
  protected def createContainerRequest(
      resource: Resource,
      nodes: Array[String],
      racks: Array[String]): ContainerRequest = {
    nodeLabelConstructor.map { constructor =>
      constructor.newInstance(resource, nodes, racks, RM_REQUEST_PRIORITY, true: java.lang.Boolean,
        labelExpression.orNull)
    }.getOrElse(new ContainerRequest(resource, nodes, racks, RM_REQUEST_PRIORITY))
  }

  /**
   * Handle containers granted by the RM by launching executors on them.
   *
   * Due to the way the YARN allocation protocol works, certain healthy race conditions can result
   * in YARN granting containers that we no longer need. In this case, we release them.
   *
   * Visible for testing.
   */
  def handleAllocatedContainers(allocatedContainers: Seq[Container]): Unit = {
    val containersToUse = new ArrayBuffer[Container](allocatedContainers.size)

    // Match incoming requests by host
    val remainingAfterHostMatches = new ArrayBuffer[Container]
    for (allocatedContainer <- allocatedContainers) {
      matchContainerToRequest(allocatedContainer, allocatedContainer.getNodeId.getHost,
        containersToUse, remainingAfterHostMatches)
    }

    // Match remaining by rack
    val remainingAfterRackMatches = new ArrayBuffer[Container]
    for (allocatedContainer <- remainingAfterHostMatches) {
      val rack = RackResolver.resolve(conf, allocatedContainer.getNodeId.getHost).getNetworkLocation
      matchContainerToRequest(allocatedContainer, rack, containersToUse,
        remainingAfterRackMatches)
    }

    // Assign remaining that are neither node-local nor rack-local
    val remainingAfterOffRackMatches = new ArrayBuffer[Container]
    for (allocatedContainer <- remainingAfterRackMatches) {
      matchContainerToRequest(allocatedContainer, ANY_HOST, containersToUse,
        remainingAfterOffRackMatches)
    }

    if (!remainingAfterOffRackMatches.isEmpty) {
      logDebug(s"Releasing ${remainingAfterOffRackMatches.size} unneeded containers that were " +
        s"allocated to us")
      for (container <- remainingAfterOffRackMatches) {
        internalReleaseContainer(container)
      }
    }

    runAllocatedContainers(containersToUse)

    logInfo("Received %d containers from YARN, launching executors on %d of them."
      .format(allocatedContainers.size, containersToUse.size))
  }

  /**
   * Looks for requests for the given location that match the given container allocation. If it
   * finds one, removes the request so that it won't be submitted again. Places the container into
   * containersToUse or remaining.
   *
   * @param allocatedContainer container that was given to us by YARN
   * @param location resource name, either a node, rack, or *
   * @param containersToUse list of containers that will be used
   * @param remaining list of containers that will not be used
   */
  private def matchContainerToRequest(
      allocatedContainer: Container,
      location: String,
      containersToUse: ArrayBuffer[Container],
      remaining: ArrayBuffer[Container]): Unit = {
    // SPARK-6050: certain Yarn configurations return a virtual core count that doesn't match the
    // request; for example, capacity scheduler + DefaultResourceCalculator. So match on requested
    // memory, but use the asked vcore count for matching, effectively disabling matching on vcore
    // count.
    val matchingResource = Resource.newInstance(allocatedContainer.getResource.getMemory,
          resource.getVirtualCores)
    val matchingRequests = amClient.getMatchingRequests(allocatedContainer.getPriority, location,
      matchingResource)

    // Match the allocation to a request
    if (!matchingRequests.isEmpty) {
      val containerRequest = matchingRequests.get(0).iterator.next
      amClient.removeContainerRequest(containerRequest)
      containersToUse += allocatedContainer
    } else {
      remaining += allocatedContainer
    }
  }

  /**
   * Launches executors in the allocated containers.
   */
  private def runAllocatedContainers(containersToUse: ArrayBuffer[Container]): Unit = {
    for (container <- containersToUse) {
      numExecutorsRunning += 1
      assert(numExecutorsRunning <= targetNumExecutors)
      val executorHostname = container.getNodeId.getHost
      val containerId = container.getId
      executorIdCounter += 1
      val executorId = executorIdCounter.toString

      assert(container.getResource.getMemory >= resource.getMemory)

      logInfo("Launching container %s for on host %s".format(containerId, executorHostname))
      executorIdToContainer(executorId) = container
      containerIdToExecutorId(container.getId) = executorId

      val containerSet = allocatedHostToContainersMap.getOrElseUpdate(executorHostname,
        new HashSet[ContainerId])

      containerSet += containerId
      allocatedContainerToHostMap.put(containerId, executorHostname)

      val executorRunnable = new ExecutorRunnable(
        container,
        conf,
        sparkConf,
        driverUrl,
        executorId,
        executorHostname,
        executorMemory,
        executorCores,
        appAttemptId.getApplicationId.toString,
        securityMgr)
      if (launchContainers) {
        logInfo("Launching ExecutorRunnable. driverUrl: %s,  executorHostname: %s".format(
          driverUrl, executorHostname))
        launcherPool.execute(executorRunnable)
      }
    }
  }

  // Visible for testing.
  private[yarn] def processCompletedContainers(completedContainers: Seq[ContainerStatus]): Unit = {
    for (completedContainer <- completedContainers) {
      val containerId = completedContainer.getContainerId
      val alreadyReleased = releasedContainers.remove(containerId)
      if (!alreadyReleased) {
        // Decrement the number of executors running. The next iteration of
        // the ApplicationMaster's reporting thread will take care of allocating.
        numExecutorsRunning -= 1
        logInfo("Completed container %s (state: %s, exit status: %s)".format(
          containerId,
          completedContainer.getState,
          completedContainer.getExitStatus))
        // Hadoop 2.2.X added a ContainerExitStatus we should switch to use
        // there are some exit status' we shouldn't necessarily count against us, but for
        // now I think its ok as none of the containers are expected to exit
        if (completedContainer.getExitStatus == ContainerExitStatus.PREEMPTED) {
          logInfo("Container preempted: " + containerId)
        } else if (completedContainer.getExitStatus == -103) { // vmem limit exceeded
          logWarning(memLimitExceededLogMessage(
            completedContainer.getDiagnostics,
            VMEM_EXCEEDED_PATTERN))
        } else if (completedContainer.getExitStatus == -104) { // pmem limit exceeded
          logWarning(memLimitExceededLogMessage(
            completedContainer.getDiagnostics,
            PMEM_EXCEEDED_PATTERN))
        } else if (completedContainer.getExitStatus != 0) {
          logInfo("Container marked as failed: " + containerId +
            ". Exit status: " + completedContainer.getExitStatus +
            ". Diagnostics: " + completedContainer.getDiagnostics)
          numExecutorsFailed += 1
        }
      }

      if (allocatedContainerToHostMap.containsKey(containerId)) {
        val host = allocatedContainerToHostMap.get(containerId).get
        val containerSet = allocatedHostToContainersMap.get(host).get

        containerSet.remove(containerId)
        if (containerSet.isEmpty) {
          allocatedHostToContainersMap.remove(host)
        } else {
          allocatedHostToContainersMap.update(host, containerSet)
        }

        allocatedContainerToHostMap.remove(containerId)
      }

      containerIdToExecutorId.remove(containerId).foreach { eid =>
        executorIdToContainer.remove(eid)

        if (!alreadyReleased) {
          // The executor could have gone away (like no route to host, node failure, etc)
          // Notify backend about the failure of the executor
          numUnexpectedContainerRelease += 1
          driverRef.send(RemoveExecutor(eid,
            s"Yarn deallocated the executor $eid (container $containerId)"))
        }
      }
    }
  }

  private def internalReleaseContainer(container: Container): Unit = {
    releasedContainers.add(container.getId())
    amClient.releaseAssignedContainer(container.getId())
  }

  private[yarn] def getNumUnexpectedContainerRelease = numUnexpectedContainerRelease

}

private object YarnAllocator {
  val MEM_REGEX = "[0-9.]+ [KMG]B"
  val PMEM_EXCEEDED_PATTERN =
    Pattern.compile(s"$MEM_REGEX of $MEM_REGEX physical memory used")
  val VMEM_EXCEEDED_PATTERN =
    Pattern.compile(s"$MEM_REGEX of $MEM_REGEX virtual memory used")

  def memLimitExceededLogMessage(diagnostics: String, pattern: Pattern): String = {
    val matcher = pattern.matcher(diagnostics)
    val diag = if (matcher.find()) " " + matcher.group() + "." else ""
    ("Container killed by YARN for exceeding memory limits." + diag
      + " Consider boosting spark.yarn.executor.memoryOverhead.")
  }
}
