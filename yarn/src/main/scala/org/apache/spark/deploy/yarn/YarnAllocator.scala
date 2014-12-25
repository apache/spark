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
import java.util.concurrent.atomic.AtomicInteger
import java.util.regex.Pattern

import scala.collection.JavaConversions._
import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}

import com.google.common.util.concurrent.ThreadFactoryBuilder

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.client.api.AMRMClient
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest
import org.apache.hadoop.yarn.util.RackResolver

import org.apache.spark.{Logging, SecurityManager, SparkConf}
import org.apache.spark.deploy.yarn.YarnSparkHadoopUtil._
import org.apache.spark.scheduler.SplitInfo
import org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend

object AllocationType extends Enumeration {
  type AllocationType = Value
  val HOST, RACK, ANY = Value
}

/**
 * YarnAllocator is charged with requesting containers from the YARN ResourceManager and deciding
 * what to do with containers when YARN fulfills these requests.
 *
 * This class makes use of YARN's AMRMClient APIs. We interact with the AMRMClient in three ways:
 * * Making our resource needs known, which updates local bookkeeping about containers requested.
 * * Calling "allocate", which syncs our local container requests with the RM, and returns any
 *   containers that YARN has granted to us.
 * * Processing the containers granted to us to possibly launch executors inside of them.
 *
 * The public methods of this class are thread-safe.  All methods that mutate state are
 * synchronized.
 */
private[yarn] class YarnAllocator(
    conf: Configuration,
    sparkConf: SparkConf,
    amClient: AMRMClient[ContainerRequest],
    appAttemptId: ApplicationAttemptId,
    args: ApplicationMasterArguments,
    securityMgr: SecurityManager)
  extends Logging {

  import YarnAllocator._

  // These two complementary data structures are locked on allocatedHostToContainersMap.
  private[yarn] val allocatedHostToContainersMap =
    new HashMap[String, collection.mutable.Set[ContainerId]]
  private[yarn] val allocatedContainerToHostMap = new HashMap[ContainerId, String]

  // Containers that we no longer care about. We've either already told the RM to release them or
  // will on the next heartbeat. Containers get removed from this map after the RM tells us they've
  // completed.
  private val releasedContainers = Collections.newSetFromMap[ContainerId](
    new ConcurrentHashMap[ContainerId, java.lang.Boolean])

  // Number of container requests that have been sent to, but not yet allocated by the
  // ApplicationMaster.
  @volatile private var numExecutorsRunning = 0
  // Used to generate a unique ID per executor
  private var executorIdCounter = 0
  @volatile private var numExecutorsFailed = 0

  @volatile private var maxExecutors = args.numExecutors

  // Keep track of which container is running which executor to remove the executors later
  private val executorIdToContainer = new HashMap[String, Container]

  // Executor memory in MB.
  protected val executorMemory = args.executorMemory
  // Additional memory overhead.
  protected val memoryOverhead: Int = sparkConf.getInt("spark.yarn.executor.memoryOverhead",
    math.max((MEMORY_OVERHEAD_FACTOR * executorMemory).toInt, MEMORY_OVERHEAD_MIN))
  // Number of cores per executor.
  protected val executorCores = args.executorCores
  // Resource capability requested for each executors
  private val resource = Resource.newInstance(executorMemory + memoryOverhead, executorCores)

  private val launcherPool = new ThreadPoolExecutor(
    // max pool size of Integer.MAX_VALUE is ignored because we use an unbounded queue
    sparkConf.getInt("spark.yarn.containerLauncherMaxThreads", 25), Integer.MAX_VALUE,
    1, TimeUnit.MINUTES,
    new LinkedBlockingQueue[Runnable](),
    new ThreadFactoryBuilder().setNameFormat("ContainerLauncher #%d").setDaemon(true).build())
  launcherPool.allowCoreThreadTimeOut(true)

  private val driverUrl = "akka.tcp://sparkDriver@%s:%s/user/%s".format(
    sparkConf.get("spark.driver.host"),
    sparkConf.get("spark.driver.port"),
    CoarseGrainedSchedulerBackend.ACTOR_NAME)

  // For testing
  private val launchContainers = sparkConf.getBoolean("spark.yarn.launchContainers", true)

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
   * Request as many executors from the ResourceManager as needed to reach the desired total.
   * This takes into account executors already running or pending.
   */
  def requestTotalExecutors(requestedTotal: Int): Unit = synchronized {
    val currentTotal = getNumPendingAllocate + numExecutorsRunning
    if (requestedTotal > currentTotal) {
      maxExecutors += (requestedTotal - currentTotal)
      // We need to call `allocateResources` here to avoid the following race condition:
      // If we request executors twice before `allocateResources` is called, then we will end up
      // double counting the number requested because `numPendingAllocate` is not updated yet.
      allocateResources()
    } else {
      logInfo(s"Not allocating more executors because there are already $currentTotal " +
        s"(application requested $requestedTotal total)")
    }
  }

  /**
   * Request that the ResourceManager release the container running the specified executor.
   */
  def killExecutor(executorId: String): Unit = synchronized {
    if (executorIdToContainer.contains(executorId)) {
      val container = executorIdToContainer.remove(executorId).get
      internalReleaseContainer(container)
      numExecutorsRunning -= 1
      maxExecutors -= 1
      assert(maxExecutors >= 0, "Allocator killed more executors than are allocated!")
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
    val numPendingAllocate = getNumPendingAllocate
    val missing = maxExecutors - numPendingAllocate - numExecutorsRunning

    if (missing > 0) {
      val totalExecutorMemory = resource.getMemory
      logInfo(s"Will request $missing executor containers, each with $totalExecutorMemory MB " +
        s"memory including $memoryOverhead MB overhead")
    }

    addResourceRequests(missing)
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
   * Request numExecutors additional containers from YARN.
   */
  private[yarn] def addResourceRequests(numExecutors: Int): Unit = {
    val containerRequests = new ArrayBuffer[ContainerRequest]
    for (i <- 0 until numExecutors) {
      containerRequests += new ContainerRequest(resource, null, null, RM_REQUEST_PRIORITY)
    }

    for (request <- containerRequests) {
      amClient.addContainerRequest(request)
    }

    for (request <- containerRequests) {
      val nodes = request.getNodes
      val hostStr = if (nodes == null || nodes.isEmpty) "Any" else nodes.last
      logInfo("Container request (host: %s, capability: %s".format(hostStr, resource))
    }
  }

  /**
   * Handle containers granted by the RM by launching executors on them.
   *
   * Due to the way the YARN allocation protocol works, certain healthy race conditions can result
   * in YARN granting containers that we no longer need. In this case, we release them.
   */
  private[yarn] def handleAllocatedContainers(allocatedContainers: Seq[Container]): Unit = {
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
      for (container <- remainingAfterOffRackMatches) {
        internalReleaseContainer(container)
      }
    }

    runAllocatedContainers(containersToUse)

    logInfo("Received %d containers from YARN, launching executors on %d."
      .format(allocatedContainers.size, containersToUse.size))
  }

  /**
   * Looks for requests for the given location that match the given container allocation. If it
   * finds one, removes the request so that it won't be submitted again. Places the container into
   * containersToUse or remaining.
   *
   * @param containersToUse list of containers that will be used
   * @param remaining list of containers that will not be used
   */
  private def matchContainerToRequest(
      allocatedContainer: Container,
      location: String,
      containersToUse: ArrayBuffer[Container],
      remaining: ArrayBuffer[Container]): Unit = {
    val matchingRequests = amClient.getMatchingRequests(allocatedContainer.getPriority, location,
      allocatedContainer.getResource)

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
      assert(numExecutorsRunning <= maxExecutors)
      val executorHostname = container.getNodeId.getHost
      val containerId = container.getId
      executorIdCounter += 1
      val executorId = executorIdCounter.toString

      assert(container.getResource.getMemory >= resource.getMemory)

      logInfo("Launching container %s for on host %s".format(containerId, executorHostname))

      allocatedHostToContainersMap.synchronized {
        val containerSet = allocatedHostToContainersMap.getOrElseUpdate(executorHostname,
          new HashSet[ContainerId])

        containerSet += containerId
        allocatedContainerToHostMap.put(containerId, executorHostname)
      }

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

  private def processCompletedContainers(completedContainers: Seq[ContainerStatus]): Unit = {
    for (completedContainer <- completedContainers) {
      val containerId = completedContainer.getContainerId

      if (releasedContainers.contains(containerId)) {
        // Already marked the container for release, so remove it from
        // `releasedContainers`.
        releasedContainers.remove(containerId)
      } else {
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
        if (completedContainer.getExitStatus == -103) { // vmem limit exceeded
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

      allocatedHostToContainersMap.synchronized {
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
      }
    }
  }

  private def internalReleaseContainer(container: Container): Unit = {
    releasedContainers.add(container.getId())
    amClient.releaseAssignedContainer(container.getId())
  }

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
