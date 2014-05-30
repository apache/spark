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

import java.util.{Set => JSet}
import java.util.concurrent.{CopyOnWriteArrayList, ConcurrentHashMap}
import java.util.concurrent.atomic.AtomicInteger

import scala.collection
import scala.collection.JavaConversions._
import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}

import org.apache.spark.{Logging, SparkConf}
import org.apache.spark.scheduler.SplitInfo
import org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId
import org.apache.hadoop.yarn.api.records.{Container, ContainerId, ContainerStatus}
import org.apache.hadoop.yarn.api.records.{Priority, Resource}
import org.apache.hadoop.yarn.client.api.AMRMClient
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest
import org.apache.hadoop.yarn.util.RackResolver


object AllocationType extends Enumeration {
  type AllocationType = Value
  val HOST, RACK, ANY = Value
}

// TODO:
// Too many params.
// Needs to be mt-safe
// Need to refactor this to make it 'cleaner' ... right now, all computation is reactive - should
// make it more proactive and decoupled.

// Note that right now, we assume all node asks as uniform in terms of capabilities and priority
// Refer to http://developer.yahoo.com/blogs/hadoop/posts/2011/03/mapreduce-nextgen-scheduler/ for
// more info on how we are requesting for containers.

/**
 * Acquires resources for executors from a ResourceManager and launches executors in new containers.
 */
private[yarn] class YarnAllocationHandler(
    val conf: Configuration,
    val amClient: AMRMClient[ContainerRequest],
    val appAttemptId: ApplicationAttemptId,
    val maxExecutors: Int,
    val executorMemory: Int,
    val executorCores: Int,
    val preferredHostToCount: Map[String, Int], 
    val preferredRackToCount: Map[String, Int],
    val sparkConf: SparkConf)
  extends Logging {

  // These three are locked on allocatedHostToContainersMap. Complementary data structures
  // allocatedHostToContainersMap : containers which are running : host, Set<containerid>
  // allocatedContainerToHostMap: container to host mapping.
  private[yarn] val allocatedHostToContainersMap =
    new HashMap[String, collection.mutable.Set[ContainerId]]()

  private[yarn] val allocatedContainerToHostMap = new HashMap[ContainerId, String]()

  // Mapping of rack names to # of executors allocated on them
  private[yarn] val allocatedRackCount = new HashMap[String, Int]()

  // Containers which have been released.
  private val releasedContainerList = new CopyOnWriteArrayList[ContainerId]()

  // Number of container requests that have been sent to, but not yet allocated by the
  // ApplicationMaster.
  private val numPendingAllocate = new AtomicInteger()
  private val numExecutorsRunning = new AtomicInteger()
  // Used to generate a unique id per executor
  private val executorIdCounter = new AtomicInteger()
  private val numExecutorsFailed = new AtomicInteger()

  def getNumPendingAllocate: Int = numPendingAllocate.intValue

  def getNumExecutorsRunning: Int = numExecutorsRunning.intValue

  def getNumExecutorsFailed: Int = numExecutorsFailed.intValue

  def releaseContainer(container: Container) {
    val containerId = container.getId
    amClient.releaseAssignedContainer(containerId)
  }

  /**
   * Heartbeat to the ResourceManager. Passes along any ContainerRequests we've added to the
   * AMRMClient. If there are no pending requests, lets the ResourceManager know we're still alive.
   */
  def allocateResources() {
    val progressIndicator = 0.1f
    val allocateResponse = amClient.allocate(progressIndicator)

    val allocatedContainers = allocateResponse.getAllocatedContainers()
    if (allocatedContainers.size > 0) {
      logDebug("""
        Allocated containers: %d
        Current executor count: %d
        Containers released: %s
        Cluster resources: %s
               """.format(
        allocatedContainers.size,
        numExecutorsRunning.get(),
        releasedContainerList,
        allocateResponse.getAvailableResources))

      handleAllocatedContainers(allocatedContainers)
    }

    val completedContainers = allocateResponse.getCompletedContainersStatuses()
    if (completedContainers.size > 0) {
      logDebug("Completed %d containers".format(completedContainers.size))

      processCompletedContainers(completedContainers)

      logDebug("""
        Finished processing %d completed containers.
        Current number of executors running: %d,
        releasedContainerList: %s,
        """.format(
          completedContainers.size,
          numExecutorsRunning.get(),
          releasedContainerList))
    }
  }

  def handleAllocatedContainers(allocatedContainers: Seq[Container]) {
    val numPendingAllocateNow = numPendingAllocate.addAndGet(-allocatedContainers.size)

    if (numPendingAllocateNow < 0) {
      numPendingAllocate.addAndGet(-numPendingAllocateNow)
    }

    val containersToUse = new ArrayBuffer[Container](allocatedContainers.size)

    // Match incoming requests by host
    val remainingAfterHostMatches = new ArrayBuffer[Container]()
    for (allocatedContainer <- allocatedContainers) {
      matchContainerToRequest(allocatedContainer, allocatedContainer.getNodeId.getHost,
        containersToUse, remainingAfterHostMatches)
    }

    // Match remaining by rack
    val remainingAfterRackMatches = new ArrayBuffer[Container]()
    for (allocatedContainer <- remainingAfterHostMatches) {
      val rack = RackResolver.resolve(conf, allocatedContainer.getNodeId.getHost).getNetworkLocation
      matchContainerToRequest(allocatedContainer, rack, containersToUse,
        remainingAfterRackMatches)
    }

    // Assign remaining that are neither node-local nor rack-local
    val remainingAfterOffRackMatches = new ArrayBuffer[Container]()
    for (allocatedContainer <- remainingAfterRackMatches) {
      matchContainerToRequest(allocatedContainer, "*", containersToUse,
        remainingAfterOffRackMatches)
    }

    if (!remainingAfterOffRackMatches.isEmpty) {
      logWarning("Received containers that did not satisfy resource constraints: "
        + remainingAfterOffRackMatches)
      for (container <- remainingAfterOffRackMatches) {
        amClient.releaseAssignedContainer(container.getId)
      }
    }

    runAllocatedContainers(containersToUse)

    logDebug("""
        Finished allocating %s containers (from %s originally).
        Current number of executors running: %d,
        releasedContainerList: %s,
             """.format(
      containersToUse,
      allocatedContainers,
      numExecutorsRunning.get(),
      releasedContainerList))
  }

  def runAllocatedContainers(containersToUse: ArrayBuffer[Container]) {
    for (container <- containersToUse) {
      val numExecutorsRunningNow = numExecutorsRunning.incrementAndGet()
      val executorHostname = container.getNodeId.getHost
      val containerId = container.getId

      val executorMemoryWithOverhead = (executorMemory + YarnAllocationHandler.MEMORY_OVERHEAD)
      assert(container.getResource.getMemory >= executorMemoryWithOverhead)

      if (numExecutorsRunningNow > maxExecutors) {
        logInfo("""Ignoring container %s at host %s, since we already have the required number of
            containers.""".format(containerId, executorHostname))
        amClient.releaseAssignedContainer(container.getId)
        numExecutorsRunning.decrementAndGet()
      } else {
        val executorId = executorIdCounter.incrementAndGet().toString
        val driverUrl = "akka.tcp://spark@%s:%s/user/%s".format(
          sparkConf.get("spark.driver.host"),
          sparkConf.get("spark.driver.port"),
          CoarseGrainedSchedulerBackend.ACTOR_NAME)

        logInfo("Launching container %s for on host %s".format(containerId, executorHostname))

        val rack = RackResolver.resolve(conf, executorHostname).getNetworkLocation
        allocatedHostToContainersMap.synchronized {
          val containerSet = allocatedHostToContainersMap.getOrElseUpdate(executorHostname,
            new HashSet[ContainerId]())

          containerSet += containerId
          allocatedContainerToHostMap.put(containerId, executorHostname)

          if (rack != null) {
            allocatedRackCount.put(rack, allocatedRackCount.getOrElse(rack, 0) + 1)
          }
        }
        logInfo("Launching ExecutorRunnable. driverUrl: %s,  executorHostname: %s".format(
          driverUrl, executorHostname))
        val executorRunnable = new ExecutorRunnable(
          container,
          conf,
          sparkConf,
          driverUrl,
          executorId,
          executorHostname,
          executorMemory,
          executorCores)
        new Thread(executorRunnable).start()
      }
    }
  }

  def processCompletedContainers(completedContainers: Seq[ContainerStatus]) {
    for (completedContainer <- completedContainers) {
      val containerId = completedContainer.getContainerId

      if (allocatedContainerToHostMap.containsKey(containerId)) {
        // Decrement the number of executors running. The next iteration of
        // the ApplicationMaster's reporting thread will take care of allocating.
        numExecutorsRunning.decrementAndGet()
        logInfo("Completed container %s (state: %s, exit status: %s)".format(
          containerId,
          completedContainer.getState,
          completedContainer.getExitStatus()))
        // Hadoop 2.2.X added a ContainerExitStatus we should switch to use
        // there are some exit status' we shouldn't necessarily count against us, but for
        // now I think its ok as none of the containers are expected to exit
        if (completedContainer.getExitStatus() != 0) {
          logInfo("Container marked as failed: " + containerId)
          numExecutorsFailed.incrementAndGet()
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

          // TODO: Move this part outside the synchronized block?
          val rack = RackResolver.resolve(conf, host).getNetworkLocation
          if (rack != null) {
            val rackCount = allocatedRackCount.getOrElse(rack, 0) - 1
            if (rackCount > 0) {
              allocatedRackCount.put(rack, rackCount)
            } else {
              allocatedRackCount.remove(rack)
            }
          }
        }
      }
    }
  }

  /**
   * Looks for requests for the given location that match the given container allocation. If it
   * finds one, removes the request so that it won't be submitted again. Places the container into
   * containersToUse or remaining.
   */
  def matchContainerToRequest(allocatedContainer: Container, location: String,
      containersToUse: ArrayBuffer[Container], remaining: ArrayBuffer[Container]) {
    val matchingRequests = amClient.getMatchingRequests(allocatedContainer.getPriority,
      location, allocatedContainer.getResource)

    // Match the allocation to a request
    if (!matchingRequests.isEmpty) {
      val containerRequest = matchingRequests.get(0).iterator.next
      amClient.removeContainerRequest(containerRequest)
      containersToUse += allocatedContainer
    } else {
      remaining += allocatedContainer
    }
  }

  def createRackResourceRequests(
      hostContainers: ArrayBuffer[ContainerRequest]
    ): ArrayBuffer[ContainerRequest] = {
    // Generate modified racks and new set of hosts under it before issuing requests.
    val rackToCounts = new HashMap[String, Int]()

    for (container <- hostContainers) {
      val candidateHost = container.getNodes.last
      assert(YarnAllocationHandler.ANY_HOST != candidateHost)

      val rack = RackResolver.resolve(conf, candidateHost).getNetworkLocation
      if (rack != null) {
        var count = rackToCounts.getOrElse(rack, 0)
        count += 1
        rackToCounts.put(rack, count)
      }
    }

    val requestedContainers = new ArrayBuffer[ContainerRequest](rackToCounts.size)
    for ((rack, count) <- rackToCounts) {
      requestedContainers ++= createResourceRequests(
        AllocationType.RACK,
        rack,
        count,
        YarnAllocationHandler.PRIORITY)
    }

    requestedContainers
  }

  def allocatedContainersOnHost(host: String): Int = {
    allocatedHostToContainersMap.synchronized {
      allocatedHostToContainersMap.getOrElse(host, Set()).size
    }
  }

  def addResourceRequests(numExecutors: Int) {
    val containerRequests: List[ContainerRequest] =
      if (numExecutors <= 0 || preferredHostToCount.isEmpty) {
        logDebug("numExecutors: " + numExecutors + ", host preferences: " +
          preferredHostToCount.isEmpty)
        createResourceRequests(
          AllocationType.ANY,
          resource = null,
          numExecutors,
          YarnAllocationHandler.PRIORITY).toList
      } else {
        // Requests for all hosts in preferred nodes and for numExecutors -
        // candidates.size, request by default allocation policy.
        val hostContainerRequests = new ArrayBuffer[ContainerRequest](preferredHostToCount.size)
        for ((candidateHost, candidateCount) <- preferredHostToCount) {
          val requiredCount = candidateCount - allocatedContainersOnHost(candidateHost)

          if (requiredCount > 0) {
            hostContainerRequests ++= createResourceRequests(
              AllocationType.HOST,
              candidateHost,
              requiredCount,
              YarnAllocationHandler.PRIORITY)
          }
        }
        val rackContainerRequests: List[ContainerRequest] = createRackResourceRequests(
          hostContainerRequests).toList

        val anyContainerRequests = createResourceRequests(
          AllocationType.ANY,
          resource = null,
          numExecutors,
          YarnAllocationHandler.PRIORITY)

        val containerRequestBuffer = new ArrayBuffer[ContainerRequest](
          hostContainerRequests.size + rackContainerRequests.size + anyContainerRequests.size)

        containerRequestBuffer ++= hostContainerRequests
        containerRequestBuffer ++= rackContainerRequests
        containerRequestBuffer ++= anyContainerRequests
        containerRequestBuffer.toList
      }

    for (request <- containerRequests) {
      amClient.addContainerRequest(request)
    }

    if (numExecutors > 0) {
      numPendingAllocate.addAndGet(numExecutors)
      logInfo("Will Allocate %d executor containers, each with %d memory".format(
        numExecutors,
        (executorMemory + YarnAllocationHandler.MEMORY_OVERHEAD)))
    } else {
      logDebug("Empty allocation request ...")
    }

    for (request <- containerRequests) {
      val nodes = request.getNodes
      val hostStr = if (nodes == null || nodes.isEmpty) {
        "Any"
      } else {
        nodes.last
      }
      logInfo("Container request (host: %s, priority: %s, capability: %s".format(
        hostStr,
        request.getPriority().getPriority,
        request.getCapability))
    }
  }

  private def createResourceRequests(
      requestType: AllocationType.AllocationType,
      resource: String,
      numExecutors: Int,
      priority: Int
    ): ArrayBuffer[ContainerRequest] = {

    requestType match {
      case AllocationType.HOST => constructContainerRequests(
        Array(resource), null, numExecutors, priority)
      case AllocationType.RACK => constructContainerRequests(
        null, Array(resource), numExecutors, priority)
      case AllocationType.ANY => constructContainerRequests(
        null, null, numExecutors, priority)
      case _ => throw new IllegalArgumentException(
        "Unexpected/unsupported request type: " + requestType)
    }
  }

  private def constructContainerRequests(
      hosts: Array[String],
      racks: Array[String],
      numExecutors: Int,
      priority: Int
    ): ArrayBuffer[ContainerRequest] = {

    val memoryRequest = executorMemory + YarnAllocationHandler.MEMORY_OVERHEAD
    val resource = Resource.newInstance(memoryRequest, executorCores)

    val prioritySetting = Priority.newInstance(priority)

    val requests = new ArrayBuffer[ContainerRequest]()
    for (i <- 0 until numExecutors) {
      requests += new ContainerRequest(resource, hosts, racks, prioritySetting)
    }
    requests
  }
}

object YarnAllocationHandler {

  val ANY_HOST = "*"
  // All requests are issued with same priority : we do not (yet) have any distinction between 
  // request types (like map/reduce in hadoop for example)
  val PRIORITY = 1

  // Additional memory overhead - in mb.
  val MEMORY_OVERHEAD = 384

  // Host to rack map - saved from allocation requests. We are expecting this not to change.
  // Note that it is possible for this to change : and ResourceManager will indicate that to us via
  // update response to allocate. But we are punting on handling that for now.
  private val hostToRack = new ConcurrentHashMap[String, String]()
  private val rackToHostSet = new ConcurrentHashMap[String, JSet[String]]()

  def newAllocator(
      conf: Configuration,
      amClient: AMRMClient[ContainerRequest],
      appAttemptId: ApplicationAttemptId,
      args: ApplicationMasterArguments,
      sparkConf: SparkConf
    ): YarnAllocationHandler = {
    new YarnAllocationHandler(
      conf,
      amClient,
      appAttemptId,
      args.numExecutors, 
      args.executorMemory,
      args.executorCores,
      Map[String, Int](),
      Map[String, Int](),
      sparkConf)
  }

  def newAllocator(
      conf: Configuration,
      amClient: AMRMClient[ContainerRequest],
      appAttemptId: ApplicationAttemptId,
      args: ApplicationMasterArguments,
      map: collection.Map[String,
      collection.Set[SplitInfo]],
      sparkConf: SparkConf
    ): YarnAllocationHandler = {
    val (hostToSplitCount, rackToSplitCount) = generateNodeToWeight(conf, map)
    new YarnAllocationHandler(
      conf,
      amClient,
      appAttemptId,
      args.numExecutors, 
      args.executorMemory,
      args.executorCores,
      hostToSplitCount,
      rackToSplitCount,
      sparkConf)
  }

  def newAllocator(
      conf: Configuration,
      amClient: AMRMClient[ContainerRequest],
      appAttemptId: ApplicationAttemptId,
      maxExecutors: Int,
      executorMemory: Int,
      executorCores: Int,
      map: collection.Map[String, collection.Set[SplitInfo]],
      sparkConf: SparkConf
    ): YarnAllocationHandler = {
    val (hostToCount, rackToCount) = generateNodeToWeight(conf, map)
    new YarnAllocationHandler(
      conf,
      amClient,
      appAttemptId,
      maxExecutors,
      executorMemory,
      executorCores,
      hostToCount,
      rackToCount,
      sparkConf)
  }

  // A simple method to copy the split info map.
  private def generateNodeToWeight(
      conf: Configuration,
      input: collection.Map[String, collection.Set[SplitInfo]]
    ): (Map[String, Int], Map[String, Int]) = {

    if (input == null) {
      return (Map[String, Int](), Map[String, Int]())
    }

    val hostToCount = new HashMap[String, Int]
    val rackToCount = new HashMap[String, Int]

    for ((host, splits) <- input) {
      val hostCount = hostToCount.getOrElse(host, 0)
      hostToCount.put(host, hostCount + splits.size)

      val rack = RackResolver.resolve(conf, host).getNetworkLocation
      if (rack != null){
        val rackCount = rackToCount.getOrElse(host, 0)
        rackToCount.put(host, rackCount + splits.size)
      }
    }

    (hostToCount.toMap, rackToCount.toMap)
  }
}
