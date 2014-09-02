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

import java.lang.{Boolean => JBoolean}
import java.util.{Collections, Set => JSet}
import java.util.concurrent.{CopyOnWriteArrayList, ConcurrentHashMap}
import java.util.concurrent.atomic.AtomicInteger

import scala.collection
import scala.collection.JavaConversions._
import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}

import org.apache.spark.{Logging, SparkConf, SparkEnv}
import org.apache.spark.scheduler.{SplitInfo,TaskSchedulerImpl}
import org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend
import org.apache.spark.util.Utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId
import org.apache.hadoop.yarn.api.records.{Container, ContainerId, ContainerStatus}
import org.apache.hadoop.yarn.api.records.{Priority, Resource, ResourceRequest}
import org.apache.hadoop.yarn.api.protocolrecords.{AllocateRequest, AllocateResponse}
import org.apache.hadoop.yarn.client.api.AMRMClient
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest
import org.apache.hadoop.yarn.util.{RackResolver, Records}


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
  private val allocatedHostToContainersMap =
    new HashMap[String, collection.mutable.Set[ContainerId]]()

  private val allocatedContainerToHostMap = new HashMap[ContainerId, String]()

  // allocatedRackCount is populated ONLY if allocation happens (or decremented if this is an
  // allocated node)
  // As with the two data structures above, tightly coupled with them, and to be locked on
  // allocatedHostToContainersMap
  private val allocatedRackCount = new HashMap[String, Int]()

  // Containers which have been released.
  private val releasedContainerList = new CopyOnWriteArrayList[ContainerId]()
  // Containers to be released in next request to RM
  private val pendingReleaseContainers = new ConcurrentHashMap[ContainerId, Boolean]

  // Additional memory overhead - in mb.
  private def memoryOverhead: Int = sparkConf.getInt("spark.yarn.executor.memoryOverhead",
    YarnAllocationHandler.MEMORY_OVERHEAD)

  // Number of container requests that have been sent to, but not yet allocated by the
  // ApplicationMaster.
  private val numPendingAllocate = new AtomicInteger()
  private val numExecutorsRunning = new AtomicInteger()
  // Used to generate a unique id per executor
  private val executorIdCounter = new AtomicInteger()
  private val lastResponseId = new AtomicInteger()
  private val numExecutorsFailed = new AtomicInteger()

  def getNumPendingAllocate: Int = numPendingAllocate.intValue

  def getNumExecutorsRunning: Int = numExecutorsRunning.intValue

  def getNumExecutorsFailed: Int = numExecutorsFailed.intValue

  def isResourceConstraintSatisfied(container: Container): Boolean = {
    container.getResource.getMemory >= (executorMemory + memoryOverhead)
  }

  def releaseContainer(container: Container) {
    val containerId = container.getId
    pendingReleaseContainers.put(containerId, true)
    amClient.releaseAssignedContainer(containerId)
  }

  def allocateResources() {
    // We have already set the container request. Poll the ResourceManager for a response.
    // This doubles as a heartbeat if there are no pending container requests.
    val progressIndicator = 0.1f
    val allocateResponse = amClient.allocate(progressIndicator)

    val allocatedContainers = allocateResponse.getAllocatedContainers()
    if (allocatedContainers.size > 0) {
      var numPendingAllocateNow = numPendingAllocate.addAndGet(-1 * allocatedContainers.size)

      if (numPendingAllocateNow < 0) {
        numPendingAllocateNow = numPendingAllocate.addAndGet(-1 * numPendingAllocateNow)
      }

      logDebug("""
        Allocated containers: %d
        Current executor count: %d
        Containers released: %s
        Containers to-be-released: %s
        Cluster resources: %s
        """.format(
          allocatedContainers.size,
          numExecutorsRunning.get(),
          releasedContainerList,
          pendingReleaseContainers,
          allocateResponse.getAvailableResources))

      val hostToContainers = new HashMap[String, ArrayBuffer[Container]]()

      for (container <- allocatedContainers) {
        if (isResourceConstraintSatisfied(container)) {
          // Add the accepted `container` to the host's list of already accepted,
          // allocated containers
          val host = container.getNodeId.getHost
          val containersForHost = hostToContainers.getOrElseUpdate(host,
            new ArrayBuffer[Container]())
          containersForHost += container
        } else {
          // Release container, since it doesn't satisfy resource constraints.
          releaseContainer(container)
        }
      }

       // Find the appropriate containers to use.
      // TODO: Cleanup this group-by...
      val dataLocalContainers = new HashMap[String, ArrayBuffer[Container]]()
      val rackLocalContainers = new HashMap[String, ArrayBuffer[Container]]()
      val offRackContainers = new HashMap[String, ArrayBuffer[Container]]()

      for (candidateHost <- hostToContainers.keySet) {
        val maxExpectedHostCount = preferredHostToCount.getOrElse(candidateHost, 0)
        val requiredHostCount = maxExpectedHostCount - allocatedContainersOnHost(candidateHost)

        val remainingContainersOpt = hostToContainers.get(candidateHost)
        assert(remainingContainersOpt.isDefined)
        var remainingContainers = remainingContainersOpt.get

        if (requiredHostCount >= remainingContainers.size) {
          // Since we have <= required containers, add all remaining containers to
          // `dataLocalContainers`.
          dataLocalContainers.put(candidateHost, remainingContainers)
          // There are no more free containers remaining.
          remainingContainers = null
        } else if (requiredHostCount > 0) {
          // Container list has more containers than we need for data locality.
          // Split the list into two: one based on the data local container count,
          // (`remainingContainers.size` - `requiredHostCount`), and the other to hold remaining
          // containers.
          val (dataLocal, remaining) = remainingContainers.splitAt(
            remainingContainers.size - requiredHostCount)
          dataLocalContainers.put(candidateHost, dataLocal)

          // Invariant: remainingContainers == remaining

          // YARN has a nasty habit of allocating a ton of containers on a host - discourage this.
          // Add each container in `remaining` to list of containers to release. If we have an
          // insufficient number of containers, then the next allocation cycle will reallocate
          // (but won't treat it as data local).
          // TODO(harvey): Rephrase this comment some more.
          for (container <- remaining) releaseContainer(container)
          remainingContainers = null
        }

        // For rack local containers
        if (remainingContainers != null) {
          val rack = YarnAllocationHandler.lookupRack(conf, candidateHost)
          if (rack != null) {
            val maxExpectedRackCount = preferredRackToCount.getOrElse(rack, 0)
            val requiredRackCount = maxExpectedRackCount - allocatedContainersOnRack(rack) -
              rackLocalContainers.getOrElse(rack, List()).size

            if (requiredRackCount >= remainingContainers.size) {
              // Add all remaining containers to to `dataLocalContainers`.
              dataLocalContainers.put(rack, remainingContainers)
              remainingContainers = null
            } else if (requiredRackCount > 0) {
              // Container list has more containers that we need for data locality.
              // Split the list into two: one based on the data local container count,
              // (`remainingContainers.size` - `requiredHostCount`), and the other to hold remaining
              // containers.
              val (rackLocal, remaining) = remainingContainers.splitAt(
                remainingContainers.size - requiredRackCount)
              val existingRackLocal = rackLocalContainers.getOrElseUpdate(rack,
                new ArrayBuffer[Container]())

              existingRackLocal ++= rackLocal

              remainingContainers = remaining
            }
          }
        }

        if (remainingContainers != null) {
          // Not all containers have been consumed - add them to the list of off-rack containers.
          offRackContainers.put(candidateHost, remainingContainers)
        }
      }

      // Now that we have split the containers into various groups, go through them in order:
      // first host-local, then rack-local, and finally off-rack.
      // Note that the list we create below tries to ensure that not all containers end up within
      // a host if there is a sufficiently large number of hosts/containers.
      val allocatedContainersToProcess = new ArrayBuffer[Container](allocatedContainers.size)
      allocatedContainersToProcess ++= TaskSchedulerImpl.prioritizeContainers(dataLocalContainers)
      allocatedContainersToProcess ++= TaskSchedulerImpl.prioritizeContainers(rackLocalContainers)
      allocatedContainersToProcess ++= TaskSchedulerImpl.prioritizeContainers(offRackContainers)

      // Run each of the allocated containers.
      for (container <- allocatedContainersToProcess) {
        val numExecutorsRunningNow = numExecutorsRunning.incrementAndGet()
        val executorHostname = container.getNodeId.getHost
        val containerId = container.getId

        val executorMemoryOverhead = (executorMemory + memoryOverhead)
        assert(container.getResource.getMemory >= executorMemoryOverhead)

        if (numExecutorsRunningNow > maxExecutors) {
          logInfo("""Ignoring container %s at host %s, since we already have the required number of
            containers for it.""".format(containerId, executorHostname))
          releaseContainer(container)
          numExecutorsRunning.decrementAndGet()
        } else {
          val executorId = executorIdCounter.incrementAndGet().toString
          val driverUrl = "akka.tcp://%s@%s:%s/user/%s".format(
            SparkEnv.driverActorSystemName,
            sparkConf.get("spark.driver.host"),
            sparkConf.get("spark.driver.port"),
            CoarseGrainedSchedulerBackend.ACTOR_NAME)

          logInfo("Launching container %s for on host %s".format(containerId, executorHostname))

          // To be safe, remove the container from `pendingReleaseContainers`.
          pendingReleaseContainers.remove(containerId)

          val rack = YarnAllocationHandler.lookupRack(conf, executorHostname)
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
      logDebug("""
        Finished allocating %s containers (from %s originally).
        Current number of executors running: %d,
        releasedContainerList: %s,
        pendingReleaseContainers: %s
        """.format(
          allocatedContainersToProcess,
          allocatedContainers,
          numExecutorsRunning.get(),
          releasedContainerList,
          pendingReleaseContainers))
    }

    val completedContainers = allocateResponse.getCompletedContainersStatuses()
    if (completedContainers.size > 0) {
      logDebug("Completed %d containers".format(completedContainers.size))

      for (completedContainer <- completedContainers) {
        val containerId = completedContainer.getContainerId

        if (pendingReleaseContainers.containsKey(containerId)) {
          // YarnAllocationHandler already marked the container for release, so remove it from
          // `pendingReleaseContainers`.
          pendingReleaseContainers.remove(containerId)
        } else {
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
            val hostOpt = allocatedContainerToHostMap.get(containerId)
            assert(hostOpt.isDefined)
            val host = hostOpt.get

            val containerSetOpt = allocatedHostToContainersMap.get(host)
            assert(containerSetOpt.isDefined)
            val containerSet = containerSetOpt.get

            containerSet.remove(containerId)
            if (containerSet.isEmpty) {
              allocatedHostToContainersMap.remove(host)
            } else {
              allocatedHostToContainersMap.update(host, containerSet)
            }

            allocatedContainerToHostMap.remove(containerId)

            // TODO: Move this part outside the synchronized block?
            val rack = YarnAllocationHandler.lookupRack(conf, host)
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
      logDebug("""
        Finished processing %d completed containers.
        Current number of executors running: %d,
        releasedContainerList: %s,
        pendingReleaseContainers: %s
        """.format(
          completedContainers.size,
          numExecutorsRunning.get(),
          releasedContainerList,
          pendingReleaseContainers))
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

      val rack = YarnAllocationHandler.lookupRack(conf, candidateHost)
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
    var retval = 0
    allocatedHostToContainersMap.synchronized {
      retval = allocatedHostToContainersMap.getOrElse(host, Set()).size
    }
    retval
  }

  def allocatedContainersOnRack(rack: String): Int = {
    var retval = 0
    allocatedHostToContainersMap.synchronized {
      retval = allocatedRackCount.getOrElse(rack, 0)
    }
    retval
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
        // Request for all hosts in preferred nodes and for numExecutors - 
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
          hostContainerRequests.size + rackContainerRequests.size() + anyContainerRequests.size)

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
        (executorMemory + memoryOverhead)))
    } else {
      logDebug("Empty allocation request ...")
    }

    for (request <- containerRequests) {
      val nodes = request.getNodes
      var hostStr = if (nodes == null || nodes.isEmpty) {
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

    // If hostname is specified, then we need at least two requests - node local and rack local.
    // There must be a third request, which is ANY. That will be specially handled.
    requestType match {
      case AllocationType.HOST => {
        assert(YarnAllocationHandler.ANY_HOST != resource)
        val hostname = resource
        val nodeLocal = constructContainerRequests(
          Array(hostname),
          racks = null,
          numExecutors,
          priority)

        // Add `hostname` to the global (singleton) host->rack mapping in YarnAllocationHandler.
        YarnAllocationHandler.populateRackInfo(conf, hostname)
        nodeLocal
      }
      case AllocationType.RACK => {
        val rack = resource
        constructContainerRequests(hosts = null, Array(rack), numExecutors, priority)
      }
      case AllocationType.ANY => constructContainerRequests(
        hosts = null, racks = null, numExecutors, priority)
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

    val memoryRequest = executorMemory + memoryOverhead
    val resource = Resource.newInstance(memoryRequest, executorCores)

    val prioritySetting = Records.newRecord(classOf[Priority])
    prioritySetting.setPriority(priority)

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
  // Note that it is possible for this to change : and ResurceManager will indicate that to us via
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

      val rack = lookupRack(conf, host)
      if (rack != null){
        val rackCount = rackToCount.getOrElse(host, 0)
        rackToCount.put(host, rackCount + splits.size)
      }
    }

    (hostToCount.toMap, rackToCount.toMap)
  }

  def lookupRack(conf: Configuration, host: String): String = {
    if (!hostToRack.contains(host)) {
      populateRackInfo(conf, host)
    }
    hostToRack.get(host)
  }

  def fetchCachedHostsForRack(rack: String): Option[Set[String]] = {
    Option(rackToHostSet.get(rack)).map { set =>
      val convertedSet: collection.mutable.Set[String] = set
      // TODO: Better way to get a Set[String] from JSet.
      convertedSet.toSet
    }
  }

  def populateRackInfo(conf: Configuration, hostname: String) {
    Utils.checkHost(hostname)

    if (!hostToRack.containsKey(hostname)) {
      // If there are repeated failures to resolve, all to an ignore list.
      val rackInfo = RackResolver.resolve(conf, hostname)
      if (rackInfo != null && rackInfo.getNetworkLocation != null) {
        val rack = rackInfo.getNetworkLocation
        hostToRack.put(hostname, rack)
        if (! rackToHostSet.containsKey(rack)) {
          rackToHostSet.putIfAbsent(rack,
            Collections.newSetFromMap(new ConcurrentHashMap[String, JBoolean]()))
        }
        rackToHostSet.get(rack).add(hostname)

        // TODO(harvey): Figure out what this comment means...
        // Since RackResolver caches, we are disabling this for now ...
      } /* else {
        // right ? Else we will keep calling rack resolver in case we cant resolve rack info ...
        hostToRack.put(hostname, null)
      } */
    }
  }
}
