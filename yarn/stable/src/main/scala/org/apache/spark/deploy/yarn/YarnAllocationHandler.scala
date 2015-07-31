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

import scala.collection.JavaConversions._
import scala.collection.mutable.{ArrayBuffer, HashMap}

import org.apache.spark.{SecurityManager, SparkConf} 
import org.apache.spark.scheduler.SplitInfo

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse
import org.apache.hadoop.yarn.client.api.AMRMClient
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest
import org.apache.hadoop.yarn.util.{RackResolver, Records}

import org.apache.log4j.{Level, Logger}

/**
 * Acquires resources for executors from a ResourceManager and launches executors in new containers.
 */
private[yarn] class YarnAllocationHandler(
    conf: Configuration,
    sparkConf: SparkConf,
    amClient: AMRMClient[ContainerRequest],
    appAttemptId: ApplicationAttemptId,
    args: ApplicationMasterArguments,
    preferredNodes: collection.Map[String, collection.Set[SplitInfo]], 
    securityMgr: SecurityManager)
  extends YarnAllocator(conf, sparkConf, appAttemptId, args, preferredNodes, securityMgr) {

  if (Logger.getLogger(classOf[RackResolver]).getLevel == null) {
    Logger.getLogger(classOf[RackResolver]).setLevel(Level.WARN)
  }

  override protected def releaseContainer(container: Container) = {
    amClient.releaseAssignedContainer(container.getId())
  }

  // pending isn't used on stable as the AMRMClient handles incremental asks
  override protected def allocateContainers(count: Int, pending: Int): YarnAllocateResponse = {
    addResourceRequests(count)

    // We have already set the container request. Poll the ResourceManager for a response.
    // This doubles as a heartbeat if there are no pending container requests.
    val progressIndicator = 0.1f
    val allocateResponse = amClient.allocate(progressIndicator)
    val allocatedContainers = allocateResponse.getAllocatedContainers()

    if (allocatedContainers.size > 0) {
      removeAllocatedContainersRequest(allocatedContainers)
    }
    new StableAllocateResponse(allocateResponse)
  }

  /**
   * Remove container requests for containers granted by YARN.
   *
   * @param allocatedContainers Sequence of containers that were given to us by YARN
   *
   * Visible for testing.
   */
  def removeAllocatedContainersRequest(allocatedContainers: Seq[Container]): Unit = {
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
      matchContainerToRequest(allocatedContainer, YarnSparkHadoopUtil.ANY_HOST, containersToUse,
        remainingAfterOffRackMatches)
    }
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
      executorCores)
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

  private def createRackResourceRequests(
      hostContainers: ArrayBuffer[ContainerRequest]
    ): ArrayBuffer[ContainerRequest] = {
    // Generate modified racks and new set of hosts under it before issuing requests.
    val rackToCounts = new HashMap[String, Int]()

    for (container <- hostContainers) {
      val candidateHost = container.getNodes.last
      assert(YarnSparkHadoopUtil.ANY_HOST != candidateHost)

      val rack = YarnSparkHadoopUtil.lookupRack(conf, candidateHost)
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
        YarnSparkHadoopUtil.RM_REQUEST_PRIORITY)
    }

    requestedContainers
  }

  private def addResourceRequests(numExecutors: Int) {
    val containerRequests: List[ContainerRequest] =
      if (numExecutors <= 0) {
        logDebug("numExecutors: " + numExecutors)
        List()
      } else if (preferredHostToCount.isEmpty) {
        logDebug("host preferences is empty")
        createResourceRequests(
          AllocationType.ANY,
          resource = null,
          numExecutors,
          YarnSparkHadoopUtil.RM_REQUEST_PRIORITY).toList
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
              YarnSparkHadoopUtil.RM_REQUEST_PRIORITY)
          }
        }
        val rackContainerRequests: List[ContainerRequest] = createRackResourceRequests(
          hostContainerRequests).toList

        val anyContainerRequests = createResourceRequests(
          AllocationType.ANY,
          resource = null,
          numExecutors,
          YarnSparkHadoopUtil.RM_REQUEST_PRIORITY)

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
        assert(YarnSparkHadoopUtil.ANY_HOST != resource)
        val hostname = resource
        val nodeLocal = constructContainerRequests(
          Array(hostname),
          racks = null,
          numExecutors,
          priority)

        // Add `hostname` to the global (singleton) host->rack mapping in YarnAllocationHandler.
        YarnSparkHadoopUtil.populateRackInfo(conf, hostname)
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

  private class StableAllocateResponse(response: AllocateResponse) extends YarnAllocateResponse {
    override def getAllocatedContainers() = response.getAllocatedContainers()
    override def getAvailableResources() = response.getAvailableResources()
    override def getCompletedContainersStatuses() = response.getCompletedContainersStatuses()
  }

}
