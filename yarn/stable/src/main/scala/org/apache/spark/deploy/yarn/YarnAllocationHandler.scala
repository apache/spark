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

import org.apache.spark.SparkConf
import org.apache.spark.scheduler.SplitInfo

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse
import org.apache.hadoop.yarn.client.api.AMRMClient
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest
import org.apache.hadoop.yarn.util.Records

/**
 * Acquires resources for executors from a ResourceManager and launches executors in new containers.
 */
private[yarn] class YarnAllocationHandler(
    conf: Configuration,
    sparkConf: SparkConf,
    amClient: AMRMClient[ContainerRequest],
    appAttemptId: ApplicationAttemptId,
    args: ApplicationMasterArguments,
    preferredNodes: collection.Map[String, collection.Set[SplitInfo]])
  extends YarnAllocator(conf, sparkConf, args, preferredNodes) {

  override protected def releaseContainer(container: Container) = {
    amClient.releaseAssignedContainer(container.getId())
  }

  override protected def allocateContainers(count: Int): YarnAllocateResponse = {
    addResourceRequests(count)

    // We have already set the container request. Poll the ResourceManager for a response.
    // This doubles as a heartbeat if there are no pending container requests.
    val progressIndicator = 0.1f
    new StableAllocateResponse(amClient.allocate(progressIndicator))
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
      if (numExecutors <= 0 || preferredHostToCount.isEmpty) {
        logDebug("numExecutors: " + numExecutors + ", host preferences: " +
          preferredHostToCount.isEmpty)
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
