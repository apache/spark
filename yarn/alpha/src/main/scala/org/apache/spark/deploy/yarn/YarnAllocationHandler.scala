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

import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConversions._
import scala.collection.mutable.{ArrayBuffer, HashMap}

import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.scheduler.SplitInfo

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.yarn.api.AMRMProtocol
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest
import org.apache.hadoop.yarn.util.Records

/**
 * Acquires resources for executors from a ResourceManager and launches executors in new containers.
 */
private[yarn] class YarnAllocationHandler(
    conf: Configuration,
    sparkConf: SparkConf,
    resourceManager: AMRMProtocol,
    appAttemptId: ApplicationAttemptId,
    args: ApplicationMasterArguments,
    preferredNodes: collection.Map[String, collection.Set[SplitInfo]],
    securityMgr: SecurityManager)
  extends YarnAllocator(conf, sparkConf, args, preferredNodes, securityMgr) {

  private val lastResponseId = new AtomicInteger()
  private val releaseList: CopyOnWriteArrayList[ContainerId] = new CopyOnWriteArrayList()

  override protected def allocateContainers(count: Int): YarnAllocateResponse = {
    var resourceRequests: List[ResourceRequest] = null

    // default.
    if (count <= 0 || preferredHostToCount.isEmpty) {
      logDebug("numExecutors: " + count + ", host preferences: " +
        preferredHostToCount.isEmpty)
      resourceRequests = List(createResourceRequest(
        AllocationType.ANY, null, count, YarnSparkHadoopUtil.RM_REQUEST_PRIORITY))
    } else {
      // request for all hosts in preferred nodes and for numExecutors -
      // candidates.size, request by default allocation policy.
      val hostContainerRequests: ArrayBuffer[ResourceRequest] =
        new ArrayBuffer[ResourceRequest](preferredHostToCount.size)
      for ((candidateHost, candidateCount) <- preferredHostToCount) {
        val requiredCount = candidateCount - allocatedContainersOnHost(candidateHost)

        if (requiredCount > 0) {
          hostContainerRequests += createResourceRequest(
            AllocationType.HOST,
            candidateHost,
            requiredCount,
            YarnSparkHadoopUtil.RM_REQUEST_PRIORITY)
        }
      }
      val rackContainerRequests: List[ResourceRequest] = createRackResourceRequests(
        hostContainerRequests.toList)

      val anyContainerRequests: ResourceRequest = createResourceRequest(
        AllocationType.ANY,
        resource = null,
        count,
        YarnSparkHadoopUtil.RM_REQUEST_PRIORITY)

      val containerRequests: ArrayBuffer[ResourceRequest] = new ArrayBuffer[ResourceRequest](
        hostContainerRequests.size + rackContainerRequests.size + 1)

      containerRequests ++= hostContainerRequests
      containerRequests ++= rackContainerRequests
      containerRequests += anyContainerRequests

      resourceRequests = containerRequests.toList
    }

    val req = Records.newRecord(classOf[AllocateRequest])
    req.setResponseId(lastResponseId.incrementAndGet)
    req.setApplicationAttemptId(appAttemptId)

    req.addAllAsks(resourceRequests)

    val releasedContainerList = createReleasedContainerList()
    req.addAllReleases(releasedContainerList)

    if (count > 0) {
      logInfo("Allocating %d executor containers with %d of memory each.".format(count,
        executorMemory + memoryOverhead))
    } else {
      logDebug("Empty allocation req ..  release : " + releasedContainerList)
    }

    for (request <- resourceRequests) {
      logInfo("ResourceRequest (host : %s, num containers: %d, priority = %s , capability : %s)".
        format(
          request.getHostName,
          request.getNumContainers,
          request.getPriority,
          request.getCapability))
    }
    new AlphaAllocateResponse(resourceManager.allocate(req).getAMResponse())
  }

  override protected def releaseContainer(container: Container) = {
    releaseList.add(container.getId())
  }

  private def createRackResourceRequests(hostContainers: List[ResourceRequest]):
    List[ResourceRequest] = {
    // First generate modified racks and new set of hosts under it : then issue requests
    val rackToCounts = new HashMap[String, Int]()

    // Within this lock - used to read/write to the rack related maps too.
    for (container <- hostContainers) {
      val candidateHost = container.getHostName
      val candidateNumContainers = container.getNumContainers
      assert(YarnSparkHadoopUtil.ANY_HOST != candidateHost)

      val rack = YarnSparkHadoopUtil.lookupRack(conf, candidateHost)
      if (rack != null) {
        var count = rackToCounts.getOrElse(rack, 0)
        count += candidateNumContainers
        rackToCounts.put(rack, count)
      }
    }

    val requestedContainers: ArrayBuffer[ResourceRequest] =
      new ArrayBuffer[ResourceRequest](rackToCounts.size)
    for ((rack, count) <- rackToCounts){
      requestedContainers +=
        createResourceRequest(AllocationType.RACK, rack, count,
          YarnSparkHadoopUtil.RM_REQUEST_PRIORITY)
    }

    requestedContainers.toList
  }

  private def createResourceRequest(
    requestType: AllocationType.AllocationType,
    resource:String,
    numExecutors: Int,
    priority: Int): ResourceRequest = {

    // If hostname specified, we need atleast two requests - node local and rack local.
    // There must be a third request - which is ANY : that will be specially handled.
    requestType match {
      case AllocationType.HOST => {
        assert(YarnSparkHadoopUtil.ANY_HOST != resource)
        val hostname = resource
        val nodeLocal = createResourceRequestImpl(hostname, numExecutors, priority)

        // Add to host->rack mapping
        YarnSparkHadoopUtil.populateRackInfo(conf, hostname)

        nodeLocal
      }
      case AllocationType.RACK => {
        val rack = resource
        createResourceRequestImpl(rack, numExecutors, priority)
      }
      case AllocationType.ANY => createResourceRequestImpl(
        YarnSparkHadoopUtil.ANY_HOST, numExecutors, priority)
      case _ => throw new IllegalArgumentException(
        "Unexpected/unsupported request type: " + requestType)
    }
  }

  private def createResourceRequestImpl(
    hostname:String,
    numExecutors: Int,
    priority: Int): ResourceRequest = {

    val rsrcRequest = Records.newRecord(classOf[ResourceRequest])
    val memCapability = Records.newRecord(classOf[Resource])
    // There probably is some overhead here, let's reserve a bit more memory.
    memCapability.setMemory(executorMemory + memoryOverhead)
    rsrcRequest.setCapability(memCapability)

    val pri = Records.newRecord(classOf[Priority])
    pri.setPriority(priority)
    rsrcRequest.setPriority(pri)

    rsrcRequest.setHostName(hostname)

    rsrcRequest.setNumContainers(java.lang.Math.max(numExecutors, 0))
    rsrcRequest
  }

  private def createReleasedContainerList(): ArrayBuffer[ContainerId] = {
    val retval = new ArrayBuffer[ContainerId](1)
    // Iterator on COW list ...
    for (container <- releaseList.iterator()){
      retval += container
    }
    // Remove from the original list.
    if (!retval.isEmpty) {
      releaseList.removeAll(retval)
      logInfo("Releasing " + retval.size + " containers.")
    }
    retval
  }

  private class AlphaAllocateResponse(response: AMResponse) extends YarnAllocateResponse {
    override def getAllocatedContainers() = response.getAllocatedContainers()
    override def getAvailableResources() = response.getAvailableResources()
    override def getCompletedContainersStatuses() = response.getCompletedContainersStatuses()
  }

}
