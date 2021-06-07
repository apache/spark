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

import java.util.{List => JList, Map => JMap, Set => JSet}
import java.util.concurrent.atomic.AtomicLong

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.api.resource.PlacementConstraints._
import org.apache.hadoop.yarn.client.api.AMRMClient
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest
import org.apache.hadoop.yarn.client.api.impl.AMRMClientImpl
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.util.constraint.PlacementConstraintParser

import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.deploy.yarn.config._
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.util.{Clock, SystemClock}


/**
 * YarnSchedulingRequestAllocator is charged with requesting containers from the
 * YARN ResourceManager using Placement Constraint and deciding what to do with
 * containers when YARN fulfills these requests.
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
private[yarn] class YarnSchedulingRequestAllocator(
    driverUrl: String,
    driverRef: RpcEndpointRef,
    conf: YarnConfiguration,
    sparkConf: SparkConf,
    amClient: AMRMClient[ContainerRequest],
    appAttemptId: ApplicationAttemptId,
    securityMgr: SecurityManager,
    localResources: Map[String, LocalResource],
    resolver: SparkRackResolver,
    clock: Clock = new SystemClock)
  extends YarnAllocator(driverUrl, driverRef, conf, sparkConf, amClient,
    appAttemptId, securityMgr, localResources, resolver, clock) with Logging {

  private[yarn] lazy val nextAllocationRequestId = new AtomicLong(0)
  private[yarn] val allocationRequestIdToNodes = new mutable.HashMap[Long, Array[String]]
  private[yarn] val allocationRequestIdToRacks = new mutable.HashMap[Long, Array[String]]
  // A container placement strategy based on pending tasks' locality preference
  private[yarn] val schedulingRequestContainerPlacementStrategy =
    new LocalityPreferredSchedulingRequestContainerPlacementStrategy(sparkConf, conf, resolver)
  private[yarn] val nodeAttributes = sparkConf.get(NODE_ATTRIBUTE)
    .map(PlacementConstraintParser.parseExpression)

  private[yarn] val outstandingSchedRequests: JMap[JSet[String], JList[SchedulingRequest]] = {
    val field = classOf[AMRMClientImpl[ContainerRequest]]
      .getDeclaredField("outstandingSchedRequests")
    field.setAccessible(true)
    val outstandingSchedRequests =
      field.get(amClient.asInstanceOf[AMRMClientImpl[ContainerRequest]])
        .asInstanceOf[JMap[JSet[String], JList[SchedulingRequest]]]
    // For pass UT, since mock AMRMClientImpl use reflection will got null.
    Option(outstandingSchedRequests)
      .getOrElse(Map.empty[JSet[String], JList[SchedulingRequest]].asJava)
  }

  /**
   * A sequence of pending container requests that have not yet been fulfilled.
   * ResourceProfile id -> pendingAllocate container request
   */
  def getPendingSchedAllocate: Map[Int, Seq[SchedulingRequest]] = {
    outstandingSchedRequests.asScala.values.flatMap(_.asScala.map { request =>
      request.getPriority.getPriority -> request
    }).groupBy(_._1).mapValues(_.map(_._2).toSet.toSeq)
  }

  override def getNumContainersPendingAllocate: Int = synchronized {
    getPendingSchedAllocate.values.flatten.size
  }

  /**
   * Update the set of SchedulingRequest that we will sync with the RM based on the number of
   * executors we have currently running and our target number of executors for each
   * ResourceProfile.
   *
   * Visible for testing.
   */
  override def updateResourceRequests(): Unit = synchronized {
    val pendingAllocatePerResourceProfileId = getPendingSchedAllocate

    // Here we lack pending size for each profile
    val missingPerProfile = targetNumExecutorsPerResourceProfileId.map { case (rpId, targetNum) =>
      val starting = getOrUpdateNumExecutorsStartingForRPId(rpId).get
      val pending = pendingAllocatePerResourceProfileId.getOrElse(rpId, Seq.empty).size
      val running = getOrUpdateRunningExecutorForRPId(rpId).size
      logDebug(s"Updating resource requests for ResourceProfile id: $rpId, target: " +
        s"$targetNum, pending: $pending, running: $running, executorsStarting: $starting")
      (rpId, targetNum - pending - running - starting)
    }.toMap

    missingPerProfile.foreach { case (rpId, missing) =>
      val hostToLocalTaskCount =
        hostToLocalTaskCountPerResourceProfileId.getOrElse(rpId, Map.empty)
      val pendingAllocate = pendingAllocatePerResourceProfileId.getOrElse(rpId, Seq.empty)
      val numPendingAllocate = pendingAllocate.size
      // Split the pending container request into three groups: locality matched list, locality
      // unmatched list and non-locality list. Take the locality matched container request into
      // consideration of container placement, treat as allocated containers.
      // For locality unmatched and locality free container requests, cancel these container
      // requests, since required locality preference has been changed, recalculating using
      // container placement strategy.
      val (localRequests, staleRequests, anyHostRequests) =
      splitPendingSchedulingRequestAllocationsByLocality(hostToLocalTaskCount, pendingAllocate)

      if (missing > 0) {
        val resource = rpIdToYarnResource.get(rpId)
        if (log.isInfoEnabled()) {
          var requestContainerMessage = s"Will request $missing executor container(s) for " +
            s" ResourceProfile Id: $rpId, each with " +
            s"${resource.getVirtualCores} core(s) and " +
            s"${resource.getMemory} MB memory."
          if (ResourceRequestHelper.isYarnResourceTypesAvailable() &&
            ResourceRequestHelper.isYarnCustomResourcesNonEmpty(resource)) {
            requestContainerMessage ++= s" with custom resources: " + resource.toString
          }
          logInfo(requestContainerMessage)
        }

        // cancel "stale" requests for locations that are no longer needed
        staleRequests.foreach { stale =>
          removeFromOutstandingSchedulingRequests(stale)
        }
        val cancelledContainers = staleRequests.size
        if (cancelledContainers > 0) {
          logInfo(s"Canceled $cancelledContainers container request(s) (locality no longer needed)")
        }

        // consider the number of new containers and cancelled stale containers available
        val availableContainers = missing + cancelledContainers

        // to maximize locality, include requests with no locality preference
        // that can be cancelled
        val potentialContainers = availableContainers + anyHostRequests.size

        val allocatedHostToContainer = getOrUpdateAllocatedHostToContainersMapForRPId(rpId)
        val numLocalityAwareTasks = numLocalityAwareTasksPerResourceProfileId.getOrElse(rpId, 0)
        val containerLocalityPreferences =
          schedulingRequestContainerPlacementStrategy.localityOfRequestedContainers(
            potentialContainers, numLocalityAwareTasks, hostToLocalTaskCount,
            allocatedHostToContainer, localRequests, allocationRequestIdToNodes,
            rpIdToResourceProfile(rpId))

        val newLocalityRequests = new mutable.ArrayBuffer[SchedulingRequest]
        containerLocalityPreferences.foreach {
          case ContainerLocalityPreferences(nodes, racks) if nodes != null =>
            newLocalityRequests += createSchedulingRequest(resource, nodes, racks, rpId)
          case _ =>
        }

        if (availableContainers >= newLocalityRequests.size) {
          // more containers are available than needed for locality, fill in requests for any host
          for (i <- 0 until (availableContainers - newLocalityRequests.size)) {
            newLocalityRequests += createSchedulingRequest(resource, rpId = rpId)
          }
        } else {
          val numToCancel = newLocalityRequests.size - availableContainers
          // cancel some requests without locality preferences to schedule more local containers
          anyHostRequests.slice(0, numToCancel).foreach { nonLocal =>
            removeFromOutstandingSchedulingRequests(nonLocal)
          }
          if (numToCancel > 0) {
            logInfo(s"Canceled $numToCancel unlocalized container requests to " +
              s"resubmit with locality")
          }
        }
        amClient.addSchedulingRequests(newLocalityRequests.asJava)
      } else if (numPendingAllocate > 0 && missing < 0) {
        val numToCancel = math.min(numPendingAllocate, -missing)
        logInfo(s"Canceling requests for $numToCancel executor container(s) to have a new " +
          s"desired total ${getOrUpdateTargetNumExecutorsForRPId(rpId)} executors.")
        // cancel pending allocate requests by taking locality preference into account
        val cancelRequests = (staleRequests ++ anyHostRequests ++ localRequests).take(numToCancel)
        cancelRequests.foreach(removeFromOutstandingSchedulingRequests)
      }
    }
  }

  def createSchedulingRequest(
      resource: Resource,
      nodes: Array[String] = Array.empty,
      racks: Array[String] = Array.empty,
      rpId: Int): SchedulingRequest = {
    val allocationRequestId = nextAllocationRequestId.getAndIncrement()
    val nodesLocality = if (nodes.nonEmpty) {
      allocationRequestIdToNodes.put(allocationRequestId, nodes)
      Some(targetIn(NODE, PlacementTargets.nodeAttribute("host", nodes: _*)))
    } else {
      None
    }
    val racksLocality = if (racks.nonEmpty) {
      allocationRequestIdToRacks.put(allocationRequestId, racks)
      Some(targetIn(RACK, PlacementTargets.nodeAttribute("rack", racks: _*)))
    } else {
      None
    }

    val locality = (nodesLocality, racksLocality) match {
      case (Some(nodes), Some(racks)) => Some(and(nodes, racks))
      case (Some(nodes), None) => Some(nodes)
      case (None, Some(racks)) => Some(racks)
      case _ => None
    }

    val expression = (nodeAttributes, locality) match {
      case (Some(attributes), Some(locality)) => Some(and(attributes, locality))
      case (Some(attribute), None) => Some(attribute)
      case (None, Some(locality)) => Some(locality)
      case _ => None
    }

    val schedulingRequestBuilder = SchedulingRequest.newBuilder()
      .executionType(ExecutionTypeRequest.newInstance)
      .allocationRequestId(allocationRequestId)
      .priority(getContainerPriority(rpId))
      .allocationTags(Set("SPARK").asJava)
      .resourceSizing(ResourceSizing.newInstance(resource))

    expression.foreach { placementConstraint =>
      schedulingRequestBuilder.placementConstraintExpression(placementConstraint.build())
    }

    schedulingRequestBuilder.build()
  }

  /**
   * Looks for locality message of each allocation request id. If matches, means YARN have
   * fullfilled this AllocationRequestId's request, remove it from allocationRequestIdToNodes and
   * allocationRequestIdToRacks. Places the matched container into
   * containersToUse or remaining.
   *
   * @param allocatedContainer container that was given to us by YARN
   * @param location resource name, either a node, rack, or *
   * @param containersToUse list of containers that will be used
   * @param remaining list of containers that will not be used
   */
  override def matchContainerToRequest(
      allocatedContainer: Container,
      location: String,
      containersToUse: ArrayBuffer[Container],
      remaining: ArrayBuffer[Container]): Unit = {
    // Match on the exact resource we requested so there shouldn't be a mismatch,
    // we are relying on YARN to return a container with resources no less then we requested.
    // If we change this, or starting validating the container, be sure the logic covers SPARK-6050.
    val rpId = getResourceProfileIdFromPriority(allocatedContainer.getPriority)
    val resourceForRP = rpIdToYarnResource.get(rpId)

    logDebug(s"Calling amClient.getMatchingRequests with parameters: " +
      s"priority: ${allocatedContainer.getPriority}, " +
      s"location: $location, resource: $resourceForRP")

    if (matchContainerToRequestLocality(allocatedContainer.getAllocationRequestId, location)) {
      // Add this method for test, since outstanding request has been removed
      // when container allocated to AMRMClient.
      removeFromOutstandingSchedulingRequests(allocatedContainer)
      containersToUse += allocatedContainer
    } else {
      remaining += allocatedContainer
    }
  }

  def matchContainerToRequestLocality(requestId: Long, location: String): Boolean = {
    if (allocationRequestIdToNodes.contains(requestId) ||
      allocationRequestIdToRacks.contains(requestId)) {
      val requestNodes = allocationRequestIdToNodes.getOrElse(requestId, Array.empty)
      val requestRacks = allocationRequestIdToRacks.getOrElse(requestId, Array.empty)
      if (requestNodes.contains(location) || requestRacks.contains(location)) {
        allocationRequestIdToNodes.remove(requestId)
        allocationRequestIdToRacks.remove(requestId)
        true
      } else {
        false
      }
    } else {
      true
    }
  }

  private def splitPendingSchedulingRequestAllocationsByLocality(
      hostToLocalTaskCount: Map[String, Int],
      pendingAllocations: Seq[SchedulingRequest]
  ): (Seq[SchedulingRequest], Seq[SchedulingRequest], Seq[SchedulingRequest]) = {
    val localityMatched = ArrayBuffer[SchedulingRequest]()
    val localityUnMatched = ArrayBuffer[SchedulingRequest]()
    val localityFree = ArrayBuffer[SchedulingRequest]()

    val preferredHosts = hostToLocalTaskCount.keySet
    pendingAllocations.foreach { cr =>
      val nodes = allocationRequestIdToNodes.getOrElse(cr.getAllocationRequestId, Array.empty)
      if (nodes.isEmpty) {
        localityFree += cr
      } else if (nodes.toSet.intersect(preferredHosts).nonEmpty) {
        localityMatched += cr
      } else {
        localityUnMatched += cr
      }
    }

    (localityMatched.toSeq, localityUnMatched.toSeq, localityFree.toSeq)
  }

  def removeFromOutstandingSchedulingRequests(request: SchedulingRequest): Unit = {
    if (request != null) {
      if (request.getAllocationTags != null && !request.getAllocationTags.isEmpty) {
        val schedReqs: JList[SchedulingRequest] =
          outstandingSchedRequests.get(request.getAllocationTags)
        if (schedReqs != null && !schedReqs.isEmpty) {
          val iter: java.util.Iterator[SchedulingRequest] = schedReqs.iterator
          while (iter.hasNext) {
            val schedReq: SchedulingRequest = iter.next
            if (schedReq.getPriority == request.getPriority &&
              schedReq.getAllocationRequestId == request.getAllocationRequestId) {
              var numAllocations: Int = schedReq.getResourceSizing.getNumAllocations
              numAllocations -= 1
              if (numAllocations == 0) {
                iter.remove()
              } else {
                schedReq.getResourceSizing.setNumAllocations(numAllocations)
              }
            }
          }
        }
      }
    }
  }

  def removeFromOutstandingSchedulingRequests(request: Container): Unit = {
    if (request != null) {
      if (request.getAllocationTags != null && !request.getAllocationTags.isEmpty) {
        val schedReqs: JList[SchedulingRequest] =
          outstandingSchedRequests.get(request.getAllocationTags)
        if (schedReqs != null && !schedReqs.isEmpty) {
          val iter: java.util.Iterator[SchedulingRequest] = schedReqs.iterator
          while (iter.hasNext) {
            val schedReq: SchedulingRequest = iter.next
            if (schedReq.getPriority == request.getPriority &&
              schedReq.getAllocationRequestId == request.getAllocationRequestId) {
              var numAllocations: Int = schedReq.getResourceSizing.getNumAllocations
              numAllocations -= 1
              if (numAllocations == 0) {
                iter.remove()
              } else {
                schedReq.getResourceSizing.setNumAllocations(numAllocations)
              }
            }
          }
        }
      }
    }
  }
}


