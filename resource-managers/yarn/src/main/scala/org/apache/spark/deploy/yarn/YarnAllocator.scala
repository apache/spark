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

import java.util.LinkedHashMap
import java.util.Map.Entry
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import javax.annotation.concurrent.GuardedBy

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.client.api.AMRMClient
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest
import org.apache.hadoop.yarn.conf.YarnConfiguration

import org.apache.spark.{SecurityManager, SparkConf, SparkException}
import org.apache.spark.deploy.ExecutorFailureTracker
import org.apache.spark.deploy.yarn.ResourceRequestHelper._
import org.apache.spark.deploy.yarn.YarnSparkHadoopUtil._
import org.apache.spark.deploy.yarn.config._
import org.apache.spark.executor.ExecutorExitCode
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.resource.ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID
import org.apache.spark.rpc.{RpcCallContext, RpcEndpointRef}
import org.apache.spark.scheduler.{ExecutorExited, ExecutorLossReason}
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.{DecommissionExecutorsOnHost, RemoveExecutor, RetrieveLastAllocatedExecutorId}
import org.apache.spark.scheduler.cluster.SchedulerBackendUtils
import org.apache.spark.util.{Clock, SystemClock, ThreadUtils}

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
    conf: YarnConfiguration,
    sparkConf: SparkConf,
    amClient: AMRMClient[ContainerRequest],
    appAttemptId: ApplicationAttemptId,
    securityMgr: SecurityManager,
    localResources: Map[String, LocalResource],
    resolver: SparkRackResolver,
    clock: Clock = new SystemClock)
  extends Logging {

  import YarnAllocator._

  // Visible for testing.
  @GuardedBy("this")
  val allocatedHostToContainersMapPerRPId =
    new HashMap[Int, HashMap[String, collection.mutable.Set[ContainerId]]]

  @GuardedBy("this")
  val allocatedContainerToHostMap = new HashMap[ContainerId, String]

  // Containers that we no longer care about. We've either already told the RM to release them or
  // will on the next heartbeat. Containers get removed from this map after the RM tells us they've
  // completed.
  @GuardedBy("this")
  private val releasedContainers = collection.mutable.HashSet[ContainerId]()

  @GuardedBy("this")
  private val launchingExecutorContainerIds = collection.mutable.HashSet[ContainerId]()

  @GuardedBy("this")
  private val runningExecutorsPerResourceProfileId = new HashMap[Int, mutable.Set[String]]()

  @GuardedBy("this")
  private val numExecutorsStartingPerResourceProfileId = new HashMap[Int, AtomicInteger]

  @GuardedBy("this")
  private val targetNumExecutorsPerResourceProfileId = new mutable.HashMap[Int, Int]

  // Executor loss reason requests that are pending - maps from executor ID for inquiry to a
  // list of requesters that should be responded to once we find out why the given executor
  // was lost.
  @GuardedBy("this")
  private val pendingLossReasonRequests = new HashMap[String, mutable.Buffer[RpcCallContext]]

  // Maintain loss reasons for already released executors, it will be added when executor loss
  // reason is got from AM-RM call, and be removed after querying this loss reason.
  @GuardedBy("this")
  private val releasedExecutorLossReasons = new HashMap[String, ExecutorLossReason]

  // Keep track of which container is running which executor to remove the executors later
  // Visible for testing.
  @GuardedBy("this")
  private[yarn] val executorIdToContainer = new HashMap[String, Container]

  @GuardedBy("this")
  private var numUnexpectedContainerRelease = 0L

  @GuardedBy("this")
  private val containerIdToExecutorIdAndResourceProfileId = new HashMap[ContainerId, (String, Int)]

  // Use a ConcurrentHashMap because this is used in matchContainerToRequest, which is called
  // from the rack resolver thread where synchronize(this) on this would cause a deadlock.
  @GuardedBy("ConcurrentHashMap")
  private[yarn] val rpIdToYarnResource = new ConcurrentHashMap[Int, Resource]()

  // note currently we don't remove ResourceProfiles
  @GuardedBy("this")
  private[yarn] val rpIdToResourceProfile = new mutable.HashMap[Int, ResourceProfile]

  // A map of ResourceProfile id to a map of preferred hostname and possible
  // task numbers running on it.
  @GuardedBy("this")
  private var hostToLocalTaskCountPerResourceProfileId: Map[Int, Map[String, Int]] =
    Map(DEFAULT_RESOURCE_PROFILE_ID -> Map.empty)

  // ResourceProfile Id to number of tasks that have locality preferences in active stages
  @GuardedBy("this")
  private[yarn] var numLocalityAwareTasksPerResourceProfileId: Map[Int, Int] =
    Map(DEFAULT_RESOURCE_PROFILE_ID -> 0)

  /**
   * Used to generate a unique ID per executor
   *
   * Init `executorIdCounter`. when AM restart, `executorIdCounter` will reset to 0. Then
   * the id of new executor will start from 1, this will conflict with the executor has
   * already created before. So, we should initialize the `executorIdCounter` by getting
   * the max executorId from driver.
   *
   * And this situation of executorId conflict is just in yarn client mode, so this is an issue
   * in yarn client mode. For more details, can check in jira.
   *
   * @see SPARK-12864
   */
  @GuardedBy("this")
  private var executorIdCounter: Int =
    driverRef.askSync[Int](RetrieveLastAllocatedExecutorId)

  private[spark] val failureTracker = new ExecutorFailureTracker(sparkConf, clock)

  private val allocatorNodeHealthTracker =
    new YarnAllocatorNodeHealthTracker(sparkConf, amClient, failureTracker)

  private val isPythonApp = sparkConf.get(IS_PYTHON_APP)

  private val minMemoryOverhead = sparkConf.get(EXECUTOR_MIN_MEMORY_OVERHEAD)

  private val memoryOverheadFactor = sparkConf.get(EXECUTOR_MEMORY_OVERHEAD_FACTOR)

  private val launcherPool = ThreadUtils.newDaemonCachedThreadPool(
    "ContainerLauncher", sparkConf.get(CONTAINER_LAUNCH_MAX_THREADS))

  // For testing
  private val launchContainers = sparkConf.getBoolean("spark.yarn.launchContainers", true)

  private val labelExpression = sparkConf.get(EXECUTOR_NODE_LABEL_EXPRESSION)

  private val resourceNameMapping = ResourceRequestHelper.getResourceNameMapping(sparkConf)

  // A container placement strategy based on pending tasks' locality preference
  private[yarn] val containerPlacementStrategy =
    new LocalityPreferredContainerPlacementStrategy(sparkConf, conf, resolver)

  private val isYarnExecutorDecommissionEnabled: Boolean = {
    (sparkConf.get(DECOMMISSION_ENABLED),
      sparkConf.get(SHUFFLE_SERVICE_ENABLED)) match {
      case (true, false) => true
      case (true, true) =>
        logWarning(s"Yarn Executor Decommissioning is supported only " +
          s"when ${SHUFFLE_SERVICE_ENABLED.key} is set to false. See: SPARK-39018.")
        false
      case (false, _) => false
    }
  }

  private val decommissioningNodesCache = new LinkedHashMap[String, Boolean]() {
    override def removeEldestEntry(entry: Entry[String, Boolean]): Boolean = {
      size() > DECOMMISSIONING_NODES_CACHE_SIZE
    }
  }

  @volatile private var shutdown = false

  // The default profile is always present so we need to initialize the datastructures keyed by
  // ResourceProfile id to ensure its present if things start running before a request for
  // executors could add it. This approach is easier than going and special casing everywhere.
  private def initDefaultProfile(): Unit = synchronized {
    allocatedHostToContainersMapPerRPId(DEFAULT_RESOURCE_PROFILE_ID) =
      new HashMap[String, mutable.Set[ContainerId]]()
    runningExecutorsPerResourceProfileId.put(DEFAULT_RESOURCE_PROFILE_ID, mutable.HashSet[String]())
    numExecutorsStartingPerResourceProfileId(DEFAULT_RESOURCE_PROFILE_ID) = new AtomicInteger(0)
    val initTargetExecNum = SchedulerBackendUtils.getInitialTargetExecutorNumber(sparkConf)
    targetNumExecutorsPerResourceProfileId(DEFAULT_RESOURCE_PROFILE_ID) = initTargetExecNum
    val defaultProfile = ResourceProfile.getOrCreateDefaultProfile(sparkConf)
    createYarnResourceForResourceProfile(defaultProfile)
  }

  initDefaultProfile()

  def setShutdown(shutdown: Boolean): Unit = this.shutdown = shutdown

  def getNumExecutorsRunning: Int = synchronized {
    runningExecutorsPerResourceProfileId.values.map(_.size).sum
  }

  def getNumLocalityAwareTasks: Int = synchronized {
    numLocalityAwareTasksPerResourceProfileId.values.sum
  }

  def getNumExecutorsStarting: Int = synchronized {
    numExecutorsStartingPerResourceProfileId.values.map(_.get()).sum
  }

  def getNumReleasedContainers: Int = synchronized {
    releasedContainers.size
  }

  def getNumExecutorsFailed: Int = failureTracker.numFailedExecutors

  def isAllNodeExcluded: Boolean = allocatorNodeHealthTracker.isAllNodeExcluded

  /**
   * A sequence of pending container requests that have not yet been fulfilled.
   * ResourceProfile id -> pendingAllocate container request
   */
  def getPendingAllocate: Map[Int, Seq[ContainerRequest]] = getPendingAtLocation(ANY_HOST)

  def getNumContainersPendingAllocate: Int = synchronized {
    getPendingAllocate.values.flatten.size
  }

  // YARN priorities are such that lower number is higher priority.
  // We need to allocate a different priority for each ResourceProfile because YARN
  // won't allow different container resource requirements within a Priority.
  // We could allocate per Stage to make sure earlier stages get priority but Spark
  // always finishes a stage before starting a later one and if we have 2 running in parallel
  // the priority doesn't matter.
  // We are using the ResourceProfile id as the priority.
  private def getContainerPriority(rpId: Int): Priority = {
    Priority.newInstance(rpId)
  }

  // The ResourceProfile id is the priority
  private def getResourceProfileIdFromPriority(priority: Priority): Int = {
    priority.getPriority()
  }

  private def getOrUpdateAllocatedHostToContainersMapForRPId(
      rpId: Int): HashMap[String, collection.mutable.Set[ContainerId]] = synchronized {
    allocatedHostToContainersMapPerRPId.getOrElseUpdate(rpId,
      new HashMap[String, mutable.Set[ContainerId]]())
  }

  private def getOrUpdateRunningExecutorForRPId(rpId: Int): mutable.Set[String] = synchronized {
    runningExecutorsPerResourceProfileId.getOrElseUpdate(rpId, mutable.HashSet[String]())
  }

  private def getOrUpdateNumExecutorsStartingForRPId(rpId: Int): AtomicInteger = synchronized {
    numExecutorsStartingPerResourceProfileId.getOrElseUpdate(rpId, new AtomicInteger(0))
  }

  private def getOrUpdateTargetNumExecutorsForRPId(rpId: Int): Int = synchronized {
    targetNumExecutorsPerResourceProfileId.getOrElseUpdate(rpId,
      SchedulerBackendUtils.getInitialTargetExecutorNumber(sparkConf))
  }

  /**
   * A sequence of pending container requests at the given location for each ResourceProfile id
   * that have not yet been fulfilled.
   */
  private def getPendingAtLocation(
      location: String): Map[Int, Seq[ContainerRequest]] = synchronized {
    val allContainerRequests = new mutable.HashMap[Int, Seq[ContainerRequest]]
    rpIdToResourceProfile.keys.foreach { id =>
      val profResource = rpIdToYarnResource.get(id)
      val result = amClient.getMatchingRequests(getContainerPriority(id), location, profResource)
        .asScala.flatMap(_.asScala)
      allContainerRequests(id) = result.toSeq
    }
    allContainerRequests.toMap
  }

  // if a ResourceProfile hasn't been seen yet, create the corresponding YARN Resource for it
  private def createYarnResourceForResourceProfile(rp: ResourceProfile): Unit = synchronized {
    if (!rpIdToYarnResource.containsKey(rp.id)) {
      // track the resource profile if not already there
      getOrUpdateRunningExecutorForRPId(rp.id)
      logInfo(s"Resource profile ${rp.id} doesn't exist, adding it")

      val resourcesWithDefaults =
        ResourceProfile.getResourcesForClusterManager(rp.id, rp.executorResources,
          minMemoryOverhead, memoryOverheadFactor, sparkConf, isPythonApp, resourceNameMapping)
      val customSparkResources =
        resourcesWithDefaults.customResources.map { case (name, execReq) =>
          (name, execReq.amount.toString)
        }
      // There is a difference in the way custom resources are handled between
      // the base default profile and custom ResourceProfiles. To allow for the user
      // to request YARN containers with extra resources without Spark scheduling on
      // them, the user can specify resources via the <code>spark.yarn.executor.resource.</code>
      // config. Those configs are only used in the base default profile though and do
      // not get propogated into any other custom ResourceProfiles. This is because
      // there would be no way to remove them if you wanted a stage to not have them.
      // This results in your default profile getting custom resources defined in
      // <code>spark.yarn.executor.resource.</code> plus spark defined resources of
      // GPU or FPGA. Spark converts GPU and FPGA resources into the YARN built in
      // types <code>yarn.io/gpu</code>) and <code>yarn.io/fpga</code>, but does not
      // know the mapping of any other resources. Any other Spark custom resources
      // are not propogated to YARN for the default profile. So if you want Spark
      // to schedule based off a custom resource and have it requested from YARN, you
      // must specify it in both YARN (<code>spark.yarn.{driver/executor}.resource.</code>)
      // and Spark (<code>spark.{driver/executor}.resource.</code>) configs. Leave the Spark
      // config off if you only want YARN containers with the extra resources but Spark not to
      // schedule using them. Now for custom ResourceProfiles, it doesn't currently have a way
      // to only specify YARN resources without Spark scheduling off of them. This means for
      // custom ResourceProfiles we propogate all the resources defined in the ResourceProfile
      // to YARN. We still convert GPU and FPGA to the YARN build in types as well. This requires
      // that the name of any custom resources you specify match what they are defined as in YARN.
      val customResources = if (rp.id == DEFAULT_RESOURCE_PROFILE_ID) {
        val gpuResource = sparkConf.get(YARN_GPU_DEVICE)
        val fpgaResource = sparkConf.get(YARN_FPGA_DEVICE)
        getYarnResourcesAndAmounts(sparkConf, config.YARN_EXECUTOR_RESOURCE_TYPES_PREFIX) ++
          customSparkResources.filter { case (r, _) =>
            (r == gpuResource || r == fpgaResource)
          }
      } else {
        customSparkResources
      }

      assert(resourcesWithDefaults.cores.nonEmpty)
      val resource = Resource.newInstance(
        resourcesWithDefaults.totalMemMiB.toInt, resourcesWithDefaults.cores.get)
      ResourceRequestHelper.setResourceRequests(customResources, resource)
      logDebug(s"Created resource capability: $resource")
      rpIdToYarnResource.putIfAbsent(rp.id, resource)
      rpIdToResourceProfile(rp.id) = rp
    }
  }

  /**
   * Request as many executors from the ResourceManager as needed to reach the desired total. If
   * the requested total is smaller than the current number of running executors, no executors will
   * be killed.
   * @param resourceProfileToTotalExecs total number of containers requested for each
   *                                    ResourceProfile
   * @param numLocalityAwareTasksPerResourceProfileId number of locality aware tasks for each
   *                                                  ResourceProfile id to be used as container
   *                                                  placement hint.
   * @param hostToLocalTaskCount a map of preferred hostname to possible task counts for each
   *                             ResourceProfile id to be used as container placement hint.
   * @param excludedNodes excluded nodes, which is passed in to avoid allocating new containers
   *                      on them. It will be used to update the applications excluded node list.
   * @return Whether the new requested total is different than the old value.
   */
  def requestTotalExecutorsWithPreferredLocalities(
      resourceProfileToTotalExecs: Map[ResourceProfile, Int],
      numLocalityAwareTasksPerResourceProfileId: Map[Int, Int],
      hostToLocalTaskCountPerResourceProfileId: Map[Int, Map[String, Int]],
      excludedNodes: Set[String]): Boolean = synchronized {
    this.numLocalityAwareTasksPerResourceProfileId = numLocalityAwareTasksPerResourceProfileId
    this.hostToLocalTaskCountPerResourceProfileId = hostToLocalTaskCountPerResourceProfileId

    if (resourceProfileToTotalExecs.isEmpty) {
      // Set target executor number to 0 to cancel pending allocate request.
      targetNumExecutorsPerResourceProfileId.keys.foreach { rp =>
        targetNumExecutorsPerResourceProfileId(rp) = 0
      }
      allocatorNodeHealthTracker.setSchedulerExcludedNodes(excludedNodes)
      true
    } else {
      val res = resourceProfileToTotalExecs.map { case (rp, numExecs) =>
        createYarnResourceForResourceProfile(rp)
        if (numExecs != getOrUpdateTargetNumExecutorsForRPId(rp.id)) {
          logInfo(s"Driver requested a total number of $numExecs executor(s) " +
            s"for resource profile id: ${rp.id}.")
          targetNumExecutorsPerResourceProfileId(rp.id) = numExecs
          allocatorNodeHealthTracker.setSchedulerExcludedNodes(excludedNodes)
          true
        } else {
          false
        }
      }
      res.exists(_ == true)
    }
  }

  /**
   * Request that the ResourceManager release the container running the specified executor.
   */
  def killExecutor(executorId: String): Unit = synchronized {
    executorIdToContainer.get(executorId) match {
      case Some(container) if !releasedContainers.contains(container.getId) =>
        val (_, rpId) = containerIdToExecutorIdAndResourceProfileId(container.getId)
        internalReleaseContainer(container)
        getOrUpdateRunningExecutorForRPId(rpId).remove(executorId)
      case _ => logWarning(s"Attempted to kill unknown executor $executorId!")
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
    allocatorNodeHealthTracker.setNumClusterNodes(allocateResponse.getNumClusterNodes)

    if (isYarnExecutorDecommissionEnabled) {
      handleNodesInDecommissioningState(allocateResponse)
    }

    if (allocatedContainers.size > 0) {
      logDebug(("Allocated containers: %d. Current executor count: %d. " +
        "Launching executor count: %d. Cluster resources: %s.")
        .format(
          allocatedContainers.size,
          getNumExecutorsRunning,
          getNumExecutorsStarting,
          allocateResponse.getAvailableResources))

      handleAllocatedContainers(allocatedContainers.asScala.toSeq)
    }

    val completedContainers = allocateResponse.getCompletedContainersStatuses()
    if (completedContainers.size > 0) {
      logDebug("Completed %d containers".format(completedContainers.size))
      processCompletedContainers(completedContainers.asScala.toSeq)
      logDebug("Finished processing %d completed containers. Current running executor count: %d."
        .format(completedContainers.size, getNumExecutorsRunning))
    }
  }

  private def handleNodesInDecommissioningState(allocateResponse: AllocateResponse): Unit = {
    // Some of the nodes are put in decommissioning state where RM did allocate
    // resources on those nodes for earlier allocateResource calls, so notifying driver
    // to put those executors in decommissioning state
    allocateResponse.getUpdatedNodes.asScala.filter (node =>
      node.getNodeState == NodeState.DECOMMISSIONING &&
        !decommissioningNodesCache.containsKey(getHostAddress(node)))
      .foreach { node =>
        val host = getHostAddress(node)
        driverRef.send(DecommissionExecutorsOnHost(host))
        decommissioningNodesCache.put(host, true)
      }
  }

  private def getHostAddress(nodeReport: NodeReport): String = nodeReport.getNodeId.getHost

  /**
   * Update the set of container requests that we will sync with the RM based on the number of
   * executors we have currently running and our target number of executors for each
   * ResourceProfile.
   *
   * Visible for testing.
   */
  def updateResourceRequests(): Unit = synchronized {
    val pendingAllocatePerResourceProfileId = getPendingAllocate
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
      val (localRequests, staleRequests, anyHostRequests) = splitPendingAllocationsByLocality(
        hostToLocalTaskCount, pendingAllocate)

      if (missing > 0) {
        val resource = rpIdToYarnResource.get(rpId)
        if (log.isInfoEnabled()) {
          var requestContainerMessage = s"Will request $missing executor container(s) for " +
            s" ResourceProfile Id: $rpId, each with " +
            s"${resource.getVirtualCores} core(s) and " +
            s"${resource.getMemorySize} MB memory."
          if (resource.getResources().nonEmpty) {
            requestContainerMessage ++= s" with custom resources: $resource"
          }
          logInfo(requestContainerMessage)
        }

        // cancel "stale" requests for locations that are no longer needed
        staleRequests.foreach { stale =>
          amClient.removeContainerRequest(stale)
        }
        val cancelledContainers = staleRequests.size
        if (cancelledContainers > 0) {
          logInfo(s"Canceled $cancelledContainers container request(s) (locality no longer needed)")
        }

        // consider the number of new containers and cancelled stale containers available
        val availableContainers = missing + cancelledContainers

        // to maximize locality, include requests with no locality preference that can be cancelled
        val potentialContainers = availableContainers + anyHostRequests.size

        val allocatedHostToContainer = getOrUpdateAllocatedHostToContainersMapForRPId(rpId)
        val numLocalityAwareTasks = numLocalityAwareTasksPerResourceProfileId.getOrElse(rpId, 0)
        val containerLocalityPreferences = containerPlacementStrategy.localityOfRequestedContainers(
          potentialContainers, numLocalityAwareTasks, hostToLocalTaskCount,
          allocatedHostToContainer, localRequests, rpIdToResourceProfile(rpId))

        val newLocalityRequests = new mutable.ArrayBuffer[ContainerRequest]
        containerLocalityPreferences.foreach {
          case ContainerLocalityPreferences(nodes, racks) if nodes != null =>
            newLocalityRequests += createContainerRequest(resource, nodes, racks, rpId)
          case _ =>
        }

        if (availableContainers >= newLocalityRequests.size) {
          // more containers are available than needed for locality, fill in requests for any host
          for (i <- 0 until (availableContainers - newLocalityRequests.size)) {
            newLocalityRequests += createContainerRequest(resource, null, null, rpId)
          }
        } else {
          val numToCancel = newLocalityRequests.size - availableContainers
          // cancel some requests without locality preferences to schedule more local containers
          anyHostRequests.slice(0, numToCancel).foreach { nonLocal =>
            amClient.removeContainerRequest(nonLocal)
          }
          if (numToCancel > 0) {
            logInfo(s"Canceled $numToCancel unlocalized container requests to " +
              s"resubmit with locality")
          }
        }

        newLocalityRequests.foreach { request =>
          amClient.addContainerRequest(request)
        }

        if (log.isInfoEnabled()) {
          val (localized, anyHost) = newLocalityRequests.partition(_.getNodes() != null)
          if (anyHost.nonEmpty) {
            logInfo(s"Submitted ${anyHost.size} unlocalized container requests.")
          }
          localized.foreach { request =>
            logInfo(s"Submitted container request for host ${hostStr(request)}.")
          }
        }
      } else if (numPendingAllocate > 0 && missing < 0) {
        val numToCancel = math.min(numPendingAllocate, -missing)
        logInfo(s"Canceling requests for $numToCancel executor container(s) to have a new " +
          s"desired total ${getOrUpdateTargetNumExecutorsForRPId(rpId)} executors.")
        // cancel pending allocate requests by taking locality preference into account
        val cancelRequests = (staleRequests ++ anyHostRequests ++ localRequests).take(numToCancel)
        cancelRequests.foreach(amClient.removeContainerRequest)
      }
    }
  }

  def stop(): Unit = {
    // Forcefully shut down the launcher pool, in case this is being called in the middle of
    // container allocation. This will prevent queued executors from being started - and
    // potentially interrupt active ExecutorRunnable instances too.
    launcherPool.shutdownNow()
  }

  private def hostStr(request: ContainerRequest): String = {
    Option(request.getNodes) match {
      case Some(nodes) => nodes.asScala.mkString(",")
      case None => "Any"
    }
  }

  /**
   * Creates a container request, handling the reflection required to use YARN features that were
   * added in recent versions.
   */
  private def createContainerRequest(
      resource: Resource,
      nodes: Array[String],
      racks: Array[String],
      rpId: Int): ContainerRequest = {
    new ContainerRequest(resource, nodes, racks, getContainerPriority(rpId),
      true, labelExpression.orNull)
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

    // Match remaining by rack. Because YARN's RackResolver swallows thread interrupts
    // (see SPARK-27094), which can cause this code to miss interrupts from the AM, use
    // a separate thread to perform the operation.
    val remainingAfterRackMatches = new ArrayBuffer[Container]
    if (remainingAfterHostMatches.nonEmpty) {
      var exception: Option[Throwable] = None
      val thread = new Thread("spark-rack-resolver") {
        override def run(): Unit = {
          try {
            for (allocatedContainer <- remainingAfterHostMatches) {
              val rack = resolver.resolve(allocatedContainer.getNodeId.getHost)
              matchContainerToRequest(allocatedContainer, rack, containersToUse,
                remainingAfterRackMatches)
            }
          } catch {
            case e: Throwable =>
              exception = Some(e)
          }
        }
      }
      thread.setDaemon(true)
      thread.start()

      try {
        thread.join()
      } catch {
        case e: InterruptedException =>
          thread.interrupt()
          throw e
      }

      if (exception.isDefined) {
        throw exception.get
      }
    }

    // Assign remaining that are neither node-local nor rack-local
    val remainingAfterOffRackMatches = new ArrayBuffer[Container]
    for (allocatedContainer <- remainingAfterRackMatches) {
      matchContainerToRequest(allocatedContainer, ANY_HOST, containersToUse,
        remainingAfterOffRackMatches)
    }

    if (remainingAfterOffRackMatches.nonEmpty) {
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
    // Match on the exact resource we requested so there shouldn't be a mismatch,
    // we are relying on YARN to return a container with resources no less than we requested.
    // If we change this, or starting validating the container, be sure the logic covers SPARK-6050.
    val rpId = getResourceProfileIdFromPriority(allocatedContainer.getPriority)
    val resourceForRP = rpIdToYarnResource.get(rpId)

    logDebug(s"Calling amClient.getMatchingRequests with parameters: " +
        s"priority: ${allocatedContainer.getPriority}, " +
        s"location: $location, resource: $resourceForRP")
    val matchingRequests = amClient.getMatchingRequests(allocatedContainer.getPriority, location,
      resourceForRP)

    // Match the allocation to a request
    if (!matchingRequests.isEmpty) {
      val containerRequest = matchingRequests.get(0).iterator.next
      logDebug(s"Removing container request via AM client: $containerRequest")
      amClient.removeContainerRequest(containerRequest)
      containersToUse += allocatedContainer
    } else {
      remaining += allocatedContainer
    }
  }

  /**
   * Launches executors in the allocated containers.
   */
  private def runAllocatedContainers(containersToUse: ArrayBuffer[Container]): Unit = synchronized {
    for (container <- containersToUse) {
      val rpId = getResourceProfileIdFromPriority(container.getPriority)
      executorIdCounter += 1
      val executorHostname = container.getNodeId.getHost
      val containerId = container.getId
      val executorId = executorIdCounter.toString
      val yarnResourceForRpId = rpIdToYarnResource.get(rpId)
      assert(container.getResource.getMemorySize >= yarnResourceForRpId.getMemorySize)
      logInfo(s"Launching container $containerId on host $executorHostname " +
        s"for executor with ID $executorId for ResourceProfile Id $rpId")

      val rp = rpIdToResourceProfile(rpId)
      val defaultResources = ResourceProfile.getDefaultProfileExecutorResources(sparkConf)
      val containerMem = rp.executorResources.get(ResourceProfile.MEMORY).
        map(_.amount).getOrElse(defaultResources.executorMemoryMiB).toInt

      assert(defaultResources.cores.nonEmpty)
      val defaultCores = defaultResources.cores.get
      val containerCores = rp.getExecutorCores.getOrElse(defaultCores)

      val rpRunningExecs = getOrUpdateRunningExecutorForRPId(rpId).size
      if (rpRunningExecs < getOrUpdateTargetNumExecutorsForRPId(rpId)) {
        getOrUpdateNumExecutorsStartingForRPId(rpId).incrementAndGet()
        launchingExecutorContainerIds.add(containerId)
        if (launchContainers) {
          launcherPool.execute(() => {
            try {
              new ExecutorRunnable(
                Some(container),
                conf,
                sparkConf,
                driverUrl,
                executorId,
                executorHostname,
                containerMem,
                containerCores,
                appAttemptId.getApplicationId.toString,
                securityMgr,
                localResources,
                rp.id
              ).run()
              updateInternalState(rpId, executorId, container)
            } catch {
              case e: Throwable =>
                getOrUpdateNumExecutorsStartingForRPId(rpId).decrementAndGet()
                launchingExecutorContainerIds.remove(containerId)
                if (NonFatal(e)) {
                  logError(s"Failed to launch executor $executorId on container $containerId", e)
                  // Assigned container should be released immediately
                  // to avoid unnecessary resource occupation.
                  amClient.releaseAssignedContainer(containerId)
                } else {
                  throw e
                }
            }
          })
        } else {
          // For test only
          updateInternalState(rpId, executorId, container)
        }
      } else {
        logInfo(("Skip launching executorRunnable as running executors count: %d " +
          "reached target executors count: %d.").format(rpRunningExecs,
          getOrUpdateTargetNumExecutorsForRPId(rpId)))
      }
    }
  }

  private def updateInternalState(rpId: Int, executorId: String,
      container: Container): Unit = synchronized {
    val containerId = container.getId
    if (launchingExecutorContainerIds.contains(containerId)) {
      getOrUpdateRunningExecutorForRPId(rpId).add(executorId)
      executorIdToContainer(executorId) = container
      containerIdToExecutorIdAndResourceProfileId(containerId) = (executorId, rpId)

      val localallocatedHostToContainersMap = getOrUpdateAllocatedHostToContainersMapForRPId(rpId)
      val executorHostname = container.getNodeId.getHost
      val containerSet = localallocatedHostToContainersMap.getOrElseUpdate(executorHostname,
        new HashSet[ContainerId])
      containerSet += containerId
      allocatedContainerToHostMap.put(containerId, executorHostname)
      launchingExecutorContainerIds.remove(containerId)
    }
    getOrUpdateNumExecutorsStartingForRPId(rpId).decrementAndGet()
  }

  // Visible for testing.
  private[yarn] def processCompletedContainers(
      completedContainers: Seq[ContainerStatus]): Unit = synchronized {
    for (completedContainer <- completedContainers) {
      val containerId = completedContainer.getContainerId
      launchingExecutorContainerIds.remove(containerId)
      val (_, rpId) = containerIdToExecutorIdAndResourceProfileId.getOrElse(containerId,
        ("", DEFAULT_RESOURCE_PROFILE_ID))
      val alreadyReleased = releasedContainers.remove(containerId)
      val hostOpt = allocatedContainerToHostMap.get(containerId)
      val onHostStr = hostOpt.map(host => s" on host: $host").getOrElse("")
      val exitReason = if (!alreadyReleased) {
        // Decrement the number of executors running. The next iteration of
        // the ApplicationMaster's reporting thread will take care of allocating.
        containerIdToExecutorIdAndResourceProfileId.get(containerId) match {
          case Some((executorId, _)) =>
            getOrUpdateRunningExecutorForRPId(rpId).remove(executorId)
          case None => logWarning(s"Cannot find executorId for container: ${containerId.toString}")
        }

        logInfo("Completed container %s%s (state: %s, exit status: %s)".format(
          containerId,
          onHostStr,
          completedContainer.getState,
          completedContainer.getExitStatus))
        val exitStatus = completedContainer.getExitStatus
        val (exitCausedByApp, containerExitReason) = exitStatus match {
          case _ if shutdown =>
            (false, s"Executor for container $containerId exited after Application shutdown.")
          case ContainerExitStatus.SUCCESS =>
            (false, s"Executor for container $containerId exited because of a YARN event (e.g., " +
              "preemption) and not because of an error in the running job.")
          case ContainerExitStatus.PREEMPTED =>
            // Preemption is not the fault of the running tasks, since YARN preempts containers
            // merely to do resource sharing, and tasks that fail due to preempted executors could
            // just as easily finish on any other executor. See SPARK-8167.
            (false, s"Container ${containerId}${onHostStr} was preempted.")
          // Should probably still count memory exceeded exit codes towards task failures
          case ContainerExitStatus.KILLED_EXCEEDED_VMEM =>
            val vmemExceededPattern = raw"$MEM_REGEX of $MEM_REGEX virtual memory used".r
            val diag = vmemExceededPattern.findFirstIn(completedContainer.getDiagnostics)
              .map(_.concat(".")).getOrElse("")
            val message = "Container killed by YARN for exceeding virtual memory limits. " +
              s"$diag Consider boosting ${EXECUTOR_MEMORY_OVERHEAD.key} or boosting " +
              s"${YarnConfiguration.NM_VMEM_PMEM_RATIO} or disabling " +
              s"${YarnConfiguration.NM_VMEM_CHECK_ENABLED} because of YARN-4714."
            (true, message)
          case ContainerExitStatus.KILLED_EXCEEDED_PMEM =>
            val pmemExceededPattern = raw"$MEM_REGEX of $MEM_REGEX physical memory used".r
            val diag = pmemExceededPattern.findFirstIn(completedContainer.getDiagnostics)
              .map(_.concat(".")).getOrElse("")
            val message = "Container killed by YARN for exceeding physical memory limits. " +
              s"$diag Consider boosting ${EXECUTOR_MEMORY_OVERHEAD.key}."
            (true, message)
          case other_exit_status =>
            val exitStatus = completedContainer.getExitStatus
            // SPARK-46920: Spark defines its own exit codes, which have overlap with
            // exit codes defined by YARN, thus diagnostics reported by YARN may be
            // misleading.
            val sparkExitCodeReason = ExecutorExitCode.explainExitCode(exitStatus)
            // SPARK-26269: follow YARN's behaviour, see details in
            // org.apache.hadoop.yarn.util.Apps#shouldCountTowardsNodeBlacklisting
            if (NOT_APP_AND_SYSTEM_FAULT_EXIT_STATUS.contains(other_exit_status)) {
              (false, s"Container marked as failed: $containerId$onHostStr. " +
                s"Exit status: $exitStatus. " +
                s"Possible causes: $sparkExitCodeReason " +
                s"Diagnostics: ${completedContainer.getDiagnostics}.")
            } else {
              // completed container from a bad node
              allocatorNodeHealthTracker.handleResourceAllocationFailure(hostOpt)
              (true, s"Container from a bad node: $containerId$onHostStr. " +
                s"Exit status: $exitStatus. " +
                s"Possible causes: $sparkExitCodeReason " +
                s"Diagnostics: ${completedContainer.getDiagnostics}.")
            }
        }
        if (exitCausedByApp) {
          logWarning(containerExitReason)
        } else {
          logInfo(containerExitReason)
        }
        ExecutorExited(exitStatus, exitCausedByApp, containerExitReason)
      } else {
        // If we have already released this container, then it must mean
        // that the driver has explicitly requested it to be killed
        ExecutorExited(completedContainer.getExitStatus, exitCausedByApp = false,
          s"Container $containerId exited from explicit termination request.")
      }

      for {
        host <- hostOpt
        containerSet <- getOrUpdateAllocatedHostToContainersMapForRPId(rpId).get(host)
      } {
        containerSet.remove(containerId)
        if (containerSet.isEmpty) {
          getOrUpdateAllocatedHostToContainersMapForRPId(rpId).remove(host)
        } else {
          getOrUpdateAllocatedHostToContainersMapForRPId(rpId).update(host, containerSet)
        }

        allocatedContainerToHostMap.remove(containerId)
      }

      containerIdToExecutorIdAndResourceProfileId.remove(containerId).foreach { case (eid, _) =>
        executorIdToContainer.remove(eid)
        pendingLossReasonRequests.remove(eid) match {
          case Some(pendingRequests) =>
            // Notify application of executor loss reasons so it can decide whether it should abort
            pendingRequests.foreach(_.reply(exitReason))

          case None =>
            // We cannot find executor for pending reasons. This is because completed container
            // is processed before querying pending result. We should store it for later query.
            // This is usually happened when explicitly killing a container, the result will be
            // returned in one AM-RM communication. So query RPC will be later than this completed
            // container process.
            releasedExecutorLossReasons.put(eid, exitReason)
        }
        if (!alreadyReleased) {
          // The executor could have gone away (like no route to host, node failure, etc)
          // Notify backend about the failure of the executor
          numUnexpectedContainerRelease += 1
          driverRef.send(RemoveExecutor(eid, exitReason))
        }
      }
    }
  }

  /**
   * Register that some RpcCallContext has asked the AM why the executor was lost. Note that
   * we can only find the loss reason to send back in the next call to allocateResources().
   */
  private[yarn] def enqueueGetLossReasonRequest(
      eid: String,
      context: RpcCallContext): Unit = synchronized {
    if (executorIdToContainer.contains(eid)) {
      pendingLossReasonRequests
        .getOrElseUpdate(eid, new ArrayBuffer[RpcCallContext]) += context
    } else if (releasedExecutorLossReasons.contains(eid)) {
      // Executor is already released explicitly before getting the loss reason, so directly send
      // the pre-stored lost reason
      context.reply(releasedExecutorLossReasons.remove(eid).get)
    } else {
      logWarning(s"Tried to get the loss reason for non-existent executor $eid")
      context.sendFailure(
        new SparkException(s"Fail to find loss reason for non-existent executor $eid"))
    }
  }

  private def internalReleaseContainer(container: Container): Unit = synchronized {
    releasedContainers.add(container.getId())
    amClient.releaseAssignedContainer(container.getId())
  }

  private[yarn] def getNumUnexpectedContainerRelease: Long = synchronized {
    numUnexpectedContainerRelease
  }

  private[yarn] def getNumPendingLossReasonRequests: Int = synchronized {
    pendingLossReasonRequests.size
  }

  /**
   * Split the pending container requests into 3 groups based on current localities of pending
   * tasks.
   * @param hostToLocalTaskCount a map of preferred hostname to possible task counts to be used as
   *                             container placement hint.
   * @param pendingAllocations A sequence of pending allocation container request.
   * @return A tuple of 3 sequences, first is a sequence of locality matched container
   *         requests, second is a sequence of locality unmatched container requests, and third is a
   *         sequence of locality free container requests.
   */
  private def splitPendingAllocationsByLocality(
      hostToLocalTaskCount: Map[String, Int],
      pendingAllocations: Seq[ContainerRequest]
    ): (Seq[ContainerRequest], Seq[ContainerRequest], Seq[ContainerRequest]) = {
    val localityMatched = ArrayBuffer[ContainerRequest]()
    val localityUnMatched = ArrayBuffer[ContainerRequest]()
    val localityFree = ArrayBuffer[ContainerRequest]()

    val preferredHosts = hostToLocalTaskCount.keySet
    pendingAllocations.foreach { cr =>
      val nodes = cr.getNodes
      if (nodes == null) {
        localityFree += cr
      } else if (nodes.asScala.toSet.intersect(preferredHosts).nonEmpty) {
        localityMatched += cr
      } else {
        localityUnMatched += cr
      }
    }

    (localityMatched.toSeq, localityUnMatched.toSeq, localityFree.toSeq)
  }

}

private object YarnAllocator {
  val MEM_REGEX = "[0-9.]+ [KMG]B"
  val DECOMMISSIONING_NODES_CACHE_SIZE = 200

  val NOT_APP_AND_SYSTEM_FAULT_EXIT_STATUS = Set(
    ContainerExitStatus.KILLED_BY_RESOURCEMANAGER,
    ContainerExitStatus.KILLED_BY_APPMASTER,
    ContainerExitStatus.KILLED_AFTER_APP_COMPLETION,
    ContainerExitStatus.ABORTED,
    ContainerExitStatus.DISKS_FAILED
  )
}
