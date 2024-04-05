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
package org.apache.spark.scheduler.cluster.k8s

import java.time.Instant
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import io.fabric8.kubernetes.api.model.{HasMetadata, PersistentVolumeClaim, Pod, PodBuilder}
import io.fabric8.kubernetes.client.{KubernetesClient, KubernetesClientException}

import org.apache.spark.{SecurityManager, SparkConf, SparkException}
import org.apache.spark.deploy.ExecutorFailureTracker
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.deploy.k8s.KubernetesConf
import org.apache.spark.deploy.k8s.KubernetesUtils.addOwnerReference
import org.apache.spark.internal.{Logging, LogKey, MDC}
import org.apache.spark.internal.config._
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.scheduler.cluster.SchedulerBackendUtils.DEFAULT_NUMBER_EXECUTORS
import org.apache.spark.util.{Clock, Utils}
import org.apache.spark.util.SparkExitCode.EXCEED_MAX_EXECUTOR_FAILURES

class ExecutorPodsAllocator(
    conf: SparkConf,
    secMgr: SecurityManager,
    executorBuilder: KubernetesExecutorBuilder,
    kubernetesClient: KubernetesClient,
    snapshotsStore: ExecutorPodsSnapshotsStore,
    clock: Clock) extends AbstractPodsAllocator() with Logging {

  protected val EXECUTOR_ID_COUNTER = new AtomicInteger(0)

  protected val PVC_COUNTER = new AtomicInteger(0)

  protected val maxPVCs = if (Utils.isDynamicAllocationEnabled(conf)) {
    conf.get(DYN_ALLOCATION_MAX_EXECUTORS)
  } else {
    conf.getInt(EXECUTOR_INSTANCES.key, DEFAULT_NUMBER_EXECUTORS)
  }

  protected val podAllocOnPVC = conf.get(KUBERNETES_DRIVER_OWN_PVC) &&
    conf.get(KUBERNETES_DRIVER_REUSE_PVC) && conf.get(KUBERNETES_DRIVER_WAIT_TO_REUSE_PVC)

  // ResourceProfile id -> total expected executors per profile, currently we don't remove
  // any resource profiles - https://issues.apache.org/jira/browse/SPARK-30749
  protected val totalExpectedExecutorsPerResourceProfileId = new ConcurrentHashMap[Int, Int]()

  protected val rpIdToResourceProfile = new mutable.HashMap[Int, ResourceProfile]

  protected val podAllocationSize = conf.get(KUBERNETES_ALLOCATION_BATCH_SIZE)

  protected val podAllocationDelay = conf.get(KUBERNETES_ALLOCATION_BATCH_DELAY)

  protected val maxPendingPods = conf.get(KUBERNETES_MAX_PENDING_PODS)

  protected val maxNumExecutorFailures = ExecutorFailureTracker.maxNumExecutorFailures(conf)

  protected val podCreationTimeout = math.max(
    podAllocationDelay * 5,
    conf.get(KUBERNETES_ALLOCATION_EXECUTOR_TIMEOUT))

  protected val driverPodReadinessTimeout = conf.get(KUBERNETES_ALLOCATION_DRIVER_READINESS_TIMEOUT)

  protected val executorIdleTimeout = conf.get(DYN_ALLOCATION_EXECUTOR_IDLE_TIMEOUT) * 1000

  protected val namespace = conf.get(KUBERNETES_NAMESPACE)

  protected val kubernetesDriverPodName = conf
    .get(KUBERNETES_DRIVER_POD_NAME)

  protected val shouldDeleteExecutors = conf.get(KUBERNETES_DELETE_EXECUTORS)

  val driverPod = kubernetesDriverPodName
    .map(name => Option(kubernetesClient.pods()
      .inNamespace(namespace)
      .withName(name)
      .get())
      .getOrElse(throw new SparkException(
        s"No pod was found named $name in the cluster in the " +
          s"namespace $namespace (this was supposed to be the driver pod.).")))

  // Executor IDs that have been requested from Kubernetes but have not been detected in any
  // snapshot yet. Mapped to the (ResourceProfile id, timestamp) when they were created.
  protected val newlyCreatedExecutors = mutable.LinkedHashMap.empty[Long, (Int, Long)]

  // Executor IDs that have been requested from Kubernetes but have not been detected in any POD
  // snapshot yet but already known by the scheduler backend. Mapped to the ResourceProfile id.
  protected val schedulerKnownNewlyCreatedExecs = mutable.LinkedHashMap.empty[Long, Int]

  protected val dynamicAllocationEnabled = Utils.isDynamicAllocationEnabled(conf)

  // visible for tests
  val numOutstandingPods = new AtomicInteger()

  protected var lastSnapshot = ExecutorPodsSnapshot()

  protected var appId: String = _

  // Executors that have been deleted by this allocator but not yet detected as deleted in
  // a snapshot from the API server. This is used to deny registration from these executors
  // if they happen to come up before the deletion takes effect.
  @volatile protected var deletedExecutorIds = Set.empty[Long]

  @volatile private var failedExecutorIds = Set.empty[Long]

  protected val failureTracker = new ExecutorFailureTracker(conf, clock)

  protected[spark] def getNumExecutorsFailed: Int = failureTracker.numFailedExecutors

  def start(applicationId: String, schedulerBackend: KubernetesClusterSchedulerBackend): Unit = {
    appId = applicationId
    driverPod.foreach { pod =>
      // Wait until the driver pod is ready before starting executors, as the headless service won't
      // be resolvable by DNS until the driver pod is ready.
      Utils.tryLogNonFatalError {
        kubernetesClient
          .pods()
          .inNamespace(namespace)
          .withName(pod.getMetadata.getName)
          .waitUntilReady(driverPodReadinessTimeout, TimeUnit.SECONDS)
      }
    }
    snapshotsStore.addSubscriber(podAllocationDelay) { executorPodsSnapshot =>
      onNewSnapshots(applicationId, schedulerBackend, executorPodsSnapshot)
      if (failureTracker.numFailedExecutors > maxNumExecutorFailures) {
        logError(log"Max number of executor failures " +
          log"(${MDC(LogKey.MAX_EXECUTOR_FAILURES, maxNumExecutorFailures)}) reached")
        stopApplication(EXCEED_MAX_EXECUTOR_FAILURES)
      }
    }
  }

  def setTotalExpectedExecutors(resourceProfileToTotalExecs: Map[ResourceProfile, Int]): Unit = {
    resourceProfileToTotalExecs.foreach { case (rp, numExecs) =>
      rpIdToResourceProfile.getOrElseUpdate(rp.id, rp)
      totalExpectedExecutorsPerResourceProfileId.put(rp.id, numExecs)
    }
    logDebug(s"Set total expected execs to $totalExpectedExecutorsPerResourceProfileId")
    if (numOutstandingPods.get() == 0) {
      snapshotsStore.notifySubscribers()
    }
  }

  def isDeleted(executorId: String): Boolean = deletedExecutorIds.contains(executorId.toLong)

  private[k8s] def stopApplication(exitCode: Int): Unit = {
    sys.exit(exitCode)
  }

  protected def onNewSnapshots(
      applicationId: String,
      schedulerBackend: KubernetesClusterSchedulerBackend,
      snapshots: Seq[ExecutorPodsSnapshot]): Unit = {
    logDebug(s"Received ${snapshots.size} snapshots")
    val k8sKnownExecIds = snapshots.flatMap(_.executorPods.keys).distinct
    newlyCreatedExecutors --= k8sKnownExecIds
    schedulerKnownNewlyCreatedExecs --= k8sKnownExecIds

    // Although we are going to delete some executors due to timeout in this function,
    // it takes undefined time before the actual deletion. Hence, we should collect all PVCs
    // in use at the beginning. False positive is okay in this context in order to be safe.
    val k8sKnownPVCNames = snapshots.flatMap(_.executorPods.values.map(_.pod)).flatMap { pod =>
      pod.getSpec.getVolumes.asScala
        .flatMap { v => Option(v.getPersistentVolumeClaim).map(_.getClaimName) }
    }.distinct

    // transfer the scheduler backend known executor requests from the newlyCreatedExecutors
    // to the schedulerKnownNewlyCreatedExecs
    val schedulerKnownExecs = schedulerBackend.getExecutorIds().map(_.toLong).toSet
    schedulerKnownNewlyCreatedExecs ++=
      newlyCreatedExecutors.filter { case (k, _) => schedulerKnownExecs.contains(k) }
        .map { case (k, v) => (k, v._1) }
    newlyCreatedExecutors --= schedulerKnownNewlyCreatedExecs.keySet

    // For all executors we've created against the API but have not seen in a snapshot
    // yet - check the current time. If the current time has exceeded some threshold,
    // assume that the pod was either never created (the API server never properly
    // handled the creation request), or the API server created the pod but we missed
    // both the creation and deletion events. In either case, delete the missing pod
    // if possible, and mark such a pod to be rescheduled below.
    val currentTime = clock.getTimeMillis()
    val timedOut = newlyCreatedExecutors.flatMap { case (execId, (_, timeCreated)) =>
      if (currentTime - timeCreated > podCreationTimeout) {
        Some(execId)
      } else {
        logDebug(s"Executor with id $execId was not found in the Kubernetes cluster since it" +
          s" was created ${currentTime - timeCreated} milliseconds ago.")
        None
      }
    }

    if (timedOut.nonEmpty) {
      logWarning(s"Executors with ids ${timedOut.mkString(",")} were not detected in the" +
        s" Kubernetes cluster after $podCreationTimeout ms despite the fact that a previous" +
        " allocation attempt tried to create them. The executors may have been deleted but the" +
        " application missed the deletion event.")

      newlyCreatedExecutors --= timedOut
      if (shouldDeleteExecutors) {
        Utils.tryLogNonFatalError {
          kubernetesClient
            .pods()
            .inNamespace(namespace)
            .withLabel(SPARK_APP_ID_LABEL, applicationId)
            .withLabel(SPARK_ROLE_LABEL, SPARK_POD_EXECUTOR_ROLE)
            .withLabelIn(SPARK_EXECUTOR_ID_LABEL, timedOut.toSeq.map(_.toString): _*)
            .delete()
        }
      }
    }

    if (snapshots.nonEmpty) {
      lastSnapshot = snapshots.last
    }

    // Make a local, non-volatile copy of the reference since it's used multiple times. This
    // is the only method that modifies the list, so this is safe.
    var _deletedExecutorIds = deletedExecutorIds
    if (snapshots.nonEmpty) {
      val existingExecs = lastSnapshot.executorPods.keySet
      _deletedExecutorIds = _deletedExecutorIds.intersect(existingExecs)
    }

    val notDeletedPods = lastSnapshot.executorPods
      .filter { case (k, _) => !_deletedExecutorIds.contains(k) }
    // Map the pods into per ResourceProfile id so we can check per ResourceProfile,
    // add a fast path if not using other ResourceProfiles.
    val rpIdToExecsAndPodState =
      mutable.HashMap[Int, mutable.HashMap[Long, ExecutorPodState]]()
    if (totalExpectedExecutorsPerResourceProfileId.size <= 1) {
      rpIdToExecsAndPodState(ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID) =
        mutable.HashMap.empty ++= notDeletedPods
    } else {
      notDeletedPods.foreach { case (execId, execPodState) =>
        val rpId = execPodState.pod.getMetadata.getLabels.get(SPARK_RESOURCE_PROFILE_ID_LABEL).toInt
        val execPods = rpIdToExecsAndPodState.getOrElseUpdate(rpId,
          mutable.HashMap[Long, ExecutorPodState]())
        execPods(execId) = execPodState
      }
    }

    // sum of all the pending pods unknown by the scheduler (total for all the resources)
    var totalPendingCount = 0
    // total not running pods (including scheduler known & unknown, pending & newly requested ones)
    var totalNotRunningPodCount = 0
    val podsToAllocateWithRpId = totalExpectedExecutorsPerResourceProfileId
      .asScala
      .toSeq
      .sortBy(_._1)
      .flatMap { case (rpId, targetNum) =>
      val podsForRpId = rpIdToExecsAndPodState.getOrElse(rpId, mutable.HashMap.empty)

      val currentRunningCount = podsForRpId.values.count {
        case PodRunning(_) => true
        case _ => false
      }

      val currentFailedExecutorIds = podsForRpId.filter {
        case (_, PodFailed(_)) => true
        case _ => false
      }.keySet

      val newFailedExecutorIds = currentFailedExecutorIds.diff(failedExecutorIds)
      if (newFailedExecutorIds.nonEmpty) {
        logWarning(s"${newFailedExecutorIds.size} new failed executors.")
        newFailedExecutorIds.foreach { _ => failureTracker.registerExecutorFailure() }
      }
      failedExecutorIds = failedExecutorIds ++ currentFailedExecutorIds

      val (schedulerKnownPendingExecsForRpId, currentPendingExecutorsForRpId) = podsForRpId.filter {
        case (_, PodPending(_)) => true
        case _ => false
      }.partition { case (k, _) =>
        schedulerKnownExecs.contains(k)
      }
      // This variable is used later to print some debug logs. It's updated when cleaning up
      // excess pod requests, since currentPendingExecutorsForRpId is immutable.
      var pendingCountForRpId = currentPendingExecutorsForRpId.size

      val newlyCreatedExecutorsForRpId =
        newlyCreatedExecutors.filter { case (_, (waitingRpId, _)) =>
          rpId == waitingRpId
        }

      val schedulerKnownNewlyCreatedExecsForRpId =
        schedulerKnownNewlyCreatedExecs.filter { case (_, waitingRpId) =>
          rpId == waitingRpId
        }

      if (podsForRpId.nonEmpty) {
        logDebug(s"ResourceProfile Id: $rpId (" +
          s"pod allocation status: $currentRunningCount running, " +
          s"${currentPendingExecutorsForRpId.size} unknown pending, " +
          s"${schedulerKnownPendingExecsForRpId.size} scheduler backend known pending, " +
          s"${newlyCreatedExecutorsForRpId.size} unknown newly created, " +
          s"${schedulerKnownNewlyCreatedExecsForRpId.size} scheduler backend known newly created)")
      }

      // It's possible that we have outstanding pods that are outdated when dynamic allocation
      // decides to downscale the application. So check if we can release any pending pods early
      // instead of waiting for them to time out. Drop them first from the unacknowledged list,
      // then from the pending. However, in order to prevent too frequent fluctuation, newly
      // requested pods are protected during executorIdleTimeout period.
      //
      // TODO: with dynamic allocation off, handle edge cases if we end up with more running
      // executors than expected.
      var notRunningPodCountForRpId =
        currentPendingExecutorsForRpId.size + schedulerKnownPendingExecsForRpId.size +
        newlyCreatedExecutorsForRpId.size + schedulerKnownNewlyCreatedExecsForRpId.size
      val podCountForRpId = currentRunningCount + notRunningPodCountForRpId

      if (podCountForRpId > targetNum) {
        val excess = podCountForRpId - targetNum
        val newlyCreatedToDelete = newlyCreatedExecutorsForRpId
          .filter { case (_, (_, createTime)) =>
            currentTime - createTime > executorIdleTimeout
          }.keys.take(excess).toList
        val pendingToDelete = currentPendingExecutorsForRpId
          .filter(x => isExecutorIdleTimedOut(x._2, currentTime))
          .take(excess - newlyCreatedToDelete.size)
          .map { case (id, _) => id }
        val toDelete = newlyCreatedToDelete ++ pendingToDelete

        if (toDelete.nonEmpty) {
          logInfo(s"Deleting ${toDelete.size} excess pod requests (${toDelete.mkString(",")}).")
          _deletedExecutorIds = _deletedExecutorIds ++ toDelete

          Utils.tryLogNonFatalError {
            kubernetesClient
              .pods()
              .inNamespace(namespace)
              .withField("status.phase", "Pending")
              .withLabel(SPARK_APP_ID_LABEL, applicationId)
              .withLabel(SPARK_ROLE_LABEL, SPARK_POD_EXECUTOR_ROLE)
              .withLabelIn(SPARK_EXECUTOR_ID_LABEL, toDelete.sorted.map(_.toString): _*)
              .delete()
            newlyCreatedExecutors --= newlyCreatedToDelete
            pendingCountForRpId -= pendingToDelete.size
            notRunningPodCountForRpId -= toDelete.size
          }
        }
      }
      totalPendingCount += pendingCountForRpId
      totalNotRunningPodCount += notRunningPodCountForRpId

      // The code below just prints debug messages, which are only useful when there's a change
      // in the snapshot state. Since the messages are a little spammy, avoid them when we know
      // there are no useful updates.
      if (log.isDebugEnabled && snapshots.nonEmpty) {
        if (currentRunningCount >= targetNum && !dynamicAllocationEnabled) {
          logDebug(s"Current number of running executors for ResourceProfile Id $rpId is " +
            "equal to the number of requested executors. Not scaling up further.")
        } else {
          if (newlyCreatedExecutorsForRpId.nonEmpty) {
            logDebug(s"Still waiting for ${newlyCreatedExecutorsForRpId.size} executors for " +
              s"ResourceProfile Id $rpId before requesting more.")
          }
        }
      }
      if (newlyCreatedExecutorsForRpId.isEmpty && podCountForRpId < targetNum) {
        Some(rpId, podCountForRpId, targetNum)
      } else {
        // for this resource profile we do not request more PODs
        None
      }
    }

    // Try to request new executors only when there exist remaining slots within the maximum
    // number of pending pods and new snapshot arrives in case of waiting for releasing of the
    // existing PVCs
    val remainingSlotFromPendingPods = maxPendingPods - totalNotRunningPodCount
    if (remainingSlotFromPendingPods > 0 && podsToAllocateWithRpId.size > 0 &&
        !(snapshots.isEmpty && podAllocOnPVC && maxPVCs <= PVC_COUNTER.get())) {
      ExecutorPodsAllocator.splitSlots(podsToAllocateWithRpId, remainingSlotFromPendingPods)
        .foreach { case ((rpId, podCountForRpId, targetNum), sharedSlotFromPendingPods) =>
        val numMissingPodsForRpId = targetNum - podCountForRpId
        val numExecutorsToAllocate =
          math.min(math.min(numMissingPodsForRpId, podAllocationSize), sharedSlotFromPendingPods)
        logInfo(s"Going to request $numExecutorsToAllocate executors from Kubernetes for " +
          s"ResourceProfile Id: $rpId, target: $targetNum, known: $podCountForRpId, " +
          s"sharedSlotFromPendingPods: $sharedSlotFromPendingPods.")
        requestNewExecutors(numExecutorsToAllocate, applicationId, rpId, k8sKnownPVCNames)
      }
    }
    deletedExecutorIds = _deletedExecutorIds

    // Update the flag that helps the setTotalExpectedExecutors() callback avoid triggering this
    // update method when not needed. PODs known by the scheduler backend are not counted here as
    // they considered running PODs and they should not block upscaling.
    numOutstandingPods.set(totalPendingCount + newlyCreatedExecutors.size)
  }

  protected def getReusablePVCs(applicationId: String, pvcsInUse: Seq[String]) = {
    if (conf.get(KUBERNETES_DRIVER_OWN_PVC) && conf.get(KUBERNETES_DRIVER_REUSE_PVC) &&
        driverPod.nonEmpty) {
      try {
        val createdPVCs = kubernetesClient
          .persistentVolumeClaims
          .inNamespace(namespace)
          .withLabel("spark-app-selector", applicationId)
          .list()
          .getItems
          .asScala

        val now = Instant.now().toEpochMilli
        val reusablePVCs = createdPVCs
          .filterNot(pvc => pvcsInUse.contains(pvc.getMetadata.getName))
          .filter(pvc => now - Instant.parse(pvc.getMetadata.getCreationTimestamp).toEpochMilli
            > podAllocationDelay)
        logInfo(s"Found ${reusablePVCs.size} reusable PVCs from ${createdPVCs.size} PVCs")
        reusablePVCs
      } catch {
        case _: KubernetesClientException =>
          logInfo("Cannot list PVC resources. Please check account permissions.")
          mutable.Buffer.empty[PersistentVolumeClaim]
      }
    } else {
      mutable.Buffer.empty[PersistentVolumeClaim]
    }
  }

  protected def requestNewExecutors(
      numExecutorsToAllocate: Int,
      applicationId: String,
      resourceProfileId: Int,
      pvcsInUse: Seq[String]): Unit = {
    // Check reusable PVCs for this executor allocation batch
    val reusablePVCs = getReusablePVCs(applicationId, pvcsInUse)
    for ( _ <- 0 until numExecutorsToAllocate) {
      if (reusablePVCs.isEmpty && podAllocOnPVC && maxPVCs <= PVC_COUNTER.get()) {
        logInfo(s"Wait to reuse one of the existing ${PVC_COUNTER.get()} PVCs.")
        return
      }
      val newExecutorId = EXECUTOR_ID_COUNTER.incrementAndGet()
      val executorConf = KubernetesConf.createExecutorConf(
        conf,
        newExecutorId.toString,
        applicationId,
        driverPod,
        resourceProfileId)
      val resolvedExecutorSpec = executorBuilder.buildFromFeatures(executorConf, secMgr,
        kubernetesClient, rpIdToResourceProfile(resourceProfileId))
      val executorPod = resolvedExecutorSpec.pod
      val podWithAttachedContainer = new PodBuilder(executorPod.pod)
        .editOrNewSpec()
        .addToContainers(executorPod.container)
        .endSpec()
        .build()
      val resources = replacePVCsIfNeeded(
        podWithAttachedContainer, resolvedExecutorSpec.executorKubernetesResources, reusablePVCs)
      val createdExecutorPod =
        kubernetesClient.pods().inNamespace(namespace).resource(podWithAttachedContainer).create()
      try {
        addOwnerReference(createdExecutorPod, resources)
        resources
          .filter(_.getKind == "PersistentVolumeClaim")
          .foreach { resource =>
            if (conf.get(KUBERNETES_DRIVER_OWN_PVC) && driverPod.nonEmpty) {
              addOwnerReference(driverPod.get, Seq(resource))
            }
            val pvc = resource.asInstanceOf[PersistentVolumeClaim]
            logInfo(s"Trying to create PersistentVolumeClaim ${pvc.getMetadata.getName} with " +
              s"StorageClass ${pvc.getSpec.getStorageClassName}")
            kubernetesClient.persistentVolumeClaims().inNamespace(namespace).resource(pvc).create()
            PVC_COUNTER.incrementAndGet()
          }
        newlyCreatedExecutors(newExecutorId) = (resourceProfileId, clock.getTimeMillis())
        logDebug(s"Requested executor with id $newExecutorId from Kubernetes.")
      } catch {
        case NonFatal(e) =>
          kubernetesClient.pods()
            .inNamespace(namespace)
            .resource(createdExecutorPod)
            .delete()
          throw e
      }
    }
  }

  protected def replacePVCsIfNeeded(
      pod: Pod,
      resources: Seq[HasMetadata],
      reusablePVCs: mutable.Buffer[PersistentVolumeClaim]): Seq[HasMetadata] = {
    val replacedResources = mutable.Set[HasMetadata]()
    resources.foreach {
      case pvc: PersistentVolumeClaim =>
        // Find one with the same storage class and size.
        val index = reusablePVCs.indexWhere { p =>
          p.getSpec.getStorageClassName == pvc.getSpec.getStorageClassName &&
            p.getSpec.getResources.getRequests.get("storage") ==
              pvc.getSpec.getResources.getRequests.get("storage")
        }
        if (index >= 0) {
          val volume = pod.getSpec.getVolumes.asScala.find { v =>
            v.getPersistentVolumeClaim != null &&
              v.getPersistentVolumeClaim.getClaimName == pvc.getMetadata.getName
          }
          if (volume.nonEmpty) {
            val matchedPVC = reusablePVCs.remove(index)
            replacedResources.add(pvc)
            logInfo(s"Reuse PersistentVolumeClaim ${matchedPVC.getMetadata.getName}")
            volume.get.getPersistentVolumeClaim.setClaimName(matchedPVC.getMetadata.getName)
          }
        }
      case _ => // no-op
    }
    resources.filterNot(replacedResources.contains)
  }

  protected def isExecutorIdleTimedOut(state: ExecutorPodState, currentTime: Long): Boolean = {
    try {
      val creationTime = Instant.parse(state.pod.getMetadata.getCreationTimestamp).toEpochMilli()
      currentTime - creationTime > executorIdleTimeout
    } catch {
      case e: Exception =>
        logError(log"Cannot get the creationTimestamp of the pod: " +
          log"${MDC(LogKey.POD_ID, state.pod)}", e)
        true
    }
  }

  override def stop(applicationId: String): Unit = {
    Utils.tryLogNonFatalError {
      kubernetesClient
        .pods()
        .inNamespace(namespace)
        .withLabel(SPARK_APP_ID_LABEL, applicationId)
        .withLabel(SPARK_ROLE_LABEL, SPARK_POD_EXECUTOR_ROLE)
        .delete()
    }
  }
}

private[spark] object ExecutorPodsAllocator {

  // A utility function to split the available slots among the specified consumers
  def splitSlots[T](consumers: Seq[T], slots: Int): Seq[(T, Int)] = {
    val d = slots / consumers.size
    val r = slots % consumers.size
    consumers.take(r).map((_, d + 1)) ++ consumers.takeRight(consumers.size - r).map((_, d))
  }
}
