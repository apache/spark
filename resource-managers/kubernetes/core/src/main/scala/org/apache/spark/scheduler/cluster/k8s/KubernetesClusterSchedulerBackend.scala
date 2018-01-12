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

import java.io.Closeable
import java.net.InetAddress
<<<<<<< HEAD
import java.util.Collections
import java.util.concurrent.{ConcurrentHashMap, ExecutorService, ScheduledExecutorService, ThreadPoolExecutor, TimeUnit}
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong, AtomicReference}
=======
import java.util.concurrent.{ConcurrentHashMap, ExecutorService, ScheduledExecutorService, TimeUnit}
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong, AtomicReference}
import javax.annotation.concurrent.GuardedBy
>>>>>>> master

import io.fabric8.kubernetes.api.model._
import io.fabric8.kubernetes.client.{KubernetesClient, KubernetesClientException, Watcher}
import io.fabric8.kubernetes.client.Watcher.Action
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

<<<<<<< HEAD
import org.apache.spark.{SparkEnv, SparkException}
import org.apache.spark.deploy.k8s.config._
import org.apache.spark.deploy.k8s.constants._
import org.apache.spark.rpc.{RpcAddress, RpcCallContext, RpcEndpointAddress, RpcEnv}
import org.apache.spark.scheduler.{ExecutorExited, SlaveLost, TaskSchedulerImpl}
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.{RetrieveSparkAppConfig, SparkAppConfig}
import org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend
=======
import org.apache.spark.SparkException
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.rpc.{RpcAddress, RpcEndpointAddress, RpcEnv}
import org.apache.spark.scheduler.{ExecutorExited, SlaveLost, TaskSchedulerImpl}
import org.apache.spark.scheduler.cluster.{CoarseGrainedSchedulerBackend, SchedulerBackendUtils}
>>>>>>> master
import org.apache.spark.util.Utils

private[spark] class KubernetesClusterSchedulerBackend(
    scheduler: TaskSchedulerImpl,
    rpcEnv: RpcEnv,
    executorPodFactory: ExecutorPodFactory,
<<<<<<< HEAD
    shuffleManager: Option[KubernetesExternalShuffleManager],
=======
>>>>>>> master
    kubernetesClient: KubernetesClient,
    allocatorExecutor: ScheduledExecutorService,
    requestExecutorsService: ExecutorService)
  extends CoarseGrainedSchedulerBackend(scheduler, rpcEnv) {

  import KubernetesClusterSchedulerBackend._

  private val EXECUTOR_ID_COUNTER = new AtomicLong(0L)
  private val RUNNING_EXECUTOR_PODS_LOCK = new Object
<<<<<<< HEAD
  // Indexed by executor IDs and guarded by RUNNING_EXECUTOR_PODS_LOCK.
  private val runningExecutorsToPods = new mutable.HashMap[String, Pod]
  // Indexed by executor pod names and guarded by RUNNING_EXECUTOR_PODS_LOCK.
  private val runningPodsToExecutors = new mutable.HashMap[String, String]
=======
  @GuardedBy("RUNNING_EXECUTOR_PODS_LOCK")
  private val runningExecutorsToPods = new mutable.HashMap[String, Pod]
>>>>>>> master
  private val executorPodsByIPs = new ConcurrentHashMap[String, Pod]()
  private val podsWithKnownExitReasons = new ConcurrentHashMap[String, ExecutorExited]()
  private val disconnectedPodsByExecutorIdPendingRemoval = new ConcurrentHashMap[String, Pod]()

  private val kubernetesNamespace = conf.get(KUBERNETES_NAMESPACE)

  private val kubernetesDriverPodName = conf
    .get(KUBERNETES_DRIVER_POD_NAME)
<<<<<<< HEAD
    .getOrElse(
      throw new SparkException("Must specify the driver pod name"))
  private implicit val requestExecutorContext = ExecutionContext.fromExecutorService(
      requestExecutorsService)

  private val driverPod = try {
    kubernetesClient.pods().inNamespace(kubernetesNamespace).
      withName(kubernetesDriverPodName).get()
  } catch {
    case throwable: Throwable =>
      logError(s"Executor cannot find driver pod.", throwable)
      throw new SparkException(s"Executor cannot find driver pod", throwable)
  }

  override val minRegisteredRatio =
=======
    .getOrElse(throw new SparkException("Must specify the driver pod name"))
  private implicit val requestExecutorContext = ExecutionContext.fromExecutorService(
    requestExecutorsService)

  private val driverPod = kubernetesClient.pods()
    .inNamespace(kubernetesNamespace)
    .withName(kubernetesDriverPodName)
    .get()

  protected override val minRegisteredRatio =
>>>>>>> master
    if (conf.getOption("spark.scheduler.minRegisteredResourcesRatio").isEmpty) {
      0.8
    } else {
      super.minRegisteredRatio
    }

  private val executorWatchResource = new AtomicReference[Closeable]
<<<<<<< HEAD
  protected var totalExpectedExecutors = new AtomicInteger(0)

  private val driverUrl = RpcEndpointAddress(
      conf.get("spark.driver.host"),
      conf.getInt("spark.driver.port", DEFAULT_DRIVER_PORT),
      CoarseGrainedSchedulerBackend.ENDPOINT_NAME).toString

  private val initialExecutors = getInitialTargetExecutorNumber()

  private val podAllocationInterval = conf.get(KUBERNETES_ALLOCATION_BATCH_DELAY)
  require(podAllocationInterval > 0, s"Allocation batch delay " +
    s"$KUBERNETES_ALLOCATION_BATCH_DELAY " +
    s"is $podAllocationInterval, should be a positive integer")

  private val podAllocationSize = conf.get(KUBERNETES_ALLOCATION_BATCH_SIZE)
  require(podAllocationSize > 0, s"Allocation batch size " +
    s"$KUBERNETES_ALLOCATION_BATCH_SIZE " +
    s"is $podAllocationSize, should be a positive integer")
=======
  private val totalExpectedExecutors = new AtomicInteger(0)

  private val driverUrl = RpcEndpointAddress(
    conf.get("spark.driver.host"),
    conf.getInt("spark.driver.port", DEFAULT_DRIVER_PORT),
    CoarseGrainedSchedulerBackend.ENDPOINT_NAME).toString

  private val initialExecutors = SchedulerBackendUtils.getInitialTargetExecutorNumber(conf)

  private val podAllocationInterval = conf.get(KUBERNETES_ALLOCATION_BATCH_DELAY)

  private val podAllocationSize = conf.get(KUBERNETES_ALLOCATION_BATCH_SIZE)

  private val executorLostReasonCheckMaxAttempts = conf.get(
    KUBERNETES_EXECUTOR_LOST_REASON_CHECK_MAX_ATTEMPTS)
>>>>>>> master

  private val allocatorRunnable = new Runnable {

    // Maintains a map of executor id to count of checks performed to learn the loss reason
    // for an executor.
    private val executorReasonCheckAttemptCounts = new mutable.HashMap[String, Int]

    override def run(): Unit = {
      handleDisconnectedExecutors()
<<<<<<< HEAD
      RUNNING_EXECUTOR_PODS_LOCK.synchronized {
        if (totalRegisteredExecutors.get() < runningExecutorsToPods.size) {
          logDebug("Waiting for pending executors before scaling")
        } else if (totalExpectedExecutors.get() <= runningExecutorsToPods.size) {
          logDebug("Maximum allowed executor limit reached. Not scaling up further.")
        } else {
          val nodeToLocalTaskCount = getNodesWithLocalTaskCounts
          for (i <- 0 until math.min(
              totalExpectedExecutors.get - runningExecutorsToPods.size, podAllocationSize)) {
            val (executorId, pod) = allocateNewExecutorPod(nodeToLocalTaskCount)
            runningExecutorsToPods.put(executorId, pod)
            runningPodsToExecutors.put(pod.getMetadata.getName, executorId)
=======

      val executorsToAllocate = mutable.Map[String, Pod]()
      val currentTotalRegisteredExecutors = totalRegisteredExecutors.get
      val currentTotalExpectedExecutors = totalExpectedExecutors.get
      val currentNodeToLocalTaskCount = getNodesWithLocalTaskCounts()
      RUNNING_EXECUTOR_PODS_LOCK.synchronized {
        if (currentTotalRegisteredExecutors < runningExecutorsToPods.size) {
          logDebug("Waiting for pending executors before scaling")
        } else if (currentTotalExpectedExecutors <= runningExecutorsToPods.size) {
          logDebug("Maximum allowed executor limit reached. Not scaling up further.")
        } else {
          for (_ <- 0 until math.min(
            currentTotalExpectedExecutors - runningExecutorsToPods.size, podAllocationSize)) {
            val executorId = EXECUTOR_ID_COUNTER.incrementAndGet().toString
            val executorPod = executorPodFactory.createExecutorPod(
              executorId,
              applicationId(),
              driverUrl,
              conf.getExecutorEnv,
              driverPod,
              currentNodeToLocalTaskCount)
            executorsToAllocate(executorId) = executorPod
>>>>>>> master
            logInfo(
              s"Requesting a new executor, total executors is now ${runningExecutorsToPods.size}")
          }
        }
      }
<<<<<<< HEAD
=======

      val allocatedExecutors = executorsToAllocate.mapValues { pod =>
        Utils.tryLog {
          kubernetesClient.pods().create(pod)
        }
      }

      RUNNING_EXECUTOR_PODS_LOCK.synchronized {
        allocatedExecutors.map {
          case (executorId, attemptedAllocatedExecutor) =>
            attemptedAllocatedExecutor.map { successfullyAllocatedExecutor =>
              runningExecutorsToPods.put(executorId, successfullyAllocatedExecutor)
            }
        }
      }
>>>>>>> master
    }

    def handleDisconnectedExecutors(): Unit = {
      // For each disconnected executor, synchronize with the loss reasons that may have been found
      // by the executor pod watcher. If the loss reason was discovered by the watcher,
      // inform the parent class with removeExecutor.
<<<<<<< HEAD
      disconnectedPodsByExecutorIdPendingRemoval.keys().asScala.foreach { case (executorId) =>
        val executorPod = disconnectedPodsByExecutorIdPendingRemoval.get(executorId)
        val knownExitReason = Option(podsWithKnownExitReasons.remove(
          executorPod.getMetadata.getName))
        knownExitReason.fold {
          removeExecutorOrIncrementLossReasonCheckCount(executorId)
        } { executorExited =>
          logDebug(s"Removing executor $executorId with loss reason " + executorExited.message)
          removeExecutor(executorId, executorExited)
          // We keep around executors that have exit conditions caused by the application. This
          // allows them to be debugged later on. Otherwise, mark them as to be deleted from the
          // the API server.
          if (!executorExited.exitCausedByApp) {
            deleteExecutorFromClusterAndDataStructures(executorId)
          }
        }
=======
      disconnectedPodsByExecutorIdPendingRemoval.asScala.foreach {
        case (executorId, executorPod) =>
          val knownExitReason = Option(podsWithKnownExitReasons.remove(
            executorPod.getMetadata.getName))
          knownExitReason.fold {
            removeExecutorOrIncrementLossReasonCheckCount(executorId)
          } { executorExited =>
            logWarning(s"Removing executor $executorId with loss reason " + executorExited.message)
            removeExecutor(executorId, executorExited)
            // We don't delete the pod running the executor that has an exit condition caused by
            // the application from the Kubernetes API server. This allows users to debug later on
            // through commands such as "kubectl logs <pod name>" and
            // "kubectl describe pod <pod name>". Note that exited containers have terminated and
            // therefore won't take CPU and memory resources.
            // Otherwise, the executor pod is marked to be deleted from the API server.
            if (executorExited.exitCausedByApp) {
              logInfo(s"Executor $executorId exited because of the application.")
              deleteExecutorFromDataStructures(executorId)
            } else {
              logInfo(s"Executor $executorId failed because of a framework error.")
              deleteExecutorFromClusterAndDataStructures(executorId)
            }
          }
>>>>>>> master
      }
    }

    def removeExecutorOrIncrementLossReasonCheckCount(executorId: String): Unit = {
      val reasonCheckCount = executorReasonCheckAttemptCounts.getOrElse(executorId, 0)
<<<<<<< HEAD
      if (reasonCheckCount >= MAX_EXECUTOR_LOST_REASON_CHECKS) {
=======
      if (reasonCheckCount >= executorLostReasonCheckMaxAttempts) {
>>>>>>> master
        removeExecutor(executorId, SlaveLost("Executor lost for unknown reasons."))
        deleteExecutorFromClusterAndDataStructures(executorId)
      } else {
        executorReasonCheckAttemptCounts.put(executorId, reasonCheckCount + 1)
      }
    }

    def deleteExecutorFromClusterAndDataStructures(executorId: String): Unit = {
<<<<<<< HEAD
      disconnectedPodsByExecutorIdPendingRemoval.remove(executorId)
      executorReasonCheckAttemptCounts -= executorId
      RUNNING_EXECUTOR_PODS_LOCK.synchronized {
        runningExecutorsToPods.remove(executorId).map { pod =>
          kubernetesClient.pods().delete(pod)
          runningPodsToExecutors.remove(pod.getMetadata.getName)
        }.getOrElse(logWarning(s"Unable to remove pod for unknown executor $executorId"))
=======
      deleteExecutorFromDataStructures(executorId).foreach { pod =>
        kubernetesClient.pods().delete(pod)
      }
    }

    def deleteExecutorFromDataStructures(executorId: String): Option[Pod] = {
      disconnectedPodsByExecutorIdPendingRemoval.remove(executorId)
      executorReasonCheckAttemptCounts -= executorId
      podsWithKnownExitReasons.remove(executorId)
      RUNNING_EXECUTOR_PODS_LOCK.synchronized {
        runningExecutorsToPods.remove(executorId).orElse {
          logWarning(s"Unable to remove pod for unknown executor $executorId")
          None
        }
>>>>>>> master
      }
    }
  }

<<<<<<< HEAD
  private def getInitialTargetExecutorNumber(defaultNumExecutors: Int = 1): Int = {
    if (Utils.isDynamicAllocationEnabled(conf)) {
      val minNumExecutors = conf.getInt("spark.dynamicAllocation.minExecutors", 0)
      val initialNumExecutors = Utils.getDynamicAllocationInitialExecutors(conf)
      val maxNumExecutors = conf.getInt("spark.dynamicAllocation.maxExecutors", 1)
      require(initialNumExecutors >= minNumExecutors && initialNumExecutors <= maxNumExecutors,
        s"initial executor number $initialNumExecutors must between min executor number " +
          s"$minNumExecutors and max executor number $maxNumExecutors")

      initialNumExecutors
    } else {
      conf.getInt("spark.executor.instances", defaultNumExecutors)
    }

  }

  override def applicationId(): String = conf.get("spark.app.id", super.applicationId())

=======
>>>>>>> master
  override def sufficientResourcesRegistered(): Boolean = {
    totalRegisteredExecutors.get() >= initialExecutors * minRegisteredRatio
  }

  override def start(): Unit = {
    super.start()
    executorWatchResource.set(
<<<<<<< HEAD
        kubernetesClient
            .pods()
            .withLabel(SPARK_APP_ID_LABEL, applicationId())
            .watch(new ExecutorPodsWatcher()))

    allocatorExecutor.scheduleWithFixedDelay(
        allocatorRunnable, 0L, podAllocationInterval, TimeUnit.SECONDS)
    shuffleManager.foreach(_.start(applicationId()))
=======
      kubernetesClient
        .pods()
        .withLabel(SPARK_APP_ID_LABEL, applicationId())
        .watch(new ExecutorPodsWatcher()))

    allocatorExecutor.scheduleWithFixedDelay(
      allocatorRunnable, 0L, podAllocationInterval, TimeUnit.MILLISECONDS)
>>>>>>> master

    if (!Utils.isDynamicAllocationEnabled(conf)) {
      doRequestTotalExecutors(initialExecutors)
    }
  }

  override def stop(): Unit = {
    // stop allocation of new resources and caches.
    allocatorExecutor.shutdown()
<<<<<<< HEAD
    shuffleManager.foreach(_.stop())
=======
    allocatorExecutor.awaitTermination(30, TimeUnit.SECONDS)
>>>>>>> master

    // send stop message to executors so they shut down cleanly
    super.stop()

<<<<<<< HEAD
    // then delete the executor pods
    // TODO investigate why Utils.tryLogNonFatalError() doesn't work in this context.
    // When using Utils.tryLogNonFatalError some of the code fails but without any logs or
    // indication as to why.
    try {
      RUNNING_EXECUTOR_PODS_LOCK.synchronized {
        runningExecutorsToPods.values.foreach(kubernetesClient.pods().delete(_))
        runningExecutorsToPods.clear()
        runningPodsToExecutors.clear()
      }
      executorPodsByIPs.clear()
=======
    try {
>>>>>>> master
      val resource = executorWatchResource.getAndSet(null)
      if (resource != null) {
        resource.close()
      }
    } catch {
<<<<<<< HEAD
      case e: Throwable => logError("Uncaught exception while shutting down controllers.", e)
    }
    try {
      logInfo("Closing kubernetes client")
      kubernetesClient.close()
    } catch {
      case e: Throwable => logError("Uncaught exception closing Kubernetes client.", e)
=======
      case e: Throwable => logWarning("Failed to close the executor pod watcher", e)
    }

    // then delete the executor pods
    Utils.tryLogNonFatalError {
      deleteExecutorPodsOnStop()
      executorPodsByIPs.clear()
    }
    Utils.tryLogNonFatalError {
      logInfo("Closing kubernetes client")
      kubernetesClient.close()
>>>>>>> master
    }
  }

  /**
   * @return A map of K8s cluster nodes to the number of tasks that could benefit from data
   *         locality if an executor launches on the cluster node.
   */
  private def getNodesWithLocalTaskCounts() : Map[String, Int] = {
<<<<<<< HEAD
    val nodeToLocalTaskCount = mutable.Map[String, Int]() ++
      KubernetesClusterSchedulerBackend.this.synchronized {
        hostToLocalTaskCount
      }
=======
    val nodeToLocalTaskCount = synchronized {
      mutable.Map[String, Int]() ++ hostToLocalTaskCount
    }

>>>>>>> master
    for (pod <- executorPodsByIPs.values().asScala) {
      // Remove cluster nodes that are running our executors already.
      // TODO: This prefers spreading out executors across nodes. In case users want
      // consolidating executors on fewer nodes, introduce a flag. See the spark.deploy.spreadOut
      // flag that Spark standalone has: https://spark.apache.org/docs/latest/spark-standalone.html
      nodeToLocalTaskCount.remove(pod.getSpec.getNodeName).nonEmpty ||
        nodeToLocalTaskCount.remove(pod.getStatus.getHostIP).nonEmpty ||
        nodeToLocalTaskCount.remove(
          InetAddress.getByName(pod.getStatus.getHostIP).getCanonicalHostName).nonEmpty
    }
    nodeToLocalTaskCount.toMap[String, Int]
  }

<<<<<<< HEAD
  /**
   * Allocates a new executor pod
   *
   * @param nodeToLocalTaskCount  A map of K8s cluster nodes to the number of tasks that could
   *                              benefit from data locality if an executor launches on the cluster
   *                              node.
   * @return A tuple of the new executor name and the Pod data structure.
   */
  private def allocateNewExecutorPod(nodeToLocalTaskCount: Map[String, Int]): (String, Pod) = {
    val executorId = EXECUTOR_ID_COUNTER.incrementAndGet().toString
    val executorPod = executorPodFactory.createExecutorPod(
        executorId,
        applicationId(),
        driverUrl,
        conf.getExecutorEnv,
        driverPod,
        nodeToLocalTaskCount)
    try {
      (executorId, kubernetesClient.pods.create(executorPod))
    } catch {
      case throwable: Throwable =>
        logError("Failed to allocate executor pod.", throwable)
        throw throwable
    }
  }

=======
>>>>>>> master
  override def doRequestTotalExecutors(requestedTotal: Int): Future[Boolean] = Future[Boolean] {
    totalExpectedExecutors.set(requestedTotal)
    true
  }

  override def doKillExecutors(executorIds: Seq[String]): Future[Boolean] = Future[Boolean] {
<<<<<<< HEAD
    RUNNING_EXECUTOR_PODS_LOCK.synchronized {
      for (executor <- executorIds) {
        val maybeRemovedExecutor = runningExecutorsToPods.remove(executor)
        maybeRemovedExecutor.foreach { executorPod =>
          kubernetesClient.pods().delete(executorPod)
          disconnectedPodsByExecutorIdPendingRemoval.put(executor, executorPod)
          runningPodsToExecutors.remove(executorPod.getMetadata.getName)
        }
        if (maybeRemovedExecutor.isEmpty) {
          logWarning(s"Unable to remove pod for unknown executor $executor")
        }
      }
    }
    true
  }

  def getExecutorPodByIP(podIP: String): Option[Pod] = {
    // Note: Per https://github.com/databricks/scala-style-guide#concurrency, we don't
    // want to be switching to scala.collection.concurrent.Map on
    // executorPodsByIPs.
      val pod = executorPodsByIPs.get(podIP)
      Option(pod)
=======
    val podsToDelete = RUNNING_EXECUTOR_PODS_LOCK.synchronized {
      executorIds.flatMap { executorId =>
        runningExecutorsToPods.remove(executorId) match {
          case Some(pod) =>
            disconnectedPodsByExecutorIdPendingRemoval.put(executorId, pod)
            Some(pod)

          case None =>
            logWarning(s"Unable to remove pod for unknown executor $executorId")
            None
        }
      }
    }

    kubernetesClient.pods().delete(podsToDelete: _*)
    true
  }

  private def deleteExecutorPodsOnStop(): Unit = {
    val executorPodsToDelete = RUNNING_EXECUTOR_PODS_LOCK.synchronized {
      val runningExecutorPodsCopy = Seq(runningExecutorsToPods.values.toSeq: _*)
      runningExecutorsToPods.clear()
      runningExecutorPodsCopy
    }
    kubernetesClient.pods().delete(executorPodsToDelete: _*)
>>>>>>> master
  }

  private class ExecutorPodsWatcher extends Watcher[Pod] {

    private val DEFAULT_CONTAINER_FAILURE_EXIT_STATUS = -1

    override def eventReceived(action: Action, pod: Pod): Unit = {
<<<<<<< HEAD
      if (action == Action.MODIFIED && pod.getStatus.getPhase == "Running"
          && pod.getMetadata.getDeletionTimestamp == null) {
        val podIP = pod.getStatus.getPodIP
        val clusterNodeName = pod.getSpec.getNodeName
        logDebug(s"Executor pod $pod ready, launched at $clusterNodeName as IP $podIP.")
          executorPodsByIPs.put(podIP, pod)
      } else if ((action == Action.MODIFIED && pod.getMetadata.getDeletionTimestamp != null) ||
          action == Action.DELETED || action == Action.ERROR) {
        val podName = pod.getMetadata.getName
        val podIP = pod.getStatus.getPodIP
        logDebug(s"Executor pod $podName at IP $podIP was at $action.")
        if (podIP != null) {
          executorPodsByIPs.remove(podIP)
        }
        if (action == Action.ERROR) {
          logInfo(s"Received pod $podName exited event. Reason: " + pod.getStatus.getReason)
          handleErroredPod(pod)
        } else if (action == Action.DELETED) {
          logInfo(s"Received delete pod $podName event. Reason: " + pod.getStatus.getReason)
          handleDeletedPod(pod)
        }
=======
      val podName = pod.getMetadata.getName
      val podIP = pod.getStatus.getPodIP

      action match {
        case Action.MODIFIED if (pod.getStatus.getPhase == "Running"
            && pod.getMetadata.getDeletionTimestamp == null) =>
          val clusterNodeName = pod.getSpec.getNodeName
          logInfo(s"Executor pod $podName ready, launched at $clusterNodeName as IP $podIP.")
          executorPodsByIPs.put(podIP, pod)

        case Action.DELETED | Action.ERROR =>
          val executorId = getExecutorId(pod)
          logDebug(s"Executor pod $podName at IP $podIP was at $action.")
          if (podIP != null) {
            executorPodsByIPs.remove(podIP)
          }

          val executorExitReason = if (action == Action.ERROR) {
            logWarning(s"Received error event of executor pod $podName. Reason: " +
              pod.getStatus.getReason)
            executorExitReasonOnError(pod)
          } else if (action == Action.DELETED) {
            logWarning(s"Received delete event of executor pod $podName. Reason: " +
              pod.getStatus.getReason)
            executorExitReasonOnDelete(pod)
          } else {
            throw new IllegalStateException(
              s"Unknown action that should only be DELETED or ERROR: $action")
          }
          podsWithKnownExitReasons.put(pod.getMetadata.getName, executorExitReason)

          if (!disconnectedPodsByExecutorIdPendingRemoval.containsKey(executorId)) {
            log.warn(s"Executor with id $executorId was not marked as disconnected, but the " +
              s"watch received an event of type $action for this executor. The executor may " +
              "have failed to start in the first place and never registered with the driver.")
          }
          disconnectedPodsByExecutorIdPendingRemoval.put(executorId, pod)

        case _ => logDebug(s"Received event of executor pod $podName: " + action)
>>>>>>> master
      }
    }

    override def onClose(cause: KubernetesClientException): Unit = {
      logDebug("Executor pod watch closed.", cause)
    }

<<<<<<< HEAD
    def getExecutorExitStatus(pod: Pod): Int = {
=======
    private def getExecutorExitStatus(pod: Pod): Int = {
>>>>>>> master
      val containerStatuses = pod.getStatus.getContainerStatuses
      if (!containerStatuses.isEmpty) {
        // we assume the first container represents the pod status. This assumption may not hold
        // true in the future. Revisit this if side-car containers start running inside executor
        // pods.
        getExecutorExitStatus(containerStatuses.get(0))
      } else DEFAULT_CONTAINER_FAILURE_EXIT_STATUS
    }

<<<<<<< HEAD
    def getExecutorExitStatus(containerStatus: ContainerStatus): Int = {
      Option(containerStatus.getState).map(containerState =>
        Option(containerState.getTerminated).map(containerStateTerminated =>
          containerStateTerminated.getExitCode.intValue()).getOrElse(UNKNOWN_EXIT_CODE)
      ).getOrElse(UNKNOWN_EXIT_CODE)
    }

    def isPodAlreadyReleased(pod: Pod): Boolean = {
      RUNNING_EXECUTOR_PODS_LOCK.synchronized {
        !runningPodsToExecutors.contains(pod.getMetadata.getName)
      }
    }

    def handleErroredPod(pod: Pod): Unit = {
      val containerExitStatus = getExecutorExitStatus(pod)
      // container was probably actively killed by the driver.
      val exitReason = if (isPodAlreadyReleased(pod)) {
          ExecutorExited(containerExitStatus, exitCausedByApp = false,
            s"Container in pod " + pod.getMetadata.getName +
              " exited from explicit termination request.")
        } else {
          val containerExitReason = containerExitStatus match {
            case VMEM_EXCEEDED_EXIT_CODE | PMEM_EXCEEDED_EXIT_CODE =>
              memLimitExceededLogMessage(pod.getStatus.getReason)
            case _ =>
              // Here we can't be sure that that exit was caused by the application but this seems
              // to be the right default since we know the pod was not explicitly deleted by
              // the user.
              s"Pod ${pod.getMetadata.getName}'s executor container exited with exit status" +
                s" code $containerExitStatus."
          }
          ExecutorExited(containerExitStatus, exitCausedByApp = true, containerExitReason)
        }
        podsWithKnownExitReasons.put(pod.getMetadata.getName, exitReason)
    }

    def handleDeletedPod(pod: Pod): Unit = {
=======
    private def getExecutorExitStatus(containerStatus: ContainerStatus): Int = {
      Option(containerStatus.getState).map { containerState =>
        Option(containerState.getTerminated).map { containerStateTerminated =>
          containerStateTerminated.getExitCode.intValue()
        }.getOrElse(UNKNOWN_EXIT_CODE)
      }.getOrElse(UNKNOWN_EXIT_CODE)
    }

    private def isPodAlreadyReleased(pod: Pod): Boolean = {
      val executorId = pod.getMetadata.getLabels.get(SPARK_EXECUTOR_ID_LABEL)
      RUNNING_EXECUTOR_PODS_LOCK.synchronized {
        !runningExecutorsToPods.contains(executorId)
      }
    }

    private def executorExitReasonOnError(pod: Pod): ExecutorExited = {
      val containerExitStatus = getExecutorExitStatus(pod)
      // container was probably actively killed by the driver.
      if (isPodAlreadyReleased(pod)) {
        ExecutorExited(containerExitStatus, exitCausedByApp = false,
          s"Container in pod ${pod.getMetadata.getName} exited from explicit termination " +
            "request.")
      } else {
        val containerExitReason = s"Pod ${pod.getMetadata.getName}'s executor container " +
          s"exited with exit status code $containerExitStatus."
        ExecutorExited(containerExitStatus, exitCausedByApp = true, containerExitReason)
      }
    }

    private def executorExitReasonOnDelete(pod: Pod): ExecutorExited = {
>>>>>>> master
      val exitMessage = if (isPodAlreadyReleased(pod)) {
        s"Container in pod ${pod.getMetadata.getName} exited from explicit termination request."
      } else {
        s"Pod ${pod.getMetadata.getName} deleted or lost."
      }
<<<<<<< HEAD
      val exitReason = ExecutorExited(
          getExecutorExitStatus(pod), exitCausedByApp = false, exitMessage)
      podsWithKnownExitReasons.put(pod.getMetadata.getName, exitReason)
=======
      ExecutorExited(getExecutorExitStatus(pod), exitCausedByApp = false, exitMessage)
    }

    private def getExecutorId(pod: Pod): String = {
      val executorId = pod.getMetadata.getLabels.get(SPARK_EXECUTOR_ID_LABEL)
      require(executorId != null, "Unexpected pod metadata; expected all executor pods " +
        s"to have label $SPARK_EXECUTOR_ID_LABEL.")
      executorId
>>>>>>> master
    }
  }

  override def createDriverEndpoint(properties: Seq[(String, String)]): DriverEndpoint = {
    new KubernetesDriverEndpoint(rpcEnv, properties)
  }

  private class KubernetesDriverEndpoint(
<<<<<<< HEAD
    rpcEnv: RpcEnv,
    sparkProperties: Seq[(String, String)])
=======
      rpcEnv: RpcEnv,
      sparkProperties: Seq[(String, String)])
>>>>>>> master
    extends DriverEndpoint(rpcEnv, sparkProperties) {

    override def onDisconnected(rpcAddress: RpcAddress): Unit = {
      addressToExecutorId.get(rpcAddress).foreach { executorId =>
        if (disableExecutor(executorId)) {
          RUNNING_EXECUTOR_PODS_LOCK.synchronized {
            runningExecutorsToPods.get(executorId).foreach { pod =>
              disconnectedPodsByExecutorIdPendingRemoval.put(executorId, pod)
            }
          }
        }
      }
    }
<<<<<<< HEAD

    override def receiveAndReply(
      context: RpcCallContext): PartialFunction[Any, Unit] = {
      new PartialFunction[Any, Unit]() {
        override def isDefinedAt(msg: Any): Boolean = {
          msg match {
            case RetrieveSparkAppConfig(_) =>
              shuffleManager.isDefined
            case _ => false
          }
        }

        override def apply(msg: Any): Unit = {
          msg match {
            case RetrieveSparkAppConfig(executorId) if shuffleManager.isDefined =>
              val runningExecutorPod = RUNNING_EXECUTOR_PODS_LOCK.synchronized {
                kubernetesClient
                  .pods()
                  .withName(runningExecutorsToPods(executorId).getMetadata.getName)
                  .get()
              }
              val shuffleSpecificProperties = shuffleManager.get
                  .getShuffleServiceConfigurationForExecutor(runningExecutorPod)
              val reply = SparkAppConfig(
                  sparkProperties ++ shuffleSpecificProperties,
                  SparkEnv.get.securityManager.getIOEncryptionKey(),
                  fetchHadoopDelegationTokens())
              context.reply(reply)
          }
        }
      }.orElse(super.receiveAndReply(context))
    }
=======
>>>>>>> master
  }
}

private object KubernetesClusterSchedulerBackend {
<<<<<<< HEAD
  private val VMEM_EXCEEDED_EXIT_CODE = -103
  private val PMEM_EXCEEDED_EXIT_CODE = -104
  private val UNKNOWN_EXIT_CODE = -111
  // Number of times we are allowed check for the loss reason for an executor before we give up
  // and assume the executor failed for good, and attribute it to a framework fault.
  val MAX_EXECUTOR_LOST_REASON_CHECKS = 10

  def memLimitExceededLogMessage(diagnostics: String): String = {
    s"Pod/Container killed for exceeding memory limits. $diagnostics" +
      " Consider boosting spark executor memory overhead."
  }
}

=======
  private val UNKNOWN_EXIT_CODE = -1
}
>>>>>>> master
