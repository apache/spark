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
import java.util.Collections
import java.util.concurrent.{ConcurrentHashMap, ExecutorService, ScheduledExecutorService, ThreadPoolExecutor, TimeUnit}
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong, AtomicReference}

import io.fabric8.kubernetes.api.model._
import io.fabric8.kubernetes.client.{KubernetesClient, KubernetesClientException, Watcher}
import io.fabric8.kubernetes.client.Watcher.Action
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

import org.apache.spark.{SparkEnv, SparkException}
import org.apache.spark.deploy.k8s.config._
import org.apache.spark.deploy.k8s.constants._
import org.apache.spark.rpc.{RpcAddress, RpcCallContext, RpcEndpointAddress, RpcEnv}
import org.apache.spark.scheduler.{ExecutorExited, SlaveLost, TaskSchedulerImpl}
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.{RetrieveSparkAppConfig, SparkAppConfig}
import org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend
import org.apache.spark.util.Utils

private[spark] class KubernetesClusterSchedulerBackend(
    scheduler: TaskSchedulerImpl,
    rpcEnv: RpcEnv,
    executorPodFactory: ExecutorPodFactory,
    shuffleManager: Option[KubernetesExternalShuffleManager],
    kubernetesClient: KubernetesClient,
    allocatorExecutor: ScheduledExecutorService,
    requestExecutorsService: ExecutorService)
  extends CoarseGrainedSchedulerBackend(scheduler, rpcEnv) {

  import KubernetesClusterSchedulerBackend._

  private val EXECUTOR_ID_COUNTER = new AtomicLong(0L)
  private val RUNNING_EXECUTOR_PODS_LOCK = new Object
  // Indexed by executor IDs and guarded by RUNNING_EXECUTOR_PODS_LOCK.
  private val runningExecutorsToPods = new mutable.HashMap[String, Pod]
  // Indexed by executor pod names and guarded by RUNNING_EXECUTOR_PODS_LOCK.
  private val runningPodsToExecutors = new mutable.HashMap[String, String]
  private val executorPodsByIPs = new ConcurrentHashMap[String, Pod]()
  private val podsWithKnownExitReasons = new ConcurrentHashMap[String, ExecutorExited]()
  private val disconnectedPodsByExecutorIdPendingRemoval = new ConcurrentHashMap[String, Pod]()

  private val kubernetesNamespace = conf.get(KUBERNETES_NAMESPACE)

  private val kubernetesDriverPodName = conf
    .get(KUBERNETES_DRIVER_POD_NAME)
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
    if (conf.getOption("spark.scheduler.minRegisteredResourcesRatio").isEmpty) {
      0.8
    } else {
      super.minRegisteredRatio
    }

  private val executorWatchResource = new AtomicReference[Closeable]
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

  private val allocatorRunnable = new Runnable {

    // Maintains a map of executor id to count of checks performed to learn the loss reason
    // for an executor.
    private val executorReasonCheckAttemptCounts = new mutable.HashMap[String, Int]

    override def run(): Unit = {
      handleDisconnectedExecutors()
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
            logInfo(
              s"Requesting a new executor, total executors is now ${runningExecutorsToPods.size}")
          }
        }
      }
    }

    def handleDisconnectedExecutors(): Unit = {
      // For each disconnected executor, synchronize with the loss reasons that may have been found
      // by the executor pod watcher. If the loss reason was discovered by the watcher,
      // inform the parent class with removeExecutor.
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
      }
    }

    def removeExecutorOrIncrementLossReasonCheckCount(executorId: String): Unit = {
      val reasonCheckCount = executorReasonCheckAttemptCounts.getOrElse(executorId, 0)
      if (reasonCheckCount >= MAX_EXECUTOR_LOST_REASON_CHECKS) {
        removeExecutor(executorId, SlaveLost("Executor lost for unknown reasons."))
        deleteExecutorFromClusterAndDataStructures(executorId)
      } else {
        executorReasonCheckAttemptCounts.put(executorId, reasonCheckCount + 1)
      }
    }

    def deleteExecutorFromClusterAndDataStructures(executorId: String): Unit = {
      disconnectedPodsByExecutorIdPendingRemoval.remove(executorId)
      executorReasonCheckAttemptCounts -= executorId
      RUNNING_EXECUTOR_PODS_LOCK.synchronized {
        runningExecutorsToPods.remove(executorId).map { pod =>
          kubernetesClient.pods().delete(pod)
          runningPodsToExecutors.remove(pod.getMetadata.getName)
        }.getOrElse(logWarning(s"Unable to remove pod for unknown executor $executorId"))
      }
    }
  }

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

  override def sufficientResourcesRegistered(): Boolean = {
    totalRegisteredExecutors.get() >= initialExecutors * minRegisteredRatio
  }

  override def start(): Unit = {
    super.start()
    executorWatchResource.set(
        kubernetesClient
            .pods()
            .withLabel(SPARK_APP_ID_LABEL, applicationId())
            .watch(new ExecutorPodsWatcher()))

    allocatorExecutor.scheduleWithFixedDelay(
        allocatorRunnable, 0L, podAllocationInterval, TimeUnit.SECONDS)
    shuffleManager.foreach(_.start(applicationId()))

    if (!Utils.isDynamicAllocationEnabled(conf)) {
      doRequestTotalExecutors(initialExecutors)
    }
  }

  override def stop(): Unit = {
    // stop allocation of new resources and caches.
    allocatorExecutor.shutdown()
    shuffleManager.foreach(_.stop())

    // send stop message to executors so they shut down cleanly
    super.stop()

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
      val resource = executorWatchResource.getAndSet(null)
      if (resource != null) {
        resource.close()
      }
    } catch {
      case e: Throwable => logError("Uncaught exception while shutting down controllers.", e)
    }
    try {
      logInfo("Closing kubernetes client")
      kubernetesClient.close()
    } catch {
      case e: Throwable => logError("Uncaught exception closing Kubernetes client.", e)
    }
  }

  /**
   * @return A map of K8s cluster nodes to the number of tasks that could benefit from data
   *         locality if an executor launches on the cluster node.
   */
  private def getNodesWithLocalTaskCounts() : Map[String, Int] = {
    val nodeToLocalTaskCount = mutable.Map[String, Int]() ++
      KubernetesClusterSchedulerBackend.this.synchronized {
        hostToLocalTaskCount
      }
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

  override def doRequestTotalExecutors(requestedTotal: Int): Future[Boolean] = Future[Boolean] {
    totalExpectedExecutors.set(requestedTotal)
    true
  }

  override def doKillExecutors(executorIds: Seq[String]): Future[Boolean] = Future[Boolean] {
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
  }

  private class ExecutorPodsWatcher extends Watcher[Pod] {

    private val DEFAULT_CONTAINER_FAILURE_EXIT_STATUS = -1

    override def eventReceived(action: Action, pod: Pod): Unit = {
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
      }
    }

    override def onClose(cause: KubernetesClientException): Unit = {
      logDebug("Executor pod watch closed.", cause)
    }

    def getExecutorExitStatus(pod: Pod): Int = {
      val containerStatuses = pod.getStatus.getContainerStatuses
      if (!containerStatuses.isEmpty) {
        // we assume the first container represents the pod status. This assumption may not hold
        // true in the future. Revisit this if side-car containers start running inside executor
        // pods.
        getExecutorExitStatus(containerStatuses.get(0))
      } else DEFAULT_CONTAINER_FAILURE_EXIT_STATUS
    }

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
      val exitMessage = if (isPodAlreadyReleased(pod)) {
        s"Container in pod ${pod.getMetadata.getName} exited from explicit termination request."
      } else {
        s"Pod ${pod.getMetadata.getName} deleted or lost."
      }
      val exitReason = ExecutorExited(
          getExecutorExitStatus(pod), exitCausedByApp = false, exitMessage)
      podsWithKnownExitReasons.put(pod.getMetadata.getName, exitReason)
    }
  }

  override def createDriverEndpoint(properties: Seq[(String, String)]): DriverEndpoint = {
    new KubernetesDriverEndpoint(rpcEnv, properties)
  }

  private class KubernetesDriverEndpoint(
    rpcEnv: RpcEnv,
    sparkProperties: Seq[(String, String)])
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
  }
}

private object KubernetesClusterSchedulerBackend {
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
