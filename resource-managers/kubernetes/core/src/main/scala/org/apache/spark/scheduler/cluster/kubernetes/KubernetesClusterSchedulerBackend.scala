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
package org.apache.spark.scheduler.cluster.kubernetes

import java.io.Closeable
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong, AtomicReference}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

import io.fabric8.kubernetes.api.model.{ContainerPortBuilder, EnvVarBuilder,
    EnvVarSourceBuilder, Pod, QuantityBuilder}
import io.fabric8.kubernetes.client.{KubernetesClientException, Watcher}
import io.fabric8.kubernetes.client.Watcher.Action

import org.apache.spark.{SparkContext, SparkException}
import org.apache.spark.deploy.kubernetes.config._
import org.apache.spark.deploy.kubernetes.constants._
import org.apache.spark.rpc.RpcEndpointAddress
import org.apache.spark.scheduler.TaskSchedulerImpl
import org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend
import org.apache.spark.util.{ThreadUtils, Utils}

private[spark] class KubernetesClusterSchedulerBackend(
    scheduler: TaskSchedulerImpl,
    val sc: SparkContext)
  extends CoarseGrainedSchedulerBackend(scheduler, sc.env.rpcEnv) {

  import KubernetesClusterSchedulerBackend._

  private val RUNNING_EXECUTOR_PODS_LOCK = new Object
  private val runningExecutorPods = new mutable.HashMap[String, Pod] // Indexed by executor IDs.

  private val EXECUTOR_PODS_BY_IPS_LOCK = new Object
  private val executorPodsByIPs = new mutable.HashMap[String, Pod] // Indexed by executor IP addrs.

  private val executorDockerImage = conf.get(EXECUTOR_DOCKER_IMAGE)
  private val kubernetesNamespace = conf.get(KUBERNETES_NAMESPACE)
  private val executorPort = conf.getInt("spark.executor.port", DEFAULT_STATIC_PORT)
  private val blockmanagerPort = conf
    .getInt("spark.blockmanager.port", DEFAULT_BLOCKMANAGER_PORT)

  private val kubernetesDriverPodName = conf
    .get(KUBERNETES_DRIVER_POD_NAME)
    .getOrElse(
      throw new SparkException("Must specify the driver pod name"))

  private val executorMemoryMb = conf.get(org.apache.spark.internal.config.EXECUTOR_MEMORY)
  private val executorMemoryString = conf.get(
    org.apache.spark.internal.config.EXECUTOR_MEMORY.key,
    org.apache.spark.internal.config.EXECUTOR_MEMORY.defaultValueString)

  private val memoryOverheadMb = conf
    .get(KUBERNETES_EXECUTOR_MEMORY_OVERHEAD)
    .getOrElse(math.max((MEMORY_OVERHEAD_FACTOR * executorMemoryMb).toInt,
      MEMORY_OVERHEAD_MIN))
  private val executorMemoryWithOverhead = executorMemoryMb + memoryOverheadMb

  private val executorCores = conf.getOption("spark.executor.cores").getOrElse("1")

  private implicit val requestExecutorContext = ExecutionContext.fromExecutorService(
    ThreadUtils.newDaemonCachedThreadPool("kubernetes-executor-requests"))

  private val kubernetesClient = new DriverPodKubernetesClientProvider(conf, kubernetesNamespace)
    .get

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
    sc.getConf.get("spark.driver.host"),
    sc.getConf.getInt("spark.driver.port", DEFAULT_DRIVER_PORT),
    CoarseGrainedSchedulerBackend.ENDPOINT_NAME).toString

  private val initialExecutors = getInitialTargetExecutorNumber(1)

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
    executorWatchResource.set(kubernetesClient.pods().withLabel(SPARK_APP_ID_LABEL, applicationId())
      .watch(new ExecutorPodsWatcher()))
    if (!Utils.isDynamicAllocationEnabled(sc.conf)) {
      doRequestTotalExecutors(initialExecutors)
    }
  }

  override def stop(): Unit = {
    // send stop message to executors so they shut down cleanly
    super.stop()

    // then delete the executor pods
    // TODO investigate why Utils.tryLogNonFatalError() doesn't work in this context.
    // When using Utils.tryLogNonFatalError some of the code fails but without any logs or
    // indication as to why.
    try {
      RUNNING_EXECUTOR_PODS_LOCK.synchronized {
        runningExecutorPods.values.foreach(kubernetesClient.pods().delete(_))
        runningExecutorPods.clear()
      }
      EXECUTOR_PODS_BY_IPS_LOCK.synchronized {
        executorPodsByIPs.clear()
      }
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

  private def allocateNewExecutorPod(): (String, Pod) = {
    val executorId = EXECUTOR_ID_COUNTER.incrementAndGet().toString
    val name = s"${applicationId()}-exec-$executorId"

    // hostname must be no longer than 63 characters, so take the last 63 characters of the pod
    // name as the hostname.  This preserves uniqueness since the end of name contains
    // executorId and applicationId
    val hostname = name.substring(Math.max(0, name.length - 63))

    val selectors = Map(SPARK_EXECUTOR_ID_LABEL -> executorId,
      SPARK_APP_ID_LABEL -> applicationId()).asJava
    val executorMemoryQuantity = new QuantityBuilder(false)
      .withAmount(s"${executorMemoryMb}M")
      .build()
    val executorMemoryLimitQuantity = new QuantityBuilder(false)
      .withAmount(s"${executorMemoryWithOverhead}M")
      .build()
    val executorCpuQuantity = new QuantityBuilder(false)
      .withAmount(executorCores)
      .build()
    val requiredEnv = Seq(
      (ENV_EXECUTOR_PORT, executorPort.toString),
      (ENV_DRIVER_URL, driverUrl),
      (ENV_EXECUTOR_CORES, executorCores),
      (ENV_EXECUTOR_MEMORY, executorMemoryString),
      (ENV_APPLICATION_ID, applicationId()),
      (ENV_EXECUTOR_ID, executorId))
      .map(env => new EnvVarBuilder()
        .withName(env._1)
        .withValue(env._2)
        .build()
      ) ++ Seq(
      new EnvVarBuilder()
        .withName(ENV_EXECUTOR_POD_IP)
        .withValueFrom(new EnvVarSourceBuilder()
          .withNewFieldRef("v1", "status.podIP")
          .build())
        .build()
      )
    val requiredPorts = Seq(
      (EXECUTOR_PORT_NAME, executorPort),
      (BLOCK_MANAGER_PORT_NAME, blockmanagerPort))
      .map(port => {
        new ContainerPortBuilder()
          .withName(port._1)
          .withContainerPort(port._2)
          .build()
      })
    try {
      (executorId, kubernetesClient.pods().createNew()
        .withNewMetadata()
          .withName(name)
          .withLabels(selectors)
          .withOwnerReferences()
          .addNewOwnerReference()
            .withController(true)
            .withApiVersion(driverPod.getApiVersion)
            .withKind(driverPod.getKind)
            .withName(driverPod.getMetadata.getName)
            .withUid(driverPod.getMetadata.getUid)
          .endOwnerReference()
        .endMetadata()
        .withNewSpec()
          .withHostname(hostname)
          .addNewContainer()
            .withName(s"executor")
            .withImage(executorDockerImage)
            .withImagePullPolicy("IfNotPresent")
            .withNewResources()
              .addToRequests("memory", executorMemoryQuantity)
              .addToLimits("memory", executorMemoryLimitQuantity)
              .addToRequests("cpu", executorCpuQuantity)
              .addToLimits("cpu", executorCpuQuantity)
              .endResources()
            .withEnv(requiredEnv.asJava)
            .withPorts(requiredPorts.asJava)
            .endContainer()
          .endSpec()
        .done())
    } catch {
      case throwable: Throwable =>
        logError("Failed to allocate executor pod.", throwable)
        throw throwable
    }
  }

  override def doRequestTotalExecutors(requestedTotal: Int): Future[Boolean] = Future[Boolean] {
    RUNNING_EXECUTOR_PODS_LOCK.synchronized {
      if (requestedTotal > totalExpectedExecutors.get) {
        logInfo(s"Requesting ${requestedTotal - totalExpectedExecutors.get}"
          + s" additional executors, expecting total $requestedTotal and currently" +
          s" expected ${totalExpectedExecutors.get}")
        for (i <- 0 until (requestedTotal - totalExpectedExecutors.get)) {
          runningExecutorPods += allocateNewExecutorPod()
        }
      }
      totalExpectedExecutors.set(requestedTotal)
    }
    true
  }

  override def doKillExecutors(executorIds: Seq[String]): Future[Boolean] = Future[Boolean] {
    RUNNING_EXECUTOR_PODS_LOCK.synchronized {
      for (executor <- executorIds) {
        runningExecutorPods.remove(executor) match {
          case Some(pod) => kubernetesClient.pods().delete(pod)
          case None => logWarning(s"Unable to remove pod for unknown executor $executor")
        }
      }
    }
    true
  }

  def getExecutorPodByIP(podIP: String): Option[Pod] = {
    EXECUTOR_PODS_BY_IPS_LOCK.synchronized {
      executorPodsByIPs.get(podIP)
    }
  }

  private class ExecutorPodsWatcher extends Watcher[Pod] {

    override def eventReceived(action: Action, pod: Pod): Unit = {
      if (action == Action.MODIFIED && pod.getStatus.getPhase == "Running"
          && pod.getMetadata.getDeletionTimestamp == null) {
        val podIP = pod.getStatus.getPodIP
        val clusterNodeName = pod.getSpec.getNodeName
        logDebug(s"Executor pod $pod ready, launched at $clusterNodeName as IP $podIP.")
        EXECUTOR_PODS_BY_IPS_LOCK.synchronized {
          executorPodsByIPs += ((podIP, pod))
        }
      } else if ((action == Action.MODIFIED && pod.getMetadata.getDeletionTimestamp != null) ||
          action == Action.DELETED || action == Action.ERROR) {
        val podName = pod.getMetadata.getName
        val podIP = pod.getStatus.getPodIP
        logDebug(s"Executor pod $podName at IP $podIP was at $action.")
        if (podIP != null) {
          EXECUTOR_PODS_BY_IPS_LOCK.synchronized {
            executorPodsByIPs -= podIP
          }
        }
      }
    }

    override def onClose(cause: KubernetesClientException): Unit = {
      logDebug("Executor pod watch closed.", cause)
    }
  }
}

private object KubernetesClusterSchedulerBackend {
  private val DEFAULT_STATIC_PORT = 10000
  private val EXECUTOR_ID_COUNTER = new AtomicLong(0L)
}
