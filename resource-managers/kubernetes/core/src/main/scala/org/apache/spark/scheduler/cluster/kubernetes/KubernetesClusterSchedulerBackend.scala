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
import java.net.InetAddress
import java.util.Collections
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong, AtomicReference}

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import io.fabric8.kubernetes.api.model._
import io.fabric8.kubernetes.client.{KubernetesClient, KubernetesClientException, Watcher}
import io.fabric8.kubernetes.client.Watcher.Action
import org.apache.commons.io.FilenameUtils
import scala.collection.{concurrent, mutable}
import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}

import org.apache.spark.{SparkContext, SparkEnv, SparkException}
import org.apache.spark.deploy.kubernetes.{ConfigurationUtils, InitContainerResourceStagingServerSecretPlugin, PodWithDetachedInitContainer, SparkPodInitContainerBootstrap}
import org.apache.spark.deploy.kubernetes.config._
import org.apache.spark.deploy.kubernetes.constants._
import org.apache.spark.deploy.kubernetes.submit.{InitContainerUtil, MountSmallFilesBootstrap}
import org.apache.spark.internal.config
import org.apache.spark.network.netty.SparkTransportConf
import org.apache.spark.network.shuffle.kubernetes.KubernetesExternalShuffleClient
import org.apache.spark.rpc.{RpcAddress, RpcCallContext, RpcEndpointAddress, RpcEnv}
import org.apache.spark.scheduler.{ExecutorExited, SlaveLost, TaskSchedulerImpl}
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.{RetrieveSparkAppConfig, SparkAppConfig}
import org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend
import org.apache.spark.util.{ThreadUtils, Utils}

private[spark] class KubernetesClusterSchedulerBackend(
    scheduler: TaskSchedulerImpl,
    val sc: SparkContext,
    executorInitContainerBootstrap: Option[SparkPodInitContainerBootstrap],
    executorMountInitContainerSecretPlugin: Option[InitContainerResourceStagingServerSecretPlugin],
    mountSmallFilesBootstrap: Option[MountSmallFilesBootstrap],
    kubernetesClient: KubernetesClient)
  extends CoarseGrainedSchedulerBackend(scheduler, sc.env.rpcEnv) {

  import KubernetesClusterSchedulerBackend._

  private val RUNNING_EXECUTOR_PODS_LOCK = new Object
  // Indexed by executor IDs and guarded by RUNNING_EXECUTOR_PODS_LOCK.
  private val runningExecutorsToPods = new mutable.HashMap[String, Pod]
  // Indexed by executor pod names and guarded by RUNNING_EXECUTOR_PODS_LOCK.
  private val runningPodsToExecutors = new mutable.HashMap[String, String]
  // TODO(varun): Get rid of this lock object by my making the underlying map a concurrent hash map.
  private val EXECUTOR_PODS_BY_IPS_LOCK = new Object
  // Indexed by executor IP addrs and guarded by EXECUTOR_PODS_BY_IPS_LOCK
  private val executorPodsByIPs = new mutable.HashMap[String, Pod]
  private val failedPods: concurrent.Map[String, ExecutorExited] = new
      ConcurrentHashMap[String, ExecutorExited]().asScala
  private val executorsToRemove = Collections.newSetFromMap[String](
    new ConcurrentHashMap[String, java.lang.Boolean]()).asScala

  private val executorExtraClasspath = conf.get(
    org.apache.spark.internal.config.EXECUTOR_CLASS_PATH)
  private val executorJarsDownloadDir = conf.get(INIT_CONTAINER_JARS_DOWNLOAD_LOCATION)

  private val executorLabels = ConfigurationUtils.combinePrefixedKeyValuePairsWithDeprecatedConf(
      conf,
      KUBERNETES_EXECUTOR_LABEL_PREFIX,
      KUBERNETES_EXECUTOR_LABELS,
      "executor label")
  require(
      !executorLabels.contains(SPARK_APP_ID_LABEL),
      s"Custom executor labels cannot contain $SPARK_APP_ID_LABEL as it is" +
        s" reserved for Spark.")
  require(
      !executorLabels.contains(SPARK_EXECUTOR_ID_LABEL),
      s"Custom executor labels cannot contain $SPARK_EXECUTOR_ID_LABEL as it is reserved for" +
        s" Spark.")

  private val executorAnnotations =
      ConfigurationUtils.combinePrefixedKeyValuePairsWithDeprecatedConf(
          conf,
          KUBERNETES_EXECUTOR_ANNOTATION_PREFIX,
          KUBERNETES_EXECUTOR_ANNOTATIONS,
          "executor annotation")
  private val nodeSelector =
      ConfigurationUtils.parsePrefixedKeyValuePairs(
          conf,
          KUBERNETES_NODE_SELECTOR_PREFIX,
          "node-selector")
  private var shufflePodCache: Option[ShufflePodCache] = None
  private val executorDockerImage = conf.get(EXECUTOR_DOCKER_IMAGE)
  private val dockerImagePullPolicy = conf.get(DOCKER_IMAGE_PULL_POLICY)
  private val kubernetesNamespace = conf.get(KUBERNETES_NAMESPACE)
  private val executorPort = conf.getInt("spark.executor.port", DEFAULT_STATIC_PORT)
  private val blockmanagerPort = conf
    .getInt("spark.blockmanager.port", DEFAULT_BLOCKMANAGER_PORT)

  private val kubernetesDriverPodName = conf
    .get(KUBERNETES_DRIVER_POD_NAME)
    .getOrElse(
      throw new SparkException("Must specify the driver pod name"))
  private val executorPodNamePrefix = conf.get(KUBERNETES_EXECUTOR_POD_NAME_PREFIX)

  private val executorMemoryMiB = conf.get(org.apache.spark.internal.config.EXECUTOR_MEMORY)
  private val executorMemoryString = conf.get(
    org.apache.spark.internal.config.EXECUTOR_MEMORY.key,
    org.apache.spark.internal.config.EXECUTOR_MEMORY.defaultValueString)

  private val memoryOverheadMiB = conf
    .get(KUBERNETES_EXECUTOR_MEMORY_OVERHEAD)
    .getOrElse(math.max((MEMORY_OVERHEAD_FACTOR * executorMemoryMiB).toInt,
      MEMORY_OVERHEAD_MIN_MIB))
  private val executorMemoryWithOverheadMiB = executorMemoryMiB + memoryOverheadMiB

  private val executorCores = conf.getDouble("spark.executor.cores", 1d)
  private val executorLimitCores = conf.getOption(KUBERNETES_EXECUTOR_LIMIT_CORES.key)

  private implicit val requestExecutorContext = ExecutionContext.fromExecutorService(
    ThreadUtils.newDaemonCachedThreadPool("kubernetes-executor-requests"))

  private val driverPod = try {
    kubernetesClient.pods().inNamespace(kubernetesNamespace).
      withName(kubernetesDriverPodName).get()
  } catch {
    case throwable: Throwable =>
      logError(s"Executor cannot find driver pod.", throwable)
      throw new SparkException(s"Executor cannot find driver pod", throwable)
  }

  private val shuffleServiceConfig: Option[ShuffleServiceConfig] =
    if (Utils.isDynamicAllocationEnabled(sc.conf)) {
      val shuffleNamespace = conf.get(KUBERNETES_SHUFFLE_NAMESPACE)
      val parsedShuffleLabels = ConfigurationUtils.parseKeyValuePairs(
        conf.get(KUBERNETES_SHUFFLE_LABELS), KUBERNETES_SHUFFLE_LABELS.key,
            "shuffle-labels")
      if (parsedShuffleLabels.isEmpty) {
        throw new SparkException(s"Dynamic allocation enabled " +
          s"but no ${KUBERNETES_SHUFFLE_LABELS.key} specified")
      }

      val shuffleDirs = conf.get(KUBERNETES_SHUFFLE_DIR).map {
        _.split(",")
      }.getOrElse(Utils.getConfiguredLocalDirs(conf))
      Some(
        ShuffleServiceConfig(shuffleNamespace,
          parsedShuffleLabels,
          shuffleDirs))
    } else {
      None
    }

  // A client for talking to the external shuffle service
  private val kubernetesExternalShuffleClient: Option[KubernetesExternalShuffleClient] = {
    if (Utils.isDynamicAllocationEnabled(sc.conf)) {
      Some(getShuffleClient())
    } else {
      None
    }
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

  private val initialExecutors = getInitialTargetExecutorNumber()

  private val podAllocationInterval = conf.get(KUBERNETES_ALLOCATION_BATCH_DELAY)
  require(podAllocationInterval > 0, s"Allocation batch delay " +
    s"${KUBERNETES_ALLOCATION_BATCH_DELAY} " +
    s"is ${podAllocationInterval}, should be a positive integer")

  private val podAllocationSize = conf.get(KUBERNETES_ALLOCATION_BATCH_SIZE)
  require(podAllocationSize > 0, s"Allocation batch size " +
    s"${KUBERNETES_ALLOCATION_BATCH_SIZE} " +
    s"is ${podAllocationSize}, should be a positive integer")

  private val allocator = ThreadUtils
    .newDaemonSingleThreadScheduledExecutor("kubernetes-pod-allocator")

  private val allocatorRunnable: Runnable = new Runnable {

    // Number of times we are allowed check for the loss reason for an executor before we give up
    // and assume the executor failed for good, and attribute it to a framework fault.
    private val MAX_EXECUTOR_LOST_REASON_CHECKS = 10
    private val executorsToRecover = new mutable.HashSet[String]
    // Maintains a map of executor id to count of checks performed to learn the loss reason
    // for an executor.
    private val executorReasonChecks = new mutable.HashMap[String, Int]

    override def run(): Unit = {
      removeFailedExecutors()
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

    def removeFailedExecutors(): Unit = {
      val localRunningExecutorsToPods = RUNNING_EXECUTOR_PODS_LOCK.synchronized {
        runningExecutorsToPods.toMap
      }
      executorsToRemove.foreach { case (executorId) =>
        localRunningExecutorsToPods.get(executorId).map { pod: Pod =>
          failedPods.get(pod.getMetadata.getName).map { executorExited: ExecutorExited =>
            logDebug(s"Removing executor $executorId with loss reason " + executorExited.message)
            removeExecutor(executorId, executorExited)
            if (!executorExited.exitCausedByApp) {
              executorsToRecover.add(executorId)
            }
          }.getOrElse(removeExecutorOrIncrementLossReasonCheckCount(executorId))
        }.getOrElse(removeExecutorOrIncrementLossReasonCheckCount(executorId))

        executorsToRecover.foreach(executorId => {
          executorsToRemove -= executorId
          executorReasonChecks -= executorId
          RUNNING_EXECUTOR_PODS_LOCK.synchronized {
            runningExecutorsToPods.remove(executorId).map { pod: Pod =>
              kubernetesClient.pods().delete(pod)
              runningPodsToExecutors.remove(pod.getMetadata.getName)
            }.getOrElse(logWarning(s"Unable to remove pod for unknown executor $executorId"))
          }
        })
        executorsToRecover.clear()
      }
    }

    def removeExecutorOrIncrementLossReasonCheckCount(executorId: String): Unit = {
      val reasonCheckCount = executorReasonChecks.getOrElse(executorId, 0)
      if (reasonCheckCount > MAX_EXECUTOR_LOST_REASON_CHECKS) {
        removeExecutor(executorId, SlaveLost("Executor lost for unknown reasons"))
        executorsToRecover.add(executorId)
        executorReasonChecks -= executorId
      } else {
        executorReasonChecks.put(executorId, reasonCheckCount + 1)
      }
    }
  }

  private val objectMapper = new ObjectMapper().registerModule(DefaultScalaModule)

  private def getShuffleClient(): KubernetesExternalShuffleClient = {
    new KubernetesExternalShuffleClient(
      SparkTransportConf.fromSparkConf(conf, "shuffle"),
      sc.env.securityManager,
      sc.env.securityManager.isAuthenticationEnabled(),
      conf.get(config.SHUFFLE_REGISTRATION_TIMEOUT))
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

    allocator.scheduleWithFixedDelay(
      allocatorRunnable, 0, podAllocationInterval, TimeUnit.SECONDS)

    if (!Utils.isDynamicAllocationEnabled(sc.conf)) {
      doRequestTotalExecutors(initialExecutors)
    } else {
      shufflePodCache = shuffleServiceConfig
        .map { config => new ShufflePodCache(
          kubernetesClient, config.shuffleNamespace, config.shuffleLabels) }
      shufflePodCache.foreach(_.start())
      kubernetesExternalShuffleClient.foreach(_.init(applicationId()))
    }
  }

  override def stop(): Unit = {
    // stop allocation of new resources and caches.
    allocator.shutdown()
    shufflePodCache.foreach(_.stop())
    kubernetesExternalShuffleClient.foreach(_.close())

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

  /**
   * @return A map of K8s cluster nodes to the number of tasks that could benefit from data
   *         locality if an executor launches on the cluster node.
   */
  private def getNodesWithLocalTaskCounts() : Map[String, Int] = {
    val executorPodsWithIPs = EXECUTOR_PODS_BY_IPS_LOCK.synchronized {
      executorPodsByIPs.values.toList  // toList makes a defensive copy.
    }
    val nodeToLocalTaskCount = mutable.Map[String, Int]() ++
      KubernetesClusterSchedulerBackend.this.synchronized {
        hostToLocalTaskCount
      }
    for (pod <- executorPodsWithIPs) {
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

  private def addNodeAffinityAnnotationIfUseful(
      baseExecutorPod: Pod, nodeToTaskCount: Map[String, Int]): Pod = {
    def scaleToRange(value: Int, baseMin: Double, baseMax: Double,
                     rangeMin: Double, rangeMax: Double): Int =
      (((rangeMax - rangeMin) * (value - baseMin) / (baseMax - baseMin)) + rangeMin).toInt

    if (nodeToTaskCount.nonEmpty) {
      val taskTotal = nodeToTaskCount.foldLeft(0)(_ + _._2)
      // Normalize to node affinity weights in 1 to 100 range.
      val nodeToWeight = nodeToTaskCount.map{
        case (node, taskCount) =>
          (node, scaleToRange(taskCount, 1, taskTotal, rangeMin = 1, rangeMax = 100))}
      val weightToNodes = nodeToWeight.groupBy(_._2).mapValues(_.keys)
      // see https://kubernetes.io/docs/concepts/configuration/assign-pod-node
      val nodeAffinityJson = objectMapper.writeValueAsString(SchedulerAffinity(NodeAffinity(
          preferredDuringSchedulingIgnoredDuringExecution =
            for ((weight, nodes) <- weightToNodes) yield
              WeightedPreference(weight,
                Preference(Array(MatchExpression("kubernetes.io/hostname", "In", nodes))))
        )))
      // TODO: Use non-annotation syntax when we switch to K8s version 1.6.
      logDebug(s"Adding nodeAffinity as annotation $nodeAffinityJson")
      new PodBuilder(baseExecutorPod).editMetadata()
        .addToAnnotations(ANNOTATION_EXECUTOR_NODE_AFFINITY, nodeAffinityJson)
        .endMetadata()
        .build()
    } else {
      baseExecutorPod
    }
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
    val name = s"$executorPodNamePrefix-exec-$executorId"

    // hostname must be no longer than 63 characters, so take the last 63 characters of the pod
    // name as the hostname.  This preserves uniqueness since the end of name contains
    // executorId and applicationId
    val hostname = name.substring(Math.max(0, name.length - 63))
    val resolvedExecutorLabels = Map(
      SPARK_EXECUTOR_ID_LABEL -> executorId,
      SPARK_APP_ID_LABEL -> applicationId(),
      SPARK_ROLE_LABEL -> SPARK_POD_EXECUTOR_ROLE) ++
      executorLabels
    val executorMemoryQuantity = new QuantityBuilder(false)
      .withAmount(s"${executorMemoryMiB}Mi")
      .build()
    val executorMemoryLimitQuantity = new QuantityBuilder(false)
      .withAmount(s"${executorMemoryWithOverheadMiB}Mi")
      .build()
    val executorCpuQuantity = new QuantityBuilder(false)
      .withAmount(executorCores.toString)
      .build()
    val executorExtraClasspathEnv = executorExtraClasspath.map { cp =>
      new EnvVarBuilder()
        .withName(ENV_EXECUTOR_EXTRA_CLASSPATH)
        .withValue(cp)
        .build()
    }
    val executorExtraJavaOptionsEnv = conf
        .get(org.apache.spark.internal.config.EXECUTOR_JAVA_OPTIONS)
        .map { opts =>
          val delimitedOpts = Utils.splitCommandString(opts)
          delimitedOpts.zipWithIndex.map {
            case (opt, index) =>
              new EnvVarBuilder().withName(s"$ENV_JAVA_OPT_PREFIX$index").withValue(opt).build()
          }
        }.getOrElse(Seq.empty[EnvVar])
    val executorEnv = (Seq(
      (ENV_EXECUTOR_PORT, executorPort.toString),
      (ENV_DRIVER_URL, driverUrl),
      // Executor backend expects integral value for executor cores, so round it up to an int.
      (ENV_EXECUTOR_CORES, math.ceil(executorCores).toInt.toString),
      (ENV_EXECUTOR_MEMORY, executorMemoryString),
      (ENV_APPLICATION_ID, applicationId()),
      (ENV_EXECUTOR_ID, executorId),
      (ENV_MOUNTED_CLASSPATH, s"$executorJarsDownloadDir/*")) ++ sc.executorEnvs.toSeq)
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
      ) ++ executorExtraJavaOptionsEnv ++ executorExtraClasspathEnv.toSeq
    val requiredPorts = Seq(
      (EXECUTOR_PORT_NAME, executorPort),
      (BLOCK_MANAGER_PORT_NAME, blockmanagerPort))
      .map(port => {
        new ContainerPortBuilder()
          .withName(port._1)
          .withContainerPort(port._2)
          .build()
      })

    val executorContainer = new ContainerBuilder()
      .withName(s"executor")
      .withImage(executorDockerImage)
      .withImagePullPolicy(dockerImagePullPolicy)
      .withNewResources()
        .addToRequests("memory", executorMemoryQuantity)
        .addToLimits("memory", executorMemoryLimitQuantity)
        .addToRequests("cpu", executorCpuQuantity)
      .endResources()
      .addAllToEnv(executorEnv.asJava)
      .withPorts(requiredPorts.asJava)
      .build()

    val executorPod = new PodBuilder()
      .withNewMetadata()
        .withName(name)
        .withLabels(resolvedExecutorLabels.asJava)
        .withAnnotations(executorAnnotations.asJava)
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
        .withRestartPolicy("Never")
        .withNodeSelector(nodeSelector.asJava)
      .endSpec()
      .build()

    val containerWithExecutorLimitCores = executorLimitCores.map {
      limitCores =>
        val executorCpuLimitQuantity = new QuantityBuilder(false)
          .withAmount(limitCores)
          .build()
        new ContainerBuilder(executorContainer)
          .editResources()
            .addToLimits("cpu", executorCpuLimitQuantity)
            .endResources()
          .build()
    }.getOrElse(executorContainer)

    val withMaybeShuffleConfigExecutorContainer = shuffleServiceConfig.map { config =>
      config.shuffleDirs.foldLeft(containerWithExecutorLimitCores) { (container, dir) =>
        new ContainerBuilder(container)
          .addNewVolumeMount()
            .withName(FilenameUtils.getBaseName(dir))
            .withMountPath(dir)
            .endVolumeMount()
          .build()
      }
    }.getOrElse(containerWithExecutorLimitCores)
    val withMaybeShuffleConfigPod = shuffleServiceConfig.map { config =>
      config.shuffleDirs.foldLeft(executorPod) { (builder, dir) =>
        new PodBuilder(builder)
          .editSpec()
            .addNewVolume()
              .withName(FilenameUtils.getBaseName(dir))
              .withNewHostPath()
                .withPath(dir)
                .endHostPath()
              .endVolume()
            .endSpec()
          .build()
      }
    }.getOrElse(executorPod)
    val (withMaybeSmallFilesMountedPod, withMaybeSmallFilesMountedContainer) =
        mountSmallFilesBootstrap.map { bootstrap =>
          bootstrap.mountSmallFilesSecret(
            withMaybeShuffleConfigPod, withMaybeShuffleConfigExecutorContainer)
        }.getOrElse((withMaybeShuffleConfigPod, withMaybeShuffleConfigExecutorContainer))
    val (executorPodWithInitContainer, initBootstrappedExecutorContainer) =
        executorInitContainerBootstrap.map { bootstrap =>
          val podWithDetachedInitContainer = bootstrap.bootstrapInitContainerAndVolumes(
              PodWithDetachedInitContainer(
                  withMaybeSmallFilesMountedPod,
                  new ContainerBuilder().build(),
                withMaybeSmallFilesMountedContainer))

          val resolvedInitContainer = executorMountInitContainerSecretPlugin.map { plugin =>
            plugin.mountResourceStagingServerSecretIntoInitContainer(
                podWithDetachedInitContainer.initContainer)
          }.getOrElse(podWithDetachedInitContainer.initContainer)

          val podWithAttachedInitContainer = InitContainerUtil.appendInitContainer(
              podWithDetachedInitContainer.pod, resolvedInitContainer)

          val resolvedPodWithMountedSecret = executorMountInitContainerSecretPlugin.map { plugin =>
            plugin.addResourceStagingServerSecretVolumeToPod(podWithAttachedInitContainer)
          }.getOrElse(podWithAttachedInitContainer)

          (resolvedPodWithMountedSecret, podWithDetachedInitContainer.mainContainer)
      }.getOrElse((withMaybeSmallFilesMountedPod, withMaybeSmallFilesMountedContainer))

    val executorPodWithNodeAffinity = addNodeAffinityAnnotationIfUseful(
        executorPodWithInitContainer, nodeToLocalTaskCount)
    val resolvedExecutorPod = new PodBuilder(executorPodWithNodeAffinity)
      .editSpec()
        .addToContainers(initBootstrappedExecutorContainer)
        .endSpec()
      .build()
    try {
      (executorId, kubernetesClient.pods.create(resolvedExecutorPod))
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
        runningExecutorsToPods.remove(executor) match {
          case Some(pod) =>
            kubernetesClient.pods().delete(pod)
            runningPodsToExecutors.remove(pod.getMetadata.getName)
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

    private val DEFAULT_CONTAINER_FAILURE_EXIT_STATUS = -1

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
      val alreadyReleased = isPodAlreadyReleased(pod)
      val containerExitStatus = getExecutorExitStatus(pod)
      // container was probably actively killed by the driver.
      val exitReason = if (alreadyReleased) {
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
              "Pod exited with following container exit status code " + containerExitStatus
          }
          ExecutorExited(containerExitStatus, exitCausedByApp = true, containerExitReason)
        }
        failedPods.put(pod.getMetadata.getName, exitReason)
    }

    def handleDeletedPod(pod: Pod): Unit = {
      val exitReason = ExecutorExited(getExecutorExitStatus(pod), exitCausedByApp = false,
        "Pod " + pod.getMetadata.getName + " deleted or lost.")
        failedPods.put(pod.getMetadata.getName, exitReason)
    }
  }

  override def createDriverEndpoint(properties: Seq[(String, String)]): DriverEndpoint = {
    new KubernetesDriverEndpoint(rpcEnv, properties)
  }

  private class KubernetesDriverEndpoint(
    rpcEnv: RpcEnv,
    sparkProperties: Seq[(String, String)])
    extends DriverEndpoint(rpcEnv, sparkProperties) {
    private val externalShufflePort = conf.getInt("spark.shuffle.service.port", 7337)

    override def onDisconnected(rpcAddress: RpcAddress): Unit = {
      addressToExecutorId.get(rpcAddress).foreach { executorId =>
        if (disableExecutor(executorId)) {
            executorsToRemove.add(executorId)
        }
      }
    }

    override def receiveAndReply(
      context: RpcCallContext): PartialFunction[Any, Unit] = {
      new PartialFunction[Any, Unit]() {
        override def isDefinedAt(msg: Any): Boolean = {
          msg match {
            case RetrieveSparkAppConfig(_) =>
              Utils.isDynamicAllocationEnabled(sc.conf)
            case _ => false
          }
        }

        override def apply(msg: Any): Unit = {
          msg match {
            case RetrieveSparkAppConfig(executorId) =>
              RUNNING_EXECUTOR_PODS_LOCK.synchronized {
                var resolvedProperties = sparkProperties
                val runningExecutorPod = kubernetesClient
                  .pods()
                  .withName(runningExecutorsToPods(executorId).getMetadata.getName)
                  .get()
                val nodeName = runningExecutorPod.getSpec.getNodeName
                val shufflePodIp = shufflePodCache.get.getShufflePodForExecutor(nodeName)

                // Inform the shuffle pod about this application so it can watch.
                kubernetesExternalShuffleClient.foreach(
                  _.registerDriverWithShuffleService(shufflePodIp, externalShufflePort))

                resolvedProperties = resolvedProperties ++ Seq(
                  (SPARK_SHUFFLE_SERVICE_HOST.key, shufflePodIp))

                val reply = SparkAppConfig(
                  resolvedProperties,
                  SparkEnv.get.securityManager.getIOEncryptionKey(),
                  hadoopDelegationCreds)
                context.reply(reply)
              }
          }
        }
      }.orElse(super.receiveAndReply(context))
    }
  }
}
case class ShuffleServiceConfig(
    shuffleNamespace: String,
    shuffleLabels: Map[String, String],
    shuffleDirs: Seq[String])

private object KubernetesClusterSchedulerBackend {
  private val DEFAULT_STATIC_PORT = 10000
  private val EXECUTOR_ID_COUNTER = new AtomicLong(0L)
  private val VMEM_EXCEEDED_EXIT_CODE = -103
  private val PMEM_EXCEEDED_EXIT_CODE = -104
  private val UNKNOWN_EXIT_CODE = -111

  def memLimitExceededLogMessage(diagnostics: String): String = {
    s"Pod/Container killed for exceeding memory limits. $diagnostics" +
      " Consider boosting spark executor memory overhead."
  }
}

/**
 * These case classes model K8s node affinity syntax for
 * preferredDuringSchedulingIgnoredDuringExecution.
 *
 * see https://kubernetes.io/docs/concepts/configuration/assign-pod-node
 */
case class SchedulerAffinity(nodeAffinity: NodeAffinity)
case class NodeAffinity(preferredDuringSchedulingIgnoredDuringExecution:
                        Iterable[WeightedPreference])
case class WeightedPreference(weight: Int, preference: Preference)
case class Preference(matchExpressions: Array[MatchExpression])
case class MatchExpression(key: String, operator: String, values: Iterable[String])
