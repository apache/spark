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

import java.util.UUID
import java.util.concurrent.Executors
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import com.google.common.util.concurrent.ThreadFactoryBuilder
import io.fabric8.kubernetes.api.model.{ContainerPort, ContainerPortBuilder, EnvVar, EnvVarBuilder, Pod, QuantityBuilder}
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{ExecutionContext, Future}

import org.apache.spark.{SparkContext, SparkException}
import org.apache.spark.deploy.kubernetes.KubernetesClientBuilder
import org.apache.spark.rpc.RpcEndpointAddress
import org.apache.spark.scheduler.TaskSchedulerImpl
import org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend
import org.apache.spark.util.Utils

private[spark] class KubernetesClusterSchedulerBackend(
  scheduler: TaskSchedulerImpl,
  val sc: SparkContext)
  extends CoarseGrainedSchedulerBackend(scheduler, sc.env.rpcEnv) {

  import KubernetesClusterSchedulerBackend._

  private val EXECUTOR_MODIFICATION_LOCK = new Object
  private val runningExecutorPods = new scala.collection.mutable.HashMap[String, Pod]

  private val kubernetesMaster = conf
    .getOption("spark.kubernetes.master")
    .getOrElse(
      throw new SparkException("Kubernetes master must be specified in kubernetes mode."))

  private val executorDockerImage = conf
    .get("spark.kubernetes.executor.docker.image", s"spark-executor:${sc.version}")

  private val kubernetesNamespace = conf
    .getOption("spark.kubernetes.namespace")
    .getOrElse(
      throw new SparkException("Kubernetes namespace must be specified in kubernetes mode."))

  private val executorPort = conf.getInt("spark.executor.port", DEFAULT_STATIC_PORT)

  private val blockmanagerPort = conf
    .getInt("spark.blockmanager.port", DEFAULT_BLOCKMANAGER_PORT)

  private val kubernetesDriverServiceName = conf
    .getOption("spark.kubernetes.driver.service.name")
    .getOrElse(
      throw new SparkException("Must specify the service name the driver is running with"))

  private val executorMemory = conf.getOption("spark.executor.memory").getOrElse("1g")
  private val executorMemoryBytes = Utils.byteStringAsBytes(executorMemory)

  private val memoryOverheadBytes = conf
    .getOption("spark.kubernetes.executor.memoryOverhead")
    .map(overhead => Utils.byteStringAsBytes(overhead))
    .getOrElse(math.max((MEMORY_OVERHEAD_FACTOR * executorMemoryBytes).toInt,
      MEMORY_OVERHEAD_MIN))
  private val executorMemoryWithOverhead = executorMemoryBytes + memoryOverheadBytes

  private val executorCores = conf.getOption("spark.executor.cores").getOrElse("1")

  private implicit val requestExecutorContext = ExecutionContext.fromExecutorService(
    Executors.newCachedThreadPool(
      new ThreadFactoryBuilder()
        .setDaemon(true)
        .setNameFormat("kubernetes-executor-requests-%d")
        .build))

  private val kubernetesClient = KubernetesClientBuilder
    .buildFromWithinPod(kubernetesMaster, kubernetesNamespace)

  override val minRegisteredRatio =
    if (conf.getOption("spark.scheduler.minRegisteredResourcesRatio").isEmpty) {
      0.8
    } else {
      super.minRegisteredRatio
    }

  protected var totalExpectedExecutors = new AtomicInteger(0)

  private val driverUrl = RpcEndpointAddress(
    System.getenv(s"${convertToEnvMode(kubernetesDriverServiceName)}_SERVICE_HOST"),
    sc.getConf.getInt("spark.driver.port", DEFAULT_DRIVER_PORT),
    CoarseGrainedSchedulerBackend.ENDPOINT_NAME).toString

  private def convertToEnvMode(value: String): String =
    value.toUpperCase.map { c => if (c == '-') '_' else c }

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

  override def sufficientResourcesRegistered(): Boolean = {
    totalRegisteredExecutors.get() >= initialExecutors * minRegisteredRatio
  }

  override def start(): Unit = {
    super.start()
    if (!Utils.isDynamicAllocationEnabled(sc.conf)) {
      doRequestTotalExecutors(initialExecutors)
    }
  }

  override def stop(): Unit = {
    // TODO investigate why Utils.tryLogNonFatalError() doesn't work in this context.
    // When using Utils.tryLogNonFatalError some of the code fails but without any logs or
    // indication as to why.
    try {
      runningExecutorPods.values.foreach(kubernetesClient.pods().delete(_))
    } catch {
      case e: Throwable => logError("Uncaught exception while shutting down controllers.", e)
    }
    try {
      kubernetesClient.services().withName(kubernetesDriverServiceName).delete()
    } catch {
      case e: Throwable => logError("Uncaught exception while shutting down driver service.", e)
    }
    try {
      kubernetesClient.close()
    } catch {
      case e: Throwable => logError("Uncaught exception closing Kubernetes client.", e)
    }
    super.stop()
  }

  private def allocateNewExecutorPod(): (String, Pod) = {
    val executorKubernetesId = UUID.randomUUID().toString.replaceAll("-", "")
    val executorId = EXECUTOR_ID_COUNTER.incrementAndGet().toString
    val name = s"exec$executorKubernetesId"
    val selectors = Map(SPARK_EXECUTOR_SELECTOR -> executorId,
      SPARK_APP_SELECTOR -> applicationId()).asJava
    val executorMemoryQuantity = new QuantityBuilder(false)
      .withAmount(executorMemoryBytes.toString)
      .build()
    val executorMemoryLimitQuantity = new QuantityBuilder(false)
      .withAmount(executorMemoryWithOverhead.toString)
      .build()
    val requiredEnv = new ArrayBuffer[EnvVar]
    requiredEnv += new EnvVarBuilder()
      .withName("SPARK_EXECUTOR_PORT")
      .withValue(executorPort.toString)
      .build()
    requiredEnv += new EnvVarBuilder()
      .withName("SPARK_DRIVER_URL")
      .withValue(driverUrl)
      .build()
    requiredEnv += new EnvVarBuilder()
      .withName("SPARK_EXECUTOR_CORES")
      .withValue(executorCores)
      .build()
    requiredEnv += new EnvVarBuilder()
      .withName("SPARK_EXECUTOR_MEMORY")
      .withValue(executorMemory)
      .build()
    requiredEnv += new EnvVarBuilder()
      .withName("SPARK_APPLICATION_ID")
      .withValue(applicationId())
      .build()
    requiredEnv += new EnvVarBuilder()
      .withName("SPARK_EXECUTOR_ID")
      .withValue(executorId)
      .build()
    val requiredPorts = new ArrayBuffer[ContainerPort]
    requiredPorts += new ContainerPortBuilder()
      .withName(EXECUTOR_PORT_NAME)
      .withContainerPort(executorPort)
      .build()
    requiredPorts += new ContainerPortBuilder()
      .withName(BLOCK_MANAGER_PORT_NAME)
      .withContainerPort(blockmanagerPort)
      .build()
    (executorKubernetesId, kubernetesClient.pods().createNew()
      .withNewMetadata()
        .withName(name)
        .withLabels(selectors)
        .endMetadata()
      .withNewSpec()
        .addNewContainer()
          .withName(s"exec-${applicationId()}-container")
          .withImage(executorDockerImage)
          .withImagePullPolicy("IfNotPresent")
          .withNewResources()
            .addToRequests("memory", executorMemoryQuantity)
            .addToLimits("memory", executorMemoryLimitQuantity)
            .endResources()
          .withEnv(requiredEnv.asJava)
          .withPorts(requiredPorts.asJava)
          .endContainer()
        .endSpec()
      .done())
  }

  override def doRequestTotalExecutors(requestedTotal: Int): Future[Boolean] = Future[Boolean] {
    EXECUTOR_MODIFICATION_LOCK.synchronized {
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
    EXECUTOR_MODIFICATION_LOCK.synchronized {
      for (executor <- executorIds) {
        runningExecutorPods.remove(executor) match {
          case Some(pod) => kubernetesClient.pods().delete(pod)
          case None => logWarning(s"Unable to remove pod for unknown executor $executor")
        }
      }
    }
    true
  }
}

private object KubernetesClusterSchedulerBackend {
  private val SPARK_EXECUTOR_SELECTOR = "spark-exec"
  private val SPARK_APP_SELECTOR = "spark-app"
  private val DEFAULT_STATIC_PORT = 10000
  private val DEFAULT_BLOCKMANAGER_PORT = 7079
  private val DEFAULT_DRIVER_PORT = 7078
  private val BLOCK_MANAGER_PORT_NAME = "blockmanager"
  private val EXECUTOR_PORT_NAME = "executor"
  private val MEMORY_OVERHEAD_FACTOR = 0.10
  private val MEMORY_OVERHEAD_MIN = 384L
  private val EXECUTOR_ID_COUNTER = new AtomicLong(0L)
}
