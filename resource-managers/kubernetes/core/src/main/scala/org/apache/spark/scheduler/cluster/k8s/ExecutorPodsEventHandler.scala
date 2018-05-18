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

import java.util.concurrent.{Future, LinkedBlockingQueue, ScheduledExecutorService, TimeUnit}
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import gnu.trove.list.array.TLongArrayList
import gnu.trove.set.hash.TLongHashSet
import io.fabric8.kubernetes.api.model.{Pod, PodBuilder}
import io.fabric8.kubernetes.client.KubernetesClient
import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.deploy.k8s.KubernetesConf
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.ExecutorExited
import org.apache.spark.util.Utils

private[spark] class ExecutorPodsEventHandler(
    conf: SparkConf,
    executorBuilder: KubernetesExecutorBuilder,
    kubernetesClient: KubernetesClient,
    eventProcessorExecutor: ScheduledExecutorService) extends Logging {

  import ExecutorPodsEventHandler._

  private val EXECUTOR_ID_COUNTER = new AtomicLong(0L)

  private val totalExpectedExecutors = new AtomicInteger(0)

  private val eventQueue = new LinkedBlockingQueue[Seq[Pod]]()

  private val podAllocationSize = conf.get(KUBERNETES_ALLOCATION_BATCH_SIZE)

  private val kubernetesDriverPodName = conf
    .get(KUBERNETES_DRIVER_POD_NAME)
    .getOrElse(throw new SparkException("Must specify the driver pod name"))

  private val driverPod = kubernetesClient.pods()
    .withName(kubernetesDriverPodName)
    .get()

  // Use sets of ids instead of counters to be able to handle duplicate events.

  // Executor IDs that have been requested from Kubernetes but are not running yet.
  private val pendingExecutors = new TLongHashSet()

  // We could use CoarseGrainedSchedulerBackend#totalRegisteredExecutors here for tallying the
  // executors that are running. But, here we choose instead to maintain all state within this
  // class from the persecptive of the k8s API. Therefore whether or not this scheduler loop
  // believes an executor is running is dictated by the K8s API rather than Spark's RPC events.
  // We may need to consider where these perspectives may differ and which perspective should
  // take precedence.
  private val runningExecutors = new TLongHashSet()

  private var eventProcessorFuture: Future[_] = null

  def start(applicationId: String, schedulerBackend: KubernetesClusterSchedulerBackend): Unit = {
    require(eventProcessorFuture == null, "Cannot start event processing twice.")
    logInfo(s"Starting Kubernetes executor pods event handler for application with" +
      s" id $applicationId.")
    val eventProcessor = new Runnable {
      override def run(): Unit = processEvents(applicationId, schedulerBackend)
    }
    eventProcessorFuture = eventProcessorExecutor.scheduleWithFixedDelay(
      eventProcessor, 0L, 5L, TimeUnit.SECONDS)
  }

  def stop(): Unit = {
    if (eventProcessorFuture != null) {
      eventProcessorFuture.cancel(true)
      eventProcessorFuture = null
    }
  }

  private def processEvents(
      applicationId: String, schedulerBackend: KubernetesClusterSchedulerBackend) {
    val currentEvents = new java.util.ArrayList[Seq[Pod]](eventQueue.size())
    eventQueue.drainTo(currentEvents)
    currentEvents.asScala.flatten.foreach { updatedPod =>
      val execId = updatedPod.getMetadata.getLabels.get(SPARK_EXECUTOR_ID_LABEL).toLong
      val podPhase = updatedPod.getStatus.getPhase.toLowerCase
      if (isDeleted(updatedPod)) {
        removeExecutorFromSpark(schedulerBackend, updatedPod, execId)
      } else {
        updatedPod.getStatus.getPhase.toLowerCase match {
          case "running" =>
            // If clause is for resililence to out of order operations - executor must be
            // pending and first reach running. Without this check you may e.g. process a
            // deletion event followed by some arbitrary modification event - we want the
            // deletion event to "stick".
            if (pendingExecutors.contains(execId)) {
              pendingExecutors.remove(execId)
              runningExecutors.add(execId)
            }
          // TODO (SPARK-24135) - handle more classes of errors
          case "error" | "failed" | "succeeded" =>
            removeExecutorFromSpark(schedulerBackend, updatedPod, execId)
            // If deletion failed on a previous try, we can try again if resync informs us the pod
            // is still around.
            // Delete as best attempt - duplicate deletes will throw an exception but the end state
            // of getting rid of the pod is what matters.
            if (!isDeleted(updatedPod)) {
              Utils.tryLogNonFatalError {
                kubernetesClient
                  .pods()
                  .withName(updatedPod.getMetadata.getName)
                  .delete()
              }
            }
        }
      }
    }

    val currentRunningExecutors = runningExecutors.size
    val currentTotalExpectedExecutors = totalExpectedExecutors.get
    if (pendingExecutors.isEmpty && currentRunningExecutors < currentTotalExpectedExecutors) {
      val numExecutorsToAllocate = math.min(
        currentTotalExpectedExecutors - currentRunningExecutors, podAllocationSize)
      logInfo(s"Going to request $numExecutorsToAllocate executors from Kubernetes.")
      val newExecutorIds = new TLongArrayList()
      val podsToAllocate = mutable.Buffer.empty[Pod]
      for ( _ <- 0 until numExecutorsToAllocate) {
        val newExecutorId = EXECUTOR_ID_COUNTER.incrementAndGet()
        val executorConf = KubernetesConf.createExecutorConf(
          conf,
          newExecutorId.toString,
          applicationId,
          driverPod)
        val executorPod = executorBuilder.buildFromFeatures(executorConf)
        val podWithAttachedContainer = new PodBuilder(executorPod.pod)
          .editOrNewSpec()
          .addToContainers(executorPod.container)
          .endSpec()
          .build()
        newExecutorIds.add(newExecutorId)
      }
      kubernetesClient.pods().create(podsToAllocate: _*)
      pendingExecutors.addAll(newExecutorIds)
    } else if (currentRunningExecutors == currentTotalExpectedExecutors) {
      logDebug("Current number of running executors is equal to the number of requested" +
        " executors. Not scaling up further.")
    } else if (!pendingExecutors.isEmpty) {
      logInfo(s"Still waiting for ${pendingExecutors.size} executors to begin running before" +
        s" requesting for more executors.")
    }
  }

  def sendUpdatedPodMetadata(updatedPod: Pod): Unit = {
    eventQueue.add(Seq(updatedPod))
  }

  def sendUpdatedPodMetadata(updatedPods: Iterable[Pod]): Unit = {
    eventQueue.add(updatedPods.toSeq)
  }

  def setTotalExpectedExecutors(newTotal: Int): Unit = totalExpectedExecutors.set(newTotal)

  private def removeExecutorFromSpark(
      schedulerBackend: KubernetesClusterSchedulerBackend,
      updatedPod: Pod,
      execId: Long): Unit = {
    // Avoid removing twice from Spark's perspective.
    if (pendingExecutors.contains(execId) || runningExecutors.contains(execId)) {
      pendingExecutors.remove(execId)
      runningExecutors.remove(execId)
      val exitReason = findExitReason(updatedPod, execId)
      schedulerBackend.doRemoveExecutor(execId.toString, exitReason)
    }
  }

  private def findExitReason(pod: Pod, execId: Long): ExecutorExited = {
    val exitCode = findExitCode(pod)
    val (exitCausedByApp, exitMessage) = if (isDeleted(pod)) {
      (false, s"The executor with id $execId was deleted by a user or the framework.")
    } else {
      val msg =
        s""" The executor with id $execId exited with exit code $exitCode.
           | The API gave the following brief reason: ${pod.getStatus.getReason}.
           | The API gave the following message: ${pod.getStatus.getMessage}.
           | The API gave the following container statuses:
           | ${pod.getStatus.getContainerStatuses.asScala.map(_.toString).mkString("\n===\n")}
         """.stripMargin
      (true, msg)
    }
    ExecutorExited(exitCode, exitCausedByApp, exitMessage)
  }

  private def isDeleted(pod: Pod): Boolean = pod.getMetadata.getDeletionTimestamp != null

  private def findExitCode(pod: Pod): Int = {
    pod.getStatus.getContainerStatuses.asScala.find { containerStatus =>
      containerStatus.getState.getTerminated != null
    }.map { terminatedContainer =>
      terminatedContainer.getState.getTerminated.getExitCode.toInt
    }.getOrElse(UNKNOWN_EXIT_CODE)
  }
}

private object ExecutorPodsEventHandler {
  val UNKNOWN_EXIT_CODE = -1
}

