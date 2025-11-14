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

import java.util.concurrent.TimeUnit

import scala.collection.mutable
import scala.jdk.CollectionConverters._

import io.fabric8.kubernetes.api.model.{Pod, PodSpec, PodSpecBuilder, PodTemplateSpec}
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder
import io.fabric8.kubernetes.client.KubernetesClient

import org.apache.spark.{SecurityManager, SparkConf, SparkException}
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.deploy.k8s.KubernetesConf
import org.apache.spark.deploy.k8s.KubernetesUtils.addOwnerReference
import org.apache.spark.internal.Logging
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.util.{Clock, Utils}

/**
 * A pods allocator backed by Kubernetes Deployments.
 *
 * The Deployment controller honours the `controller.kubernetes.io/pod-deletion-cost`
 * annotation, so executors selected by Spark for removal can be prioritised when the
 * deployment scales down. This provides predictable downscale behaviour for dynamic
 * allocation that is not possible with StatefulSets which only remove pods in ordinal order.
 */
class DeploymentPodsAllocator(
    conf: SparkConf,
    secMgr: SecurityManager,
    executorBuilder: KubernetesExecutorBuilder,
    kubernetesClient: KubernetesClient,
    snapshotsStore: ExecutorPodsSnapshotsStore,
    clock: Clock) extends AbstractPodsAllocator() with Logging {

  private val rpIdToResourceProfile = new mutable.HashMap[Int, ResourceProfile]

  private val driverPodReadinessTimeout = conf.get(KUBERNETES_ALLOCATION_DRIVER_READINESS_TIMEOUT)

  private val namespace = conf.get(KUBERNETES_NAMESPACE)

  private val kubernetesDriverPodName = conf.get(KUBERNETES_DRIVER_POD_NAME)

  val driverPod: Option[Pod] = kubernetesDriverPodName
    .map(name => Option(kubernetesClient.pods()
      .inNamespace(namespace)
      .withName(name)
      .get())
      .getOrElse(throw new SparkException(
        s"No pod was found named $name in the cluster in the " +
          s"namespace $namespace (this was supposed to be the driver pod.).")))

  private var appId: String = _

  private val deploymentsCreated = new mutable.HashSet[Int]()

  private val podDeletionCostAnnotation = "controller.kubernetes.io/pod-deletion-cost"

  override def start(
      applicationId: String,
      schedulerBackend: KubernetesClusterSchedulerBackend): Unit = {
    appId = applicationId
    driverPod.foreach { pod =>
      Utils.tryLogNonFatalError {
        kubernetesClient
          .pods()
          .inNamespace(namespace)
          .withName(pod.getMetadata.getName)
          .waitUntilReady(driverPodReadinessTimeout, TimeUnit.SECONDS)
      }
    }
  }

  override def setTotalExpectedExecutors(
      resourceProfileToTotalExecs: Map[ResourceProfile, Int]): Unit = {
    if (appId == null) {
      throw new SparkException("setTotalExpectedExecutors called before start of allocator.")
    }
    resourceProfileToTotalExecs.foreach { case (rp, numExecs) =>
      rpIdToResourceProfile.getOrElseUpdate(rp.id, rp)
      setTargetExecutorsDeployment(numExecs, appId, rp.id)
    }
  }

  override def isDeleted(executorId: String): Boolean = false

  private def setName(applicationId: String, resourceProfileId: Int): String = {
    s"spark-d-$applicationId-$resourceProfileId"
  }

  private def setTargetExecutorsDeployment(
      expected: Int,
      applicationId: String,
      resourceProfileId: Int): Unit = {
    if (deploymentsCreated.contains(resourceProfileId)) {
      kubernetesClient
        .apps()
        .deployments()
        .inNamespace(namespace)
        .withName(setName(applicationId, resourceProfileId))
        .scale(expected)
    } else {
      val executorConf = KubernetesConf.createExecutorConf(
        conf,
        "EXECID",
        applicationId,
        driverPod,
        resourceProfileId)
      val resolvedExecutorSpec = executorBuilder.buildFromFeatures(
        executorConf,
        secMgr,
        kubernetesClient,
        rpIdToResourceProfile(resourceProfileId))
      val executorPod = resolvedExecutorSpec.pod

      val podSpecBuilder = executorPod.pod.getSpec match {
        case null => new PodSpecBuilder()
        case s => new PodSpecBuilder(s)
      }
      val podWithAttachedContainer: PodSpec = podSpecBuilder
        .addToContainers(executorPod.container)
        .build()

      val meta = executorPod.pod.getMetadata
      val resources = resolvedExecutorSpec.executorKubernetesResources
      val failureMessage =
        "PersistentVolumeClaims are not supported with the deployment allocator. " +
          "Please remove PVC requirements or choose a different pods allocator."
      val dynamicVolumeClaims = resources.filter(_.getKind == "PersistentVolumeClaim")
      if (dynamicVolumeClaims.nonEmpty) {
        throw new SparkException(failureMessage)
      }
      val staticVolumeClaims = Option(podWithAttachedContainer.getVolumes)
        .map(_.asScala.filter(_.getPersistentVolumeClaim != null))
        .getOrElse(Seq.empty)
      if (staticVolumeClaims.nonEmpty) {
        throw new SparkException(failureMessage)
      }

      val currentAnnotations = Option(meta.getAnnotations)
        .map(_.asScala).getOrElse(Map.empty[String, String])
      if (!currentAnnotations.contains(podDeletionCostAnnotation)) {
        val newAnnotations = currentAnnotations.concat(Seq(podDeletionCostAnnotation -> "0"))
        meta.setAnnotations(newAnnotations.asJava)
      }

      val podTemplateSpec = new PodTemplateSpec(meta, podWithAttachedContainer)

      val deployment = new DeploymentBuilder()
        .withNewMetadata()
          .withName(setName(applicationId, resourceProfileId))
          .withNamespace(namespace)
        .endMetadata()
        .withNewSpec()
          .withReplicas(expected)
          .withNewSelector()
            .addToMatchLabels(SPARK_APP_ID_LABEL, applicationId)
            .addToMatchLabels(SPARK_ROLE_LABEL, SPARK_POD_EXECUTOR_ROLE)
            .addToMatchLabels(SPARK_RESOURCE_PROFILE_ID_LABEL, resourceProfileId.toString)
          .endSelector()
          .withTemplate(podTemplateSpec)
        .endSpec()
        .build()

      addOwnerReference(driverPod.get, Seq(deployment))
      kubernetesClient.apps().deployments().inNamespace(namespace).resource(deployment).create()
      deploymentsCreated += resourceProfileId
    }
  }

  override def stop(applicationId: String): Unit = {
    deploymentsCreated.foreach { rpid =>
      Utils.tryLogNonFatalError {
        kubernetesClient
          .apps()
          .deployments()
          .inNamespace(namespace)
          .withName(setName(applicationId, rpid))
          .delete()
      }
    }
  }
}
