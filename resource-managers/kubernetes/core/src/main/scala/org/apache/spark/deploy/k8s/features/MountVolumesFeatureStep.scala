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
package org.apache.spark.deploy.k8s.features

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

import io.fabric8.kubernetes.api.model._

import org.apache.spark.deploy.k8s._
import org.apache.spark.deploy.k8s.Config.KUBERNETES_USE_LEGACY_PVC_ACCESS_MODE
import org.apache.spark.deploy.k8s.Constants.{ENV_EXECUTOR_ID, SPARK_APP_ID_LABEL}

private[spark] class MountVolumesFeatureStep(conf: KubernetesConf)
  extends KubernetesFeatureConfigStep {
  import MountVolumesFeatureStep._

  val additionalResources = ArrayBuffer.empty[HasMetadata]
  val accessMode = if (conf.get(KUBERNETES_USE_LEGACY_PVC_ACCESS_MODE)) {
    "ReadWriteOnce"
  } else {
    PVC_ACCESS_MODE
  }

  override def configurePod(pod: SparkPod): SparkPod = {
    val (volumeMounts, volumes) = constructVolumes(conf.volumes).unzip

    val podWithVolumes = new PodBuilder(pod.pod)
      .editSpec()
      .addToVolumes(volumes.toSeq: _*)
      .endSpec()
      .build()

    val containerWithVolumeMounts = new ContainerBuilder(pod.container)
      .addToVolumeMounts(volumeMounts.toSeq: _*)
      .build()

    SparkPod(podWithVolumes, containerWithVolumeMounts)
  }

  private def constructVolumes(
    volumeSpecs: Iterable[KubernetesVolumeSpec]
  ): Iterable[(VolumeMount, Volume)] = {
    val duplicateMountPaths = volumeSpecs.map(_.mountPath).toSeq.groupBy(identity).collect {
      case (x, ys) if ys.length > 1 => s"'$x'"
    }
    require(duplicateMountPaths.isEmpty,
      s"Found duplicated mountPath: ${duplicateMountPaths.mkString(", ")}")
    volumeSpecs.zipWithIndex.map { case (spec, i) =>
      val volumeMount = new VolumeMountBuilder()
        .withMountPath(spec.mountPath)
        .withReadOnly(spec.mountReadOnly)
        .withSubPath(spec.mountSubPath)
        .withSubPathExpr(spec.mountSubPathExpr)
        .withName(spec.volumeName)
        .build()

      val volumeBuilder = spec.volumeConf match {
        case KubernetesHostPathVolumeConf(hostPath, volumeType) =>
          new VolumeBuilder()
            .withHostPath(new HostPathVolumeSource(hostPath, volumeType))

        case KubernetesPVCVolumeConf(claimNameTemplate, storageClass, size, labels) =>
          val claimName = conf match {
            case c: KubernetesExecutorConf =>
              claimNameTemplate
                .replaceAll(PVC_ON_DEMAND,
                  s"${conf.resourceNamePrefix}-exec-${c.executorId}$PVC_POSTFIX-$i")
                .replaceAll(ENV_EXECUTOR_ID, c.executorId)
            case _ =>
              claimNameTemplate
                .replaceAll(PVC_ON_DEMAND, s"${conf.resourceNamePrefix}-driver$PVC_POSTFIX-$i")
          }
          if (storageClass.isDefined && size.isDefined) {
            val defaultVolumeLabels = Map(SPARK_APP_ID_LABEL -> conf.appId)
            val volumeLabels = labels match {
              case Some(customLabelsMap) => (customLabelsMap ++ defaultVolumeLabels).asJava
              case None => defaultVolumeLabels.asJava
            }
            additionalResources.append(new PersistentVolumeClaimBuilder()
              .withKind(PVC)
              .withApiVersion("v1")
              .withNewMetadata()
                .withName(claimName)
                .addToLabels(volumeLabels)
                .endMetadata()
              .withNewSpec()
                .withStorageClassName(storageClass.get)
                .withAccessModes(accessMode)
                .withResources(new VolumeResourceRequirementsBuilder()
                  .withRequests(Map("storage" -> new Quantity(size.get)).asJava).build())
                .endSpec()
              .build())
          }

          new VolumeBuilder()
            .withPersistentVolumeClaim(
              new PersistentVolumeClaimVolumeSource(claimName, spec.mountReadOnly))

        case KubernetesEmptyDirVolumeConf(medium, sizeLimit) =>
          new VolumeBuilder()
            .withEmptyDir(
              new EmptyDirVolumeSource(medium.getOrElse(""),
                sizeLimit.map(new Quantity(_)).orNull))

        case KubernetesNFSVolumeConf(path, server) =>
          new VolumeBuilder()
            .withNfs(new NFSVolumeSource(path, null, server))
      }

      val volume = volumeBuilder.withName(spec.volumeName).build()

      (volumeMount, volume)
    }
  }

  override def getAdditionalKubernetesResources(): Seq[HasMetadata] = {
    additionalResources.toSeq
  }
}

private[spark] object MountVolumesFeatureStep {
  val PVC_ON_DEMAND = "OnDemand"
  val PVC = "PersistentVolumeClaim"
  val PVC_POSTFIX = "-pvc"
  val PVC_ACCESS_MODE = "ReadWriteOncePod"
}
