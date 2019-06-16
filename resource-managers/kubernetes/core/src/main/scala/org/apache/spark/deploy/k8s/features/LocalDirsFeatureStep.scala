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

import java.util.UUID

import collection.JavaConverters._
import io.fabric8.kubernetes.api.model.{ContainerBuilder, PodBuilder, VolumeBuilder, VolumeMount, VolumeMountBuilder}

import org.apache.spark.deploy.k8s.{KubernetesConf, SparkPod}
import org.apache.spark.deploy.k8s.Config._

private[spark] class LocalDirsFeatureStep(
    conf: KubernetesConf,
    defaultLocalDir: String = s"/var/data/spark-${UUID.randomUUID}")
  extends KubernetesFeatureConfigStep {

  // Cannot use Utils.getConfiguredLocalDirs because that will default to the Java system
  // property - we want to instead default to mounting an emptydir volume that doesn't already
  // exist in the image.
  // We could make utils.getConfiguredLocalDirs opinionated about Kubernetes, as it is already
  // a bit opinionated about YARN and Mesos.
  private val resolvedLocalDirs = Option(conf.sparkConf.getenv("SPARK_LOCAL_DIRS"))
    .orElse(conf.getOption("spark.local.dir"))
    .getOrElse(defaultLocalDir)
    .split(",")
  private val useLocalDirTmpFs = conf.get(KUBERNETES_LOCAL_DIRS_TMPFS)

  override def configurePod(pod: SparkPod): SparkPod = {
    val localDirVolumes = resolvedLocalDirs
      .zipWithIndex
      .map { case (localDir, index) =>
        val name = s"spark-local-dir-${index + 1}"
        hasVolume(pod, name) match {
          case true =>
            pod.pod.getSpec.getVolumes().asScala.find(v => v.getName.equals(name)).get
          case false =>
            new VolumeBuilder()
              .withName(s"spark-local-dir-${index + 1}")
              .withNewEmptyDir()
              .withMedium(if (useLocalDirTmpFs) "Memory" else null)
              .endEmptyDir()
              .build()
        }
      }
    val localDirVolumeMounts = localDirVolumes
      .zip(resolvedLocalDirs)
      .map { case (localDirVolume, localDirPath) =>
        hasVolumeMount(pod, localDirVolume.getName, localDirPath) match {
          case true =>
            pod.container.getVolumeMounts().asScala
              .find(m => m.getName.equals(localDirVolume.getName)
                && m.getMountPath.equals(localDirPath))
              .get
          case false =>
            new VolumeMountBuilder()
              .withName(localDirVolume.getName)
              .withMountPath(localDirPath)
              .build()
        }
      }
    val podWithLocalDirVolumes = new PodBuilder(pod.pod)
      .editSpec()
        .addToVolumes(localDirVolumes.filter(v => !hasVolume(pod, v.getName)): _*)
        .endSpec()
      .build()
    val containerWithLocalDirVolumeMounts = new ContainerBuilder(pod.container)
      .addNewEnv()
        .withName("SPARK_LOCAL_DIRS")
        .withValue(resolvedLocalDirs.mkString(","))
        .endEnv()
      .addToVolumeMounts(localDirVolumeMounts
        .filter(m => !hasVolumeMount(pod, m.getName, m.getMountPath)): _*)
      .build()
    SparkPod(podWithLocalDirVolumes, containerWithLocalDirVolumeMounts)
  }

  def hasVolume(pod: SparkPod, name: String): Boolean = {
    pod.pod.getSpec().getVolumes().asScala.exists(v => v.getName.equals(name))
  }

  def hasVolumeMount(pod: SparkPod, name: String, path: String): Boolean = {
    pod.container.getVolumeMounts().asScala
      .exists(m => m.getName.equals(name) && m.getMountPath.equals(path))
  }
}
