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
import io.fabric8.kubernetes.api.model._

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
    val prefix = "spark-local-dir-"
    val localDirVolumes = resolvedLocalDirs
      .zipWithIndex
      .map {
        case (localDir, index) => findVolumeMount(pod, prefix, localDir) match {
          case Some(volumeMount) => findVolume(pod, volumeMount.getName).get
          case None =>
            new VolumeBuilder()
              .withName(s"$prefix${index + 1}")
              .withNewEmptyDir()
              .withMedium(if (useLocalDirTmpFs) "Memory" else null)
              .endEmptyDir()
              .build()
        }
      }
    val localDirVolumeMounts = localDirVolumes
      .zip(resolvedLocalDirs)
      .map { case (localDirVolume, localDirPath) =>
        findVolumeMount(pod, localDirVolume.getName, localDirPath) match {
          case Some(volumeMount) => volumeMount
          case None =>
            new VolumeMountBuilder()
              .withName(localDirVolume.getName)
              .withMountPath(localDirPath)
              .build()
        }
      }
    val podWithLocalDirVolumes = new PodBuilder(pod.pod)
      .editSpec()
        .addToVolumes(localDirVolumes.filter(v => findVolume(pod, v.getName).isEmpty): _*)
        .endSpec()
      .build()
    val containerWithLocalDirVolumeMounts = new ContainerBuilder(pod.container)
      .addNewEnv()
        .withName("SPARK_LOCAL_DIRS")
        .withValue(resolvedLocalDirs.mkString(","))
        .endEnv()
      .addToVolumeMounts(localDirVolumeMounts
        .filter(m => findVolumeMount(pod, m.getName, m.getMountPath).isEmpty): _*)
      .build()
    SparkPod(podWithLocalDirVolumes, containerWithLocalDirVolumeMounts)
  }

  def findVolume(pod: SparkPod, name: String): Option[Volume] = {
    pod.pod.getSpec.getVolumes.asScala.find(v => v.getName.equals(name))
  }

  def findVolumeMount(pod: SparkPod, prefix: String, path: String): Option[VolumeMount] = {
    pod.container.getVolumeMounts.asScala
      .find(m => m.getName.startsWith(prefix) && m.getMountPath.equals(path))
  }
}
