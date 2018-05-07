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

import io.fabric8.kubernetes.api.model._

import org.apache.spark.deploy.k8s.{KubernetesConf, KubernetesRoleSpecificConf, KubernetesVolumeSpec, SparkPod}
import org.apache.spark.deploy.k8s.Config._

private[spark] class MountVolumesFeatureStep(
  kubernetesConf: KubernetesConf[_ <: KubernetesRoleSpecificConf])
  extends KubernetesFeatureConfigStep {

  override def configurePod(pod: SparkPod): SparkPod = {
    val (volumeMounts, volumes) = constructVolumes(kubernetesConf.roleVolumes).unzip

    val podWithVolumes = new PodBuilder(pod.pod)
      .editSpec()
      .addToVolumes(volumes.toSeq: _*)
      .endSpec()
      .build()

    val containerWithLocalDirVolumeMounts = new ContainerBuilder(pod.container)
      .addToVolumeMounts(volumeMounts.toSeq: _*)
      .build()

    SparkPod(podWithVolumes, containerWithLocalDirVolumeMounts)
  }

  override def getAdditionalPodSystemProperties(): Map[String, String] = Map.empty

  override def getAdditionalKubernetesResources(): Seq[HasMetadata] = Seq.empty

  private def constructVolumes(
    volumeSpecs: Iterable[KubernetesVolumeSpec]): Iterable[(VolumeMount, Volume)] = {
    volumeSpecs.map { spec =>
      val volumeMount = new VolumeMountBuilder()
        .withMountPath(spec.mountPath)
        .withReadOnly(spec.mountReadOnly)
        .withName(spec.volumeName)
        .build()

      val volume = spec.volumeType match {
        case KUBERNETES_VOLUMES_HOSTPATH_KEY =>
          val hostPath = spec.optionsSpec(KUBERNETES_VOLUMES_PATH_KEY)
          new VolumeBuilder()
            .withHostPath(new HostPathVolumeSource(hostPath))
            .withName(spec.volumeName)
            .build()
      }

      (volumeMount, volume)
    }
  }
}
