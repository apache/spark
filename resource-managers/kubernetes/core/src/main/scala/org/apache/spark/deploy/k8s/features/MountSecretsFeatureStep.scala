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

import io.fabric8.kubernetes.api.model.{ContainerBuilder, HasMetadata, PodBuilder, VolumeBuilder, VolumeMountBuilder}

import org.apache.spark.deploy.k8s.{KubernetesConf, SparkPod}

private[spark] class MountSecretsFeatureStep(kubernetesConf: KubernetesConf)
  extends KubernetesFeatureConfigStep {
  override def configurePod(pod: SparkPod): SparkPod = {
    val addedVolumes = kubernetesConf
      .secretNamesToMountPaths
      .keys
      .map(secretName =>
        new VolumeBuilder()
          .withName(secretVolumeName(secretName))
          .withNewSecret()
            .withSecretName(secretName)
            .endSecret()
          .build())
    val podWithVolumes = new PodBuilder(pod.pod)
      .editOrNewSpec()
        .addToVolumes(addedVolumes.toSeq: _*)
        .endSpec()
      .build()
    val addedVolumeMounts = kubernetesConf
      .secretNamesToMountPaths
      .map {
        case (secretName, mountPath) =>
          new VolumeMountBuilder()
            .withName(secretVolumeName(secretName))
            .withMountPath(mountPath)
            .build()
      }
    val containerWithMounts = new ContainerBuilder(pod.container)
      .addToVolumeMounts(addedVolumeMounts.toSeq: _*)
      .build()
    SparkPod(podWithVolumes, containerWithMounts)
  }

  private def secretVolumeName(secretName: String): String = s"$secretName-volume"
}
