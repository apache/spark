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

import org.apache.spark.deploy.k8s._

private[spark] class MountVolumesFeatureStep(conf: KubernetesConf)
  extends KubernetesFeatureConfigStep {

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
    volumeSpecs.map { spec =>
      val volumeMount = new VolumeMountBuilder()
        .withMountPath(spec.mountPath)
        .withReadOnly(spec.mountReadOnly)
        .withSubPath(spec.mountSubPath)
        .withName(spec.volumeName)
        .build()

      val volumeBuilder = spec.volumeConf match {
        case KubernetesHostPathVolumeConf(hostPath) =>
          /* "" means that no checks will be performed before mounting the hostPath volume */
          new VolumeBuilder()
            .withHostPath(new HostPathVolumeSource(hostPath, ""))

        case KubernetesPVCVolumeConf(claimName) =>
          new VolumeBuilder()
            .withPersistentVolumeClaim(
              new PersistentVolumeClaimVolumeSource(claimName, spec.mountReadOnly))

        case KubernetesEmptyDirVolumeConf(medium, sizeLimit) =>
          new VolumeBuilder()
            .withEmptyDir(
              new EmptyDirVolumeSource(medium.getOrElse(""),
              new Quantity(sizeLimit.orNull)))
      }

      val volume = volumeBuilder.withName(spec.volumeName).build()

      (volumeMount, volume)
    }
  }
}
