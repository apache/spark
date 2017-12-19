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
package org.apache.spark.deploy.k8s

import io.fabric8.kubernetes.api.model.{Container, ContainerBuilder, Pod, PodBuilder}

/**
 * Bootstraps a driver or executor container or an init-container with needed secrets mounted.
 */
private[spark] class MountSecretsBootstrap(secretNamesToMountPaths: Map[String, String]) {

  /**
   * Mounts Kubernetes secrets as secret volumes into the given container in the given pod.
   *
   * @param pod the pod into which the secret volumes are being added.
   * @param container the container into which the secret volumes are being mounted.
   * @return the updated pod and container with the secrets mounted.
   */
  def mountSecrets(pod: Pod, container: Container): (Pod, Container) = {
    var podBuilder = new PodBuilder(pod)
    secretNamesToMountPaths.keys.foreach { name =>
      podBuilder = podBuilder
        .editOrNewSpec()
          .addNewVolume()
          .withName(secretVolumeName(name))
          .withNewSecret()
            .withSecretName(name)
            .endSecret()
          .endVolume()
          .endSpec()
    }

    var containerBuilder = new ContainerBuilder(container)
    secretNamesToMountPaths.foreach { namePath =>
      containerBuilder = containerBuilder
        .addNewVolumeMount()
          .withName(secretVolumeName(namePath._1))
          .withMountPath(namePath._2)
          .endVolumeMount()
    }

    (podBuilder.build(), containerBuilder.build())
  }

  private def secretVolumeName(secretName: String): String = {
    secretName + "-volume"
  }
}
