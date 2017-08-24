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
package org.apache.spark.deploy.kubernetes.submit

import io.fabric8.kubernetes.api.model.{Container, ContainerBuilder, Pod, PodBuilder}

import org.apache.spark.deploy.kubernetes.constants._

private[spark] trait MountSmallFilesBootstrap {
  def mountSmallFilesSecret(pod: Pod, container: Container): (Pod, Container)
}

private[spark] class MountSmallFilesBootstrapImpl(
    secretName: String, secretMountPath: String) extends MountSmallFilesBootstrap {
  def mountSmallFilesSecret(pod: Pod, container: Container): (Pod, Container) = {
    val resolvedPod = new PodBuilder(pod)
      .editOrNewSpec()
        .addNewVolume()
          .withName("submitted-files")
          .withNewSecret()
            .withSecretName(secretName)
            .endSecret()
          .endVolume()
        .endSpec()
      .build()
    val resolvedContainer = new ContainerBuilder(container)
      .addNewEnv()
        .withName(ENV_MOUNTED_FILES_FROM_SECRET_DIR)
        .withValue(secretMountPath)
        .endEnv()
      .addNewVolumeMount()
        .withName("submitted-files")
        .withMountPath(secretMountPath)
        .endVolumeMount()
      .build()
    (resolvedPod, resolvedContainer)
  }
}
