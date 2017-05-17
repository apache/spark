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
package org.apache.spark.deploy.kubernetes

import io.fabric8.kubernetes.api.model.{ContainerBuilder, PodBuilder, Secret}

import org.apache.spark.deploy.kubernetes.constants._

private[spark] trait InitContainerResourceStagingServerSecretPlugin {

  /**
   * Configure the init-container to mount the secret files that allow it to retrieve dependencies
   * from a resource staging server.
   */
  def mountResourceStagingServerSecretIntoInitContainer(
      initContainer: ContainerBuilder): ContainerBuilder

  /**
   * Configure the pod to attach a Secret volume which hosts secret files allowing the
   * init-container to retrieve dependencies from the resource staging server.
   */
  def addResourceStagingServerSecretVolumeToPod(basePod: PodBuilder): PodBuilder
}

private[spark] class InitContainerResourceStagingServerSecretPluginImpl(
    initContainerSecretName: String,
    initContainerSecretMountPath: String)
    extends InitContainerResourceStagingServerSecretPlugin {

  override def mountResourceStagingServerSecretIntoInitContainer(
      initContainer: ContainerBuilder): ContainerBuilder = {
    initContainer.addNewVolumeMount()
      .withName(INIT_CONTAINER_SECRET_VOLUME_NAME)
      .withMountPath(initContainerSecretMountPath)
      .endVolumeMount()
  }

  override def addResourceStagingServerSecretVolumeToPod(basePod: PodBuilder): PodBuilder = {
    basePod.editSpec()
      .addNewVolume()
        .withName(INIT_CONTAINER_SECRET_VOLUME_NAME)
        .withNewSecret()
          .withSecretName(initContainerSecretName)
          .endSecret()
        .endVolume()
      .endSpec()
  }
}
