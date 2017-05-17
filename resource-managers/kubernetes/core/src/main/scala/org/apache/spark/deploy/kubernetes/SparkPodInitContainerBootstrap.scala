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

import io.fabric8.kubernetes.api.model.{ContainerBuilder, EmptyDirVolumeSource, PodBuilder, VolumeMount, VolumeMountBuilder}

import org.apache.spark.deploy.kubernetes.constants._
import org.apache.spark.deploy.kubernetes.submit.v2.{ContainerNameEqualityPredicate, InitContainerUtil}

private[spark] trait SparkPodInitContainerBootstrap {
  /**
   * Bootstraps an init-container that downloads dependencies to be used by a main container.
   * Note that this primarily assumes that the init-container's configuration is being provided
   * by a ConfigMap that was installed by some other component; that is, the implementation
   * here makes no assumptions about how the init-container is specifically configured. For
   * example, this class is unaware if the init-container is fetching remote dependencies or if
   * it is fetching dependencies from a resource staging server.
   */
  def bootstrapInitContainerAndVolumes(
      mainContainerName: String, originalPodSpec: PodBuilder): PodBuilder
}

private[spark] class SparkPodInitContainerBootstrapImpl(
    initContainerImage: String,
    jarsDownloadPath: String,
    filesDownloadPath: String,
    downloadTimeoutMinutes: Long,
    initContainerConfigMapName: String,
    initContainerConfigMapKey: String,
    resourceStagingServerSecretPlugin: Option[InitContainerResourceStagingServerSecretPlugin])
  extends SparkPodInitContainerBootstrap {

  override def bootstrapInitContainerAndVolumes(
      mainContainerName: String,
      originalPodSpec: PodBuilder): PodBuilder = {
    val sharedVolumeMounts = Seq[VolumeMount](
      new VolumeMountBuilder()
        .withName(INIT_CONTAINER_DOWNLOAD_JARS_VOLUME_NAME)
        .withMountPath(jarsDownloadPath)
        .build(),
      new VolumeMountBuilder()
        .withName(INIT_CONTAINER_DOWNLOAD_FILES_VOLUME_NAME)
        .withMountPath(filesDownloadPath)
        .build())

    val initContainer = new ContainerBuilder()
      .withName(s"spark-init")
      .withImage(initContainerImage)
      .withImagePullPolicy("IfNotPresent")
      .addNewVolumeMount()
        .withName(INIT_CONTAINER_PROPERTIES_FILE_VOLUME)
        .withMountPath(INIT_CONTAINER_PROPERTIES_FILE_DIR)
        .endVolumeMount()
      .addToVolumeMounts(sharedVolumeMounts: _*)
      .addToArgs(INIT_CONTAINER_PROPERTIES_FILE_PATH)
    val resolvedInitContainer = resourceStagingServerSecretPlugin.map { plugin =>
      plugin.mountResourceStagingServerSecretIntoInitContainer(initContainer)
    }.getOrElse(initContainer).build()
    val podWithBasicVolumes = InitContainerUtil.appendInitContainer(
        originalPodSpec, resolvedInitContainer)
      .editSpec()
        .addNewVolume()
          .withName(INIT_CONTAINER_PROPERTIES_FILE_VOLUME)
          .withNewConfigMap()
            .withName(initContainerConfigMapName)
            .addNewItem()
              .withKey(initContainerConfigMapKey)
              .withPath(INIT_CONTAINER_PROPERTIES_FILE_NAME)
              .endItem()
            .endConfigMap()
          .endVolume()
        .addNewVolume()
          .withName(INIT_CONTAINER_DOWNLOAD_JARS_VOLUME_NAME)
          .withEmptyDir(new EmptyDirVolumeSource())
          .endVolume()
        .addNewVolume()
          .withName(INIT_CONTAINER_DOWNLOAD_FILES_VOLUME_NAME)
          .withEmptyDir(new EmptyDirVolumeSource())
          .endVolume()
        .editMatchingContainer(new ContainerNameEqualityPredicate(mainContainerName))
          .addToVolumeMounts(sharedVolumeMounts: _*)
          .endContainer()
        .endSpec()
    resourceStagingServerSecretPlugin.map { plugin =>
      plugin.addResourceStagingServerSecretVolumeToPod(podWithBasicVolumes)
    }.getOrElse(podWithBasicVolumes)
  }

}
