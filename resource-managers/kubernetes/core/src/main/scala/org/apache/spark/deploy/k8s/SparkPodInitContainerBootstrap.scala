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

import io.fabric8.kubernetes.api.model.{ContainerBuilder, EmptyDirVolumeSource, PodBuilder, VolumeMount, VolumeMountBuilder}

import org.apache.spark.deploy.k8s.constants._

/**
 * This is separated out from the init-container steps API because this component can be reused to
 * set up the init-container for executors as well.
 */
private[spark] trait SparkPodInitContainerBootstrap {
  /**
   * Bootstraps an init-container that downloads dependencies to be used by a main container.
   * Note that this primarily assumes that the init-container's configuration is being provided
   * by a ConfigMap that was installed by some other component; that is, the implementation
   * here makes no assumptions about how the init-container is specifically configured. For
   * example, this class is unaware if the init-container is fetching remote dependencies or if
   * it is fetching dependencies from a resource staging server. Additionally, the container itself
   * is not actually attached to the pod, but the init container is returned so it can be attached
   * by InitContainerUtil after the caller has decided to make any changes to it.
   */
  def bootstrapInitContainerAndVolumes(
      originalPodWithUnattachedInitContainer: PodWithDetachedInitContainer)
      : PodWithDetachedInitContainer
}

private[spark] class SparkPodInitContainerBootstrapImpl(
    initContainerImage: String,
    dockerImagePullPolicy: String,
    jarsDownloadPath: String,
    filesDownloadPath: String,
    downloadTimeoutMinutes: Long,
    initContainerConfigMapName: String,
    initContainerConfigMapKey: String)
  extends SparkPodInitContainerBootstrap {

  override def bootstrapInitContainerAndVolumes(
      podWithDetachedInitContainer: PodWithDetachedInitContainer): PodWithDetachedInitContainer = {
    val sharedVolumeMounts = Seq[VolumeMount](
      new VolumeMountBuilder()
        .withName(INIT_CONTAINER_DOWNLOAD_JARS_VOLUME_NAME)
        .withMountPath(jarsDownloadPath)
        .build(),
      new VolumeMountBuilder()
        .withName(INIT_CONTAINER_DOWNLOAD_FILES_VOLUME_NAME)
        .withMountPath(filesDownloadPath)
        .build())

    val initContainer = new ContainerBuilder(podWithDetachedInitContainer.initContainer)
      .withName(s"spark-init")
      .withImage(initContainerImage)
      .withImagePullPolicy(dockerImagePullPolicy)
      .addNewVolumeMount()
        .withName(INIT_CONTAINER_PROPERTIES_FILE_VOLUME)
        .withMountPath(INIT_CONTAINER_PROPERTIES_FILE_DIR)
        .endVolumeMount()
      .addToVolumeMounts(sharedVolumeMounts: _*)
      .addToArgs(INIT_CONTAINER_PROPERTIES_FILE_PATH)
      .build()
    val podWithBasicVolumes = new PodBuilder(podWithDetachedInitContainer.pod)
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
        .endSpec()
      .build()
    val mainContainerWithMountedFiles = new ContainerBuilder(
      podWithDetachedInitContainer.mainContainer)
        .addToVolumeMounts(sharedVolumeMounts: _*)
        .addNewEnv()
          .withName(ENV_MOUNTED_FILES_DIR)
          .withValue(filesDownloadPath)
          .endEnv()
        .build()
    PodWithDetachedInitContainer(
      podWithBasicVolumes,
      initContainer,
      mainContainerWithMountedFiles)
  }

}
