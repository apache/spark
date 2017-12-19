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

import scala.collection.JavaConverters._

import io.fabric8.kubernetes.api.model.{ContainerBuilder, EmptyDirVolumeSource, EnvVarBuilder, PodBuilder, VolumeMount, VolumeMountBuilder}

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.Constants._

/**
 * Bootstraps an init-container for downloading remote dependencies. This is separated out from
 * the init-container steps API because this component can be used to bootstrap init-containers
 * for both the driver and executors.
 */
private[spark] class InitContainerBootstrap(
    initContainerImage: String,
    imagePullPolicy: String,
    jarsDownloadPath: String,
    filesDownloadPath: String,
    configMapName: String,
    configMapKey: String,
    sparkRole: String,
    sparkConf: SparkConf) {

  /**
   * Bootstraps an init-container that downloads dependencies to be used by a main container.
   */
  def bootstrapInitContainer(
      original: PodWithDetachedInitContainer): PodWithDetachedInitContainer = {
    val sharedVolumeMounts = Seq[VolumeMount](
      new VolumeMountBuilder()
        .withName(INIT_CONTAINER_DOWNLOAD_JARS_VOLUME_NAME)
        .withMountPath(jarsDownloadPath)
        .build(),
      new VolumeMountBuilder()
        .withName(INIT_CONTAINER_DOWNLOAD_FILES_VOLUME_NAME)
        .withMountPath(filesDownloadPath)
        .build())

    val customEnvVarKeyPrefix = sparkRole match {
      case SPARK_POD_DRIVER_ROLE => KUBERNETES_DRIVER_ENV_KEY
      case SPARK_POD_EXECUTOR_ROLE => "spark.executorEnv."
      case _ => throw new SparkException(s"$sparkRole is not a valid Spark pod role")
    }
    val customEnvVars = sparkConf.getAllWithPrefix(customEnvVarKeyPrefix).toSeq.map { env =>
      new EnvVarBuilder()
        .withName(env._1)
        .withValue(env._2)
        .build()
    }

    val initContainer = new ContainerBuilder(original.initContainer)
      .withName("spark-init")
      .withImage(initContainerImage)
      .withImagePullPolicy(imagePullPolicy)
      .addAllToEnv(customEnvVars.asJava)
      .addNewVolumeMount()
        .withName(INIT_CONTAINER_PROPERTIES_FILE_VOLUME)
        .withMountPath(INIT_CONTAINER_PROPERTIES_FILE_DIR)
        .endVolumeMount()
      .addToVolumeMounts(sharedVolumeMounts: _*)
      .addToArgs(INIT_CONTAINER_PROPERTIES_FILE_PATH)
      .build()

    val podWithBasicVolumes = new PodBuilder(original.pod)
      .editSpec()
      .addNewVolume()
        .withName(INIT_CONTAINER_PROPERTIES_FILE_VOLUME)
        .withNewConfigMap()
          .withName(configMapName)
          .addNewItem()
            .withKey(configMapKey)
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

    val mainContainer = new ContainerBuilder(
      original.mainContainer)
        .addToVolumeMounts(sharedVolumeMounts: _*)
        .addNewEnv()
          .withName(ENV_MOUNTED_FILES_DIR)
          .withValue(filesDownloadPath)
          .endEnv()
        .build()

    PodWithDetachedInitContainer(
      podWithBasicVolumes,
      initContainer,
      mainContainer)
  }
}
