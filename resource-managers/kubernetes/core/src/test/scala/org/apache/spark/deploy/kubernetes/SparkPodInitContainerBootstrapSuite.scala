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

import io.fabric8.kubernetes.api.model._
import org.scalatest.BeforeAndAfter
import scala.collection.JavaConverters._

import org.apache.spark.SparkFunSuite
import org.apache.spark.deploy.kubernetes.constants._

class SparkPodInitContainerBootstrapSuite extends SparkFunSuite with BeforeAndAfter {
  private val INIT_CONTAINER_IMAGE = "spark-init:latest"
  private val DOCKER_IMAGE_PULL_POLICY = "IfNotPresent"
  private val JARS_DOWNLOAD_PATH = "/var/data/spark-jars"
  private val FILES_DOWNLOAD_PATH = "/var/data/spark-files"
  private val DOWNLOAD_TIMEOUT_MINUTES = 5
  private val INIT_CONTAINER_CONFIG_MAP_NAME = "spark-init-config-map"
  private val INIT_CONTAINER_CONFIG_MAP_KEY = "spark-init-config-map-key"
  private val MAIN_CONTAINER_NAME = "spark-main"

  private val sparkPodInit = new SparkPodInitContainerBootstrapImpl(
    INIT_CONTAINER_IMAGE,
    DOCKER_IMAGE_PULL_POLICY,
    JARS_DOWNLOAD_PATH,
    FILES_DOWNLOAD_PATH,
    DOWNLOAD_TIMEOUT_MINUTES,
    INIT_CONTAINER_CONFIG_MAP_NAME,
    INIT_CONTAINER_CONFIG_MAP_KEY)
  private val expectedSharedVolumeMap = Map(
    JARS_DOWNLOAD_PATH -> INIT_CONTAINER_DOWNLOAD_JARS_VOLUME_NAME,
    FILES_DOWNLOAD_PATH -> INIT_CONTAINER_DOWNLOAD_FILES_VOLUME_NAME)

  test("InitContainer: Volume mounts, args, and builder specs") {
    val returnedPodWithCont = sparkPodInit.bootstrapInitContainerAndVolumes(
      PodWithDetachedInitContainer(
        pod = basePod().build(),
        initContainer = new Container(),
        mainContainer = new ContainerBuilder().withName(MAIN_CONTAINER_NAME).build()))
    val initContainer: Container = returnedPodWithCont.initContainer
    val volumes = initContainer.getVolumeMounts.asScala
    assert(volumes.map(vm => (vm.getMountPath, vm.getName)).toMap === expectedSharedVolumeMap
      ++ Map("/etc/spark-init" -> "spark-init-properties"))
    assert(initContainer.getName === "spark-init")
    assert(initContainer.getImage === INIT_CONTAINER_IMAGE)
    assert(initContainer.getImagePullPolicy === DOCKER_IMAGE_PULL_POLICY)
    assert(initContainer.getArgs.asScala.head === INIT_CONTAINER_PROPERTIES_FILE_PATH)
  }
  test("Main: Volume mounts and env") {
    val returnedPodWithCont = sparkPodInit.bootstrapInitContainerAndVolumes(
      PodWithDetachedInitContainer(
        pod = basePod().build(),
        initContainer = new Container(),
        mainContainer = new ContainerBuilder().withName(MAIN_CONTAINER_NAME).build()))
    val mainContainer: Container = returnedPodWithCont.mainContainer
    assert(mainContainer.getName === MAIN_CONTAINER_NAME)
    val volumeMounts = mainContainer.getVolumeMounts.asScala
    assert(volumeMounts.map(vm => (vm.getMountPath, vm.getName)).toMap === expectedSharedVolumeMap)
    assert(mainContainer.getEnv.asScala.map(e => (e.getName, e.getValue)).toMap ===
      Map(ENV_MOUNTED_FILES_DIR -> FILES_DOWNLOAD_PATH))
  }
  test("Pod: Volume Mounts") {
    val returnedPodWithCont = sparkPodInit.bootstrapInitContainerAndVolumes(
      PodWithDetachedInitContainer(
        pod = basePod().build(),
        initContainer = new Container(),
        mainContainer = new ContainerBuilder().withName(MAIN_CONTAINER_NAME).build()))
    val returnedPod = returnedPodWithCont.pod
    assert(returnedPod.getMetadata.getName === "spark-pod")
    val volumes = returnedPod.getSpec.getVolumes.asScala.toList
    assert(volumes.head.getName === INIT_CONTAINER_PROPERTIES_FILE_VOLUME)
    assert(volumes.head.getConfigMap.getName === INIT_CONTAINER_CONFIG_MAP_NAME)
    assert(volumes.head.getConfigMap.getItems.asScala.map(
      i => (i.getKey, i.getPath)) ===
        List((INIT_CONTAINER_CONFIG_MAP_KEY, INIT_CONTAINER_PROPERTIES_FILE_NAME)))
    assert(volumes(1).getName === INIT_CONTAINER_DOWNLOAD_JARS_VOLUME_NAME)
    assert(volumes(1).getEmptyDir === new EmptyDirVolumeSource())
    assert(volumes(2).getName === INIT_CONTAINER_DOWNLOAD_FILES_VOLUME_NAME)
    assert(volumes(2).getEmptyDir === new EmptyDirVolumeSource())
  }

  private def basePod(): PodBuilder = {
    new PodBuilder()
      .withNewMetadata()
        .withName("spark-pod")
        .endMetadata()
      .withNewSpec()
      .endSpec()
  }
}
