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

import com.fasterxml.jackson.databind.ObjectMapper
import io.fabric8.kubernetes.api.model.{Container, ContainerBuilder, Pod, PodBuilder}
import org.scalatest.BeforeAndAfter
import scala.collection.JavaConverters._

import org.apache.spark.SparkFunSuite
import org.apache.spark.deploy.kubernetes.constants._

class SparkPodInitContainerBootstrapSuite extends SparkFunSuite with BeforeAndAfter {
  private val OBJECT_MAPPER = new ObjectMapper()
  private val INIT_CONTAINER_IMAGE = "spark-init:latest"
  private val JARS_DOWNLOAD_PATH = "/var/data/spark-jars"
  private val FILES_DOWNLOAD_PATH = "/var/data/spark-files"
  private val DOWNLOAD_TIMEOUT_MINUTES = 5
  private val INIT_CONTAINER_CONFIG_MAP_NAME = "spark-init-config-map"
  private val INIT_CONTAINER_CONFIG_MAP_KEY = "spark-init-config-map-key"
  private val ADDED_SUBMITTED_DEPENDENCY_ENV = "ADDED_SUBMITTED_DEPENDENCY"
  private val ADDED_SUBMITTED_DEPENDENCY_ANNOTATION = "added-submitted-dependencies"
  private val MAIN_CONTAINER_NAME = "spark-main"
  private val TRUE = "true"

  private val submittedDependencyPlugin = new InitContainerResourceStagingServerSecretPlugin {
    override def addResourceStagingServerSecretVolumeToPod(basePod: PodBuilder)
        : PodBuilder = {
      basePod.editMetadata()
        .addToAnnotations(ADDED_SUBMITTED_DEPENDENCY_ANNOTATION, TRUE)
        .endMetadata()
    }

    override def mountResourceStagingServerSecretIntoInitContainer(container: ContainerBuilder)
        : ContainerBuilder = {
      container
        .addNewEnv()
          .withName(ADDED_SUBMITTED_DEPENDENCY_ENV)
          .withValue(TRUE)
          .endEnv()
    }
  }

  test("Running without submitted dependencies adds init-container with volume mounts.") {
    val bootstrappedPod = bootstrapPodWithoutSubmittedDependencies()
    val podAnnotations = bootstrappedPod.getMetadata.getAnnotations.asScala
    assert(podAnnotations.contains(INIT_CONTAINER_ANNOTATION))
    val initContainers = OBJECT_MAPPER.readValue(
        podAnnotations(INIT_CONTAINER_ANNOTATION), classOf[Array[Container]])
    assert(initContainers.length === 1)
    val initContainer = initContainers.head
    val initContainerVolumeMounts = initContainer.getVolumeMounts.asScala.map {
      mount => (mount.getName, mount.getMountPath)
    }.toMap
    val expectedInitContainerVolumeMounts = Map(
      INIT_CONTAINER_PROPERTIES_FILE_VOLUME -> INIT_CONTAINER_PROPERTIES_FILE_DIR,
      INIT_CONTAINER_DOWNLOAD_JARS_VOLUME_NAME -> JARS_DOWNLOAD_PATH,
      INIT_CONTAINER_DOWNLOAD_FILES_VOLUME_NAME -> FILES_DOWNLOAD_PATH)
    assert(initContainerVolumeMounts === expectedInitContainerVolumeMounts)
    assert(initContainer.getName === "spark-init")
    assert(initContainer.getImage === INIT_CONTAINER_IMAGE)
    assert(initContainer.getImagePullPolicy === "IfNotPresent")
    assert(initContainer.getArgs.asScala === List(INIT_CONTAINER_PROPERTIES_FILE_PATH))
  }

  test("Running without submitted dependencies adds volume mounts to main container.") {
    val bootstrappedPod = bootstrapPodWithoutSubmittedDependencies()
    val containers = bootstrappedPod.getSpec.getContainers.asScala
    val mainContainer = containers.find(_.getName === MAIN_CONTAINER_NAME)
    assert(mainContainer.isDefined)
    val volumeMounts = mainContainer.map(_.getVolumeMounts.asScala).toSeq.flatten.map {
      mount => (mount.getName, mount.getMountPath)
    }.toMap
    val expectedVolumeMounts = Map(
      INIT_CONTAINER_DOWNLOAD_JARS_VOLUME_NAME -> JARS_DOWNLOAD_PATH,
      INIT_CONTAINER_DOWNLOAD_FILES_VOLUME_NAME -> FILES_DOWNLOAD_PATH)
    assert(volumeMounts === expectedVolumeMounts)
  }

  test("Running without submitted dependencies adds volumes to the pod") {
    val bootstrappedPod = bootstrapPodWithoutSubmittedDependencies()
    val podVolumes = bootstrappedPod.getSpec.getVolumes.asScala
    assert(podVolumes.size === 3)
    assert(podVolumes.exists { volume =>
      volume.getName == INIT_CONTAINER_PROPERTIES_FILE_VOLUME &&
        Option(volume.getConfigMap).map { configMap =>
          configMap.getItems.asScala.map {
            keyToPath => (keyToPath.getKey, keyToPath.getPath)
          }.toMap
        }.contains(Map(INIT_CONTAINER_CONFIG_MAP_KEY -> INIT_CONTAINER_PROPERTIES_FILE_NAME))
    })
    assert(podVolumes.exists { volume =>
      volume.getName == INIT_CONTAINER_DOWNLOAD_JARS_VOLUME_NAME && volume.getEmptyDir != null
    })
    assert(podVolumes.exists { volume =>
      volume.getName == INIT_CONTAINER_DOWNLOAD_FILES_VOLUME_NAME && volume.getEmptyDir != null
    })
  }

  test("Running with submitted dependencies modifies the init container with the plugin.") {
    val bootstrappedPod = bootstrapPodWithSubmittedDependencies()
    val podAnnotations = bootstrappedPod.getMetadata.getAnnotations.asScala
    assert(podAnnotations(ADDED_SUBMITTED_DEPENDENCY_ANNOTATION) === TRUE)
    val initContainers = OBJECT_MAPPER.readValue(
      podAnnotations(INIT_CONTAINER_ANNOTATION), classOf[Array[Container]])
    assert(initContainers.length === 1)
    val initContainer = initContainers.head
    assert(initContainer.getEnv.asScala.exists {
      env => env.getName === ADDED_SUBMITTED_DEPENDENCY_ENV && env.getValue === TRUE
    })
  }

  private def bootstrapPodWithoutSubmittedDependencies(): Pod = {
    val bootstrapUnderTest = new SparkPodInitContainerBootstrapImpl(
      INIT_CONTAINER_IMAGE,
      JARS_DOWNLOAD_PATH,
      FILES_DOWNLOAD_PATH,
      DOWNLOAD_TIMEOUT_MINUTES,
      INIT_CONTAINER_CONFIG_MAP_NAME,
      INIT_CONTAINER_CONFIG_MAP_KEY,
      None)
    bootstrapUnderTest.bootstrapInitContainerAndVolumes(
      MAIN_CONTAINER_NAME, basePod()).build()
  }

  private def bootstrapPodWithSubmittedDependencies(): Pod = {
    val bootstrapUnderTest = new SparkPodInitContainerBootstrapImpl(
      INIT_CONTAINER_IMAGE,
      JARS_DOWNLOAD_PATH,
      FILES_DOWNLOAD_PATH,
      DOWNLOAD_TIMEOUT_MINUTES,
      INIT_CONTAINER_CONFIG_MAP_NAME,
      INIT_CONTAINER_CONFIG_MAP_KEY,
      Some(submittedDependencyPlugin))
    bootstrapUnderTest.bootstrapInitContainerAndVolumes(
      MAIN_CONTAINER_NAME, basePod()).build()
  }

  private def basePod(): PodBuilder = {
    new PodBuilder()
      .withNewMetadata()
        .withName("spark-pod")
        .endMetadata()
      .withNewSpec()
        .addNewContainer()
          .withName(MAIN_CONTAINER_NAME)
          .endContainer()
        .endSpec()
  }
}
