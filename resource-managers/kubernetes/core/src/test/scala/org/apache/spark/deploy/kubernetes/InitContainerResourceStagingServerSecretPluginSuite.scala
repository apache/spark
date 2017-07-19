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

import org.apache.spark.deploy.kubernetes.constants._
import org.apache.spark.SparkFunSuite

class InitContainerResourceStagingServerSecretPluginSuite extends SparkFunSuite with BeforeAndAfter{
  private val INIT_CONTAINER_SECRET_NAME = "init-secret"
  private val INIT_CONTAINER_SECRET_MOUNT = "/tmp/secret"

  private val initContainerSecretPlugin = new InitContainerResourceStagingServerSecretPluginImpl(
    INIT_CONTAINER_SECRET_NAME,
    INIT_CONTAINER_SECRET_MOUNT)

  test("Volume Mount into InitContainer") {
    val returnedCont = initContainerSecretPlugin.mountResourceStagingServerSecretIntoInitContainer(
      new ContainerBuilder().withName("init-container").build())
    assert(returnedCont.getName === "init-container")
    assert(returnedCont.getVolumeMounts.asScala.map(
      vm => (vm.getName, vm.getMountPath)) ===
        List((INIT_CONTAINER_SECRET_VOLUME_NAME, INIT_CONTAINER_SECRET_MOUNT)))
  }

  test("Add Volume with Secret to Pod") {
    val returnedPod = initContainerSecretPlugin.addResourceStagingServerSecretVolumeToPod(
      basePod().build)
    assert(returnedPod.getMetadata.getName === "spark-pod")
    val volume = returnedPod.getSpec.getVolumes.asScala.head
    assert(volume.getName === INIT_CONTAINER_SECRET_VOLUME_NAME)
    assert(volume.getSecret.getSecretName === INIT_CONTAINER_SECRET_NAME)
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
