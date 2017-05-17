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

import io.fabric8.kubernetes.api.model.{ContainerBuilder, PodBuilder}
import scala.collection.JavaConverters._

import org.apache.spark.SparkFunSuite
import org.apache.spark.deploy.kubernetes.constants._

class SubmittedDependencyInitContainerVolumesPluginSuite extends SparkFunSuite {

  private val SECRET_NAME = "secret"
  private val SECRET_MOUNT_PATH = "/mnt/secrets"
  private val plugin = new InitContainerResourceStagingServerSecretPluginImpl(
      SECRET_NAME, SECRET_MOUNT_PATH)

  test("The init container should have the secret volume mount.") {
    val baseInitContainer = new ContainerBuilder().withName("container")
    val configuredInitContainer = plugin.mountResourceStagingServerSecretIntoInitContainer(
        baseInitContainer).build()
    val volumeMounts = configuredInitContainer.getVolumeMounts.asScala
    assert(volumeMounts.size === 1)
    assert(volumeMounts.exists { volumeMount =>
      volumeMount.getName === INIT_CONTAINER_SECRET_VOLUME_NAME &&
          volumeMount.getMountPath === SECRET_MOUNT_PATH
    })
  }

  test("The pod should have the secret volume.") {
    val basePod = new PodBuilder()
      .withNewMetadata().withName("pod").endMetadata()
      .withNewSpec()
        .addNewContainer()
          .withName("container")
          .endContainer()
        .endSpec()
    val configuredPod = plugin.addResourceStagingServerSecretVolumeToPod(basePod).build()
    val volumes = configuredPod.getSpec.getVolumes.asScala
    assert(volumes.size === 1)
    assert(volumes.exists { volume =>
      volume.getName === INIT_CONTAINER_SECRET_VOLUME_NAME &&
          Option(volume.getSecret).map(_.getSecretName).contains(SECRET_NAME)
    })
  }
}
