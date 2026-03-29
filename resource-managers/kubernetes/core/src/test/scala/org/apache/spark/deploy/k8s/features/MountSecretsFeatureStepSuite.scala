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
package org.apache.spark.deploy.k8s.features

import org.apache.spark.SparkFunSuite
import org.apache.spark.deploy.k8s.{KubernetesTestConf, SecretVolumeUtils, SparkPod}

class MountSecretsFeatureStepSuite extends SparkFunSuite {

  private val SECRET_FOO = "foo"
  private val SECRET_BAR = "bar"
  private val SECRET_MOUNT_PATH = "/etc/secrets/driver"

  test("mounts all given secrets") {
    val baseDriverPod = SparkPod.initialPod()
    val secretNamesToMountPaths = Map(
      SECRET_FOO -> SECRET_MOUNT_PATH,
      SECRET_BAR -> SECRET_MOUNT_PATH)
    val kubernetesConf = KubernetesTestConf.createExecutorConf(
      secretNamesToMountPaths = secretNamesToMountPaths)

    val step = new MountSecretsFeatureStep(kubernetesConf)
    val driverPodWithSecretsMounted = step.configurePod(baseDriverPod).pod
    val driverContainerWithSecretsMounted = step.configurePod(baseDriverPod).container

    Seq(s"$SECRET_FOO-volume", s"$SECRET_BAR-volume").foreach { volumeName =>
      assert(SecretVolumeUtils.podHasVolume(driverPodWithSecretsMounted, volumeName))
    }
    Seq(s"$SECRET_FOO-volume", s"$SECRET_BAR-volume").foreach { volumeName =>
      assert(SecretVolumeUtils.containerHasVolume(
        driverContainerWithSecretsMounted, volumeName, SECRET_MOUNT_PATH))
    }
  }
}
