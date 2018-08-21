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

import org.mockito.Mockito
import org.scalatest.BeforeAndAfter

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.deploy.k8s._

class TemplateVolumeStepSuite extends SparkFunSuite with BeforeAndAfter {
  private val podTemplateLocalFile = "/path/to/file"
  private var sparkConf: SparkConf = _
  private var kubernetesConf : KubernetesConf[_ <: KubernetesRoleSpecificConf] = _

  before {
    sparkConf = Mockito.mock(classOf[SparkConf])
    kubernetesConf = KubernetesConf(
      sparkConf,
      KubernetesDriverSpecificConf(
        None,
        "app-name",
        "main",
        Seq.empty),
      "resource",
      "app-id",
      Map.empty,
      Map.empty,
      Map.empty,
      Map.empty,
      Map.empty,
      Nil,
      Seq.empty[String])
  }

  test("Mounts executor template volume if config specified") {
    Mockito.doReturn(Option(podTemplateLocalFile)).when(sparkConf)
      .get(Config.KUBERNETES_EXECUTOR_PODTEMPLATE_FILE)
    val step = new TemplateVolumeStep(kubernetesConf)
    val configuredPod = step.configurePod(SparkPod.initialPod())

    assert(configuredPod.pod.getSpec.getVolumes.size() === 1)
    assert(configuredPod.pod.getSpec.getVolumes.get(0).getName === Constants.POD_TEMPLATE_VOLUME)
    assert(configuredPod.pod.getSpec.getVolumes.get(0).getHostPath.getPath === "/path/to/file")
    assert(configuredPod.container.getVolumeMounts.size() === 1)
    assert(configuredPod.container.getVolumeMounts.get(0).getName === Constants.POD_TEMPLATE_VOLUME)
    assert(configuredPod.container.getVolumeMounts.get(0).getMountPath ===
      Constants.EXECUTOR_POD_SPEC_TEMPLATE_FILE)
  }
}
