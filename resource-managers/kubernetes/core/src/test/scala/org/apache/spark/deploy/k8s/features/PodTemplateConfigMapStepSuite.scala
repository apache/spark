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

import java.io.{File, PrintWriter}
import java.nio.file.Files

import io.fabric8.kubernetes.api.model.ConfigMap
import org.mockito.Mockito
import org.scalatest.BeforeAndAfter

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.deploy.k8s._
import org.apache.spark.deploy.k8s.submit.JavaMainAppResource

class PodTemplateConfigMapStepSuite extends SparkFunSuite with BeforeAndAfter {
  private var sparkConf: SparkConf = _
  private var kubernetesConf : KubernetesConf[_ <: KubernetesRoleSpecificConf] = _
  private var templateFile: File = _

  before {
    sparkConf = Mockito.mock(classOf[SparkConf])
    kubernetesConf = KubernetesConf(
      sparkConf,
      KubernetesDriverSpecificConf(
        JavaMainAppResource(None),
        "app-name",
        "main",
        Seq.empty),
      "resource",
      "app-id",
      None,
      Map.empty,
      Map.empty,
      Map.empty,
      Map.empty,
      Map.empty,
      Nil,
      Option.empty)
    templateFile = Files.createTempFile("pod-template", "yml").toFile
    templateFile.deleteOnExit()
    Mockito.doReturn(Option(templateFile.getAbsolutePath)).when(sparkConf)
      .get(Config.KUBERNETES_EXECUTOR_PODTEMPLATE_FILE)
  }

  test("Mounts executor template volume if config specified") {
    val writer = new PrintWriter(templateFile)
    writer.write("pod-template-contents")
    writer.close()

    val step = new PodTemplateConfigMapStep(kubernetesConf)
    val configuredPod = step.configurePod(SparkPod.initialPod())

    assert(configuredPod.pod.getSpec.getVolumes.size() === 1)
    val volume = configuredPod.pod.getSpec.getVolumes.get(0)
    assert(volume.getName === Constants.POD_TEMPLATE_VOLUME)
    assert(volume.getConfigMap.getName === Constants.POD_TEMPLATE_CONFIGMAP)
    assert(volume.getConfigMap.getItems.size() === 1)
    assert(volume.getConfigMap.getItems.get(0).getKey === Constants.POD_TEMPLATE_KEY)
    assert(volume.getConfigMap.getItems.get(0).getPath ===
      Constants.EXECUTOR_POD_SPEC_TEMPLATE_FILE_NAME)

    assert(configuredPod.container.getVolumeMounts.size() === 1)
    val volumeMount = configuredPod.container.getVolumeMounts.get(0)
    assert(volumeMount.getMountPath === Constants.EXECUTOR_POD_SPEC_TEMPLATE_MOUNTPATH)
    assert(volumeMount.getName === Constants.POD_TEMPLATE_VOLUME)

    val resources = step.getAdditionalKubernetesResources()
    assert(resources.size === 1)
    assert(resources.head.getMetadata.getName === Constants.POD_TEMPLATE_CONFIGMAP)
    assert(resources.head.isInstanceOf[ConfigMap])
    val configMap = resources.head.asInstanceOf[ConfigMap]
    assert(configMap.getData.size() === 1)
    assert(configMap.getData.containsKey(Constants.POD_TEMPLATE_KEY))
    assert(configMap.getData.containsValue("pod-template-contents"))

    val systemProperties = step.getAdditionalPodSystemProperties()
    assert(systemProperties.size === 1)
    assert(systemProperties.contains(Config.KUBERNETES_EXECUTOR_PODTEMPLATE_FILE.key))
    assert(systemProperties.get(Config.KUBERNETES_EXECUTOR_PODTEMPLATE_FILE.key).get ===
      (Constants.EXECUTOR_POD_SPEC_TEMPLATE_MOUNTPATH + "/" +
        Constants.EXECUTOR_POD_SPEC_TEMPLATE_FILE_NAME))
  }
}
