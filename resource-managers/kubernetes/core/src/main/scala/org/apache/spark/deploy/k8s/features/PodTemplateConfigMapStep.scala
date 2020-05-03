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

import java.io.File
import java.nio.charset.StandardCharsets

import com.google.common.io.Files
import io.fabric8.kubernetes.api.model.{ConfigMapBuilder, ContainerBuilder, HasMetadata, PodBuilder}

import org.apache.spark.deploy.k8s.{KubernetesConf, SparkPod}
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.Constants._

private[spark] class PodTemplateConfigMapStep(conf: KubernetesConf)
  extends KubernetesFeatureConfigStep {

  private val hasTemplate = conf.contains(KUBERNETES_EXECUTOR_PODTEMPLATE_FILE)

  def configurePod(pod: SparkPod): SparkPod = {
    if (hasTemplate) {
      val podWithVolume = new PodBuilder(pod.pod)
          .editSpec()
            .addNewVolume()
              .withName(POD_TEMPLATE_VOLUME)
              .withNewConfigMap()
                .withName(POD_TEMPLATE_CONFIGMAP)
                .addNewItem()
                  .withKey(POD_TEMPLATE_KEY)
                  .withPath(EXECUTOR_POD_SPEC_TEMPLATE_FILE_NAME)
                .endItem()
              .endConfigMap()
            .endVolume()
          .endSpec()
        .build()

      val containerWithVolume = new ContainerBuilder(pod.container)
          .addNewVolumeMount()
            .withName(POD_TEMPLATE_VOLUME)
            .withMountPath(EXECUTOR_POD_SPEC_TEMPLATE_MOUNTPATH)
          .endVolumeMount()
        .build()
      SparkPod(podWithVolume, containerWithVolume)
    } else {
      pod
    }
  }

  override def getAdditionalPodSystemProperties(): Map[String, String] = {
    if (hasTemplate) {
      Map[String, String](
        KUBERNETES_EXECUTOR_PODTEMPLATE_FILE.key ->
          (EXECUTOR_POD_SPEC_TEMPLATE_MOUNTPATH + "/" + EXECUTOR_POD_SPEC_TEMPLATE_FILE_NAME))
    } else {
      Map.empty
    }
  }

  override def getAdditionalKubernetesResources(): Seq[HasMetadata] = {
    if (hasTemplate) {
      val podTemplateFile = conf.get(KUBERNETES_EXECUTOR_PODTEMPLATE_FILE).get
      val podTemplateString = Files.toString(new File(podTemplateFile), StandardCharsets.UTF_8)
      Seq(new ConfigMapBuilder()
          .withNewMetadata()
            .withName(POD_TEMPLATE_CONFIGMAP)
          .endMetadata()
          .addToData(POD_TEMPLATE_KEY, podTemplateString)
        .build())
    } else {
      Nil
    }
  }
}
