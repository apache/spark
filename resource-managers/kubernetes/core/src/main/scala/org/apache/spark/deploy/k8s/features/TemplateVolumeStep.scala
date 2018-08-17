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

import io.fabric8.kubernetes.api.model.{Config => _, _}

import org.apache.spark.deploy.k8s._

private[spark] class TemplateVolumeStep(
   conf: KubernetesConf[_ <: KubernetesRoleSpecificConf])
  extends KubernetesFeatureConfigStep {
  def configurePod(pod: SparkPod): SparkPod = {
    require(conf.get(Config.KUBERNETES_EXECUTOR_PODTEMPLATE_FILE).isDefined)
    val podTemplateFile = conf.get(Config.KUBERNETES_EXECUTOR_PODTEMPLATE_FILE).get
    val podWithVolume = new PodBuilder(pod.pod)
      .editSpec()
      .addNewVolume()
      .withName(Constants.POD_TEMPLATE_VOLUME)
      .withHostPath(new HostPathVolumeSource(podTemplateFile))
      .endVolume()
      .endSpec()
      .build()

    val containerWithVolume = new ContainerBuilder(pod.container)
        .withVolumeMounts(new VolumeMountBuilder()
          .withName(Constants.POD_TEMPLATE_VOLUME)
          .withMountPath(Constants.EXECUTOR_POD_SPEC_TEMPLATE_FILE)
          .build())
        .build()
    SparkPod(podWithVolume, containerWithVolume)
  }

  def getAdditionalPodSystemProperties(): Map[String, String] = Map[String, String](
    Config.KUBERNETES_EXECUTOR_PODTEMPLATE_FILE.key -> Constants.EXECUTOR_POD_SPEC_TEMPLATE_FILE)

  def getAdditionalKubernetesResources(): Seq[HasMetadata] = Seq.empty
}
