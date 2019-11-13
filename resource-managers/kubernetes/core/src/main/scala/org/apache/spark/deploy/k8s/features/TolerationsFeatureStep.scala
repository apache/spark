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

import io.fabric8.kubernetes.api.model.{ContainerBuilder, HasMetadata, PodBuilder, Toleration, TolerationBuilder}

import org.apache.spark.deploy.k8s.{KubernetesConf, KubernetesRoleSpecificConf, KubernetesTolerationSpec, SparkPod}

private[spark] class TolerationsFeatureStep(
    kubernetesConf: KubernetesConf[_ <: KubernetesRoleSpecificConf])
  extends KubernetesFeatureConfigStep {
  override def configurePod(pod: SparkPod): SparkPod = {

    val toleration = new TolerationBuilder()

    def build(
      tolerationConf: KubernetesTolerationSpec): Toleration = {
        val newToleration = new TolerationBuilder()
                              .withNewOperator(tolerationConf.operator.toString)

        tolerationConf.key match {
          case Some(i) => newToleration.withNewKey(i)
          case _ =>
        }

        tolerationConf.effect match {
          case Some(i) => newToleration.withNewEffect(i)
          case _ =>
        }

        tolerationConf.value match {
          case Some(i) => newToleration.withNewValue(i)
          case _ =>
        }

        tolerationConf.tolerationSeconds match {
          case Some(i) => newToleration.withTolerationSeconds(i)
          case _ =>
        }

        newToleration.build()
    }

    val addedTolerations = kubernetesConf
      .roleTolerations
      .map(toleration => build(toleration))

    val podWithTolerations = new PodBuilder(pod.pod)
      .editOrNewSpec()
        .addToTolerations(addedTolerations.toSeq: _*)
        .endSpec()
      .build()
    SparkPod(podWithTolerations, pod.container)
  }

  override def getAdditionalPodSystemProperties(): Map[String, String] = Map.empty

  override def getAdditionalKubernetesResources(): Seq[HasMetadata] = Seq.empty

}
