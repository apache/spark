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

import io.fabric8.kubernetes.api.model._

import org.apache.spark.deploy.k8s.{KubernetesConf, SparkPod}
import org.apache.spark.deploy.k8s.Config._

/**
 * Create by xxh on 2021/10/25 10:40
 */
private[spark] class AffinityFeatureStep(conf: KubernetesConf)
  extends KubernetesFeatureConfigStep {

  private val hasAffinity = conf.contains(KUBERNETES_EXECUTOR_POD_AffinityKey)

  override def configurePod(pod: SparkPod): SparkPod = {
    if (hasAffinity) {

      val affinity = {
        val podAntiAffinity = {
          val labelSelector = {
            val labelSelectorRequirement = new LabelSelectorRequirementBuilder()
              .withKey(conf.get(KUBERNETES_EXECUTOR_POD_AffinityKey.key))
              .withOperator(conf.get(KUBERNETES_EXECUTOR_POD_AffinityOperator))
              .withValues(conf.get(KUBERNETES_EXECUTOR_POD_AffinityValue))
              .build()
            new LabelSelectorBuilder()
              .withMatchExpressions(labelSelectorRequirement)
              .build()
          }
          val podAffinityTerm = new PodAffinityTermBuilder()
            .withLabelSelector(labelSelector)
            .withTopologyKey("kubernetes.io/hostname")
            .build()
          new PodAntiAffinityBuilder()
            .withRequiredDuringSchedulingIgnoredDuringExecution(podAffinityTerm)
            .build()
        }

        new AffinityBuilder().withPodAntiAffinity(podAntiAffinity).build()
      }

      val podWithAffinity = new PodBuilder(pod.pod)
        .editSpec()
        .withAffinity(affinity)
        .endSpec()
        .build()

      SparkPod(podWithAffinity, pod.container)
    } else {
      pod
    }
  }
}
