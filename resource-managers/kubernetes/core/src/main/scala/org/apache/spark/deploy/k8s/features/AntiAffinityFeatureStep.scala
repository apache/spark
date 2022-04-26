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

import io.fabric8.kubernetes.api.model.PodBuilder

import org.apache.spark.deploy.k8s.{KubernetesExecutorConf, SparkPod}
import org.apache.spark.deploy.k8s.Config.KUBERNETES_EXECUTOR_POD_ANTI_AFFINITY
import org.apache.spark.deploy.k8s.Constants.SPARK_APP_ID_LABEL
import org.apache.spark.internal.Logging

class AntiAffinityFeatureStep(kubernetesConf: KubernetesExecutorConf)
  extends KubernetesFeatureConfigStep with Logging {

  private val enableExecutorPodAntiAffinity =
    kubernetesConf.get(KUBERNETES_EXECUTOR_POD_ANTI_AFFINITY)

  override def configurePod(pod: SparkPod): SparkPod = {
    if (enableExecutorPodAntiAffinity) {
      val podWithAntiAffinity = new PodBuilder(pod.pod)
        .editOrNewSpec()
          .editOrNewAffinity()
            .editOrNewPodAntiAffinity()
              .addNewPreferredDuringSchedulingIgnoredDuringExecution()
                .editOrNewPodAffinityTerm()
                  .editOrNewLabelSelector()
                    .addNewMatchExpression()
                    .withKey(SPARK_APP_ID_LABEL)
                    .withOperator("IN")
                    .withValues(kubernetesConf.appId)
                    .endMatchExpression()
                  .endLabelSelector()
                .endPodAffinityTerm()
              .endPreferredDuringSchedulingIgnoredDuringExecution()
            .endPodAntiAffinity()
          .endAffinity()
        .endSpec()
        .build()
      SparkPod(podWithAntiAffinity, pod.container)
    } else {
      pod
    }
  }
}

