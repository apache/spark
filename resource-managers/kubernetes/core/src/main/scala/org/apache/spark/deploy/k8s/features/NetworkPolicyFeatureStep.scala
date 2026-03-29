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

import io.fabric8.kubernetes.api.model.HasMetadata
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicyBuilder

import org.apache.spark.deploy.k8s.{KubernetesDriverConf, SparkPod}
import org.apache.spark.deploy.k8s.Constants._

/**
 * A feature step that configures a NetworkPolicy for Spark executors.
 * It restricts ingress traffic to executors so that they only accept connections
 * from the driver and other executors within the same Spark application.
 */
private[spark] class NetworkPolicyFeatureStep(conf: KubernetesDriverConf)
  extends KubernetesFeatureConfigStep {
  private val policyName = conf.resourceNamePrefix + "-policy"

  override def configurePod(pod: SparkPod): SparkPod = pod

  override def getAdditionalKubernetesResources(): Seq[HasMetadata] = {
    val appId = conf.appId
    val policy = new NetworkPolicyBuilder()
      .withNewMetadata()
        .withName(policyName)
        .withNamespace(conf.namespace)
        .addToLabels(SPARK_APP_ID_LABEL, appId)
      .endMetadata()
      .withNewSpec()
        .withNewPodSelector()
          .addToMatchLabels(SPARK_ROLE_LABEL, SPARK_POD_EXECUTOR_ROLE)
          .addToMatchLabels(SPARK_APP_ID_LABEL, appId)
          .endPodSelector()
        .addNewIngress()
          .addNewFrom()
            .withNewPodSelector()
              .addToMatchLabels(SPARK_APP_ID_LABEL, appId)
              .endPodSelector()
            .endFrom()
          .endIngress()
        .endSpec()
      .build();
    Seq(policy)
  }
}
