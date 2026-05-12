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

import scala.jdk.CollectionConverters._

import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicy

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.deploy.k8s.{KubernetesTestConf, SparkPod}
import org.apache.spark.deploy.k8s.Constants._

class NetworkPolicyFeatureStepSuite extends SparkFunSuite {

  test("NetworkPolicy creation") {
    val conf = KubernetesTestConf.createDriverConf(sparkConf = new SparkConf(false))
    val step = new NetworkPolicyFeatureStep(conf)

    // configures pod identically
    val pod = SparkPod.initialPod()
    assert(step.configurePod(pod) === pod)

    // additional pod system properties is empty
    assert(step.getAdditionalPodSystemProperties().isEmpty)

    // Check additional resources
    val resources = step.getAdditionalKubernetesResources()
    assert(resources.size === 1)

    val policy = resources.head.asInstanceOf[NetworkPolicy]
    assert(policy.getMetadata.getName === s"${conf.resourceNamePrefix}-policy")
    assert(policy.getMetadata.getNamespace === conf.namespace)
    assert(policy.getMetadata.getLabels.get(SPARK_APP_ID_LABEL) === conf.appId)

    val labels = policy.getSpec.getPodSelector.getMatchLabels
    assert(labels.get(SPARK_ROLE_LABEL) === SPARK_POD_EXECUTOR_ROLE)
    assert(labels.get(SPARK_APP_ID_LABEL) === conf.appId)

    val ingress = policy.getSpec.getIngress.asScala
    assert(ingress.size === 1)

    val from = ingress.head.getFrom.asScala
    assert(from.size === 1)
    assert(from.head.getPodSelector.getMatchLabels.get(SPARK_APP_ID_LABEL) === conf.appId)
  }
}
