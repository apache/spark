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

import io.fabric8.kubernetes.api.model.Quantity
import io.fabric8.volcano.scheduling.v1beta1.PodGroup

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.deploy.k8s._
import org.apache.spark.deploy.k8s.Config._

class PodGroupFeatureStepSuite extends SparkFunSuite {
  test("Do nothing when KUBERNETES_ENABLE_PODGROUP is false") {
    val conf = KubernetesTestConf.createDriverConf()
    val step = new PodGroupFeatureStep(conf)

    val initialPod = SparkPod.initialPod()
    val configuredPod = step.configurePod(initialPod)
    assert(configuredPod === initialPod)

    assert(step.getAdditionalKubernetesResources().isEmpty)
    assert(step.getAdditionalPodSystemProperties().isEmpty)
  }

  test("Driver Pod with Volcano PodGroup") {
    val sparkConf = new SparkConf()
      .set(KUBERNETES_DRIVER_SCHEDULER_NAME, "volcano")
      .set(KUBERNETES_ENABLE_PODGROUP, true)
    val kubernetesConf = KubernetesTestConf.createDriverConf(sparkConf)
    val step = new PodGroupFeatureStep(kubernetesConf)
    val configuredPod = step.configurePod(SparkPod.initialPod())

    val annotation = configuredPod.pod.getMetadata.getAnnotations

    assert(annotation.get("scheduling.k8s.io/group-name") === step.podGroupName)
    val podGroup = step.getAdditionalKubernetesResources().head
    assert(podGroup.getMetadata.getName === step.podGroupName)
  }

  test("Executor Pod with Volcano PodGroup") {
    val sparkConf = new SparkConf()
      .set(KUBERNETES_EXECUTOR_SCHEDULER_NAME, "volcano")
      .set(KUBERNETES_ENABLE_PODGROUP, true)
    val kubernetesConf = KubernetesTestConf.createExecutorConf(sparkConf)
    val step = new PodGroupFeatureStep(kubernetesConf)
    val configuredPod = step.configurePod(SparkPod.initialPod())

    val annotation = configuredPod.pod.getMetadata.getAnnotations

    assert(annotation.get("scheduling.k8s.io/group-name") === step.podGroupName)
    val podGroup = step.getAdditionalKubernetesResources().head.asInstanceOf[PodGroup]
    assert(podGroup.getMetadata.getName === s"${kubernetesConf.appId}-podgroup")
    assert(podGroup.getSpec.getMinResources.get("cpu") === new Quantity("2.0"))
    assert(podGroup.getSpec.getMinResources.get("memory") === new Quantity("3072"))
  }
}
