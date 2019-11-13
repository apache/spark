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

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.deploy.k8s.{KubernetesConf, KubernetesDriverSpecificConf, KubernetesExecutorSpecificConf, KubernetesTolerationSpec, SparkPod}

class TolerationsFeatureStepSuite extends SparkFunSuite {
    private val sparkConf = new SparkConf(false)
    private val emptyKubernetesConf = KubernetesConf(
      sparkConf = sparkConf,
      roleSpecificConf = KubernetesDriverSpecificConf(
        None,
        "app-name",
        "main",
        Seq.empty),
      appResourceNamePrefix = "resource",
      appId = "app-id",
      roleLabels = Map.empty,
      roleAnnotations = Map.empty,
      roleSecretNamesToMountPaths = Map.empty,
      roleSecretEnvNamesToKeyRefs = Map.empty,
      roleEnvs = Map.empty,
      roleVolumes = Nil,
      roleTolerations = Nil,
      sparkFiles = Nil)

    test("Adds single toleration") {
      val tolerationsConf = KubernetesTolerationSpec(
        Some("key1"),
        "Equal",
        Some("NoSchedule"),
        Some("value1"),
        None
      )

      val kubernetesConf = emptyKubernetesConf.copy(roleTolerations = tolerationsConf :: Nil)
      val step = new TolerationsFeatureStep(kubernetesConf)
      val configuredPod = step.configurePod(SparkPod.initialPod())

      assert(configuredPod.pod.getSpec.getTolerations.size() === 1)
      assert(configuredPod.pod.getSpec.getTolerations.get(0).getKey === "key1")
      assert(configuredPod.pod.getSpec.getTolerations.get(0).getOperator === "Equal")
      assert(configuredPod.pod.getSpec.getTolerations.get(0).getEffect === "NoSchedule")
      assert(configuredPod.pod.getSpec.getTolerations.get(0).getValue === "value1")
    }

    test("Adds multiple tolerations") {
      val tolerationsConf = Set(
        KubernetesTolerationSpec(
            Some("key1"),
            "Equal",
            Some("NoSchedule"),
            Some("value1"),
            None
        ),
        KubernetesTolerationSpec(
            Some("key2"),
            "Equal",
            Some("PreferNoSchedule"),
            None,
            Some(30)
        ),
        KubernetesTolerationSpec(
            None,
            "Exists",
            None,
            None,
            None
        )
      )

      val kubernetesConf = emptyKubernetesConf.copy(roleTolerations = tolerationsConf)
      val step = new TolerationsFeatureStep(kubernetesConf)
      val configuredPod = step.configurePod(SparkPod.initialPod())

      assert(configuredPod.pod.getSpec.getTolerations.size() === 3)
      assert(configuredPod.pod.getSpec.getTolerations.get(0).getKey === "key1")
      assert(configuredPod.pod.getSpec.getTolerations.get(0).getOperator === "Equal")
      assert(configuredPod.pod.getSpec.getTolerations.get(0).getEffect === "NoSchedule")
      assert(configuredPod.pod.getSpec.getTolerations.get(0).getValue === "value1")
      assert(configuredPod.pod.getSpec.getTolerations.get(0).getTolerationSeconds === null)
      assert(configuredPod.pod.getSpec.getTolerations.get(1).getKey === "key2")
      assert(configuredPod.pod.getSpec.getTolerations.get(1).getOperator === "Equal")
      assert(configuredPod.pod.getSpec.getTolerations.get(1).getEffect === "PreferNoSchedule")
      assert(configuredPod.pod.getSpec.getTolerations.get(1).getValue === null)
      assert(configuredPod.pod.getSpec.getTolerations.get(1).getTolerationSeconds === 30)
      assert(configuredPod.pod.getSpec.getTolerations.get(2).getKey === null)
      assert(configuredPod.pod.getSpec.getTolerations.get(2).getOperator === "Exists")
      assert(configuredPod.pod.getSpec.getTolerations.get(2).getEffect === null)
      assert(configuredPod.pod.getSpec.getTolerations.get(2).getValue === null)
      assert(configuredPod.pod.getSpec.getTolerations.get(2).getTolerationSeconds === null)
    }
}
