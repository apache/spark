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

package org.apache.spark.deploy.k8s

import java.util.{Arrays, List => JList}

import io.fabric8.kubernetes.api.model.{ContainerBuilder, HasMetadata, PodBuilder, SecretBuilder}

import org.apache.spark.SparkFunSuite

class KubernetesExecutorSpecSuite extends SparkFunSuite {

  private val POD = new PodBuilder()
    .withNewMetadata().withName("executor").endMetadata()
    .withNewSpec().endSpec()
    .build()
  private val CONTAINER = new ContainerBuilder().withName("executor-container").build()
  private val SPARK_POD = SparkPod(POD, CONTAINER)

  test("create from Java collections") {
    val secret = new SecretBuilder()
      .withNewMetadata().withName("test-secret").endMetadata()
      .build()
    val resources = Arrays.asList[HasMetadata](secret)

    val spec = KubernetesExecutorSpec.create(SPARK_POD, resources)

    assert(spec.pod === SPARK_POD)
    assert(spec.executorKubernetesResources.length === 1)
    assert(spec.executorKubernetesResources.head === secret)

    val javaResources = spec.getExecutorKubernetesResourcesAsJavaList
    assert(javaResources.isInstanceOf[JList[_]])
    assert(javaResources.size() === 1)
    assert(javaResources.get(0) === secret)
  }

  test("create from empty Java collections") {
    val resources = Arrays.asList[HasMetadata]()

    val spec = KubernetesExecutorSpec.create(SPARK_POD, resources)

    assert(spec.pod === SPARK_POD)
    assert(spec.executorKubernetesResources.isEmpty)
    assert(spec.getExecutorKubernetesResourcesAsJavaList.isEmpty)
  }
}
