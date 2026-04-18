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

import java.util.{Arrays, HashMap}

import io.fabric8.kubernetes.api.model.{ContainerBuilder, HasMetadata, PodBuilder, SecretBuilder}

import org.apache.spark.SparkFunSuite

class KubernetesDriverSpecSuite extends SparkFunSuite {

  private val POD = new PodBuilder()
    .withNewMetadata().withName("driver").endMetadata()
    .withNewSpec().endSpec()
    .build()
  private val CONTAINER = new ContainerBuilder().withName("driver-container").build()
  private val SPARK_POD = SparkPod(POD, CONTAINER)

  test("create from Java collections") {
    val secret = new SecretBuilder()
      .withNewMetadata().withName("test-secret").endMetadata()
      .build()
    val preResources = Arrays.asList[HasMetadata](secret)
    val resources = Arrays.asList[HasMetadata](secret)
    val props = new HashMap[String, String]()
    props.put("spark.key1", "value1")
    props.put("spark.key2", "value2")

    val spec = KubernetesDriverSpec.create(SPARK_POD, preResources, resources, props)

    assert(spec.pod === SPARK_POD)
    assert(spec.driverPreKubernetesResources.length === 1)
    assert(spec.driverPreKubernetesResources.head === secret)
    assert(spec.driverKubernetesResources.length === 1)
    assert(spec.driverKubernetesResources.head === secret)
    assert(spec.systemProperties === Map("spark.key1" -> "value1", "spark.key2" -> "value2"))
  }

  test("create from empty Java collections") {
    val preResources = Arrays.asList[HasMetadata]()
    val resources = Arrays.asList[HasMetadata]()
    val props = new HashMap[String, String]()

    val spec = KubernetesDriverSpec.create(SPARK_POD, preResources, resources, props)

    assert(spec.pod === SPARK_POD)
    assert(spec.driverPreKubernetesResources.isEmpty)
    assert(spec.driverKubernetesResources.isEmpty)
    assert(spec.systemProperties.isEmpty)
  }
}
