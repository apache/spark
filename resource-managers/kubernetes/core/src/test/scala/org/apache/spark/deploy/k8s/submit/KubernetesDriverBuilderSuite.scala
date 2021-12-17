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
package org.apache.spark.deploy.k8s.submit

import io.fabric8.kubernetes.api.model.HasMetadata
import io.fabric8.kubernetes.api.model.apiextensions.v1.CustomResourceDefinitionBuilder
import io.fabric8.kubernetes.client.KubernetesClient

import org.apache.spark.SparkConf
import org.apache.spark.deploy.k8s._
import org.apache.spark.deploy.k8s.features.KubernetesFeatureConfigStep
import org.apache.spark.internal.config.ConfigEntry

class KubernetesDriverBuilderSuite extends PodBuilderSuite {

  override protected def templateFileConf: ConfigEntry[_] = {
    Config.KUBERNETES_DRIVER_PODTEMPLATE_FILE
  }

  override protected def userFeatureStepsConf: ConfigEntry[_] = {
    Config.KUBERNETES_DRIVER_POD_FEATURE_STEPS
  }

  override protected def buildPod(sparkConf: SparkConf, client: KubernetesClient): SparkPod = {
    val conf = KubernetesTestConf.createDriverConf(sparkConf = sparkConf)
    new KubernetesDriverBuilder().buildFromFeatures(conf, client).pod
  }

  private val ADDITION_PRE_RESOURCES = Seq(
    new CustomResourceDefinitionBuilder().withNewMetadata().withName("preCRD").endMetadata().build()
  )

  test("SPARK-37331: check driver pre kubernetes resource, empty by default") {
    val sparkConf = new SparkConf(false)
      .set(Config.CONTAINER_IMAGE, "spark-driver:latest")
    val client = mockKubernetesClient()
    val conf = KubernetesTestConf.createDriverConf(sparkConf)
    val spec = new KubernetesDriverBuilder().buildFromFeatures(conf, client)
    assert(spec.driverPreKubernetesResources.size === 0)
  }

  test("SPARK-37331: check driver pre kubernetes resource as expected") {
    val sparkConf = new SparkConf(false)
      .set(Config.CONTAINER_IMAGE, "spark-driver:latest")
      .set(Config.KUBERNETES_DRIVER_POD_FEATURE_STEPS.key,
        "org.apache.spark.deploy.k8s.submit.TestStep")
    val client = mockKubernetesClient()
    val conf = KubernetesTestConf.createDriverConf(
      sparkConf = sparkConf
    )
    val spec = new KubernetesDriverBuilder().buildFromFeatures(conf, client)
    assert(spec.driverPreKubernetesResources.size === 1)
    assert(spec.driverPreKubernetesResources === ADDITION_PRE_RESOURCES)
  }
}

class TestStep extends KubernetesFeatureConfigStep {

  override def configurePod(pod: SparkPod): SparkPod = {
    pod
  }

  override def getAdditionalPreKubernetesResources(): Seq[HasMetadata] = Seq(
    new CustomResourceDefinitionBuilder()
        .withNewMetadata()
          .withName("preCRD")
        .endMetadata()
      .build()
  )
}
