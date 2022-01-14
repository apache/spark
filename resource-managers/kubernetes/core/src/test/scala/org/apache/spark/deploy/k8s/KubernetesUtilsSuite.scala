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

import scala.collection.JavaConverters._

import io.fabric8.kubernetes.api.model.{ContainerBuilder, PodBuilder}

import org.apache.spark.{SparkException, SparkFunSuite}
import org.apache.spark.deploy.k8s.features.KubernetesFeatureConfigStep

class KubernetesUtilsSuite extends SparkFunSuite {
  private val HOST = "test-host"
  private val POD = new PodBuilder()
    .withNewSpec()
    .withHostname(HOST)
    .withContainers(
      new ContainerBuilder().withName("first").build(),
      new ContainerBuilder().withName("second").build())
    .endSpec()
    .build()

  test("Selects the given container as spark container.") {
    val sparkPod = KubernetesUtils.selectSparkContainer(POD, Some("second"))
    assert(sparkPod.pod.getSpec.getHostname == HOST)
    assert(sparkPod.pod.getSpec.getContainers.asScala.toList.map(_.getName) == List("first"))
    assert(sparkPod.container.getName == "second")
  }

  test("Selects the first container if no container name is given.") {
    val sparkPod = KubernetesUtils.selectSparkContainer(POD, Option.empty)
    assert(sparkPod.pod.getSpec.getHostname == HOST)
    assert(sparkPod.pod.getSpec.getContainers.asScala.toList.map(_.getName) == List("second"))
    assert(sparkPod.container.getName == "first")
  }

  test("Falls back to the first container if given container name does not exist.") {
    val sparkPod = KubernetesUtils.selectSparkContainer(POD, Some("does-not-exist"))
    assert(sparkPod.pod.getSpec.getHostname == HOST)
    assert(sparkPod.pod.getSpec.getContainers.asScala.toList.map(_.getName) == List("second"))
    assert(sparkPod.container.getName == "first")
  }

  test("constructs spark pod correctly with pod template with no containers") {
    val noContainersPod = new PodBuilder(POD).editSpec().withContainers().endSpec().build()
    val sparkPod = KubernetesUtils.selectSparkContainer(noContainersPod, Some("does-not-exist"))
    assert(sparkPod.pod.getSpec.getHostname == HOST)
    assert(sparkPod.container.getName == null)
    val sparkPodWithNoContainerName =
      KubernetesUtils.selectSparkContainer(noContainersPod, Option.empty)
    assert(sparkPodWithNoContainerName.pod.getSpec.getHostname == HOST)
    assert(sparkPodWithNoContainerName.container.getName == null)
  }

  test("SPARK-37145: feature step load test") {
    val execConf: KubernetesExecutorConf = KubernetesTestConf.createExecutorConf()
    val driverConf: KubernetesDriverConf = KubernetesTestConf.createDriverConf()
    val basicFeatureNames = Seq(
      "org.apache.spark.deploy.k8s.TestStep",
      "org.apache.spark.deploy.k8s.TestStepWithConf"
    )
    val driverFeatureNames = "org.apache.spark.deploy.k8s.TestStepWithDrvConf"
    val execFeatureNames = "org.apache.spark.deploy.k8s.TestStepWithExecConf"

    (basicFeatureNames :+ driverFeatureNames).foreach { featureName =>
      val drvFeatureStep = KubernetesUtils.loadFeatureStep(driverConf, featureName)
      assert(drvFeatureStep.isInstanceOf[KubernetesFeatureConfigStep])
    }

    (basicFeatureNames :+ execFeatureNames).foreach { featureName =>
      val execFeatureStep = KubernetesUtils.loadFeatureStep(execConf, featureName)
      assert(execFeatureStep.isInstanceOf[KubernetesFeatureConfigStep])
    }

    val e1 = intercept[SparkException] {
      KubernetesUtils.loadFeatureStep(execConf, driverFeatureNames)
    }
    assert(e1.getMessage.contains(s"Failed to load feature step: $driverFeatureNames"))
    assert(e1.getMessage.contains("with only KubernetesExecutorConf/KubernetesConf param"))

    val e2 = intercept[SparkException] {
      KubernetesUtils.loadFeatureStep(driverConf, execFeatureNames)
    }
    assert(e2.getMessage.contains(s"Failed to load feature step: $execFeatureNames"))
    assert(e2.getMessage.contains("with only KubernetesDriverConf/KubernetesConf param"))

    val e3 = intercept[ClassNotFoundException] {
      KubernetesUtils.loadFeatureStep(execConf, "unknow.class")
    }
    assert(e3.getMessage.contains("unknow.class"))
  }
}
