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
package org.apache.spark.scheduler.cluster.kubernetes

import scala.collection.JavaConverters._

import io.fabric8.kubernetes.api.model._
import io.fabric8.kubernetes.client.KubernetesClient
import org.mockito.MockitoAnnotations
import org.scalatest.BeforeAndAfter

import org.apache.spark.{SparkConf, SparkContext, SparkFunSuite}
import org.apache.spark.deploy.kubernetes.config._
import org.apache.spark.deploy.kubernetes.constants
import org.apache.spark.network.netty.SparkTransportConf

class ExecutorPodFactoryImplSuite extends SparkFunSuite with BeforeAndAfter {
  private val driverPodName: String = "driver-pod"
  private val driverPodUid: String = "driver-uid"
  private val driverUrl: String = "driver-url"
  private val executorPrefix: String = "base"
  private val executorImage: String = "executor-image"
  private val driverPod = new PodBuilder()
    .withNewMetadata()
    .withName(driverPodName)
    .withUid(driverPodUid)
    .endMetadata()
    .withNewSpec()
    .withNodeName("some-node")
    .endSpec()
    .withNewStatus()
    .withHostIP("192.168.99.100")
    .endStatus()
    .build()
  private var baseConf: SparkConf = _
  private var sc: SparkContext = _

  before {
    SparkContext.clearActiveContext()
    MockitoAnnotations.initMocks(this)
    baseConf = new SparkConf()
      .set(KUBERNETES_DRIVER_POD_NAME, driverPodName)
      .set(KUBERNETES_EXECUTOR_POD_NAME_PREFIX, executorPrefix)
      .set(EXECUTOR_DOCKER_IMAGE, executorImage)
    sc = new SparkContext("local", "test")
  }
  private var kubernetesClient: KubernetesClient = _

  test("basic executor pod has reasonable defaults") {
    val factory = new ExecutorPodFactoryImpl(baseConf)
    val executor = factory.createExecutorPod("1", "dummy", "dummy",
      Seq[(String, String)](), driverPod, Map[String, Int]())

    // The executor pod name and default labels.
    assert(executor.getMetadata.getName == s"$executorPrefix-exec-1")
    assert(executor.getMetadata.getLabels.size() == 3)

    // There is exactly 1 container with no volume mounts and default memory limits.
    // Default memory limit is 1024M + 384M (minimum overhead constant).
    assert(executor.getSpec.getContainers.size() == 1)
    assert(executor.getSpec.getContainers.get(0).getImage == executorImage)
    assert(executor.getSpec.getContainers.get(0).getVolumeMounts.size() == 0)
    assert(executor.getSpec.getContainers.get(0).getResources.getLimits.size() == 1)
    assert(executor.getSpec.getContainers.get(0).getResources.
      getLimits.get("memory").getAmount == "1408Mi")

    // The pod has no node selector, volumes.
    assert(executor.getSpec.getNodeSelector.size() == 0)
    assert(executor.getSpec.getVolumes.size() == 0)

    checkEnv(executor, Set())
    checkOwnerReferences(executor, driverPodUid)
  }

  test("executor pod hostnames get truncated to 63 characters") {
    val conf = baseConf.clone()
    conf.set(KUBERNETES_EXECUTOR_POD_NAME_PREFIX,
      "loremipsumdolorsitametvimatelitrefficiendisuscipianturvixlegeresple")

    val factory = new ExecutorPodFactoryImpl(conf)
    val executor = factory.createExecutorPod("1",
      "dummy", "dummy", Seq[(String, String)](), driverPod, Map[String, Int]())

    assert(executor.getSpec.getHostname.length == 63)
  }

  test("classpath and extra java options get translated into environment variables") {
    val conf = baseConf.clone()
    conf.set(org.apache.spark.internal.config.EXECUTOR_JAVA_OPTIONS, "foo=bar")
    conf.set(org.apache.spark.internal.config.EXECUTOR_CLASS_PATH, "bar=baz")

    val factory = new ExecutorPodFactoryImpl(conf)
    val executor = factory.createExecutorPod("1",
      "dummy", "dummy", Seq[(String, String)]("qux" -> "quux"), driverPod, Map[String, Int]())

    checkEnv(executor, Set("SPARK_JAVA_OPT_0", "SPARK_EXECUTOR_EXTRA_CLASSPATH", "qux"))
    checkOwnerReferences(executor, driverPodUid)
  }

  // There is always exactly one controller reference, and it points to the driver pod.
  private def checkOwnerReferences(executor: Pod, driverPodUid: String): Unit = {
    assert(executor.getMetadata.getOwnerReferences.size() == 1)
    assert(executor.getMetadata.getOwnerReferences.get(0).getUid == driverPodUid)
    assert(executor.getMetadata.getOwnerReferences.get(0).getController == true)
  }

  // Check that the expected environment variables are present.
  private def checkEnv(executor: Pod, additionalEnvVars: Set[String]): Unit = {
    val defaultEnvs = Set(constants.ENV_EXECUTOR_ID,
      constants.ENV_DRIVER_URL, constants.ENV_EXECUTOR_CORES,
      constants.ENV_EXECUTOR_MEMORY, constants.ENV_APPLICATION_ID,
      constants.ENV_MOUNTED_CLASSPATH, constants.ENV_EXECUTOR_POD_IP,
      constants.ENV_EXECUTOR_PORT) ++ additionalEnvVars

    assert(executor.getSpec.getContainers.size() == 1)
    assert(executor.getSpec.getContainers.get(0).getEnv().size() == defaultEnvs.size)
    val setEnvs = executor.getSpec.getContainers.get(0).getEnv.asScala.map {
      x => x.getName
    }.toSet
    assert(defaultEnvs == setEnvs)
  }
}