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
package org.apache.spark.deploy.k8s.integrationtest

import scala.collection.JavaConverters._
import org.scalatest.concurrent.Eventually

import org.apache.spark.deploy.k8s.integrationtest.KubernetesSuite.{k8sTestTag, INTERVAL, TIMEOUT}

private[spark] trait PythonTestsSuite { k8sSuite: KubernetesSuite =>

  import PythonTestsSuite._
  import KubernetesSuite.k8sTestTag

  test("Run PySpark on simple pi.py example", k8sTestTag) {
    sparkAppConf
      .set("spark.kubernetes.container.image", pyImage)
    runSparkApplicationAndVerifyCompletion(
      appResource = PYSPARK_PI,
      mainClass = "",
      expectedLogOnCompletion = Seq("Pi is roughly 3"),
      appArgs = Array("5"),
      driverPodChecker = doBasicDriverPyPodCheck,
      executorPodChecker = doBasicExecutorPyPodCheck,
      appLocator = appLocator,
      isJVM = false)
  }

  test("Run PySpark with Python2 to test a pyfiles example", k8sTestTag) {
    sparkAppConf
      .set("spark.kubernetes.container.image", pyImage)
      .set("spark.kubernetes.pyspark.pythonVersion", "2")
    runSparkApplicationAndVerifyCompletion(
      appResource = PYSPARK_FILES,
      mainClass = "",
      expectedLogOnCompletion = Seq(
        "Python runtime version check is: True",
        "Python environment version check is: True"),
      appArgs = Array("python"),
      driverPodChecker = doBasicDriverPyPodCheck,
      executorPodChecker = doBasicExecutorPyPodCheck,
      appLocator = appLocator,
      isJVM = false,
      pyFiles = Some(PYSPARK_CONTAINER_TESTS))
  }

  test("Run PySpark with Python3 to test a pyfiles example", k8sTestTag) {
    sparkAppConf
      .set("spark.kubernetes.container.image", pyImage)
      .set("spark.kubernetes.pyspark.pythonVersion", "3")
    runSparkApplicationAndVerifyCompletion(
      appResource = PYSPARK_FILES,
      mainClass = "",
      expectedLogOnCompletion = Seq(
        "Python runtime version check is: True",
        "Python environment version check is: True"),
      appArgs = Array("python3"),
      driverPodChecker = doBasicDriverPyPodCheck,
      executorPodChecker = doBasicExecutorPyPodCheck,
      appLocator = appLocator,
      isJVM = false,
      pyFiles = Some(PYSPARK_CONTAINER_TESTS))
  }

  test("Run PySpark with memory customization", k8sTestTag) {
    sparkAppConf
      .set("spark.kubernetes.container.image", pyImage)
      .set("spark.kubernetes.pyspark.pythonVersion", "3")
      .set("spark.kubernetes.memoryOverheadFactor", s"$memOverheadConstant")
      .set("spark.executor.pyspark.memory", s"${additionalMemory}m")
    runSparkApplicationAndVerifyCompletion(
      appResource = PYSPARK_MEMORY_CHECK,
      mainClass = "",
      expectedLogOnCompletion = Seq(
        "PySpark Worker Memory Check is: True"),
      appArgs = Array(s"$additionalMemoryInBytes"),
      driverPodChecker = doDriverMemoryCheck,
      executorPodChecker = doExecutorMemoryCheck,
      appLocator = appLocator,
      isJVM = false,
      pyFiles = Some(PYSPARK_CONTAINER_TESTS))
  }

  test("Run PySpark shell", k8sTestTag) {
    val labels = Map("spark-app-selector" -> driverPodName)
    val driverPort = 7077
    val blockManagerPort = 10000
    val driverService = testBackend
      .getKubernetesClient
      .services()
      .inNamespace(kubernetesTestComponents.namespace)
      .createNew()
      .withNewMetadata()
      .withName(s"$driverPodName-svc")
      .endMetadata()
      .withNewSpec()
      .withClusterIP("None")
      .withSelector(labels.asJava)
      .addNewPort()
      .withName("driver-port")
      .withPort(driverPort)
      .withNewTargetPort(driverPort)
      .endPort()
      .addNewPort()
      .withName("block-manager")
      .withPort(blockManagerPort)
      .withNewTargetPort(blockManagerPort)
      .endPort()
      .endSpec()
      .done()
    try {
      val driverPod = testBackend
        .getKubernetesClient
        .pods()
        .inNamespace(kubernetesTestComponents.namespace)
        .createNew()
        .withNewMetadata()
        .withName(driverPodName)
        .withLabels(labels.asJava)
        .endMetadata()
        .withNewSpec()
        .withServiceAccountName(kubernetesTestComponents.serviceAccountName)
        .addNewContainer()
        .withName("pyspark-shell")
        .withImage(pyImage)
        .withImagePullPolicy("IfNotPresent")
        .withCommand("/opt/spark/bin/pyspark")
        .addToArgs("--master", s"k8s://https://kubernetes.default.svc")
        .addToArgs("--deploy-mode", "client")
        .addToArgs("--conf", s"spark.kubernetes.container.image="+pyImage)
        .addToArgs(
          "--conf",
          s"spark.kubernetes.namespace=${kubernetesTestComponents.namespace}")
        .addToArgs("--conf", "spark.kubernetes.authenticate.oauthTokenFile=" +
          "/var/run/secrets/kubernetes.io/serviceaccount/token")
        .addToArgs("--conf", "spark.kubernetes.authenticate.caCertFile=" +
          "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt")
        .addToArgs("--conf", s"spark.kubernetes.driver.pod.name=$driverPodName")
        .addToArgs("--conf", "spark.executor.memory=500m")
        .addToArgs("--conf", "spark.executor.cores=1")
        .addToArgs("--conf", "spark.executor.instances=1")
        .addToArgs("--conf",
          s"spark.driver.host=" +
            s"${driverService.getMetadata.getName}.${kubernetesTestComponents.namespace}.svc")
        .addToArgs("--conf", s"spark.driver.port=$driverPort")
        .addToArgs("--conf", s"spark.driver.blockManager.port=$blockManagerPort")
        .endContainer()
        .endSpec()
        .done()
      Eventually.eventually(TIMEOUT, INTERVAL) {
        assert(kubernetesTestComponents.kubernetesClient
          .pods()
          .withName(driverPodName)
          .getLog
          .contains("SparkSession available"), "The application did not complete.")
      }
    } finally {
      // Have to delete the service manually since it doesn't have an owner reference
      kubernetesTestComponents
        .kubernetesClient
        .services()
        .inNamespace(kubernetesTestComponents.namespace)
        .delete(driverService)
    }
  }

}

private[spark] object PythonTestsSuite {
  val CONTAINER_LOCAL_PYSPARK: String = "local:///opt/spark/examples/src/main/python/"
  val PYSPARK_PI: String = CONTAINER_LOCAL_PYSPARK + "pi.py"
  val TEST_LOCAL_PYSPARK: String = "local:///opt/spark/tests/"
  val PYSPARK_FILES: String = TEST_LOCAL_PYSPARK + "pyfiles.py"
  val PYSPARK_CONTAINER_TESTS: String = TEST_LOCAL_PYSPARK + "py_container_checks.py"
  val PYSPARK_MEMORY_CHECK: String = TEST_LOCAL_PYSPARK + "worker_memory_check.py"
}
