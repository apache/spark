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

private[spark] trait PythonTestsSuite { k8sSuite: KubernetesSuite =>

  import PythonTestsSuite._
  import KubernetesSuite.k8sTestTag

  test("Run PySpark on simple pi.py example", k8sTestTag) {
    sparkAppConf
      .set("spark.kubernetes.container.image", pyImage)
    runSparkApplicationAndVerifyCompletion(
      appResource = PYSPARK_PI,
      mainClass = "",
      expectedDriverLogOnCompletion = Seq("Pi is roughly 3"),
      appArgs = Array("5"),
      driverPodChecker = doBasicDriverPyPodCheck,
      executorPodChecker = doBasicExecutorPyPodCheck,
      isJVM = false)
  }

  test("Run PySpark to test a pyfiles example", k8sTestTag) {
    sparkAppConf
      .set("spark.kubernetes.container.image", pyImage)
    runSparkApplicationAndVerifyCompletion(
      appResource = PYSPARK_FILES,
      mainClass = "",
      expectedDriverLogOnCompletion = Seq(
        "Python runtime version check is: True",
        "Python environment version check is: True",
        "Python runtime version check for executor is: True"),
      appArgs = Array("python3"),
      driverPodChecker = doBasicDriverPyPodCheck,
      executorPodChecker = doBasicExecutorPyPodCheck,
      isJVM = false,
      pyFiles = Some(PYSPARK_CONTAINER_TESTS))
  }

  test("Run PySpark with memory customization", k8sTestTag) {
    sparkAppConf
      .set("spark.kubernetes.container.image", pyImage)
      .set("spark.kubernetes.memoryOverheadFactor", s"$memOverheadConstant")
      .set("spark.executor.pyspark.memory", s"${additionalMemory}m")
    runSparkApplicationAndVerifyCompletion(
      appResource = PYSPARK_MEMORY_CHECK,
      mainClass = "",
      expectedDriverLogOnCompletion = Seq(
        "PySpark Worker Memory Check is: True"),
      appArgs = Array(s"$additionalMemoryInBytes"),
      driverPodChecker = doDriverMemoryCheck,
      executorPodChecker = doExecutorMemoryCheck,
      isJVM = false,
      pyFiles = Some(PYSPARK_CONTAINER_TESTS))
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
