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

private[spark] trait DecommissionSuite { k8sSuite: KubernetesSuite =>

  import KubernetesSuite.k8sTestTag
  import KubernetesSuite.SPARK_PI_MAIN_CLASS

  test("Test basic decommissioning", k8sTestTag) {
    sparkAppConf
      .set("spark.worker.decommission.enabled", "true")

    runSparkApplicationAndVerifyCompletion(
      appResource = containerLocalSparkDistroExamplesJar,
      mainClass = SPARK_PI_MAIN_CLASS,
      expectedLogOnCompletion = Seq("Decommissioning executor"),
      appArgs = Array("100"), // Give it some time to run
      driverPodChecker = doBasicDriverPodCheck,
      executorPodChecker = doBasicExecutorPodCheck,
      appLocator = appLocator,
      isJVM = true,
      decomissioningTest = true)
  }
}
