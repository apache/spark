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

private[spark] trait RTestsSuite { k8sSuite: KubernetesSuite =>

  import RTestsSuite._
  import KubernetesSuite.k8sTestTag

  test("Run SparkR on simple dataframe.R example", k8sTestTag) {
    sparkAppConf.set("spark.kubernetes.container.image", rImage)
    runSparkApplicationAndVerifyCompletion(
      appResource = SPARK_R_DATAFRAME_TEST,
      mainClass = "",
      expectedLogOnCompletion = Seq("name: string (nullable = true)", "1 Justin"),
      appArgs = Array.empty[String],
      driverPodChecker = doBasicDriverRPodCheck,
      executorPodChecker = doBasicExecutorRPodCheck,
      appLocator = appLocator,
      isJVM = false)
  }
}

private[spark] object RTestsSuite {
  val CONTAINER_LOCAL_SPARKR: String = "local:///opt/spark/examples/src/main/r/"
  val SPARK_R_DATAFRAME_TEST: String = CONTAINER_LOCAL_SPARKR + "dataframe.R"
}
