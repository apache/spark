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

import io.fabric8.kubernetes.api.model.{Pod, SecretBuilder}
import org.apache.commons.codec.binary.Base64
import org.apache.commons.io.output.ByteArrayOutputStream
import org.scalatest.concurrent.Eventually

import org.apache.spark.deploy.k8s.integrationtest.KubernetesSuite._

private[spark] trait DecommissionSuite { k8sSuite: KubernetesSuite =>

  test("Run SparkPi with env and mount secrets.", k8sTestTag) {
    createTestSecret()
    sparkAppConf
      .set("spark.worker.decommission.enabled", "true")
      .set("spark.kubernetes.container.image", pySparkDockerImage)
      .set("spark.kubernetes.pyspark.pythonVersion", "2")
    // We should be able to run Spark PI now
    runSparkPiAndVerifyCompletion()
    // Now we manually trigger decomissioning
    runSparkApplicationAndVerifyCompletion(
      appResource = PYSPARK_FILES,
      mainClass = "",
      expectedLogOnCompletion = Seq(
        "Spark is working before decommissioning: True",
        "Called decommissionWorker on on success: True",
        "Spark decommissioned worker is not scheduled: True"),
      appArgs = Array("python"),
      driverPodChecker = doBasicDriverPyPodCheck,
      executorPodChecker = doBasicExecutorPyPodCheck,
      appLocator = appLocator,
      isJVM = false,
      pyFiles = Some(PYSPARK_CONTAINER_TESTS))
    // Now we expect this to fail
    runSparkPiAndVerifyCompletion()
  }
}

prviate[spark] object DecommissionSuite {
  val TEST_LOCAL_PYSPARK: String = "local:///opt/spark/tests/"
  val PYSPARK_DECOMISSIONING: String = TEST_LOCAL_PYSPARK + "trigger_decomissioning.py"
}
