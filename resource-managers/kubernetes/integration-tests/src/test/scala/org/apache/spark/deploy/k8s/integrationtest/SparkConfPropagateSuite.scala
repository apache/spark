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

import java.io.File
import java.net.URL
import java.nio.file.Files

import scala.io.Source

private[spark] trait SparkConfPropagateSuite { k8sSuite: KubernetesSuite =>
  import KubernetesSuite.{k8sTestTag, SPARK_PI_MAIN_CLASS}

  test("Verify logging configuration is picked from the provided SPARK_CONF_DIR/log4j2.properties",
    k8sTestTag) {
    val loggingConfigFileName = "log-config-test-log4j.properties"
    val loggingConfURL: URL = this.getClass.getClassLoader.getResource(loggingConfigFileName)
    assert(loggingConfURL != null, "Logging configuration file not available.")

    val content = Source.createBufferedSource(loggingConfURL.openStream()).getLines().mkString("\n")
    val logConfFilePath = s"${sparkHomeDir.toFile}/conf/log4j2.properties"

    try {
      Files.write(new File(logConfFilePath).toPath, content.getBytes)

      sparkAppConf.set("spark.driver.extraJavaOptions", "-Dlog4j2.debug")
      sparkAppConf.set("spark.executor.extraJavaOptions", "-Dlog4j2.debug")
      sparkAppConf.set("spark.kubernetes.executor.deleteOnTermination", "false")

      val log4jExpectedLog =
        Seq("Reconfiguration complete for context", "at URI /opt/spark/conf/log4j2.properties")

      runSparkApplicationAndVerifyCompletion(
        appResource = containerLocalSparkDistroExamplesJar,
        mainClass = SPARK_PI_MAIN_CLASS,
        expectedDriverLogOnCompletion = Seq("DEBUG", "Pi is roughly 3") ++ log4jExpectedLog,
        expectedExecutorLogOnCompletion = log4jExpectedLog,
        appArgs = Array.empty[String],
        driverPodChecker = doBasicDriverPodCheck,
        executorPodChecker = doBasicExecutorPodCheck,
        isJVM = true)
    } finally {
      new File(logConfFilePath).delete()
    }
  }
}
