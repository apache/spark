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

import java.io.{BufferedWriter, File, FileWriter}
import java.net.URL

import scala.io.{BufferedSource, Source}

import io.fabric8.kubernetes.api.model._

import org.apache.spark.internal.config

private[spark] trait SparkConfPropagateSuite { k8sSuite: KubernetesSuite =>
  import KubernetesSuite.{k8sTestTag, SPARK_PI_MAIN_CLASS}

  test("Verify logging configuration is picked from the provided SPARK_CONF_DIR/log4j.properties",
    k8sTestTag) {
    val loggingConfigFileName = "log-config-test-log4j.properties"
    val loggingConfURL: URL = this.getClass.getClassLoader.getResource(loggingConfigFileName)
    assert(loggingConfURL != null, "Logging configuration file not available.")

    val content = Source.createBufferedSource(loggingConfURL.openStream()).getLines().mkString("\n")
    val logConfFilePath = s"${sparkHomeDir.toFile}/conf/log4j.properties"

    try {
      val writer = new BufferedWriter(new FileWriter(logConfFilePath))
      writer.write(content)
      writer.close()

      sparkAppConf.set("spark.driver.extraJavaOptions", "-Dlog4j.debug")

      runSparkApplicationAndVerifyCompletion(
        appResource = containerLocalSparkDistroExamplesJar,
        mainClass = SPARK_PI_MAIN_CLASS,
        expectedLogOnCompletion = (Seq("DEBUG",
          s"log4j: Reading configuration from URL file:/opt/spark/conf/log4j.properties",
          "Pi is roughly 3")),
        appArgs = Array.empty[String],
        driverPodChecker = doBasicDriverPodCheck,
        executorPodChecker = doBasicExecutorPodCheck,
        appLocator = appLocator,
        isJVM = true)
    } finally {
      new File(logConfFilePath).delete()
    }
  }
}
