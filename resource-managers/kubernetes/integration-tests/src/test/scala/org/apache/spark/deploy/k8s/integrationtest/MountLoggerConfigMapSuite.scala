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

import io.fabric8.kubernetes.api.model.{ConfigMap, ConfigMapBuilder}
import scala.io.{BufferedSource, Source}

private[spark] trait MountLoggerConfigMapSuite { k8sSuite: KubernetesSuite =>
  import KubernetesSuite._

  private var configMap: ConfigMap = _

  private def buildConfigMap(
      loggingConfUrl: URL, loggingConfigFileName: String, configMapName: String): ConfigMap = {
    logInfo(s"Logging configuration is picked up from: $loggingConfigFileName")
    val loggerConfStream = loggingConfUrl.openStream()
    val loggerConfString = Source.createBufferedSource(loggerConfStream).getLines().mkString("\n")
    new ConfigMapBuilder()
      .withNewMetadata()
      .withName(configMapName)
      .endMetadata()
      .addToData(loggingConfigFileName, loggerConfString)
      .build()
  }

  test("Run with logging configuration provided as a k8s config map", k8sTestTag) {
    val loggingConfigFileName = "log-config-test-log4j.properties"
    val configMapName = "test-logging-config-map"
    val loggingConfURL: URL = this.getClass.getClassLoader.getResource(loggingConfigFileName)
    assert(loggingConfURL != null, "Test not properly setup, logging configuration file not available.")
    try {
      configMap = buildConfigMap(loggingConfURL, loggingConfigFileName, configMapName)

      kubernetesTestComponents.kubernetesClient.configMaps().createOrReplace(configMap)

      sparkAppConf
        .set("spark.kubernetes.loggingConf.configMapName", configMapName)
        .set("spark.kubernetes.loggingConf.fileName", loggingConfigFileName)
      runSparkApplicationAndVerifyCompletion(
        appResource = containerLocalSparkDistroExamplesJar,
        mainClass = SPARK_PI_MAIN_CLASS,
        expectedLogOnCompletion = (Seq("DEBUG",
          s"Using an existing config map ${configMapName}", "Pi is roughly 3")),
        appArgs = Array.empty[String],
        driverPodChecker = doBasicDriverPodCheck,
        executorPodChecker = doBasicExecutorPodCheck,
        appLocator = appLocator,
        isJVM = true)
    } finally {
      kubernetesTestComponents.kubernetesClient.configMaps().delete(configMap)
    }
  }

  test("Run with logging configuration provided from log4j properties file", k8sTestTag) {
    val loggingConfigFileName = "log-config-test-log4j.properties"
    val loggingConfURL: URL = this.getClass.getClassLoader.getResource(loggingConfigFileName)
    assert(loggingConfURL != null, "Test not properly setup, logging configuration file not available.")

    val content = Source.createBufferedSource(loggingConfURL.openStream()).getLines().mkString("\n")
    val logConfFilePath = s"${sparkHomeDir.toFile}/conf/$loggingConfigFileName"

    try {
      val writer = new BufferedWriter(new FileWriter(logConfFilePath))
      writer.write(content)
      writer.close()

      sparkAppConf
        .set("spark.kubernetes.loggingConf.fileName", loggingConfigFileName)
        .set("spark.driver.extraJavaOptions", "-Dlog4j.debug")

      runSparkApplicationAndVerifyCompletion(
        appResource = containerLocalSparkDistroExamplesJar,
        mainClass = SPARK_PI_MAIN_CLASS,
        expectedLogOnCompletion = (Seq("DEBUG",
          s"log4j: Reading configuration from URL file:/opt/spark/log/$loggingConfigFileName",
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
