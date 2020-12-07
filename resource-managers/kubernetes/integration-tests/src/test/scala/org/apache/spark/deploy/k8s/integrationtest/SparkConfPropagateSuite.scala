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

  test("Verify logging configuration is picked from the provided SPARK_CONF_DIR/log4j.properties",
    k8sTestTag) {
    val loggingConfigFileName = "log-config-test-log4j.properties"
    val loggingConfURL: URL = this.getClass.getClassLoader.getResource(loggingConfigFileName)
    assert(loggingConfURL != null, "Logging configuration file not available.")

    val content = Source.createBufferedSource(loggingConfURL.openStream()).getLines().mkString("\n")
    val logConfFilePath = s"${sparkHomeDir.toFile}/conf/log4j.properties"

    try {
      Files.write(new File(logConfFilePath).toPath, content.getBytes)

      sparkAppConf.set("spark.driver.extraJavaOptions", "-Dlog4j.debug")
      sparkAppConf.set("spark.executor.extraJavaOptions", "-Dlog4j.debug")
      sparkAppConf.set("spark.kubernetes.executor.deleteOnTermination", "false")

      val log4jExpectedLog =
        s"log4j: Reading configuration from URL file:/opt/spark/conf/log4j.properties"

      runSparkApplicationAndVerifyCompletion(
        appResource = containerLocalSparkDistroExamplesJar,
        mainClass = SPARK_PI_MAIN_CLASS,
        expectedDriverLogOnCompletion = (Seq("DEBUG",
          log4jExpectedLog,
          "Pi is roughly 3")),
        expectedExecutorLogOnCompletion = Seq(log4jExpectedLog),
        appArgs = Array.empty[String],
        driverPodChecker = doBasicDriverPodCheck,
        executorPodChecker = doBasicExecutorPodCheck,
        appLocator = appLocator,
        isJVM = true)
    } finally {
      new File(logConfFilePath).delete()
    }
  }

  test("Verify large files and unsupported binary/non-utf8 encoded files are skipped.",
    k8sTestTag) {
    val loggingConfigFileName = "log-config-test-log4j.properties"
    val loggingConfURL: URL = this.getClass.getClassLoader.getResource(loggingConfigFileName)
    assert(loggingConfURL != null, "Logging configuration file not available.")

    val content = Source.createBufferedSource(loggingConfURL.openStream()).getLines().mkString("\n")
    val sparkConfDirPath = s"${sparkHomeDir.toFile}/conf"
    val filesToGenerateBinary =
      Seq("test.jar", "non-utf8.txt", "test.zip", "some-random-binary-file")
    val binaryContent: Array[Byte] = (1 to 10000).map(_.toByte).toArray
    val someValidFiles = Seq("config.xml", "log4j.properties", "utf8.txt")
    try {
      filesToGenerateBinary.foreach(f =>
        Files.write(new File(s"$sparkConfDirPath/$f").toPath, binaryContent))

      someValidFiles.foreach(f =>
        Files.write(new File(s"$sparkConfDirPath/$f").toPath, content.getBytes))

      val expectedLogMessages =
        Seq(s"Spark configuration files loaded from Some(/opt/spark/conf)") ++ someValidFiles
      runSparkApplicationAndVerifyCompletion(
        appResource = containerLocalSparkDistroExamplesJar,
        mainClass = SPARK_PI_MAIN_CLASS,
        expectedDriverLogOnCompletion = expectedLogMessages,
        appArgs = Array.empty[String],
        driverPodChecker = doBasicDriverPodCheck,
        executorPodChecker = doBasicExecutorPodCheck,
        appLocator = appLocator,
        isJVM = true)
    } finally {
      (filesToGenerateBinary ++ someValidFiles)
        .foreach(f => new File(s"$sparkConfDirPath/$f").delete())
    }
  }
}
