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

package org.apache.spark.scheduler.cluster.nomad

import java.io.File
import java.nio.charset.StandardCharsets.UTF_8

import scala.concurrent.duration._
import scala.language.postfixOps

import org.apache.commons.io.IOUtils
import org.scalatest.concurrent.Eventually._

import org.apache.spark._
import org.apache.spark.deploy.nomad.BaseNomadClusterSuite
import org.apache.spark.launcher._
import org.apache.spark.tags.ExtendedNomadTest

/**
 * Integration tests for Nomad.
 *
 * These tests will run on the nomad cluster whose URL (e.g. http://127.0.0.1:4646) is given in the
 * nomad.test.url system property.
 *
 * These tests require the spark-distribution tgz to have been built and to be available over HTTP/S
 * at the location provided in the nomad.test.url system property.
 */
@ExtendedNomadTest
class NomadClusterSuite extends BaseNomadClusterSuite {

  test("run in client mode") {
    val finalState = runSpark(
      ClientMode,
      nomadTestApp[ConfigurationCheckingSparkApp.type],
      appArgs = Seq(httpServer.url("/result"))
    )
    checkResult(finalState, "/result" -> "SUCCESS without driver logs with 1 executor(s)")
  }

  test("run in cluster mode") {
    val finalState = runSpark(
      ClusterMode,
      nomadTestApp[ConfigurationCheckingSparkApp.type],
      appArgs = Seq(httpServer.url("/result"))
    )
    checkResult(finalState, "/result" -> "SUCCESS with driver logs with 1 executor(s)")
  }

  test("run in client mode with additional jar") {
    testWithAdditionalJar(ClientMode)
  }

  test("run in cluster mode with additional jar") {
    testWithAdditionalJar(ClusterMode)
  }

  private def testWithAdditionalJar(deployMode: DeployMode): Unit = {
    val additionalJar = new File(TestUtils.createJarWithFiles(Map(
      "test.resource" -> "RESOURCE CONTENTS"
    ), tempDir).getFile)

    val finalState = runSpark(
      deployMode,
      nomadTestApp[ResourceUploaderTestApp.type],
      appArgs = Seq(httpServer.url("/driver"), httpServer.url("/executor")),
      extraJars = Seq(additionalJar)
    )
    checkResult(finalState,
      "/driver" -> "RESOURCE CONTENTS",
      "/executor" -> "RESOURCE CONTENTS"
    )
  }


  test("run in client mode with additional file") {
    testWithAdditionalFile(ClientMode)
  }

  test("run in cluster mode with additional file") {
    testWithAdditionalFile(ClusterMode)
  }

  private def testWithAdditionalFile(deployMode: DeployMode): Unit = {
    val additionalFile = createFile("additionalFile", ".txt", "FILE CONTENTS")

    val finalState = runSpark(
      deployMode,
      nomadTestApp[FileDistributionTestDriver.type],
      appArgs = Seq(
        deployMode match {
          case ClientMode => additionalFile.getAbsolutePath
          case ClusterMode => additionalFile.getName
        },
        httpServer.url("/driver"),
        additionalFile.getName,
        httpServer.url("/executor")),
      extraFiles = Seq(additionalFile)
    )
    checkResult(finalState,
      "/driver" -> "FILE CONTENTS",
      "/executor" -> "FILE CONTENTS"
    )
  }


  test("run in cluster mode unsuccessfully") {
    // Don't provide arguments so the driver will fail.
    val finalState = runSpark(
      ClusterMode,
      nomadTestApp[ConfigurationCheckingSparkApp.type]
    )
    finalState should be (SparkAppHandle.State.FAILED)
  }

  test("failure in cluster after sc initialization is reported to launcher") {
    val finalState = runSpark(ClusterMode, nomadTestApp[ThrowExceptionAfterContextInit.type],
      extraConf = Map("spark.nomad.driver.retryAttempts" -> "2")
    )
    finalState should be (SparkAppHandle.State.FAILED)
  }


  test("user class path first in client mode") {
    testUserClassPathFirst(ClientMode)
  }

  test("user class path first in cluster mode") {
    testUserClassPathFirst(ClusterMode)
  }

  private def testUserClassPathFirst(deployMode: DeployMode): Unit = {

    val extraClassPathJar = TestUtils.createJarWithFiles(Map(
      "test.resource" -> "EXTRA CLASSPATH CONTENTS"
    ), tempDir)

    val userJar = new File(TestUtils.createJarWithFiles(Map(
      "test.resource" -> "USER JAR CONTENTS"
    ), tempDir).getFile)

    val finalState = runSpark(
      deployMode,
      nomadTestApp[ResourceUploaderTestApp.type],
      appArgs = Seq(httpServer.url("/driver"), httpServer.url("/executor")),
      extraClassPath = Seq(extraClassPathJar.getPath), // TODO: this test is wrong for cluster mode
      extraJars = Seq(userJar),
      extraConf = Map(
        "spark.driver.userClassPathFirst" -> "true",
        "spark.executor.userClassPathFirst" -> "true"
      )
    )
    checkResult(finalState,
      "/driver" -> "USER JAR CONTENTS",
      "/executor" -> "USER JAR CONTENTS"
    )
  }


  test("monitor app using launcher library") {
    val propsFile = createConfFile()
    val mainClass = mainClassName(NomadLauncherTestApp.getClass)
    val handle = new SparkLauncher()
      .setSparkHome(sys.props("spark.test.home"))
      .setConf("spark.ui.enabled", "false")
      .setPropertiesFile(propsFile)
      .setMaster(nomadTestAddress.fold("nomad")("nomad:" +))
      .setDeployMode("client")
      .setAppResource(SparkLauncher.NO_RESOURCE)
      .setMainClass(mainClass)
      .startApplication()

    try {
      eventually(timeout(30 seconds), interval(100 millis)) {
        handle.getState() should be (SparkAppHandle.State.RUNNING)
      }

      handle.getAppId() should not be (null)
      handle.getAppId() should startWith (mainClass + "-")
      handle.stop()

      eventually(timeout(30 seconds), interval(100 millis)) {
        handle.getState() should be (SparkAppHandle.State.KILLED)
      }
    } finally {
      handle.kill()
    }
  }

  test("use spark auth in client mode") {
    val finalState = runSpark(
      ClientMode,
      nomadTestApp[ConfigurationCheckingSparkApp.type],
      appArgs = Seq(httpServer.url("/result")),
      extraConf = Map("spark.authenticate" -> "true")
    )
    checkResult(finalState, "/result" -> "SUCCESS without driver logs with 1 executor(s)")
  }

  test("use spark auth in cluster mode") {
    val finalState = runSpark(
      ClusterMode,
      nomadTestApp[ConfigurationCheckingSparkApp.type],
      appArgs = Seq(httpServer.url("/result")),
      extraConf = Map("spark.authenticate" -> "true")
    )
    checkResult(finalState, "/result" -> "SUCCESS with driver logs with 1 executor(s)")
  }

  test("streaming app in client mode") {
    val finalState = runSpark(
      ClientMode,
      nomadTestApp[StreamingApp.type],
      appArgs = Seq(httpServer.url("/result"))
    )
    checkResult(finalState, "/result" -> "success")
  }

  test("streaming app in cluster mode") {
    val finalState = runSpark(
      ClusterMode,
      nomadTestApp[StreamingApp.type],
      appArgs = Seq(httpServer.url("/result"))
    )
    checkResult(finalState, "/result" -> "success")
  }

  test("writing to disk in client mode") {
    val finalState = runSpark(
      ClientMode,
      nomadTestApp[LocalDiskWritingApp.type],
      appArgs = Seq("NO DRIVER WRITE", httpServer.url("/executor"))
    )
    checkResult(finalState, "/executor" -> "EXEC")
  }

  test("writing to disk in cluster mode") {
    val finalState = runSpark(
      ClusterMode,
      nomadTestApp[LocalDiskWritingApp.type],
      appArgs = Seq(httpServer.url("/driver"), httpServer.url("/executor"))
    )
    checkResult(finalState, "/driver" -> "DRIVE", "/executor" -> "EXEC")
  }

  test("persisting RDD to disk in client mode") {
    val finalState = runSpark(
      ClientMode,
      nomadTestApp[DiskPersistedRddTestDriver.type],
      appArgs = Seq(httpServer.url("/result"))
    )
    checkResult(finalState, "/result" -> ((123456 + 234567) * 2).toString)
  }

  test("persisting RDD to disk in cluster mode") {
    val finalState = runSpark(
      ClusterMode,
      nomadTestApp[DiskPersistedRddTestDriver.type],
      appArgs = Seq(httpServer.url("/result"))
    )
    checkResult(finalState, "/result" -> ((123456 + 234567) * 2).toString)
  }

  test("writing file to disk in client mode") {
    val finalState = runSpark(
      ClientMode,
      nomadTestApp[SaveToDiskTestApp.type],
      appArgs = Seq(httpServer.url("/driver"), httpServer.url("/executor"))
    )
    checkResult(finalState,
      "/driver" -> "CONTENTS WRITTEN TO DRIVER FILE",
      "/executor" -> "CONTENTS WRITTEN TO EXECUTOR FILE")
  }

  test("writing file to disk in cluster mode") {
    val finalState = runSpark(
      ClusterMode,
      nomadTestApp[SaveToDiskTestApp.type],
      appArgs = Seq(httpServer.url("/driver"), httpServer.url("/executor"))
    )
    checkResult(finalState,
      "/driver" -> "CONTENTS WRITTEN TO DRIVER FILE",
      "/executor" -> "CONTENTS WRITTEN TO EXECUTOR FILE")
  }

  test("dynamic allocation of executors in client mode") {
    val finalState = runSpark(
      ClientMode,
      nomadTestApp[DiskPersistedRddTestDriver.type],
      appArgs = Seq(httpServer.url("/result")),
      extraConf = Map(
        "spark.dynamicAllocation.enabled" -> "true",
        "spark.shuffle.service.enabled" -> "true"
      )
    )
    checkResult(finalState, "/result" -> ((123456 + 234567) * 2).toString)
  }

  test("dynamic allocation of executors in cluster mode") {
    val finalState = runSpark(
      ClusterMode,
      nomadTestApp[DiskPersistedRddTestDriver.type],
      appArgs = Seq(httpServer.url("/result")),
      extraConf = Map(
        "spark.dynamicAllocation.enabled" -> "true",
        "spark.shuffle.service.enabled" -> "true"
      )
    )
    checkResult(finalState, "/result" -> ((123456 + 234567) * 2).toString)
  }

  test("use a job template in client mode") {
    val jobTemplate = IOUtils.toString(getClass.getResourceAsStream("job-template.json"), UTF_8)
    val finalState = runSpark(
      ClientMode,
      nomadTestApp[EnvironmentPostingTestDriver.type],
      appArgs = Seq(httpServer.url("/driver/"), httpServer.url("/executor/"),
        "NOMAD_ADDR_some_driver_sidecar_foo",
        "NOMAD_ADDR_some_executor_sidecar_bar"
      ),
      extraConf = Map(
        "spark.nomad.job.template" -> createFile("job-template", ".json", jobTemplate).toString
      )
    )
    checkResult(finalState,
      "/driver/NOMAD_ADDR_some_driver_sidecar_foo" -> "NOT SET",
      "/driver/NOMAD_ADDR_some_executor_sidecar_bar" -> "NOT SET",
      "/executor/NOMAD_ADDR_some_driver_sidecar_foo" -> "NOT SET")

    val executorBar = httpServer.valuePutToPath("/executor/NOMAD_ADDR_some_executor_sidecar_bar")
    executorBar.get should not be "NOT SET"
  }

  test("use a job template in cluster mode") {
    val jobTemplate = IOUtils.toString(getClass.getResourceAsStream("job-template.json"), UTF_8)
    val finalState = runSpark(
      ClusterMode,
      nomadTestApp[EnvironmentPostingTestDriver.type],
      appArgs = Seq(httpServer.url("/driver/"), httpServer.url("/executor/"),
        "NOMAD_ADDR_some_driver_sidecar_foo",
        "NOMAD_ADDR_some_executor_sidecar_bar"
      ),
      extraConf = Map(
        "spark.nomad.job.template" -> createFile("job-template", ".json", jobTemplate).toString
      )
    )
    checkResult(finalState,
      "/driver/NOMAD_ADDR_some_executor_sidecar_bar" -> "NOT SET",
      "/executor/NOMAD_ADDR_some_driver_sidecar_foo" -> "NOT SET")

    val driverFoo = httpServer.valuePutToPath("/driver/NOMAD_ADDR_some_driver_sidecar_foo")
    val executorBar = httpServer.valuePutToPath("/executor/NOMAD_ADDR_some_executor_sidecar_bar")

    driverFoo.get should not be "NOT SET"
    executorBar.get should not be "NOT SET"
  }


}


private object NomadLauncherTestApp {

  def main(args: Array[String]): Unit = {
    // Do not stop the application; the test will stop it using the launcher lib. Just run a task
    // that will prevent the process from exiting.
    val sc = new SparkContext(new SparkConf())
    sc.parallelize(Seq(1)).foreach { i =>
      this.synchronized {
        wait()
      }
    }
  }

}
