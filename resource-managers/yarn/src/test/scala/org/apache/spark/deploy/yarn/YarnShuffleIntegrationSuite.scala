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

package org.apache.spark.deploy.yarn

import java.io.File
import java.nio.charset.StandardCharsets

import com.google.common.io.Files
import org.apache.commons.io.FileUtils
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.scalatest.Matchers

import org.apache.spark._
import org.apache.spark.deploy.yarn.config._
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.network.shuffle.ShuffleTestAccessor
import org.apache.spark.network.yarn.{YarnShuffleService, YarnTestAccessor}
import org.apache.spark.tags.ExtendedYarnTest

/**
 * Integration test for the external shuffle service with a yarn mini-cluster
 */
@ExtendedYarnTest
class YarnShuffleIntegrationSuite extends BaseYarnClusterSuite {

  override def newYarnConfig(): YarnConfiguration = {
    val yarnConfig = new YarnConfiguration()
    yarnConfig.set(YarnConfiguration.NM_AUX_SERVICES, "spark_shuffle")
    yarnConfig.set(YarnConfiguration.NM_AUX_SERVICE_FMT.format("spark_shuffle"),
      classOf[YarnShuffleService].getCanonicalName)
    yarnConfig.set("spark.shuffle.service.port", "0")
    yarnConfig
  }

  protected def extraSparkConf(): Map[String, String] = {
    val shuffleServicePort = YarnTestAccessor.getShuffleServicePort
    val shuffleService = YarnTestAccessor.getShuffleServiceInstance
    logInfo("Shuffle service port = " + shuffleServicePort)

    Map(
      "spark.shuffle.service.enabled" -> "true",
      "spark.shuffle.service.port" -> shuffleServicePort.toString,
      MAX_EXECUTOR_FAILURES.key -> "1"
    )
  }

  test("external shuffle service") {
    val shuffleServicePort = YarnTestAccessor.getShuffleServicePort
    val shuffleService = YarnTestAccessor.getShuffleServiceInstance

    val registeredExecFile = YarnTestAccessor.getRegisteredExecutorFile(shuffleService)

    val result = File.createTempFile("result", null, tempDir)
    val finalState = runSpark(
      false,
      mainClassName(YarnExternalShuffleDriver.getClass),
      appArgs = Seq(result.getAbsolutePath(), registeredExecFile.getAbsolutePath),
      extraConf = extraSparkConf()
    )
    checkResult(finalState, result)
    assert(YarnTestAccessor.getRegisteredExecutorFile(shuffleService).exists())
  }
}

/**
 * Integration test for the external shuffle service with auth on.
 */
@ExtendedYarnTest
class YarnShuffleAuthSuite extends YarnShuffleIntegrationSuite {

  override def newYarnConfig(): YarnConfiguration = {
    val yarnConfig = super.newYarnConfig()
    yarnConfig.set(NETWORK_AUTH_ENABLED.key, "true")
    yarnConfig.set(NETWORK_ENCRYPTION_ENABLED.key, "true")
    yarnConfig
  }

  override protected def extraSparkConf(): Map[String, String] = {
    super.extraSparkConf() ++ Map(
      NETWORK_AUTH_ENABLED.key -> "true",
      NETWORK_ENCRYPTION_ENABLED.key -> "true"
    )
  }

}

private object YarnExternalShuffleDriver extends Logging with Matchers {

  val WAIT_TIMEOUT_MILLIS = 10000

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      // scalastyle:off println
      System.err.println(
        s"""
        |Invalid command line: ${args.mkString(" ")}
        |
        |Usage: ExternalShuffleDriver [result file] [registered exec file]
        """.stripMargin)
      // scalastyle:on println
      System.exit(1)
    }

    val sc = new SparkContext(new SparkConf()
      .setAppName("External Shuffle Test"))
    val conf = sc.getConf
    val status = new File(args(0))
    val registeredExecFile = new File(args(1))
    logInfo("shuffle service executor file = " + registeredExecFile)
    var result = "failure"
    val execStateCopy = new File(registeredExecFile.getAbsolutePath + "_dup")
    try {
      val data = sc.parallelize(0 until 100, 10).map { x => (x % 10) -> x }.reduceByKey{ _ + _ }.
        collect().toSet
      sc.listenerBus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS)
      data should be ((0 until 10).map{x => x -> (x * 10 + 450)}.toSet)
      result = "success"
      // only one process can open a leveldb file at a time, so we copy the files
      FileUtils.copyDirectory(registeredExecFile, execStateCopy)
      assert(!ShuffleTestAccessor.reloadRegisteredExecutors(execStateCopy).isEmpty)
    } finally {
      sc.stop()
      FileUtils.deleteDirectory(execStateCopy)
      Files.write(result, status, StandardCharsets.UTF_8)
    }
  }

}
