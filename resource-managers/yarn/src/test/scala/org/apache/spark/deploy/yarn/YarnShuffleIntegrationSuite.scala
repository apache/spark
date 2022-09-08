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
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers._

import org.apache.spark._
import org.apache.spark.deploy.yarn.config._
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.internal.config.Network._
import org.apache.spark.network.shuffle.ShuffleTestAccessor
import org.apache.spark.network.shuffledb.DBBackend
import org.apache.spark.network.yarn.{YarnShuffleService, YarnTestAccessor}
import org.apache.spark.tags.ExtendedYarnTest

/**
 * Integration test for the external shuffle service with a yarn mini-cluster
 */
abstract class YarnShuffleIntegrationSuite extends BaseYarnClusterSuite {

  protected def dbBackend: DBBackend

  override def newYarnConfig(): YarnConfiguration = {
    val yarnConfig = new YarnConfiguration()
    yarnConfig.set(YarnConfiguration.NM_AUX_SERVICES, "spark_shuffle")
    yarnConfig.set(YarnConfiguration.NM_AUX_SERVICE_FMT.format("spark_shuffle"),
      classOf[YarnShuffleService].getCanonicalName)
    yarnConfig.set(SHUFFLE_SERVICE_PORT.key, "0")
    yarnConfig.set(SHUFFLE_SERVICE_DB_BACKEND.key, dbBackend.name())
    yarnConfig
  }

  protected def extraSparkConf(): Map[String, String] = {
    val shuffleServicePort = YarnTestAccessor.getShuffleServicePort
    val shuffleService = YarnTestAccessor.getShuffleServiceInstance
    logInfo("Shuffle service port = " + shuffleServicePort)

    Map(
      SHUFFLE_SERVICE_ENABLED.key -> "true",
      SHUFFLE_SERVICE_PORT.key -> shuffleServicePort.toString,
      MAX_EXECUTOR_FAILURES.key -> "1",
      SHUFFLE_SERVICE_DB_BACKEND.key -> dbBackend.name()
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
      appArgs = if (registeredExecFile != null) {
        Seq(result.getAbsolutePath, registeredExecFile.getAbsolutePath)
      } else {
        Seq(result.getAbsolutePath)
      },
      extraConf = extraSparkConf()
    )
    checkResult(finalState, result)

    if (registeredExecFile != null) {
      assert(YarnTestAccessor.getRegisteredExecutorFile(shuffleService).exists())
    }
  }
}

@ExtendedYarnTest
class YarnShuffleIntegrationWithLevelDBBackendSuite
  extends YarnShuffleIntegrationSuite {
  override protected def dbBackend: DBBackend = DBBackend.LEVELDB
}

@ExtendedYarnTest
class YarnShuffleIntegrationWithRocksDBBackendSuite
  extends YarnShuffleIntegrationSuite {
  override protected def dbBackend: DBBackend = DBBackend.ROCKSDB
}

/**
 * Integration test for the external shuffle service with auth on.
 */
abstract class YarnShuffleAuthSuite extends YarnShuffleIntegrationSuite {
  override def newYarnConfig(): YarnConfiguration = {
    val yarnConfig = super.newYarnConfig()
    yarnConfig.set(NETWORK_AUTH_ENABLED.key, "true")
    yarnConfig.set(NETWORK_CRYPTO_ENABLED.key, "true")
    yarnConfig
  }

  override protected def extraSparkConf(): Map[String, String] = {
    super.extraSparkConf() ++ Map(
      NETWORK_AUTH_ENABLED.key -> "true",
      NETWORK_CRYPTO_ENABLED.key -> "true"
    )
  }
}

@ExtendedYarnTest
class YarnShuffleAuthWithLevelDBBackendSuite extends YarnShuffleAuthSuite {
  override protected def dbBackend: DBBackend = DBBackend.LEVELDB
}

@ExtendedYarnTest
class YarnShuffleAuthWithRocksDBBackendSuite extends YarnShuffleAuthSuite {
  override protected def dbBackend: DBBackend = DBBackend.ROCKSDB
}

private object YarnExternalShuffleDriver extends Logging with Matchers {

  val WAIT_TIMEOUT_MILLIS = 10000

  def main(args: Array[String]): Unit = {
    if (args.length > 2) {
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
    val registeredExecFile = if (args.length == 2) {
      new File(args(1))
    } else {
      null
    }
    logInfo("shuffle service executor file = " + registeredExecFile)
    var result = "failure"
    val execStateCopy = Option(registeredExecFile).map { file =>
      new File(file.getAbsolutePath + "_dup")
    }.orNull
    try {
      val data = sc.parallelize(0 until 100, 10).map { x => (x % 10) -> x }.reduceByKey{ _ + _ }.
        collect().toSet
      sc.listenerBus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS)
      data should be ((0 until 10).map{x => x -> (x * 10 + 450)}.toSet)
      result = "success"
      // only one process can open a leveldb file at a time, so we copy the files
      if (registeredExecFile != null && execStateCopy != null) {
        val dbBackendName = conf.get(SHUFFLE_SERVICE_DB_BACKEND.key)
        val dbBackend = DBBackend.byName(dbBackendName)
        logWarning(s"Use ${dbBackend.name()} as the implementation of " +
          s"${SHUFFLE_SERVICE_DB_BACKEND.key}")
        FileUtils.copyDirectory(registeredExecFile, execStateCopy)
        assert(!ShuffleTestAccessor
          .reloadRegisteredExecutors(dbBackend, execStateCopy).isEmpty)
      }
    } finally {
      sc.stop()
      if (execStateCopy != null) {
        FileUtils.deleteDirectory(execStateCopy)
      }
      Files.write(result, status, StandardCharsets.UTF_8)
    }
  }

}
