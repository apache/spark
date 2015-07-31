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

import java.io.{File, FileOutputStream, OutputStreamWriter}
import java.net.URL
import java.util.Properties
import java.util.concurrent.TimeUnit

import scala.collection.JavaConversions._
import scala.collection.mutable

import com.google.common.base.Charsets.UTF_8
import com.google.common.io.ByteStreams
import com.google.common.io.Files
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.server.MiniYARNCluster
import org.scalatest.{BeforeAndAfterAll, Matchers}

import org.apache.spark._
import org.apache.spark.scheduler.cluster.ExecutorInfo
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationStart,
  SparkListenerExecutorAdded}
import org.apache.spark.util.Utils

/**
 * Integration test for the external shuffle service with a yarn mini-cluster
 */
class YarnExternalShuffleSuite extends BaseYarnClusterSuite {

  override def yarnConfig: YarnConfiguration = {
    val yarnConfig = new YarnConfiguration()
    yarnConfig.set(YarnConfiguration.NM_AUX_SERVICES, "spark_shuffle")
    yarnConfig.set(YarnConfiguration.NM_AUX_SERVICE_FMT.format("spark_shuffle"),
      "org.apache.spark.network.yarn.YarnShuffleService")
    yarnConfig
  }

  test("external shuffle service") {
    val result = File.createTempFile("result", null, tempDir)
    runSpark(
      false,
      mainClassName(YarnExternalShuffleDriver.getClass),
      appArgs = Seq(result.getAbsolutePath()),
      extraConf = Map("spark.shuffle.service.enabled" -> "true")
    )
    checkResult(result)
  }
}

private object YarnExternalShuffleDriver extends Logging with Matchers {

  val WAIT_TIMEOUT_MILLIS = 10000

  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      // scalastyle:off println
      System.err.println(
        s"""
        |Invalid command line: ${args.mkString(" ")}
        |
        |Usage: ExternalShuffleDriver [result file]
        """.stripMargin)
      // scalastyle:on println
      System.exit(1)
    }

    val sc = new SparkContext(new SparkConf()
      .setAppName("External Shuffle Test"))
    val conf = sc.getConf
    val status = new File(args(0))
    var result = "failure"
    try {
      val data = sc.parallelize(0 until 100, 10).map { x => (x % 10) -> x }.reduceByKey{ _ + _ }.
        collect().toSet
      sc.listenerBus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS)
      data should be ((0 until 10).map{x => x -> (x * 10 + 450)}.toSet)
      result = "success"
    } finally {
      sc.stop()
      Files.write(result, status, UTF_8)
    }
  }

}
