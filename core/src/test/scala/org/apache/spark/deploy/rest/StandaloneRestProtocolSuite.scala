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

package org.apache.spark.deploy.rest

import java.io.{File, FileInputStream, FileOutputStream, PrintWriter}
import java.util.jar.{JarEntry, JarOutputStream}
import java.util.zip.ZipEntry

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

import akka.actor.ActorSystem
import com.google.common.io.ByteStreams
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuite}
import org.scalatest.exceptions.TestFailedException

import org.apache.spark._
import org.apache.spark.util.Utils
import org.apache.spark.deploy.{SparkSubmit, SparkSubmitArguments}
import org.apache.spark.deploy.master.{DriverState, Master}
import org.apache.spark.deploy.worker.Worker

/**
 * End-to-end tests for the REST application submission protocol in standalone mode.
 */
class StandaloneRestProtocolSuite extends FunSuite with BeforeAndAfterAll with BeforeAndAfterEach {
  private val systemsToStop = new ArrayBuffer[ActorSystem]
  private val masterRestUrl = startLocalCluster()
  private val client = new StandaloneRestClient
  private val mainJar = StandaloneRestProtocolSuite.createJar()
  private val mainClass = StandaloneRestApp.getClass.getName.stripSuffix("$")

  override def afterAll() {
    systemsToStop.foreach(_.shutdown())
  }

  test("simple submit until completion") {
    val resultsFile = File.createTempFile("test-submit", ".txt")
    val numbers = Seq(1, 2, 3)
    val size = 500
    val driverId = submitApplication(resultsFile, numbers, size)
    waitUntilFinished(driverId)
    validateResult(resultsFile, numbers, size)
  }

  test("kill empty driver") {
    val killResponse = client.killDriver(masterRestUrl, "driver-that-does-not-exist")
    val killSuccess = killResponse.getSuccess
    assert(killSuccess === "false")
  }

  test("kill running driver") {
    val resultsFile = File.createTempFile("test-kill", ".txt")
    val numbers = Seq(1, 2, 3)
    val size = 500
    val driverId = submitApplication(resultsFile, numbers, size)
    val killResponse = client.killDriver(masterRestUrl, driverId)
    val killSuccess = killResponse.getSuccess
    waitUntilFinished(driverId)
    val statusResponse = client.requestDriverStatus(masterRestUrl, driverId)
    val statusSuccess = statusResponse.getSuccess
    val driverState = statusResponse.getDriverState
    assert(killSuccess === "true")
    assert(statusSuccess === "true")
    assert(driverState === DriverState.KILLED.toString)
    // we should not see the expected results because we killed the driver
    intercept[TestFailedException] { validateResult(resultsFile, numbers, size) }
  }

  test("request status for empty driver") {
    val statusResponse = client.requestDriverStatus(masterRestUrl, "driver-that-does-not-exist")
    val statusSuccess = statusResponse.getSuccess
    assert(statusSuccess === "false")
  }

  /**
   * Start a local cluster containing one Master and a few Workers.
   * Do not use [[org.apache.spark.deploy.LocalSparkCluster]] here because we want the REST URL.
   * Return the Master's REST URL to which applications should be submitted.
   */
  private def startLocalCluster(): String = {
    val conf = new SparkConf(false)
      .set("spark.master.rest.enabled", "true")
      .set("spark.master.rest.port", "0")
    val (numWorkers, coresPerWorker, memPerWorker) = (2, 1, 512)
    val localHostName = Utils.localHostName()
    val (masterSystem, masterPort, _, _masterRestPort) =
      Master.startSystemAndActor(localHostName, 0, 0, conf)
    val masterRestPort = _masterRestPort.getOrElse { fail("REST server not started on Master!") }
    val masterUrl = "spark://" + localHostName + ":" + masterPort
    val masterRestUrl = "spark://" + localHostName + ":" + masterRestPort
    (1 to numWorkers).foreach { n =>
      val (workerSystem, _) = Worker.startSystemAndActor(
        localHostName, 0, 0, coresPerWorker, memPerWorker, Array(masterUrl), null, Some(n))
      systemsToStop.append(workerSystem)
    }
    systemsToStop.append(masterSystem)
    masterRestUrl
  }

  /** Submit the [[StandaloneRestApp]] and return the corresponding driver ID. */
  private def submitApplication(resultsFile: File, numbers: Seq[Int], size: Int): String = {
    val appArgs = Seq(resultsFile.getAbsolutePath) ++ numbers.map(_.toString) ++ Seq(size.toString)
    val commandLineArgs = Array(
      "--deploy-mode", "cluster",
      "--master", masterRestUrl,
      "--name", mainClass,
      "--class", mainClass,
      "--conf", "spark.submit.rest.enabled=true",
      mainJar) ++ appArgs
    val args = new SparkSubmitArguments(commandLineArgs)
    SparkSubmit.prepareSubmitEnvironment(args)
    val submitResponse = client.submitDriver(args)
    submitResponse.getDriverId
  }

  /** Wait until the given driver has finished running up to the specified timeout. */
  private def waitUntilFinished(driverId: String, maxSeconds: Int = 10): Unit = {
    var finished = false
    val expireTime = System.currentTimeMillis + maxSeconds * 1000
    while (!finished) {
      val statusResponse = client.requestDriverStatus(masterRestUrl, driverId)
      val driverState = statusResponse.getDriverState
      finished =
        driverState != DriverState.SUBMITTED.toString &&
        driverState != DriverState.RUNNING.toString
      if (System.currentTimeMillis > expireTime) {
        fail(s"Driver $driverId did not finish within $maxSeconds seconds.")
      }
      Thread.sleep(1000)
    }
  }

  /** Validate whether the application produced the corrupt output. */
  private def validateResult(resultsFile: File, numbers: Seq[Int], size: Int): Unit = {
    val lines = Source.fromFile(resultsFile.getAbsolutePath).getLines().toSeq
    val unexpectedContent =
      if (lines.nonEmpty) {
        "[\n" + lines.map { l => "  " + l }.mkString("\n") + "\n]"
      } else {
        "[EMPTY]"
      }
    assert(lines.size === 2, s"Unexpected content in file: $unexpectedContent")
    assert(lines(0).toInt === numbers.sum, s"Sum of ${numbers.mkString(",")} is incorrect")
    assert(lines(1).toInt === (size / 2) + 1, "Result of Spark job is incorrect")
  }
}

private object StandaloneRestProtocolSuite {
  private val pathPrefix = "org/apache/spark/deploy/rest"

  /**
   * Create a jar that contains all the class files needed for running the [[StandaloneRestApp]].
   * Return the absolute path to that jar.
   */
  def createJar(): String = {
    val jarFile = File.createTempFile("test-standalone-rest-protocol", ".jar")
    val jarFileStream = new FileOutputStream(jarFile)
    val jarStream = new JarOutputStream(jarFileStream, new java.util.jar.Manifest)
    jarStream.putNextEntry(new ZipEntry(pathPrefix))
    getClassFiles.foreach { cf =>
      jarStream.putNextEntry(new JarEntry(pathPrefix + "/" + cf.getName))
      val in = new FileInputStream(cf)
      ByteStreams.copy(in, jarStream)
      in.close()
    }
    jarStream.close()
    jarFileStream.close()
    jarFile.getAbsolutePath
  }

  /**
   * Return a list of class files compiled for [[StandaloneRestApp]].
   * This includes all the anonymous classes used in the application.
   */
  private def getClassFiles: Seq[File] = {
    val className = Utils.getFormattedClassName(StandaloneRestApp)
    val clazz = StandaloneRestApp.getClass
    val basePath = clazz.getProtectionDomain.getCodeSource.getLocation.toURI.getPath
    val baseDir = new File(basePath + "/" + pathPrefix)
    baseDir.listFiles().filter(_.getName.contains(className))
  }
}

/**
 * Sample application to be submitted to the cluster using the REST gateway.
 * All relevant classes will be packaged into a jar at run time.
 */
object StandaloneRestApp {
  // Usage: [path to results file] [num1] [num2] [num3] [rddSize]
  // The first line of the results file should be (num1 + num2 + num3)
  // The second line should be (rddSize / 2) + 1
  def main(args: Array[String]) {
    assert(args.size == 5, s"Expected exactly 5 arguments: ${args.mkString(",")}")
    val resultFile = new File(args(0))
    val writer = new PrintWriter(resultFile)
    try {
      val conf = new SparkConf()
      val sc = new SparkContext(conf)
      val firstLine = args(1).toInt + args(2).toInt + args(3).toInt
      val secondLine = sc.parallelize(1 to args(4).toInt)
        .map { i => (i / 2, i) }
        .reduceByKey(_ + _)
        .count()
      writer.println(firstLine)
      writer.println(secondLine)
    } catch {
      case e: Exception =>
        writer.println(e)
        e.getStackTrace.foreach { l => writer.println("  " + l) }
    } finally {
      writer.close()
    }
  }
}
