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

import java.io.File

import akka.actor.ActorRef

import org.apache.spark.{SPARK_VERSION => sparkVersion}
import org.apache.spark.SparkConf
import org.apache.spark.util.{AkkaUtils, Utils}
import org.apache.spark.deploy.{Command, DriverDescription}
import org.apache.spark.deploy.ClientArguments._
import org.apache.spark.deploy.DeployMessages
import org.apache.spark.deploy.master.Master

/**
 * A server that responds to requests submitted by the StandaloneRestClient.
 * This is intended to be embedded in the standalone Master. Cluster mode only.
 */
private[spark] class StandaloneRestServer(master: Master, host: String, requestedPort: Int)
  extends SubmitRestServer(host, requestedPort, master.conf) {
  override protected val handler = new StandaloneRestServerHandler(master)
}

/**
 * A handler for requests submitted to the standalone Master
 * via the stable application submission REST protocol.
 */
private[spark] class StandaloneRestServerHandler(
    conf: SparkConf,
    masterActor: ActorRef,
    masterUrl: String)
  extends SubmitRestServerHandler {

  private val askTimeout = AkkaUtils.askTimeout(conf)

  def this(master: Master) = {
    this(master.conf, master.self, master.masterUrl)
  }

  /** Handle a request to submit a driver. */
  override protected def handleSubmit(
      request: SubmitDriverRequest): SubmitDriverResponse = {
    val driverDescription = buildDriverDescription(request)
    val response = AkkaUtils.askWithReply[DeployMessages.SubmitDriverResponse](
      DeployMessages.RequestSubmitDriver(driverDescription), masterActor, askTimeout)
    new SubmitDriverResponse()
      .setSparkVersion(sparkVersion)
      .setMessage(response.message)
      .setSuccess(response.success.toString)
      .setDriverId(response.driverId.orNull)
  }

  /** Handle a request to kill a driver. */
  override protected def handleKill(
      request: KillDriverRequest): KillDriverResponse = {
    val driverId = request.getDriverId
    val response = AkkaUtils.askWithReply[DeployMessages.KillDriverResponse](
      DeployMessages.RequestKillDriver(driverId), masterActor, askTimeout)
    new KillDriverResponse()
      .setSparkVersion(sparkVersion)
      .setMessage(response.message)
      .setDriverId(driverId)
      .setSuccess(response.success.toString)
  }

  /** Handle a request for a driver's status. */
  override protected def handleStatus(
      request: DriverStatusRequest): DriverStatusResponse = {
    val driverId = request.getDriverId
    val response = AkkaUtils.askWithReply[DeployMessages.DriverStatusResponse](
      DeployMessages.RequestDriverStatus(driverId), masterActor, askTimeout)
    // Format exception nicely, if it exists
    val message = response.exception.map { e =>
      val stackTraceString = e.getStackTrace.map { "\t" + _ }.mkString("\n")
      s"Exception from the cluster:\n$e\n$stackTraceString"
    }
    new DriverStatusResponse()
      .setSparkVersion(sparkVersion)
      .setDriverId(driverId)
      .setSuccess(response.found.toString)
      .setDriverState(response.state.map(_.toString).orNull)
      .setWorkerId(response.workerId.orNull)
      .setWorkerHostPort(response.workerHostPort.orNull)
      .setMessage(message.orNull)
  }

  /**
   * Build a driver description from the fields specified in the submit request.
   * This does not currently consider fields used by python applications since
   * python is not supported in standalone cluster mode yet.
   */
  private def buildDriverDescription(request: SubmitDriverRequest): DriverDescription = {
    // Required fields, including the main class because python is not yet supported
    val appName = request.getAppName
    val appResource = request.getAppResource
    val mainClass = request.getMainClass

    // Optional fields
    val jars = Option(request.getJars)
    val files = Option(request.getFiles)
    val driverMemory = Option(request.getDriverMemory)
    val driverCores = Option(request.getDriverCores)
    val driverExtraJavaOptions = Option(request.getDriverExtraJavaOptions)
    val driverExtraClassPath = Option(request.getDriverExtraClassPath)
    val driverExtraLibraryPath = Option(request.getDriverExtraLibraryPath)
    val superviseDriver = Option(request.getSuperviseDriver)
    val executorMemory = Option(request.getExecutorMemory)
    val totalExecutorCores = Option(request.getTotalExecutorCores)
    val appArgs = request.getAppArgs
    val sparkProperties = request.getSparkProperties
    val environmentVariables = request.getEnvironmentVariables

    // Translate all fields to the relevant Spark properties
    val conf = new SparkConf(false)
      .setAll(sparkProperties)
      .set("spark.master", masterUrl)
      .set("spark.app.name", appName)
    jars.foreach { j => conf.set("spark.jars", j) }
    files.foreach { f => conf.set("spark.files", f) }
    driverExtraJavaOptions.foreach { j => conf.set("spark.driver.extraJavaOptions", j) }
    driverExtraClassPath.foreach { cp => conf.set("spark.driver.extraClassPath", cp) }
    driverExtraLibraryPath.foreach { lp => conf.set("spark.driver.extraLibraryPath", lp) }
    executorMemory.foreach { m => conf.set("spark.executor.memory", m) }
    totalExecutorCores.foreach { c => conf.set("spark.cores.max", c) }

    // Construct driver description and submit it
    val extraClassPath = driverExtraClassPath.toSeq.flatMap(_.split(File.pathSeparator))
    val extraLibraryPath = driverExtraLibraryPath.toSeq.flatMap(_.split(File.pathSeparator))
    val extraJavaOpts = driverExtraJavaOptions.map(Utils.splitCommandString).getOrElse(Seq.empty)
    val sparkJavaOpts = Utils.sparkJavaOpts(conf)
    val javaOpts = sparkJavaOpts ++ extraJavaOpts
    val command = new Command(
      "org.apache.spark.deploy.worker.DriverWrapper",
      Seq("{{WORKER_URL}}", mainClass) ++ appArgs, // args to the DriverWrapper
      environmentVariables, extraClassPath, extraLibraryPath, javaOpts)
    val actualDriverMemory = driverMemory.map(Utils.memoryStringToMb).getOrElse(DEFAULT_MEMORY)
    val actualDriverCores = driverCores.map(_.toInt).getOrElse(DEFAULT_CORES)
    val actualSuperviseDriver = superviseDriver.map(_.toBoolean).getOrElse(DEFAULT_SUPERVISE)
    new DriverDescription(
      appResource, actualDriverMemory, actualDriverCores, actualSuperviseDriver, command)
  }
}
