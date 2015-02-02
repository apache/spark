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
import org.apache.spark.deploy.{Command, DeployMessages, DriverDescription}
import org.apache.spark.deploy.ClientArguments._
import org.apache.spark.deploy.master.Master

/**
 * A server that responds to requests submitted by the [[StandaloneRestClient]].
 * This is intended to be embedded in the standalone Master. Cluster mode only
 */
private[spark] class StandaloneRestServer(master: Master, host: String, requestedPort: Int)
  extends SubmitRestServer(host, requestedPort, master.conf) {
  protected override val handler = new StandaloneRestServerHandler(master)
}

/**
 * A handler for requests submitted to the standalone
 * Master via the REST application submission protocol.
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
  protected override def handleSubmit(request: SubmitDriverRequest): SubmitDriverResponse = {
    val driverDescription = buildDriverDescription(request)
    val response = AkkaUtils.askWithReply[DeployMessages.SubmitDriverResponse](
      DeployMessages.RequestSubmitDriver(driverDescription), masterActor, askTimeout)
    val s = new SubmitDriverResponse
    s.serverSparkVersion = sparkVersion
    s.message = response.message
    s.success = response.success.toString
    s.driverId = response.driverId.orNull
    s
  }

  /** Handle a request to kill a driver. */
  protected override def handleKill(request: KillDriverRequest): KillDriverResponse = {
    val driverId = request.driverId
    val response = AkkaUtils.askWithReply[DeployMessages.KillDriverResponse](
      DeployMessages.RequestKillDriver(driverId), masterActor, askTimeout)
    val k = new KillDriverResponse
    k.serverSparkVersion = sparkVersion
    k.message = response.message
    k.driverId = driverId
    k.success = response.success.toString
    k
  }

  /** Handle a request for a driver's status. */
  protected override def handleStatus(request: DriverStatusRequest): DriverStatusResponse = {
    val driverId = request.driverId
    val response = AkkaUtils.askWithReply[DeployMessages.DriverStatusResponse](
      DeployMessages.RequestDriverStatus(driverId), masterActor, askTimeout)
    val message = response.exception.map { s"Exception from the cluster:\n" + formatException(_) }
    val d = new DriverStatusResponse
    d.serverSparkVersion = sparkVersion
    d.driverId = driverId
    d.success = response.found.toString
    d.driverState = response.state.map(_.toString).orNull
    d.workerId = response.workerId.orNull
    d.workerHostPort = response.workerHostPort.orNull
    d.message = message.orNull
    d
  }

  /**
   * Build a driver description from the fields specified in the submit request.
   *
   * This does not currently consider fields used by python applications since
   * python is not supported in standalone cluster mode yet.
   */
  private def buildDriverDescription(request: SubmitDriverRequest): DriverDescription = {
    // Required fields, including the main class because python is not yet supported
    val appName = request.appName
    val appResource = request.appResource
    val mainClass = request.mainClass
    if (mainClass == null) {
      throw new SubmitRestMissingFieldException("Main class must be set in submit request.")
    }

    // Optional fields
    val jars = Option(request.jars)
    val files = Option(request.files)
    val driverMemory = Option(request.driverMemory)
    val driverCores = Option(request.driverCores)
    val driverExtraJavaOptions = Option(request.driverExtraJavaOptions)
    val driverExtraClassPath = Option(request.driverExtraClassPath)
    val driverExtraLibraryPath = Option(request.driverExtraLibraryPath)
    val superviseDriver = Option(request.superviseDriver)
    val executorMemory = Option(request.executorMemory)
    val totalExecutorCores = Option(request.totalExecutorCores)
    val appArgs = request.appArgs
    val sparkProperties = request.sparkProperties
    val environmentVariables = request.environmentVariables

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
