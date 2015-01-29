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

import java.net.URL

import org.apache.spark.{SPARK_VERSION => sparkVersion}
import org.apache.spark.deploy.SparkSubmitArguments

/**
 * A client that submits applications to the standalone Master using the REST protocol
 * This client is intended to communicate with the [[StandaloneRestServer]]. Cluster mode only.
 */
private[spark] class StandaloneRestClient extends SubmitRestClient {
  import StandaloneRestClient._

  /**
   * Request that the REST server submit a driver specified by the provided arguments.
   *
   * If the driver was successfully submitted, this polls the status of the driver that was
   * just submitted and reports it to the user. Otherwise, if the submission was unsuccessful,
   * this reports failure and logs an error message provided by the REST server.
   */
  override def submitDriver(args: SparkSubmitArguments): SubmitDriverResponse = {
    validateSubmitArgs(args)
    val submitResponse = super.submitDriver(args)
    val submitSuccess = submitResponse.getSuccess.toBoolean
    if (submitSuccess) {
      val driverId = submitResponse.getDriverId
      logInfo(s"Driver successfully submitted as $driverId. Polling driver state...")
      pollSubmittedDriverStatus(args.master, driverId)
    } else {
      val submitMessage = submitResponse.getMessage
      logError(s"Application submission failed: $submitMessage")
    }
    submitResponse
  }

  /** Request that the REST server kill the specified driver. */
  override def killDriver(master: String, driverId: String): KillDriverResponse = {
    validateMaster(master)
    super.killDriver(master, driverId)
  }

  /** Request the status of the specified driver from the REST server. */
  override def requestDriverStatus(master: String, driverId: String): DriverStatusResponse = {
    validateMaster(master)
    super.requestDriverStatus(master, driverId)
  }

  /**
   * Poll the status of the driver that was just submitted and log it.
   * This retries up to a fixed number of times before giving up.
   */
  private def pollSubmittedDriverStatus(master: String, driverId: String): Unit = {
    (1 to REPORT_DRIVER_STATUS_MAX_TRIES).foreach { _ =>
      val statusResponse = requestDriverStatus(master, driverId)
      val statusSuccess = statusResponse.getSuccess.toBoolean
      if (statusSuccess) {
        val driverState = statusResponse.getDriverState
        val workerId = Option(statusResponse.getWorkerId)
        val workerHostPort = Option(statusResponse.getWorkerHostPort)
        val exception = Option(statusResponse.getMessage)
        logInfo(s"State of driver $driverId is now $driverState.")
        // Log worker node, if present
        (workerId, workerHostPort) match {
          case (Some(id), Some(hp)) => logInfo(s"Driver is running on worker $id at $hp.")
          case _ =>
        }
        // Log exception stack trace, if present
        exception.foreach { e => logError(e) }
        return
      }
      Thread.sleep(REPORT_DRIVER_STATUS_INTERVAL)
    }
    logError(s"Error: Master did not recognize driver $driverId.")
  }

  /** Construct a submit driver request message. */
  protected override def constructSubmitRequest(args: SparkSubmitArguments): SubmitDriverRequest = {
    val message = new SubmitDriverRequest()
      .setSparkVersion(sparkVersion)
      .setAppName(args.name)
      .setAppResource(args.primaryResource)
      .setMainClass(args.mainClass)
      .setJars(args.jars)
      .setFiles(args.files)
      .setDriverMemory(args.driverMemory)
      .setDriverCores(args.driverCores)
      .setDriverExtraJavaOptions(args.driverExtraJavaOptions)
      .setDriverExtraClassPath(args.driverExtraClassPath)
      .setDriverExtraLibraryPath(args.driverExtraLibraryPath)
      .setSuperviseDriver(args.supervise.toString)
      .setExecutorMemory(args.executorMemory)
      .setTotalExecutorCores(args.totalExecutorCores)
    args.childArgs.foreach(message.addAppArg)
    args.sparkProperties.foreach { case (k, v) => message.setSparkProperty(k, v) }
    sys.env.foreach { case (k, v) =>
      if (k.startsWith("SPARK_")) { message.setEnvironmentVariable(k, v) }
    }
    message
  }

  /** Construct a kill driver request message. */
  protected override def constructKillRequest(
      master: String,
      driverId: String): KillDriverRequest = {
    new KillDriverRequest()
      .setSparkVersion(sparkVersion)
      .setDriverId(driverId)
  }

  /** Construct a driver status request message. */
  protected override def constructStatusRequest(
      master: String,
      driverId: String): DriverStatusRequest = {
    new DriverStatusRequest()
      .setSparkVersion(sparkVersion)
      .setDriverId(driverId)
  }

  /** Extract the URL portion of the master address. */
  protected override def getHttpUrl(master: String): URL = {
    validateMaster(master)
    new URL("http://" + master.stripPrefix("spark://"))
  }

  /** Throw an exception if this is not standalone mode. */
  private def validateMaster(master: String): Unit = {
    if (!master.startsWith("spark://")) {
      throw new IllegalArgumentException("This REST client is only supported in standalone mode.")
    }
  }

  /** Throw an exception if this is not standalone cluster mode. */
  private def validateSubmitArgs(args: SparkSubmitArguments): Unit = {
    if (!args.isStandaloneCluster) {
      throw new IllegalArgumentException(
        "This REST client is only supported in standalone cluster mode.")
    }
  }
}

private object StandaloneRestClient {
  val REPORT_DRIVER_STATUS_INTERVAL = 1000
  val REPORT_DRIVER_STATUS_MAX_TRIES = 10
}
