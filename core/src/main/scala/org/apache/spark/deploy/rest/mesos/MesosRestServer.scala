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

package org.apache.spark.deploy.rest.mesos

import java.io.File
import javax.servlet.http.HttpServletResponse

import scala.collection.mutable

import org.apache.spark.deploy.Command
import org.apache.spark.deploy.mesos.MesosDriverDescription
import org.apache.spark.deploy.rest._
import org.apache.spark.scheduler.cluster.mesos.MesosClusterScheduler
import org.apache.spark.util.Utils
import org.apache.spark.{SPARK_VERSION => sparkVersion, SparkConf}


/**
 * A server that responds to requests submitted by the [[RestClient]].
 * This is intended to be used in Mesos cluster mode only, which forwards all requests to
 * [[MesosClusterScheduler]].
 *
 * For more details about the RestServer Spark protocol and status codes please refer to
 * [[RestServer]] javadocs.
 */
private[spark] class MesosRestServer(
    val host: String,
    val requestedPort: Int,
    val masterConf: SparkConf,
    scheduler: MesosClusterScheduler)
  extends RestServer {
  val submitRequestServlet = new MesosSubmitRequestServlet(scheduler, masterConf)
  val killRequestServlet = new MesosKillRequestServlet(scheduler, masterConf)
  val statusRequestServlet = new MesosStatusRequestServlet(scheduler, masterConf)
}

private[mesos] class MesosSubmitRequestServlet(
    scheduler: MesosClusterScheduler,
    conf: SparkConf)
  extends SubmitRequestServlet {

  private val DEFAULT_SUPERVISE = false
  private val DEFAULT_MEMORY = 512 // mb
  private val DEFAULT_CORES = 1

  /**
   * Build a driver description from the fields specified in the submit request.
   *
   * This involves constructing a command that launches a mesos framework for the job.
   * This does not currently consider fields used by python applications since python
   * is not supported in mesos cluster mode yet.
   */
  private def buildDriverDescription(request: CreateSubmissionRequest): MesosDriverDescription = {
    // Required fields, including the main class because python is not yet supported
    val appResource = Option(request.appResource).getOrElse {
      throw new SubmitRestMissingFieldException("Application jar is missing.")
    }
    val mainClass = Option(request.mainClass).getOrElse {
      throw new SubmitRestMissingFieldException("Main class is missing.")
    }

    // Optional fields
    val sparkProperties = request.sparkProperties
    val driverExtraJavaOptions = sparkProperties.get("spark.driver.extraJavaOptions")
    val driverExtraClassPath = sparkProperties.get("spark.driver.extraClassPath")
    val driverExtraLibraryPath = sparkProperties.get("spark.driver.extraLibraryPath")
    val superviseDriver = sparkProperties.get("spark.driver.supervise")
    val driverMemory = sparkProperties.get("spark.driver.memory")
    val driverCores = sparkProperties.get("spark.driver.cores")
    val appArgs = request.appArgs
    val environmentVariables = request.environmentVariables
    val schedulerProperties = new mutable.HashMap[String, String]
    // Store Spark submit specific arguments here to pass to the scheduler.
    schedulerProperties("spark.app.name") = sparkProperties.getOrElse("spark.app.name", mainClass)

    sparkProperties.get("spark.executor.memory").foreach { v =>
      schedulerProperties("spark.executor.memory") = v
    }
    sparkProperties.get("spark.cores.max").foreach { v =>
      schedulerProperties("spark.cores.max") = v
    }
    sparkProperties.get("spark.executor.uri").foreach { v =>
      schedulerProperties("spark.executor.uri") = v
    }
    sparkProperties.get("spark.mesos.executor.home").foreach { v =>
      schedulerProperties("spark.mesos.executor.home") = v
    }

    // Construct driver description
    val conf = new SparkConf(false)
      .setAll(sparkProperties)
    val extraClassPath = driverExtraClassPath.toSeq.flatMap(_.split(File.pathSeparator))
    val extraLibraryPath = driverExtraLibraryPath.toSeq.flatMap(_.split(File.pathSeparator))
    val extraJavaOpts = driverExtraJavaOptions.map(Utils.splitCommandString).getOrElse(Seq.empty)
    val sparkJavaOpts = Utils.sparkJavaOpts(conf)
    val javaOpts = sparkJavaOpts ++ extraJavaOpts
    val command = new Command(
      mainClass, appArgs, environmentVariables, extraClassPath, extraLibraryPath, javaOpts)
    val actualSuperviseDriver = superviseDriver.map(_.toBoolean).getOrElse(DEFAULT_SUPERVISE)
    val actualDriverMemory = driverMemory.map(Utils.memoryStringToMb).getOrElse(DEFAULT_MEMORY)
    val actualDriverCores = driverCores.map(_.toInt).getOrElse(DEFAULT_CORES)

    new MesosDriverDescription(
      appResource, actualDriverMemory, actualDriverCores,
      actualSuperviseDriver, command, schedulerProperties)
  }

  protected override def handleSubmit(
      requestMessageJson: String,
      requestMessage: SubmitRestProtocolMessage,
      responseServlet: HttpServletResponse): SubmitRestProtocolResponse = {
    requestMessage match {
      case submitRequest: CreateSubmissionRequest =>
        val driverDescription = buildDriverDescription(submitRequest)
        val s = scheduler.submitDriver(driverDescription)
        s.serverSparkVersion = sparkVersion
        val unknownFields = findUnknownFields(requestMessageJson, requestMessage)
        if (unknownFields.nonEmpty) {
          // If there are fields that the server does not know about, warn the client
          s.unknownFields = unknownFields
        }
        s
      case unexpected =>
        responseServlet.setStatus(HttpServletResponse.SC_BAD_REQUEST)
        handleError(s"Received message of unexpected type ${unexpected.messageType}.")
    }
  }
}

private[mesos] class MesosKillRequestServlet(scheduler: MesosClusterScheduler, conf: SparkConf)
  extends KillRequestServlet {
  protected override def handleKill(submissionId: String): KillSubmissionResponse = {
    val k = scheduler.killDriver(submissionId)
    k.serverSparkVersion = sparkVersion
    k
  }
}

private[mesos] class MesosStatusRequestServlet(scheduler: MesosClusterScheduler, conf: SparkConf)
  extends StatusRequestServlet {
  protected override def handleStatus(submissionId: String): SubmissionStatusResponse = {
    val d = scheduler.getStatus(submissionId)
    d.serverSparkVersion = sparkVersion
    d
  }
}
