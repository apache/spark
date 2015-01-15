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

import java.io.DataOutputStream
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}

import scala.io.Source

import com.google.common.base.Charsets
import org.eclipse.jetty.server.{Request, Server}
import org.eclipse.jetty.server.handler.AbstractHandler

import org.apache.spark.{Logging, SPARK_VERSION => sparkVersion}
import org.apache.spark.deploy.rest.StandaloneRestProtocolAction._
import org.apache.spark.util.Utils

/**
 * A server that responds to requests submitted by the StandaloneRestClient.
 */
private[spark] class StandaloneRestServer(requestedPort: Int) {
  val server = new Server(requestedPort)
  server.setHandler(new StandaloneRestHandler)
  server.start()
  server.join()
}

/**
 * A Jetty handler that responds to requests submitted via the standalone REST protocol.
 */
private[spark] class StandaloneRestHandler extends AbstractHandler with Logging {

  override def handle(
      target: String,
      baseRequest: Request,
      request: HttpServletRequest,
      response: HttpServletResponse): Unit = {
    try {
      val requestMessageJson = Source.fromInputStream(request.getInputStream).mkString
      val requestMessage = StandaloneRestProtocolMessage.fromJson(requestMessageJson)
      val responseMessage = constructResponseMessage(requestMessage)
      response.setContentType("application/json")
      response.setCharacterEncoding("utf-8")
      response.setStatus(HttpServletResponse.SC_OK)
      val content = responseMessage.toJson.getBytes(Charsets.UTF_8)
      val out = new DataOutputStream(response.getOutputStream)
      out.write(content)
      out.close()
      baseRequest.setHandled(true)
    } catch {
      case e: Exception => logError("Exception while handling request", e)
    }
  }

  private def constructResponseMessage(
      request: StandaloneRestProtocolMessage): StandaloneRestProtocolMessage = {
    // If the request is sent via the StandaloneRestClient, it should have already been
    // validated remotely. In case this is not true, validate the request here to guard
    // against potential NPEs. If validation fails, return an ERROR message to the sender.
    try {
      request.validate()
    } catch {
      case e: IllegalArgumentException =>
        return handleError(e.getMessage)
    }
    request match {
      case submit: SubmitDriverRequestMessage => handleSubmitRequest(submit)
      case kill: KillDriverRequestMessage => handleKillRequest(kill)
      case status: DriverStatusRequestMessage => handleStatusRequest(status)
      case unexpected => handleError(
        s"Received message of unexpected type ${Utils.getFormattedClassName(unexpected)}.")
    }
  }

  private def handleSubmitRequest(
      request: SubmitDriverRequestMessage): SubmitDriverResponseMessage = {
    import SubmitDriverResponseField._
    // TODO: Actually submit the driver
    val message = "Driver is submitted successfully..."
    val master = request.getField(SubmitDriverRequestField.MASTER)
    val driverId = "new_driver_id"
    val driverState = "SUBMITTED"
    new SubmitDriverResponseMessage()
      .setField(SPARK_VERSION, sparkVersion)
      .setField(MESSAGE, message)
      .setField(MASTER, master)
      .setField(DRIVER_ID, driverId)
      .setField(DRIVER_STATE, driverState)
      .validate()
  }

  private def handleKillRequest(request: KillDriverRequestMessage): KillDriverResponseMessage = {
    import KillDriverResponseField._
    // TODO: Actually kill the driver
    val message = "Driver is killed successfully..."
    val master = request.getField(KillDriverRequestField.MASTER)
    val driverId = request.getField(KillDriverRequestField.DRIVER_ID)
    val driverState = "KILLED"
    new KillDriverResponseMessage()
      .setField(SPARK_VERSION, sparkVersion)
      .setField(MESSAGE, message)
      .setField(MASTER, master)
      .setField(DRIVER_ID, driverId)
      .setField(DRIVER_STATE, driverState)
      .validate()
  }

  private def handleStatusRequest(
      request: DriverStatusRequestMessage): DriverStatusResponseMessage = {
    import DriverStatusResponseField._
    // TODO: Actually look up the status of the driver
    val master = request.getField(DriverStatusRequestField.MASTER)
    val driverId = request.getField(DriverStatusRequestField.DRIVER_ID)
    val driverState = "HEALTHY"
    new DriverStatusResponseMessage()
      .setField(SPARK_VERSION, sparkVersion)
      .setField(MASTER, master)
      .setField(DRIVER_ID, driverId)
      .setField(DRIVER_STATE, driverState)
      .validate()
  }

  private def handleError(message: String): ErrorMessage = {
    import ErrorField._
    new ErrorMessage()
      .setField(SPARK_VERSION, sparkVersion)
      .setField(MESSAGE, message)
      .validate()
  }
}

object StandaloneRestServer {
  def main(args: Array[String]): Unit = {
    println("Hey boy I'm starting a server.")
    new StandaloneRestServer(6677)
    readLine()
  }
}