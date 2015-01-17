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
import java.net.InetSocketAddress
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}

import scala.io.Source

import com.google.common.base.Charsets
import org.eclipse.jetty.server.{Request, Server}
import org.eclipse.jetty.server.handler.AbstractHandler

import org.apache.spark.{SPARK_VERSION => sparkVersion, Logging}
import org.apache.spark.deploy.master.Master
import org.apache.spark.util.{AkkaUtils, Utils}

/**
 * A server that responds to requests submitted by the StandaloneRestClient.
 */
private[spark] class StandaloneRestServer(master: Master, host: String, requestedPort: Int) {
  val server = new Server(new InetSocketAddress(host, requestedPort))
  server.setHandler(new StandaloneRestServerHandler(master))
  server.start()
}

/**
 * A Jetty handler that responds to requests submitted via the standalone REST protocol.
 */
private[spark] abstract class StandaloneRestHandler(master: Master)
  extends AbstractHandler with Logging {

  private implicit val askTimeout = AkkaUtils.askTimeout(master.conf)

  /** Handle a request to submit a driver. */
  protected def handleSubmit(request: SubmitDriverRequestMessage): SubmitDriverResponseMessage
  /** Handle a request to kill a driver. */
  protected def handleKill(request: KillDriverRequestMessage): KillDriverResponseMessage
  /** Handle a request for a driver's status. */
  protected def handleStatus(request: DriverStatusRequestMessage): DriverStatusResponseMessage

  /**
   * Handle a request submitted by the StandaloneRestClient.
   */
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

  /**
   * Construct the appropriate response message based on the type of the request message.
   * If an IllegalArgumentException is thrown in the process, construct an error message.
   */
  private def constructResponseMessage(
      request: StandaloneRestProtocolMessage): StandaloneRestProtocolMessage = {
    // If the request is sent via the StandaloneRestClient, it should have already been
    // validated remotely. In case this is not true, validate the request here to guard
    // against potential NPEs. If validation fails, return an ERROR message to the sender.
    try {
      request.validate()
      request match {
        case submit: SubmitDriverRequestMessage => handleSubmit(submit)
        case kill: KillDriverRequestMessage => handleKill(kill)
        case status: DriverStatusRequestMessage => handleStatus(status)
        case unexpected => handleError(
          s"Received message of unexpected type ${Utils.getFormattedClassName(unexpected)}.")
      }
    } catch {
      // Propagate exception to user in an ErrorMessage. If the construction of the
      // ErrorMessage itself throws an exception, log the exception and ignore the request.
      case e: IllegalArgumentException => handleError(e.getMessage)
    }
  }

  /** Construct an error message to signal the fact that an exception has been thrown. */
  private def handleError(message: String): ErrorMessage = {
    import ErrorField._
    new ErrorMessage()
      .setField(SPARK_VERSION, sparkVersion)
      .setField(MESSAGE, message)
      .validate()
  }
}

//object StandaloneRestServer {
//  def main(args: Array[String]): Unit = {
//    println("Hey boy I'm starting a server.")
//    new StandaloneRestServer(6677)
//    readLine()
//  }
//}
