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
import org.eclipse.jetty.util.thread.QueuedThreadPool

import org.apache.spark.{SPARK_VERSION => sparkVersion, Logging, SparkConf}
import org.apache.spark.util.Utils

/**
 * An abstract server that responds to requests submitted by the
 * [[SubmitRestClient]] in the REST application submission protocol.
 */
private[spark] abstract class SubmitRestServer(host: String, requestedPort: Int, conf: SparkConf)
  extends Logging {

  protected val handler: SubmitRestServerHandler
  private var _server: Option[Server] = None

  /** Start the server and return the bound port. */
  def start(): Int = {
    val (server, boundPort) = Utils.startServiceOnPort[Server](requestedPort, doStart, conf)
    _server = Some(server)
    logInfo(s"Started REST server for submitting applications on port $boundPort")
    boundPort
  }

  def stop(): Unit = {
    _server.foreach(_.stop())
  }

  private def doStart(startPort: Int): (Server, Int) = {
    val server = new Server(new InetSocketAddress(host, requestedPort))
    val threadPool = new QueuedThreadPool
    threadPool.setDaemon(true)
    server.setThreadPool(threadPool)
    server.setHandler(handler)
    server.start()
    val boundPort = server.getConnectors()(0).getLocalPort
    (server, boundPort)
  }
}

/**
 * An abstract handler for requests submitted via the REST application submission protocol.
 * This represents the main handler used in the [[SubmitRestServer]].
 */
private[spark] abstract class SubmitRestServerHandler extends AbstractHandler with Logging {
  protected def handleSubmit(request: SubmitDriverRequest): SubmitDriverResponse
  protected def handleKill(request: KillDriverRequest): KillDriverResponse
  protected def handleStatus(request: DriverStatusRequest): DriverStatusResponse

  /**
   * Handle a request submitted by the [[SubmitRestClient]].
   * This assumes that both the request and the response use the JSON format.
   */
  override def handle(
      target: String,
      baseRequest: Request,
      request: HttpServletRequest,
      response: HttpServletResponse): Unit = {
    try {
      val requestMessageJson = Source.fromInputStream(request.getInputStream).mkString
      val requestMessage = SubmitRestProtocolMessage.fromJson(requestMessageJson)
        .asInstanceOf[SubmitRestProtocolRequest]
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
   * If an [[IllegalArgumentException]] is thrown, construct an error message instead.
   */
  private def constructResponseMessage(
      request: SubmitRestProtocolRequest): SubmitRestProtocolResponse = {
    // Validate the request message to ensure that it is correctly constructed. If the request
    // is sent via the SubmitRestClient, it should have already been validated remotely. In case
    // this is not true, do it again here to guard against potential NPEs. If validation fails,
    // send an error message back to the sender.
    val response =
      try {
        request.validate()
        request match {
          case submit: SubmitDriverRequest => handleSubmit(submit)
          case kill: KillDriverRequest => handleKill(kill)
          case status: DriverStatusRequest => handleStatus(status)
          case unexpected => handleError(
            s"Received message of unexpected type ${Utils.getFormattedClassName(unexpected)}.")
        }
      } catch {
        case e: IllegalArgumentException => handleError(formatException(e))
      }
    // Validate the response message to ensure that it is correctly constructed. If it is not,
    // propagate the exception back to the client and signal that it is a server error.
    try {
      response.validate()
    } catch {
      case e: IllegalArgumentException =>
        handleError("Internal server error: " + formatException(e))
    }
    response
  }

  /** Construct an error message to signal the fact that an exception has been thrown. */
  private def handleError(message: String): ErrorResponse = {
    new ErrorResponse()
      .setSparkVersion(sparkVersion)
      .setMessage(message)
  }

  /** Return a human readable String representation of the exception. */
  protected def formatException(e: Exception): String = {
    val stackTraceString = e.getStackTrace.map { "\t" + _ }.mkString("\n")
    s"$e\n$stackTraceString"
  }
}
