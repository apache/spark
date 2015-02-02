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

import java.io.{DataOutputStream, FileNotFoundException}
import java.net.{HttpURLConnection, URL}

import scala.io.Source

import com.google.common.base.Charsets

import org.apache.spark.{Logging, SparkException}
import org.apache.spark.deploy.SparkSubmitArguments

/**
 * An abstract client that submits applications using the REST protocol.
 * This client is intended to communicate with the [[SubmitRestServer]].
 */
private[spark] abstract class SubmitRestClient extends Logging {

  /** Request that the REST server submit a driver using the provided arguments. */
  def submitDriver(args: SparkSubmitArguments): SubmitRestProtocolResponse = {
    logInfo(s"Submitting a request to launch a driver in ${args.master}.")
    val url = getHttpUrl(args.master)
    val request = constructSubmitRequest(args)
    val response = sendHttp(url, request)
    handleResponse(response)
  }

  /** Request that the REST server kill the specified driver. */
  def killDriver(master: String, driverId: String): SubmitRestProtocolResponse = {
    logInfo(s"Submitting a request to kill driver $driverId in $master.")
    val url = getHttpUrl(master)
    val request = constructKillRequest(master, driverId)
    val response = sendHttp(url, request)
    handleResponse(response)
  }

  /** Request the status of the specified driver from the REST server. */
  def requestDriverStatus(master: String, driverId: String): SubmitRestProtocolResponse = {
    logInfo(s"Submitting a request for the status of driver $driverId in $master.")
    val url = getHttpUrl(master)
    val request = constructStatusRequest(master, driverId)
    val response = sendHttp(url, request)
    handleResponse(response)
  }

  /** Return the HTTP URL of the REST server that corresponds to the given master URL. */
  protected def getHttpUrl(master: String): URL

  // Construct the appropriate type of message based on the request type
  protected def constructSubmitRequest(args: SparkSubmitArguments): SubmitDriverRequest
  protected def constructKillRequest(master: String, driverId: String): KillDriverRequest
  protected def constructStatusRequest(master: String, driverId: String): DriverStatusRequest

  /**
   * Send the provided request in an HTTP message to the given URL.
   * This assumes that both the request and the response use the JSON format.
   * Return the response received from the REST server.
   */
  private def sendHttp(url: URL, request: SubmitRestProtocolRequest): SubmitRestProtocolResponse = {
    try {
      val conn = url.openConnection().asInstanceOf[HttpURLConnection]
      conn.setRequestMethod("POST")
      conn.setRequestProperty("Content-Type", "application/json")
      conn.setRequestProperty("charset", "utf-8")
      conn.setDoOutput(true)
      request.validate()
      val requestJson = request.toJson
      logDebug(s"Sending the following request to the REST server:\n$requestJson")
      val out = new DataOutputStream(conn.getOutputStream)
      out.write(requestJson.getBytes(Charsets.UTF_8))
      out.close()
      val responseJson = Source.fromInputStream(conn.getInputStream).mkString
      logDebug(s"Response from the REST server:\n$responseJson")
      SubmitRestProtocolMessage.fromJson(responseJson).asInstanceOf[SubmitRestProtocolResponse]
    } catch {
      case e: FileNotFoundException =>
        throw new SparkException(s"Unable to connect to REST server $url", e)
    }
  }

  /** Validate the response and log any error messages provided by the server. */
  private def handleResponse(response: SubmitRestProtocolResponse): SubmitRestProtocolResponse = {
    try {
      response.validate()
      response match {
        case e: ErrorResponse => logError(s"Server responded with error:\n${e.message}")
        case _ =>
      }
    } catch {
      case e: SubmitRestProtocolException =>
        throw new SubmitRestProtocolException("Malformed response received from server", e)
    }
    response
  }
}
