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
import java.net.{HttpURLConnection, URL}

import scala.io.Source

import com.google.common.base.Charsets

import org.apache.spark.Logging
import org.apache.spark.deploy.SparkSubmitArguments

/**
 * An abstract client that submits Spark applications using a stable REST protocol.
 * This client is intended to communicate with the SubmitRestServer.
 */
private[spark] abstract class SubmitRestClient extends Logging {

  /** Request that the REST server submits a driver specified by the provided arguments. */
  def submitDriver(args: SparkSubmitArguments): Unit = {
    validateSubmitArguments(args)
    val url = getHttpUrl(args.master)
    val request = constructSubmitRequest(args)
    logInfo(s"Submitting a request to launch a driver in ${args.master}.")
    sendHttp(url, request)
  }

  /** Request that the REST server kills the specified driver. */
  def killDriver(master: String, driverId: String): Unit = {
    validateMaster(master)
    val url = getHttpUrl(master)
    val request = constructKillRequest(master, driverId)
    logInfo(s"Submitting a request to kill driver $driverId in $master.")
    sendHttp(url, request)
  }

  /** Request the status of the specified driver from the REST server. */
  def requestDriverStatus(master: String, driverId: String): Unit = {
    validateMaster(master)
    val url = getHttpUrl(master)
    val request = constructStatusRequest(master, driverId)
    logInfo(s"Submitting a request for the status of driver $driverId in $master.")
    sendHttp(url, request)
  }

  /** Return the HTTP URL of the REST server that corresponds to the given master URL. */
  protected def getHttpUrl(master: String): URL

  // Construct the appropriate type of message based on the request type
  protected def constructSubmitRequest(args: SparkSubmitArguments): SubmitDriverRequestMessage
  protected def constructKillRequest(master: String, driverId: String): KillDriverRequestMessage
  protected def constructStatusRequest(master: String, driverId: String): DriverStatusRequestMessage

  // If the provided arguments are not as expected, throw an exception
  protected def validateMaster(master: String): Unit
  protected def validateDeployMode(deployMode: String): Unit
  protected def validateSubmitArguments(args: SparkSubmitArguments): Unit = {
    validateMaster(args.master)
    validateDeployMode(args.deployMode)
  }

  /**
   * Send the provided request in an HTTP message to the given URL.
   * Return the response received from the REST server.
   */
  private def sendHttp(url: URL, request: SubmitRestProtocolMessage): SubmitRestProtocolMessage = {
    val conn = url.openConnection().asInstanceOf[HttpURLConnection]
    conn.setRequestMethod("POST")
    conn.setRequestProperty("Content-Type", "application/json")
    conn.setRequestProperty("charset", "utf-8")
    conn.setDoOutput(true)
    val requestJson = request.toJson
    logDebug(s"Sending the following request to the REST server:\n$requestJson")
    val out = new DataOutputStream(conn.getOutputStream)
    out.write(requestJson.getBytes(Charsets.UTF_8))
    out.close()
    val responseJson = Source.fromInputStream(conn.getInputStream).mkString
    logDebug(s"Response from the REST server:\n$responseJson")
    SubmitRestProtocolMessage.fromJson(responseJson)
  }
}
