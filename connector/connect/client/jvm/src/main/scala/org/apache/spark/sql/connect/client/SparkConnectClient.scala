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

package org.apache.spark.sql.connect.client

import scala.language.existentials

import io.grpc.{ManagedChannel, ManagedChannelBuilder}

import org.apache.spark.connect.proto
import org.apache.spark.sql.connect.common.config.ConnectCommon

class SparkConnectClient(
    private val userContext: proto.UserContext,
    private val channel: ManagedChannel) {

  private[this] val stub = proto.SparkConnectServiceGrpc.newBlockingStub(channel)

  /**
   * Placeholder method.
   * @return
   *   User ID.
   */
  def userId: String = userContext.getUserId()

  /**
   * Dispatch the [[proto.AnalyzePlanRequest]] to the Spark Connect server.
   * @return A [[proto.AnalyzePlanResponse]] from the Spark Connect server.
   */
  def analyze(request: proto.AnalyzePlanRequest): proto.AnalyzePlanResponse =
    stub.analyzePlan(request)

  /**
   * Shutdown the client's connection to the server.
   */
  def shutdown(): Unit = {
    channel.shutdownNow()
  }
}

object SparkConnectClient {
  def builder(): Builder = new Builder()

  class Builder() {
    private val userContextBuilder = proto.UserContext.newBuilder()
    private var _host: String = "localhost"
    private var _port: Int = ConnectCommon.CONNECT_GRPC_BINDING_PORT
    private var _connectionString: Option[String] = None

    def userId(id: String): Builder = {
      userContextBuilder.setUserId(id)
      this
    }

    def host(host: String): Builder = {
      require(host != null)
      _host = host
      this
    }

    def port(port: Int): Builder = {
      _port = port
      this
    }

    /**
     * Creates the channel with a target connection string, which can be either a valid
     * NameResolver-compliant URI, or an authority string. Note: The connection string, if used,
     * will override any host/port settings.
     */
    def connectionString(connectionString: String): Builder = {
      _connectionString = Some(connectionString)
      this
    }

    def build(): SparkConnectClient = {
      val channelBuilder = if (_connectionString.isDefined) {
        ManagedChannelBuilder.forTarget(_connectionString.get).usePlaintext()
      } else {
        ManagedChannelBuilder.forAddress(_host, _port).usePlaintext()
      }
      new SparkConnectClient(userContextBuilder.build(), channelBuilder.build())
    }
  }
}
