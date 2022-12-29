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

class SparkConnectClient(
  private val userContext: proto.UserContext,
  private val channel: ManagedChannel) {

  private[this] val stub = proto.SparkConnectServiceGrpc.newBlockingStub(channel)
  /**
   * Placeholder method.
   * @return User ID.
   */
  def userId: String = userContext.getUserId()

  def analyze(request: proto.AnalyzePlanRequest): proto.AnalyzePlanResponse =
    stub.analyzePlan(request)
}

object SparkConnectClient {
  def builder(): Builder = new Builder()

  class Builder() {
    private val userContextBuilder = proto.UserContext.newBuilder()
    private var _host: String = "localhost"
    // TODO: pull out config from server
    private var _port: Int = 15002

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

    def build(): SparkConnectClient = {
      // TODO: connection string
      val channelBuilder = ManagedChannelBuilder.forAddress(_host, _port).usePlaintext()
      new SparkConnectClient(userContextBuilder.build(), channelBuilder.build())
    }
  }
}
