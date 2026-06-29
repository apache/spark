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

package org.apache.spark.sql.connect

import java.util.UUID

import org.apache.spark.{SparkEnv, SparkFunSuite}
import org.apache.spark.sql
import org.apache.spark.sql.classic
import org.apache.spark.sql.connect.client.SparkConnectClient
import org.apache.spark.sql.connect.config.Connect
import org.apache.spark.sql.connect.service.SparkConnectService

/**
 * Provides a [[SparkSession connect.SparkSession]] backed by an in-process gRPC server.
 * Extends [[sql.SparkSessionBinder sql.SparkSessionBinder]] (which creates a
 * [[classic.SparkSession classic.SparkSession]] and SparkContext), then layers a Connect client
 * session on top by starting the gRPC service in-process.
 */
trait SparkSessionBinder extends sql.SparkSessionBinder { self: SparkFunSuite =>

  private var _connectSpark: SparkSession = _

  protected override def spark: SparkSession = _connectSpark

  /** The underlying classic session used by the in-process server. */
  private def classicSpark: classic.SparkSession = super.spark.asInstanceOf[classic.SparkSession]

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    // Other suites using mocks leave a mess in the global executionManager,
    // shut it down so that it's cleared before starting server.
    SparkConnectService.executionManager.shutdown()
    val prevPort = SparkEnv.get.conf.get(Connect.CONNECT_GRPC_BINDING_PORT)
    try {
      // set GRPC_BINDING_PORT to 0 so that the server picks a random, freely available port.
      SparkEnv.get.conf.set(Connect.CONNECT_GRPC_BINDING_PORT, 0)
      SparkConnectService.start(classicSpark.sparkContext)
    } finally {
      SparkEnv.get.conf.set(Connect.CONNECT_GRPC_BINDING_PORT, prevPort)
    }
    val client = SparkConnectClient
      .builder()
      .port(SparkConnectService.localPort)
      .sessionId(UUID.randomUUID().toString)
      .userId("test")
      .build()
    _connectSpark = SparkSession
      .builder()
      .client(client)
      .create()
  }

  override def afterAll(): Unit = {
    if (_connectSpark != null) {
      _connectSpark.close()
      _connectSpark = null
    }
    SparkConnectService.stop()
    super.afterAll()
  }
}
