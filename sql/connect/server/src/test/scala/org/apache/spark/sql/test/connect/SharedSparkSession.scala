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

package org.apache.spark.sql.test.connect

import java.util.UUID

import scala.concurrent.duration._

import org.scalatest.concurrent.Eventually

import org.apache.spark.DebugFilesystem
import org.apache.spark.sql.{classic => classicApi, connect => connectApi}
import org.apache.spark.sql.connect.client.SparkConnectClient
import org.apache.spark.sql.connect.common.config.ConnectCommon
import org.apache.spark.sql.connect.config.Connect
import org.apache.spark.sql.connect.service.SparkConnectService
import org.apache.spark.sql.test.{SharedSparkSession => BaseSharedSparkSession}

/**
 * A test trait that provides a Connect
 * [[connectApi.SparkSession SparkSession]] backed by an in-process gRPC server. Extends the
 * base [[BaseSharedSparkSession]] (which creates a classic
 * [[classicApi.SparkSession SparkSession]] and SparkContext), then layers a Connect client
 * session on top by starting the gRPC service in-process.
 *
 * Mix in this trait to exercise existing sql/core test suites through the Connect path:
 * {{{
 *   class FooWithConnectSuite extends FooSuite with connect.SharedSparkSession
 * }}}
 */
trait SharedSparkSession
  extends BaseSharedSparkSession
    with QueryTest {

  private val serverPort: Int =
    ConnectCommon.CONNECT_GRPC_BINDING_PORT + util.Random.nextInt(1000)

  @volatile private var _connectSpark: connectApi.SparkSession = _

  protected override def spark: connectApi.SparkSession = _connectSpark

  /** The underlying classic session used by the in-process server. */
  protected def classicSpark: classicApi.SparkSession =
    super.spark.asInstanceOf[classicApi.SparkSession]

  override def beforeAll(): Unit = {
    super.beforeAll()
    withSparkEnvConfs((Connect.CONNECT_GRPC_BINDING_PORT.key, serverPort.toString)) {
      SparkConnectService.start(classicSpark.sparkContext)
    }
    val client = SparkConnectClient
      .builder()
      .port(serverPort)
      .sessionId(UUID.randomUUID().toString)
      .userId("test")
      .build()
    _connectSpark = connectApi.SparkSession
      .builder()
      .client(client)
      .create()
  }

  override def afterAll(): Unit = {
    try {
      if (_connectSpark != null) {
        _connectSpark.close()
        _connectSpark = null
      }
      SparkConnectService.stop()
    } finally {
      super.afterAll()
    }
  }

  // The base SharedSparkSessionBase.afterEach calls spark.sharedState which is not supported
  // on Connect. Override to use the classic session for cleanup.
  protected override def afterEach(): Unit = {
    // super.afterEach() from BeforeAndAfterEach (skipping SharedSparkSessionBase)
    classicSpark.sharedState.cacheManager.clearCache()
    Eventually.eventually(Eventually.timeout(10.seconds), Eventually.interval(2.seconds)) {
      DebugFilesystem.assertNoOpenStreams()
    }
  }
}
