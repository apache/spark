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
package org.apache.spark.sql

import io.grpc.Server
import io.grpc.netty.NettyServerBuilder
import java.util.concurrent.TimeUnit
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite // scalastyle:ignore funsuite

import org.apache.spark.connect.proto
import org.apache.spark.sql.connect.client.{DummySparkConnectService, SparkConnectClient}

class DatasetSuite
    extends AnyFunSuite // scalastyle:ignore funsuite
    with BeforeAndAfterEach {

  private var server: Server = _
  private var service: DummySparkConnectService = _
  private val SERVER_PORT = 15250

  private def startDummyServer(port: Int): Unit = {
    service = new DummySparkConnectService()
    val sb = NettyServerBuilder
      .forPort(port)
      .addService(service)

    server = sb.build
    server.start()
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    startDummyServer(SERVER_PORT)
  }

  override def afterEach(): Unit = {
    if (server != null) {
      server.shutdownNow()
      assert(server.awaitTermination(5, TimeUnit.SECONDS), "server failed to shutdown")
    }
  }

  test("limit") {
    val ss = SparkSession
      .builder()
      .client(
        SparkConnectClient
          .builder()
          .connectionString(s"sc://localhost:$SERVER_PORT")
          .build())
      .build()
    val df = ss.newDataset(_ => ())
    val builder = proto.Relation.newBuilder()
    builder.getLimitBuilder.setInput(df.plan.getRoot).setLimit(10)

    val expectedPlan = proto.Plan.newBuilder().setRoot(builder).build()
    df.limit(10).analyze
    val actualPlan = service.getAndClearLatestInputPlan()
    assert(actualPlan.equals(expectedPlan))
  }
}
