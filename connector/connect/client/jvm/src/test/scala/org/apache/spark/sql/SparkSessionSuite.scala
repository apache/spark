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

class SparkSessionSuite
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

  test("SparkSession initialisation with connection string") {
    val ss = SparkSession
      .builder()
      .client(
        SparkConnectClient
          .builder()
          .connectionString(s"sc://localhost:$SERVER_PORT")
          .build())
      .build()
    val plan = proto.Plan.newBuilder().build()
    ss.analyze(plan, proto.Explain.ExplainMode.SIMPLE)
    assert(plan.equals(service.getAndClearLatestInputPlan()))
  }

  private def rangePlanCreator(
      start: Long,
      end: Long,
      step: Long,
      numPartitions: Option[Int]): proto.Plan = {
    val builder = proto.Relation.newBuilder()
    val rangeBuilder = builder.getRangeBuilder
      .setStart(start)
      .setEnd(end)
      .setStep(step)
    numPartitions.foreach(rangeBuilder.setNumPartitions)
    proto.Plan.newBuilder().setRoot(builder).build()
  }

  private def testRange(
      start: Long,
      end: Long,
      step: Long,
      numPartitions: Option[Int],
      failureHint: String): Unit = {
    val expectedPlan = rangePlanCreator(start, end, step, numPartitions)
    val actualPlan = service.getAndClearLatestInputPlan()
    assert(actualPlan.equals(expectedPlan), failureHint)
  }

  test("range query") {
    val ss = SparkSession
      .builder()
      .client(
        SparkConnectClient
          .builder()
          .connectionString(s"sc://localhost:$SERVER_PORT")
          .build())
      .build()

    ss.range(10).analyze
    testRange(0, 10, 1, None, "Case: range(10)")

    ss.range(0, 20).analyze
    testRange(0, 20, 1, None, "Case: range(0, 20)")

    ss.range(6, 20, 3).analyze
    testRange(6, 20, 3, None, "Case: range(6, 20, 3)")

    ss.range(10, 100, 5, 2).analyze
    testRange(10, 100, 5, Some(2), "Case: range(6, 20, 3, Some(2))")
  }

}
