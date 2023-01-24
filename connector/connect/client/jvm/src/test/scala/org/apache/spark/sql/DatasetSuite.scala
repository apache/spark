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

import scala.collection.JavaConverters._

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
  private var ss: SparkSession = _

  private def getNewSparkSession(port: Int): SparkSession = {
    assert(port != 0)
    SparkSession
      .builder()
      .client(
        SparkConnectClient
          .builder()
          .connectionString(s"sc://localhost:$port")
          .build())
      .build()
  }

  private def startDummyServer(): Unit = {
    service = new DummySparkConnectService()
    val sb = NettyServerBuilder
      // Let server bind to any free port
      .forPort(0)
      .addService(service)

    server = sb.build
    server.start()
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    startDummyServer()
    ss = getNewSparkSession(server.getPort)
  }

  override def afterEach(): Unit = {
    if (server != null) {
      server.shutdownNow()
      assert(server.awaitTermination(5, TimeUnit.SECONDS), "server failed to shutdown")
    }
  }

  test("limit") {
    val df = ss.newDataset(_ => ())
    val builder = proto.Relation.newBuilder()
    builder.getLimitBuilder.setInput(df.plan.getRoot).setLimit(10)

    val expectedPlan = proto.Plan.newBuilder().setRoot(builder).build()
    df.limit(10).analyze
    val actualPlan = service.getAndClearLatestInputPlan()
    assert(actualPlan.equals(expectedPlan))
  }

  test("select") {
    val df = ss.newDataset(_ => ())

    val builder = proto.Relation.newBuilder()
    val dummyCols = Seq[Column](Column("a"), Column("b"))
    builder.getProjectBuilder
      .setInput(df.plan.getRoot)
      .addAllExpressions(dummyCols.map(_.expr).asJava)
    val expectedPlan = proto.Plan.newBuilder().setRoot(builder).build()

    df.select(dummyCols: _*).analyze
    val actualPlan = service.getAndClearLatestInputPlan()
    assert(actualPlan.equals(expectedPlan))
  }

  test("filter") {
    val df = ss.newDataset(_ => ())

    val builder = proto.Relation.newBuilder()
    val dummyCondition = Column.fn("dummy func", Column("a"))
    builder.getFilterBuilder
      .setInput(df.plan.getRoot)
      .setCondition(dummyCondition.expr)
    val expectedPlan = proto.Plan.newBuilder().setRoot(builder).build()

    df.filter(dummyCondition).analyze
    val actualPlan = service.getAndClearLatestInputPlan()
    assert(actualPlan.equals(expectedPlan))
  }
}
