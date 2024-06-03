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

import java.util.Properties
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

import io.grpc.Server
import io.grpc.inprocess.{InProcessChannelBuilder, InProcessServerBuilder}
import org.scalatest.BeforeAndAfterEach

import org.apache.spark.connect.proto
import org.apache.spark.sql.connect.client.{DummySparkConnectService, SparkConnectClient}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.ConnectFunSuite
import org.apache.spark.util.SparkSerDeUtils

// Add sample tests.
// - sample fraction: simple.sample(0.1)
// - sample withReplacement_fraction: simple.sample(withReplacement = true, 0.11)
// Add tests for exceptions thrown
class ClientDatasetSuite extends ConnectFunSuite with BeforeAndAfterEach {

  private var server: Server = _
  private var service: DummySparkConnectService = _
  private var ss: SparkSession = _

  private def newSparkSession(): SparkSession = {
    val client = SparkConnectClient(
      InProcessChannelBuilder.forName(getClass.getName).directExecutor().build())
    new SparkSession(client, planIdGenerator = new AtomicLong)
  }

  private def startDummyServer(): Unit = {
    service = new DummySparkConnectService()
    server = InProcessServerBuilder
      .forName(getClass.getName)
      .addService(service)
      .build()
    server.start()
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    startDummyServer()
    ss = newSparkSession()
  }

  override def afterEach(): Unit = {
    if (server != null) {
      server.shutdownNow()
      assert(server.awaitTermination(5, TimeUnit.SECONDS), "server failed to shutdown")
    }
  }

  test("write") {
    val df = ss.newDataFrame(_ => ()).limit(10)

    val builder = proto.WriteOperation.newBuilder()
    builder
      .setInput(df.plan.getRoot)
      .setPath("my/test/path")
      .setMode(proto.WriteOperation.SaveMode.SAVE_MODE_ERROR_IF_EXISTS)
      .setSource("parquet")
      .addSortColumnNames("col1")
      .addPartitioningColumns("col99")
      .setBucketBy(
        proto.WriteOperation.BucketBy
          .newBuilder()
          .setNumBuckets(2)
          .addBucketColumnNames("col1")
          .addBucketColumnNames("col2"))

    val expectedPlan = proto.Plan
      .newBuilder()
      .setCommand(proto.Command.newBuilder().setWriteOperation(builder))
      .build()

    df.write
      .sortBy("col1")
      .partitionBy("col99")
      .bucketBy(2, "col1", "col2")
      .parquet("my/test/path")
    val actualPlan = service.getAndClearLatestInputPlan()
    assert(actualPlan.equals(expectedPlan))
  }

  test("write jdbc") {
    val df = ss.newDataFrame(_ => ()).limit(10)

    val builder = proto.WriteOperation.newBuilder()
    builder
      .setInput(df.plan.getRoot)
      .setMode(proto.WriteOperation.SaveMode.SAVE_MODE_ERROR_IF_EXISTS)
      .setSource("jdbc")
      .putOptions("a", "b")
      .putOptions("1", "2")
      .putOptions("url", "url")
      .putOptions("dbtable", "table")

    val expectedPlan = proto.Plan
      .newBuilder()
      .setCommand(proto.Command.newBuilder().setWriteOperation(builder))
      .build()

    val connectionProperties = new Properties
    connectionProperties.put("a", "b")
    connectionProperties.put("1", "2")
    df.write.jdbc("url", "table", connectionProperties)

    val actualPlan = service.getAndClearLatestInputPlan()
    assert(actualPlan.equals(expectedPlan))
  }

  test("write V2") {
    val df = ss.newDataFrame(_ => ()).limit(10)

    val builder = proto.WriteOperationV2.newBuilder()
    builder
      .setInput(df.plan.getRoot)
      .setTableName("t1")
      .addPartitioningColumns(col("col99").expr)
      .setProvider("json")
      .putTableProperties("key", "value")
      .putOptions("key2", "value2")
      .setMode(proto.WriteOperationV2.Mode.MODE_CREATE_OR_REPLACE)

    val expectedPlan = proto.Plan
      .newBuilder()
      .setCommand(proto.Command.newBuilder().setWriteOperationV2(builder))
      .build()

    df.writeTo("t1")
      .partitionedBy(col("col99"))
      .using("json")
      .tableProperty("key", "value")
      .options(Map("key2" -> "value2"))
      .createOrReplace()
    val actualPlan = service.getAndClearLatestInputPlan()
    assert(actualPlan.equals(expectedPlan))
  }

  test("Pivot") {
    val df = ss.newDataFrame(_ => ())
    intercept[IllegalArgumentException] {
      df.groupBy().pivot(Column("c"), Seq(Column("col")))
    }
  }

  test("serialize as null") {
    val session = newSparkSession()
    val ds = session.range(10)
    val bytes = SparkSerDeUtils.serialize(ds)
    assert(SparkSerDeUtils.deserialize[Dataset[Long]](bytes) == null)
  }
}
