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
package org.apache.spark.sql.connect.planner

import scala.collection.mutable

import io.grpc.stub.StreamObserver
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{BigIntVector, Float8Vector}
import org.apache.arrow.vector.ipc.ArrowStreamReader

import org.apache.spark.SparkException
import org.apache.spark.connect.proto
import org.apache.spark.sql.connect.dsl.MockRemoteSession
import org.apache.spark.sql.connect.dsl.plans._
import org.apache.spark.sql.connect.service.SparkConnectService
import org.apache.spark.sql.execution.ExplainMode
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Testing Connect Service implementation.
 */
class SparkConnectServiceSuite extends SharedSparkSession {

  test("Test schema in analyze response") {
    withTable("test") {
      spark.sql("""
          | CREATE TABLE test (col1 INT, col2 STRING)
          | USING parquet
          |""".stripMargin)

      val instance = new SparkConnectService(false)
      val relation = proto.Relation
        .newBuilder()
        .setRead(
          proto.Read
            .newBuilder()
            .setNamedTable(proto.Read.NamedTable.newBuilder.setUnparsedIdentifier("test").build())
            .build())
        .build()

      val response =
        instance.handleAnalyzePlanRequest(relation, spark, ExplainMode.fromString("simple"))

      assert(response.getSchema.hasStruct)
      val schema = response.getSchema.getStruct
      assert(schema.getFieldsCount == 2)
      assert(
        schema.getFields(0).getName == "col1"
          && schema.getFields(0).getDataType.getKindCase == proto.DataType.KindCase.INTEGER)
      assert(
        schema.getFields(1).getName == "col2"
          && schema.getFields(1).getDataType.getKindCase == proto.DataType.KindCase.STRING)

      assert(!response.getIsLocal)
      assert(!response.getIsLocal)

      assert(response.getTreeString.contains("root"))
      assert(response.getTreeString.contains("|-- col1: integer (nullable = true)"))
      assert(response.getTreeString.contains("|-- col2: string (nullable = true)"))

      assert(response.getInputFilesCount === 0)
    }
  }

  test("SPARK-41224: collect data using arrow") {
    val instance = new SparkConnectService(false)
    val connect = new MockRemoteSession()
    val context = proto.UserContext
      .newBuilder()
      .setUserId("c1")
      .build()
    val plan = proto.Plan
      .newBuilder()
      .setRoot(connect.sql("select id, exp(id) as eid from range(0, 100, 1, 4)"))
      .build()
    val request = proto.ExecutePlanRequest
      .newBuilder()
      .setPlan(plan)
      .setUserContext(context)
      .build()

    // Execute plan.
    @volatile var done = false
    val responses = mutable.Buffer.empty[proto.ExecutePlanResponse]
    instance.executePlan(
      request,
      new StreamObserver[proto.ExecutePlanResponse] {
        override def onNext(v: proto.ExecutePlanResponse): Unit = responses += v

        override def onError(throwable: Throwable): Unit = throw throwable

        override def onCompleted(): Unit = done = true
      })

    // The current implementation is expected to be blocking. This is here to make sure it is.
    assert(done)

    // 4 Partitions + Metrics
    assert(responses.size == 5)

    // Make sure the last response is metrics only
    val last = responses.last
    assert(last.hasMetrics && !last.hasArrowBatch)

    val allocator = new RootAllocator()

    // Check the 'data' batches
    var expectedId = 0L
    var previousEId = 0.0d
    responses.dropRight(1).foreach { response =>
      assert(response.hasArrowBatch)
      val batch = response.getArrowBatch
      assert(batch.getData != null)
      assert(batch.getRowCount == 25)

      val reader = new ArrowStreamReader(batch.getData.newInput(), allocator)
      while (reader.loadNextBatch()) {
        val root = reader.getVectorSchemaRoot
        val idVector = root.getVector(0).asInstanceOf[BigIntVector]
        val eidVector = root.getVector(1).asInstanceOf[Float8Vector]
        val numRows = root.getRowCount
        var i = 0
        while (i < numRows) {
          assert(idVector.get(i) == expectedId)
          expectedId += 1
          val eid = eidVector.get(i)
          assert(eid > previousEId)
          previousEId = eid
          i += 1
        }
      }
      reader.close()
    }
    allocator.close()
  }

  test("SPARK-41165: failures in the arrow collect path should not cause hangs") {
    val instance = new SparkConnectService(false)

    // Add an always crashing UDF
    val session = SparkConnectService.getOrCreateIsolatedSession("c1").session
    val instaKill: Long => Long = { _ =>
      throw new Exception("Kaboom")
    }
    session.udf.register("insta_kill", instaKill)

    val connect = new MockRemoteSession()
    val context = proto.UserContext
      .newBuilder()
      .setUserId("c1")
      .build()
    val plan = proto.Plan
      .newBuilder()
      .setRoot(connect.sql("select insta_kill(id) from range(10)"))
      .build()
    val request = proto.ExecutePlanRequest
      .newBuilder()
      .setPlan(plan)
      .setUserContext(context)
      .build()

    // The observer is executed inside this thread. So
    // we can perform the checks inside the observer.
    instance.executePlan(
      request,
      new StreamObserver[proto.ExecutePlanResponse] {
        override def onNext(v: proto.ExecutePlanResponse): Unit = {
          fail("this should not receive responses")
        }

        override def onError(throwable: Throwable): Unit = {
          assert(throwable.isInstanceOf[SparkException])
        }

        override def onCompleted(): Unit = {
          fail("this should not complete")
        }
      })
  }

  test("Test explain mode in analyze response") {
    withTable("test") {
      spark.sql("""
          | CREATE TABLE test (col1 INT, col2 STRING)
          | USING parquet
          |""".stripMargin)
      val instance = new SparkConnectService(false)
      val relation = proto.Relation
        .newBuilder()
        .setProject(
          proto.Project
            .newBuilder()
            .addExpressions(
              proto.Expression
                .newBuilder()
                .setUnresolvedFunction(
                  proto.Expression.UnresolvedFunction
                    .newBuilder()
                    .setFunctionName("abs")
                    .addArguments(proto.Expression
                      .newBuilder()
                      .setLiteral(proto.Expression.Literal.newBuilder().setInteger(-1)))))
            .setInput(
              proto.Relation
                .newBuilder()
                .setRead(proto.Read
                  .newBuilder()
                  .setNamedTable(
                    proto.Read.NamedTable.newBuilder.setUnparsedIdentifier("test").build()))))
        .build()

      val response =
        instance
          .handleAnalyzePlanRequest(relation, spark, ExplainMode.fromString("extended"))
          .build()
      assert(response.getExplainString.contains("Parsed Logical Plan"))
      assert(response.getExplainString.contains("Analyzed Logical Plan"))
      assert(response.getExplainString.contains("Optimized Logical Plan"))
      assert(response.getExplainString.contains("Physical Plan"))
    }
  }
}
