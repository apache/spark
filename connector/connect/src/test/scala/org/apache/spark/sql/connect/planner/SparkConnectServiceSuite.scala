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

import scala.concurrent.Promise
import scala.concurrent.duration._

import io.grpc.stub.StreamObserver

import org.apache.spark.SparkException
import org.apache.spark.connect.proto
import org.apache.spark.sql.connect.dsl.MockRemoteSession
import org.apache.spark.sql.connect.dsl.plans._
import org.apache.spark.sql.connect.service.SparkConnectService
import org.apache.spark.sql.execution.ExplainMode
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.ThreadUtils

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
          && schema.getFields(0).getType.getKindCase == proto.DataType.KindCase.I32)
      assert(
        schema.getFields(1).getName == "col2"
          && schema.getFields(1).getType.getKindCase == proto.DataType.KindCase.STRING)
    }
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

    val promise = Promise[Seq[proto.ExecutePlanResponse]]
    instance.executePlan(
      request,
      new StreamObserver[proto.ExecutePlanResponse] {
        private val responses = Seq.newBuilder[proto.ExecutePlanResponse]

        override def onNext(v: proto.ExecutePlanResponse): Unit = responses += v

        override def onError(throwable: Throwable): Unit = promise.failure(throwable)

        override def onCompleted(): Unit = promise.success(responses.result())
      })
    intercept[SparkException] {
      ThreadUtils.awaitResult(promise.future, 2.seconds)
    }
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
                    .addParts("abs")
                    .addArguments(proto.Expression
                      .newBuilder()
                      .setLiteral(proto.Expression.Literal.newBuilder().setI32(-1)))))
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
