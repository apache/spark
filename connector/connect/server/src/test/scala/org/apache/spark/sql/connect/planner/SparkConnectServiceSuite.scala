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

import scala.collection.JavaConverters._
import scala.collection.mutable

import io.grpc.StatusRuntimeException
import io.grpc.stub.StreamObserver
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{BigIntVector, Float8Vector}
import org.apache.arrow.vector.ipc.ArrowStreamReader

import org.apache.spark.connect.proto
import org.apache.spark.sql.connect.dsl.MockRemoteSession
import org.apache.spark.sql.connect.dsl.expressions._
import org.apache.spark.sql.connect.dsl.plans._
import org.apache.spark.sql.connect.service.{SparkConnectAnalyzeHandler, SparkConnectService}
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

      val plan = proto.Plan
        .newBuilder()
        .setRoot(
          proto.Relation
            .newBuilder()
            .setRead(
              proto.Read
                .newBuilder()
                .setNamedTable(
                  proto.Read.NamedTable.newBuilder.setUnparsedIdentifier("test").build())
                .build())
            .build())
        .build()

      val handler = new SparkConnectAnalyzeHandler(null)

      val request1 = proto.AnalyzePlanRequest
        .newBuilder()
        .setSchema(proto.AnalyzePlanRequest.Schema.newBuilder().setPlan(plan).build())
        .build()
      val response1 = handler.process(request1, spark)
      assert(response1.hasSchema)
      assert(response1.getSchema.getSchema.hasStruct)
      val schema = response1.getSchema.getSchema.getStruct
      assert(schema.getFieldsCount == 2)
      assert(
        schema.getFields(0).getName == "col1"
          && schema.getFields(0).getDataType.getKindCase == proto.DataType.KindCase.INTEGER)
      assert(
        schema.getFields(1).getName == "col2"
          && schema.getFields(1).getDataType.getKindCase == proto.DataType.KindCase.STRING)

      val request2 = proto.AnalyzePlanRequest
        .newBuilder()
        .setExplain(
          proto.AnalyzePlanRequest.Explain
            .newBuilder()
            .setPlan(plan)
            .setExplainMode(proto.AnalyzePlanRequest.Explain.ExplainMode.EXPLAIN_MODE_SIMPLE)
            .build())
        .build()
      val response2 = handler.process(request2, spark)
      assert(response2.hasExplain)
      assert(response2.getExplain.getExplainString.size > 0)

      val request3 = proto.AnalyzePlanRequest
        .newBuilder()
        .setIsLocal(proto.AnalyzePlanRequest.IsLocal.newBuilder().setPlan(plan).build())
        .build()
      val response3 = handler.process(request3, spark)
      assert(response3.hasIsLocal)
      assert(!response3.getIsLocal.getIsLocal)

      val request4 = proto.AnalyzePlanRequest
        .newBuilder()
        .setIsStreaming(proto.AnalyzePlanRequest.IsStreaming.newBuilder().setPlan(plan).build())
        .build()
      val response4 = handler.process(request4, spark)
      assert(response4.hasIsStreaming)
      assert(!response4.getIsStreaming.getIsStreaming)

      val request5 = proto.AnalyzePlanRequest
        .newBuilder()
        .setTreeString(proto.AnalyzePlanRequest.TreeString.newBuilder().setPlan(plan).build())
        .build()
      val response5 = handler.process(request5, spark)
      assert(response5.hasTreeString)
      val treeString = response5.getTreeString.getTreeString
      assert(treeString.contains("root"))
      assert(treeString.contains("|-- col1: integer (nullable = true)"))
      assert(treeString.contains("|-- col2: string (nullable = true)"))

      val request6 = proto.AnalyzePlanRequest
        .newBuilder()
        .setInputFiles(proto.AnalyzePlanRequest.InputFiles.newBuilder().setPlan(plan).build())
        .build()
      val response6 = handler.process(request6, spark)
      assert(response6.hasInputFiles)
      assert(response6.getInputFiles.getFilesCount === 0)
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
    assert(responses.size == 6)

    // Make sure the first response is schema only
    val head = responses.head
    assert(head.hasSchema && !head.hasArrowBatch && !head.hasMetrics)

    // Make sure the last response is metrics only
    val last = responses.last
    assert(last.hasMetrics && !last.hasSchema && !last.hasArrowBatch)

    val allocator = new RootAllocator()

    // Check the 'data' batches
    var expectedId = 0L
    var previousEId = 0.0d
    responses.tail.dropRight(1).foreach { response =>
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
    val session = SparkConnectService.getOrCreateIsolatedSession("c1", "session").session
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
      .setSessionId("session")
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
          assert(throwable.isInstanceOf[StatusRuntimeException])
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

      val plan = proto.Plan.newBuilder().setRoot(relation).build()

      val handler = new SparkConnectAnalyzeHandler(null)

      val request = proto.AnalyzePlanRequest
        .newBuilder()
        .setExplain(
          proto.AnalyzePlanRequest.Explain
            .newBuilder()
            .setPlan(plan)
            .setExplainMode(proto.AnalyzePlanRequest.Explain.ExplainMode.EXPLAIN_MODE_EXTENDED)
            .build())
        .build()

      val response = handler.process(request, spark)

      assert(response.getExplain.getExplainString.contains("Parsed Logical Plan"))
      assert(response.getExplain.getExplainString.contains("Analyzed Logical Plan"))
      assert(response.getExplain.getExplainString.contains("Optimized Logical Plan"))
      assert(response.getExplain.getExplainString.contains("Physical Plan"))
    }
  }

  test("Test observe response") {
    withTable("test") {
      spark.sql("""
                  | CREATE TABLE test (col1 INT, col2 STRING)
                  | USING parquet
                  |""".stripMargin)

      val instance = new SparkConnectService(false)

      val connect = new MockRemoteSession()
      val context = proto.UserContext
        .newBuilder()
        .setUserId("c1")
        .build()
      val collectMetrics = proto.Relation
        .newBuilder()
        .setCollectMetrics(
          proto.CollectMetrics
            .newBuilder()
            .setInput(connect.sql("select id, exp(id) as eid from range(0, 100, 1, 4)"))
            .setName("my_metric")
            .addAllMetrics(Seq(
              proto_min("id".protoAttr).as("min_val"),
              proto_max("id".protoAttr).as("max_val")).asJava))
        .build()
      val plan = proto.Plan
        .newBuilder()
        .setRoot(collectMetrics)
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

      assert(responses.size == 7)

      // Make sure the first response is schema only
      val head = responses.head
      assert(head.hasSchema && !head.hasArrowBatch && !head.hasMetrics)

      // Make sure the last response is observed metrics only
      val last = responses.last
      assert(last.getObservedMetricsCount == 1 && !last.hasSchema && !last.hasArrowBatch)

      val observedMetricsList = last.getObservedMetricsList.asScala
      val observedMetric = observedMetricsList.head
      assert(observedMetric.getName == "my_metric")
      assert(observedMetric.getValuesCount == 2)
      val valuesList = observedMetric.getValuesList.asScala
      assert(valuesList.head.hasLong && valuesList.head.getLong == 0)
      assert(valuesList.last.hasLong && valuesList.last.getLong == 99)
    }
  }
}
