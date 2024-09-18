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

import java.util.UUID
import java.util.concurrent.Semaphore

import scala.collection.mutable
import scala.jdk.CollectionConverters._

import com.google.protobuf
import com.google.protobuf.ByteString
import io.grpc.StatusRuntimeException
import io.grpc.stub.StreamObserver
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{BigIntVector, Float8Vector}
import org.apache.arrow.vector.ipc.ArrowStreamReader
import org.mockito.Mockito.when
import org.scalatest.Tag
import org.scalatest.concurrent.Eventually
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark.{SparkContext, SparkEnv}
import org.apache.spark.connect.proto
import org.apache.spark.connect.proto.CreateDataFrameViewCommand
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent}
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, UnsafeProjection}
import org.apache.spark.sql.connect.SparkConnectTestUtils
import org.apache.spark.sql.connect.common.DataTypeProtoConverter
import org.apache.spark.sql.connect.config.Connect
import org.apache.spark.sql.connect.dsl.MockRemoteSession
import org.apache.spark.sql.connect.dsl.expressions._
import org.apache.spark.sql.connect.dsl.plans._
import org.apache.spark.sql.connect.plugin.SparkConnectPluginRegistry
import org.apache.spark.sql.connect.service.{ExecuteHolder, ExecuteStatus, SessionStatus, SparkConnectAnalyzeHandler, SparkConnectService, SparkListenerConnectOperationStarted}
import org.apache.spark.sql.connector.catalog.InMemoryPartitionTableCatalog
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.Utils

/**
 * Testing Connect Service implementation.
 */
class SparkConnectServiceSuite
    extends SharedSparkSession
    with MockitoSugar
    with Logging
    with SparkConnectPlanTest {

  private def sparkSessionHolder = SparkConnectTestUtils.createDummySessionHolder(spark)
  private def DEFAULT_UUID = UUID.fromString("89ea6117-1f45-4c03-ae27-f47c6aded093")

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
      val response1 = handler.process(request1, sparkSessionHolder)
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
      val response2 = handler.process(request2, sparkSessionHolder)
      assert(response2.hasExplain)
      assert(response2.getExplain.getExplainString.length > 0)

      val request3 = proto.AnalyzePlanRequest
        .newBuilder()
        .setIsLocal(proto.AnalyzePlanRequest.IsLocal.newBuilder().setPlan(plan).build())
        .build()
      val response3 = handler.process(request3, sparkSessionHolder)
      assert(response3.hasIsLocal)
      assert(!response3.getIsLocal.getIsLocal)

      val request4 = proto.AnalyzePlanRequest
        .newBuilder()
        .setIsStreaming(proto.AnalyzePlanRequest.IsStreaming.newBuilder().setPlan(plan).build())
        .build()
      val response4 = handler.process(request4, sparkSessionHolder)
      assert(response4.hasIsStreaming)
      assert(!response4.getIsStreaming.getIsStreaming)

      val request5 = proto.AnalyzePlanRequest
        .newBuilder()
        .setTreeString(proto.AnalyzePlanRequest.TreeString.newBuilder().setPlan(plan).build())
        .build()
      val response5 = handler.process(request5, sparkSessionHolder)
      assert(response5.hasTreeString)
      val treeString = response5.getTreeString.getTreeString
      assert(treeString.contains("root"))
      assert(treeString.contains("|-- col1: integer (nullable = true)"))
      assert(treeString.contains("|-- col2: string (nullable = true)"))

      val request6 = proto.AnalyzePlanRequest
        .newBuilder()
        .setInputFiles(proto.AnalyzePlanRequest.InputFiles.newBuilder().setPlan(plan).build())
        .build()
      val response6 = handler.process(request6, sparkSessionHolder)
      assert(response6.hasInputFiles)
      assert(response6.getInputFiles.getFilesCount === 0)
    }
  }

  test("SPARK-41224: collect data using arrow") {
    withEvents { verifyEvents =>
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
        .setSessionId(UUID.randomUUID.toString())
        .build()

      // Execute plan.
      @volatile var done = false
      val responses = mutable.Buffer.empty[proto.ExecutePlanResponse]
      instance.executePlan(
        request,
        new StreamObserver[proto.ExecutePlanResponse] {
          override def onNext(v: proto.ExecutePlanResponse): Unit = {
            responses += v
            verifyEvents.onNext(v)
          }

          override def onError(throwable: Throwable): Unit = {
            verifyEvents.onError(throwable)
            throw throwable
          }

          override def onCompleted(): Unit = {
            done = true
          }
        })
      verifyEvents.onCompleted(Some(100))
      // The current implementation is expected to be blocking. This is here to make sure it is.
      assert(done)

      // 4 Partitions + Metrics + optional progress messages
      val filteredResponses = responses.filter(!_.hasExecutionProgress)
      assert(filteredResponses.size == 6)

      // Make sure the first response is schema only
      val head = filteredResponses.head
      assert(head.hasSchema && !head.hasArrowBatch && !head.hasMetrics)

      // Make sure the last response is metrics only
      val last = filteredResponses.last
      assert(last.hasMetrics && !last.hasSchema && !last.hasArrowBatch)

      val allocator = new RootAllocator()

      // Check the 'data' batches
      var expectedId = 0L
      var previousEId = 0.0d
      filteredResponses.tail.dropRight(1).foreach { response =>
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
  }

  test("SPARK-44776: LocalTableScanExec") {
    withEvents { verifyEvents =>
      val instance = new SparkConnectService(false)
      val connect = new MockRemoteSession()
      val context = proto.UserContext
        .newBuilder()
        .setUserId("c1")
        .build()

      val rows = (0L to 5L).map { i =>
        new GenericInternalRow(Array(i, UTF8String.fromString("" + (i - 1 + 'a').toChar)))
      }

      val schema = StructType(Array(StructField("id", LongType), StructField("data", StringType)))
      val inputRows = rows.map { row =>
        val proj = UnsafeProjection.create(schema)
        proj(row).copy()
      }

      val localRelation = createLocalRelationProto(schema, inputRows)
      val plan = proto.Plan
        .newBuilder()
        .setRoot(localRelation)
        .build()

      val request = proto.ExecutePlanRequest
        .newBuilder()
        .setPlan(plan)
        .setUserContext(context)
        .setSessionId(UUID.randomUUID.toString())
        .build()

      // Execute plan.
      @volatile var done = false
      val responses = mutable.Buffer.empty[proto.ExecutePlanResponse]
      instance.executePlan(
        request,
        new StreamObserver[proto.ExecutePlanResponse] {
          override def onNext(v: proto.ExecutePlanResponse): Unit = {
            responses += v
            verifyEvents.onNext(v)
          }

          override def onError(throwable: Throwable): Unit = {
            verifyEvents.onError(throwable)
            throw throwable
          }

          override def onCompleted(): Unit = {
            done = true
          }
        })
      verifyEvents.onCompleted(Some(6))
      // The current implementation is expected to be blocking. This is here to make sure it is.
      assert(done)

      // 1 Partitions + Metrics
      val filteredResponses = responses.filter(!_.hasExecutionProgress)
      assert(filteredResponses.size == 3)

      // Make sure the first response is schema only
      val head = filteredResponses.head
      assert(head.hasSchema && !head.hasArrowBatch && !head.hasMetrics)

      // Make sure the last response is metrics only
      val last = filteredResponses.last
      assert(last.hasMetrics && !last.hasSchema && !last.hasArrowBatch)
    }
  }

  test("SPARK-44657: Arrow batches respect max batch size limit") {
    // Set 10 KiB as the batch size limit
    val batchSize = 10 * 1024
    withSparkConf("spark.connect.grpc.arrow.maxBatchSize" -> batchSize.toString) {
      val instance = new SparkConnectService(false)
      val connect = new MockRemoteSession()
      val context = proto.UserContext
        .newBuilder()
        .setUserId("c1")
        .build()
      val plan = proto.Plan
        .newBuilder()
        .setRoot(connect.sql("select * from range(0, 15000, 1, 1)"))
        .build()
      val request = proto.ExecutePlanRequest
        .newBuilder()
        .setPlan(plan)
        .setUserContext(context)
        .setSessionId(UUID.randomUUID.toString())
        .build()

      // Execute plan.
      @volatile var done = false
      val responses = mutable.Buffer.empty[proto.ExecutePlanResponse]
      instance.executePlan(
        request,
        new StreamObserver[proto.ExecutePlanResponse] {
          override def onNext(v: proto.ExecutePlanResponse): Unit = {
            responses += v
          }

          override def onError(throwable: Throwable): Unit = {
            throw throwable
          }

          override def onCompleted(): Unit = {
            done = true
          }
        })
      // The current implementation is expected to be blocking. This is here to make sure it is.
      assert(done)

      // 1 schema + 1 metric + at least 2 data batches
      val filteredResponses = responses.filter(!_.hasExecutionProgress)
      assert(filteredResponses.size > 3)

      val allocator = new RootAllocator()

      // Check the 'data' batches
      filteredResponses.tail.dropRight(1).foreach { response =>
        assert(response.hasArrowBatch)
        val batch = response.getArrowBatch
        assert(batch.getData != null)
        // Batch size must be <= 70% since we intentionally use this multiplier for the size
        // estimator.
        assert(batch.getData.size() <= batchSize * 0.7)
      }
    }
  }

  gridTest("SPARK-43923: commands send events")(
    Seq(
      (
        proto.Command
          .newBuilder()
          .setSqlCommand(proto.SqlCommand.newBuilder().setSql("select 1").build()),
        Some(0L)),
      (
        proto.Command
          .newBuilder()
          .setSqlCommand(proto.SqlCommand.newBuilder().setSql("show databases").build()),
        Some(1L)),
      (
        proto.Command
          .newBuilder()
          .setWriteOperation(
            proto.WriteOperation
              .newBuilder()
              .setInput(
                proto.Relation.newBuilder().setSql(proto.SQL.newBuilder().setQuery("select 1")))
              .setPath(Utils.createTempDir().getAbsolutePath)
              .setMode(proto.WriteOperation.SaveMode.SAVE_MODE_OVERWRITE)),
        None),
      (
        proto.Command
          .newBuilder()
          .setWriteOperationV2(
            proto.WriteOperationV2
              .newBuilder()
              .setInput(proto.Relation.newBuilder.setRange(
                proto.Range.newBuilder().setStart(0).setEnd(2).setStep(1L)))
              .setTableName("testcat.testtable")
              .setMode(proto.WriteOperationV2.Mode.MODE_CREATE)),
        None),
      (
        proto.Command
          .newBuilder()
          .setCreateDataframeView(
            CreateDataFrameViewCommand
              .newBuilder()
              .setName("testview")
              .setInput(
                proto.Relation.newBuilder().setSql(proto.SQL.newBuilder().setQuery("select 1")))),
        None),
      (
        proto.Command
          .newBuilder()
          .setGetResourcesCommand(proto.GetResourcesCommand.newBuilder()),
        None),
      (
        proto.Command
          .newBuilder()
          .setExtension(
            protobuf.Any.pack(
              proto.ExamplePluginCommand
                .newBuilder()
                .setCustomField("SPARK-43923")
                .build())),
        None),
      (
        proto.Command
          .newBuilder()
          .setWriteStreamOperationStart(
            proto.WriteStreamOperationStart
              .newBuilder()
              .setInput(
                proto.Relation
                  .newBuilder()
                  .setRead(proto.Read
                    .newBuilder()
                    .setIsStreaming(true)
                    .setDataSource(proto.Read.DataSource.newBuilder().setFormat("rate").build())
                    .build())
                  .build())
              .setOutputMode("Append")
              .setAvailableNow(true)
              .setQueryName("test")
              .setFormat("memory")
              .putOptions("checkpointLocation", Utils.createTempDir().getAbsolutePath)
              .setPath("test-path")
              .build()),
        None),
      (
        proto.Command
          .newBuilder()
          .setStreamingQueryCommand(
            proto.StreamingQueryCommand
              .newBuilder()
              .setQueryId(
                proto.StreamingQueryInstanceId
                  .newBuilder()
                  .setId(DEFAULT_UUID.toString)
                  .setRunId(DEFAULT_UUID.toString)
                  .build())
              .setStop(true)),
        None),
      (
        proto.Command
          .newBuilder()
          .setStreamingQueryManagerCommand(proto.StreamingQueryManagerCommand
            .newBuilder()
            .setListListeners(true)),
        None),
      (
        proto.Command
          .newBuilder()
          .setRegisterFunction(
            proto.CommonInlineUserDefinedFunction
              .newBuilder()
              .setFunctionName("function")
              .setPythonUdf(
                proto.PythonUDF
                  .newBuilder()
                  .setEvalType(100)
                  .setOutputType(DataTypeProtoConverter.toConnectProtoType(IntegerType))
                  .setCommand(ByteString.copyFrom("command".getBytes()))
                  .setPythonVer("3.10")
                  .build())),
        None))) { case (command, producedNumRows) =>
    val sessionId = UUID.randomUUID.toString()
    withCommandTest(sessionId) { verifyEvents =>
      val instance = new SparkConnectService(false)
      val context = proto.UserContext
        .newBuilder()
        .setUserId("c1")
        .build()
      val plan = proto.Plan
        .newBuilder()
        .setCommand(command)
        .build()

      val request = proto.ExecutePlanRequest
        .newBuilder()
        .setPlan(plan)
        .setSessionId(sessionId)
        .setUserContext(context)
        .build()

      // Execute plan.
      @volatile var done = false
      val responses = mutable.Buffer.empty[proto.ExecutePlanResponse]
      instance.executePlan(
        request,
        new StreamObserver[proto.ExecutePlanResponse] {
          override def onNext(v: proto.ExecutePlanResponse): Unit = {
            responses += v
            verifyEvents.onNext(v)
          }

          override def onError(throwable: Throwable): Unit = {
            verifyEvents.onError(throwable)
            throw throwable
          }

          override def onCompleted(): Unit = {
            done = true
          }
        })
      verifyEvents.onCompleted(producedNumRows)
      // The current implementation is expected to be blocking.
      // This is here to make sure it is.
      assert(done)

      // Result + Metrics
      val filteredResponses = responses.filter(!_.hasExecutionProgress)
      if (filteredResponses.size > 1) {
        assert(filteredResponses.size == 2)

        // Make sure the first response result only
        val head = filteredResponses.head
        assert(head.hasSqlCommandResult && !head.hasMetrics)

        // Make sure the last response is metrics only
        val last = filteredResponses.last
        assert(last.hasMetrics && !last.hasSqlCommandResult)
      }
    }
  }

  test("SPARK-43923: canceled request send events") {
    val sessionId = UUID.randomUUID.toString
    withEvents { verifyEvents =>
      val instance = new SparkConnectService(false)

      // Add an always crashing UDF
      val session = SparkConnectService.getOrCreateIsolatedSession("c1", sessionId, None).session
      val sleep: Long => Long = { time =>
        Thread.sleep(time)
        time
      }
      session.udf.register("sleep", sleep)

      val connect = new MockRemoteSession()
      val context = proto.UserContext
        .newBuilder()
        .setUserId("c1")
        .build()
      val plan = proto.Plan
        .newBuilder()
        .setRoot(connect.sql("select sleep(10000)"))
        .build()
      val request = proto.ExecutePlanRequest
        .newBuilder()
        .setPlan(plan)
        .setUserContext(context)
        .setSessionId(sessionId)
        .build()

      val thread = new Thread {
        override def run: Unit = {
          verifyEvents.listener.semaphoreStarted.acquire()
          instance.interrupt(
            proto.InterruptRequest
              .newBuilder()
              .setSessionId(sessionId)
              .setUserContext(context)
              .setInterruptType(proto.InterruptRequest.InterruptType.INTERRUPT_TYPE_ALL)
              .build(),
            new StreamObserver[proto.InterruptResponse] {
              override def onNext(v: proto.InterruptResponse): Unit = {}

              override def onError(throwable: Throwable): Unit = {}

              override def onCompleted(): Unit = {}
            })
        }
      }
      thread.start()
      // The observer is executed inside this thread. So
      // we can perform the checks inside the observer.
      instance.executePlan(
        request,
        new StreamObserver[proto.ExecutePlanResponse] {
          override def onNext(v: proto.ExecutePlanResponse): Unit = {
            logInfo(s"$v")
          }

          override def onError(throwable: Throwable): Unit = {
            verifyEvents.onCanceled()
          }

          override def onCompleted(): Unit = {
            fail("this should not complete")
          }
        })
      thread.join()
      verifyEvents.onCompleted()
    }
  }

  test("SPARK-41165: failures in the arrow collect path should not cause hangs") {
    val sessionId = UUID.randomUUID.toString
    withEvents { verifyEvents =>
      val instance = new SparkConnectService(false)

      // Add an always crashing UDF
      val session = SparkConnectService.getOrCreateIsolatedSession("c1", sessionId, None).session
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
        .setSessionId(sessionId)
        .build()

      // Even though the observer is executed inside this thread, this thread is also executing
      // the SparkConnectService. If we throw an exception inside it, it will be caught by
      // the ErrorUtils.handleError wrapping instance.executePlan and turned into an onError
      // call with StatusRuntimeException, which will be eaten here.
      val failures: mutable.ArrayBuffer[String] = new mutable.ArrayBuffer[String]()
      instance.executePlan(
        request,
        new StreamObserver[proto.ExecutePlanResponse] {
          override def onNext(v: proto.ExecutePlanResponse): Unit = {
            // The query receives some pre-execution responses such as schema, but should
            // never proceed to execution and get query results.
            if (v.hasArrowBatch) {
              failures += s"this should not receive query results but got $v"
            }
          }

          override def onError(throwable: Throwable): Unit = {
            try {
              assert(throwable.isInstanceOf[StatusRuntimeException])
              verifyEvents.onError(throwable)
            } catch {
              case t: Throwable =>
                failures += s"assertion $t validating processing onError($throwable)."
            }
          }

          override def onCompleted(): Unit = {
            failures += "this should not complete"
          }
        })
      assert(failures.isEmpty, s"this should have no failures but got $failures")
      verifyEvents.onCompleted()
    }
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

      val response = handler.process(request, sparkSessionHolder)

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
        .setSessionId(UUID.randomUUID.toString())
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

      val filteredResponses = responses.filter(!_.hasExecutionProgress)
      assert(filteredResponses.size == 7)

      // Make sure the first response is schema only
      val head = filteredResponses.head
      assert(head.hasSchema && !head.hasArrowBatch && !head.hasMetrics)

      // Make sure the last response is observed metrics only
      val last = filteredResponses.last
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

  protected def withCommandTest(sessionId: String)(f: VerifyEvents => Unit): Unit = {
    withView("testview") {
      withTable("testcat.testtable") {
        withSparkConf(
          "spark.sql.catalog.testcat" -> classOf[InMemoryPartitionTableCatalog].getName,
          Connect.CONNECT_EXTENSIONS_COMMAND_CLASSES.key ->
            "org.apache.spark.sql.connect.plugin.ExampleCommandPlugin") {
          withEvents { verifyEvents =>
            val restartedQuery = mock[StreamingQuery]
            when(restartedQuery.id).thenReturn(DEFAULT_UUID)
            when(restartedQuery.runId).thenReturn(DEFAULT_UUID)
            SparkConnectService.streamingSessionManager.registerNewStreamingQuery(
              SparkConnectService.getOrCreateIsolatedSession("c1", sessionId, None),
              restartedQuery,
              Set.empty[String],
              "")
            f(verifyEvents)
          }
        }
      }
    }
  }

  protected def withSparkConf(pairs: (String, String)*)(f: => Unit): Unit = {
    val conf = SparkEnv.get.conf
    pairs.foreach { kv => conf.set(kv._1, kv._2) }
    try f
    finally {
      pairs.foreach { kv => conf.remove(kv._1) }
    }
  }

  protected def withEvents(f: VerifyEvents => Unit): Unit = {
    val verifyEvents = new VerifyEvents(spark.sparkContext)
    spark.sparkContext.addSparkListener(verifyEvents.listener)
    Utils.tryWithSafeFinally({
      f(verifyEvents)
      SparkConnectService.sessionManager.invalidateAllSessions()
      verifyEvents.onSessionClosed()
    }) {
      verifyEvents.waitUntilEmpty()
      spark.sparkContext.removeSparkListener(verifyEvents.listener)
      SparkConnectService.sessionManager.invalidateAllSessions()
      SparkConnectPluginRegistry.reset()
    }
  }

  protected def gridTest[A](testNamePrefix: String, testTags: Tag*)(params: Seq[A])(
      testFun: A => Unit): Unit = {
    for (param <- params) {
      test(testNamePrefix + s" ($param)", testTags: _*)(testFun(param))
    }
  }

  class VerifyEvents(val sparkContext: SparkContext) {
    val listener: MockSparkListener = new MockSparkListener()
    val listenerBus = sparkContext.listenerBus
    val EVENT_WAIT_TIMEOUT = timeout(10.seconds)
    val LISTENER_BUS_TIMEOUT = 30000
    def executeHolder: ExecuteHolder = {
      // An ExecuteHolder shall be set eventually through MockSparkListener
      Eventually.eventually(EVENT_WAIT_TIMEOUT) {
        assert(
          listener.executeHolder.isDefined,
          s"No events have been posted in $EVENT_WAIT_TIMEOUT")
        listener.executeHolder.get
      }
    }
    def onNext(v: proto.ExecutePlanResponse): Unit = {
      if (v.hasSchema) {
        assert(executeHolder.eventsManager.status == ExecuteStatus.Analyzed)
      }
      if (v.hasMetrics) {
        assert(executeHolder.eventsManager.status == ExecuteStatus.Finished)
      }
    }
    def onError(throwable: Throwable): Unit = {
      assert(executeHolder.eventsManager.hasCanceled.isEmpty)
      assert(executeHolder.eventsManager.hasError.isDefined)
    }
    def onCompleted(producedRowCount: Option[Long] = None): Unit = {
      assert(executeHolder.eventsManager.getProducedRowCount == producedRowCount)
      // The eventsManager is closed asynchronously
      Eventually.eventually(EVENT_WAIT_TIMEOUT) {
        assert(
          executeHolder.eventsManager.status == ExecuteStatus.Closed,
          s"Execution has not been completed in $EVENT_WAIT_TIMEOUT")
      }
    }
    def onCanceled(): Unit = {
      assert(executeHolder.eventsManager.hasCanceled.contains(true))
      assert(executeHolder.eventsManager.hasError.isEmpty)
    }
    def onSessionClosed(): Unit = {
      assert(executeHolder.sessionHolder.eventManager.status == SessionStatus.Closed)
    }
    def onSessionStarted(): Unit = {
      assert(executeHolder.sessionHolder.eventManager.status == SessionStatus.Started)
    }
    def waitUntilEmpty(): Unit = {
      listenerBus.waitUntilEmpty(LISTENER_BUS_TIMEOUT)
    }
  }
  class MockSparkListener() extends SparkListener {
    val semaphoreStarted = new Semaphore(0)
    var executeHolder = Option.empty[ExecuteHolder]
    override def onOtherEvent(event: SparkListenerEvent): Unit = {
      event match {
        case e: SparkListenerConnectOperationStarted =>
          semaphoreStarted.release()
          val sessionHolder =
            SparkConnectService.getOrCreateIsolatedSession(e.userId, e.sessionId, None)
          executeHolder = sessionHolder.executeHolder(e.operationId)
        case _ =>
      }
    }
  }
}
