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

import scala.collection.JavaConverters._
import scala.collection.mutable

import com.google.protobuf
import com.google.protobuf.ByteString
import io.grpc.StatusRuntimeException
import io.grpc.stub.StreamObserver
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{BigIntVector, Float8Vector}
import org.apache.arrow.vector.ipc.ArrowStreamReader
import org.apache.commons.lang3.{JavaVersion, SystemUtils}
import org.mockito.Mockito.when
import org.scalatest.Tag
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark.{SparkContext, SparkEnv}
import org.apache.spark.connect.proto
import org.apache.spark.connect.proto.CreateDataFrameViewCommand
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent}
import org.apache.spark.sql.connect.common.DataTypeProtoConverter
import org.apache.spark.sql.connect.config.Connect
import org.apache.spark.sql.connect.dsl.MockRemoteSession
import org.apache.spark.sql.connect.dsl.expressions._
import org.apache.spark.sql.connect.dsl.plans._
import org.apache.spark.sql.connect.plugin.SparkConnectPluginRegistry
import org.apache.spark.sql.connect.service.{SparkConnectAnalyzeHandler, SparkConnectService, SparkListenerConnectOperationAnalyzed, SparkListenerConnectOperationCanceled, SparkListenerConnectOperationClosed, SparkListenerConnectOperationFailed, SparkListenerConnectOperationFinished, SparkListenerConnectOperationReadyForExecution, SparkListenerConnectOperationStarted, SparkListenerConnectSessionClosed, SparkListenerConnectSessionStarted}
import org.apache.spark.sql.connect.service.SessionHolder
import org.apache.spark.sql.connector.catalog.InMemoryPartitionTableCatalog
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.util.Utils

/**
 * Testing Connect Service implementation.
 */
class SparkConnectServiceSuite extends SharedSparkSession with MockitoSugar with Logging {

  private def sparkSessionHolder = SessionHolder.forTesting(spark)
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
      assert(response2.getExplain.getExplainString.size > 0)

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
      // TODO(SPARK-44121) Renable Arrow-based connect tests in Java 21
      assume(SystemUtils.isJavaVersionAtMost(JavaVersion.JAVA_17))
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
      verifyEvents.onCompleted()
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
  }

  gridTest("SPARK-43923: commands send events")(
    Seq(
      proto.Command
        .newBuilder()
        .setSqlCommand(proto.SqlCommand.newBuilder().setSql("select 1").build()),
      proto.Command
        .newBuilder()
        .setSqlCommand(proto.SqlCommand.newBuilder().setSql("show tables").build()),
      proto.Command
        .newBuilder()
        .setWriteOperation(
          proto.WriteOperation
            .newBuilder()
            .setInput(
              proto.Relation.newBuilder().setSql(proto.SQL.newBuilder().setQuery("select 1")))
            .setPath("my/test/path")
            .setMode(proto.WriteOperation.SaveMode.SAVE_MODE_OVERWRITE)),
      proto.Command
        .newBuilder()
        .setWriteOperationV2(
          proto.WriteOperationV2
            .newBuilder()
            .setInput(proto.Relation.newBuilder.setRange(
              proto.Range.newBuilder().setStart(0).setEnd(2).setStep(1L)))
            .setTableName("testcat.testtable")
            .setMode(proto.WriteOperationV2.Mode.MODE_CREATE)),
      proto.Command
        .newBuilder()
        .setCreateDataframeView(
          CreateDataFrameViewCommand
            .newBuilder()
            .setName("testview")
            .setInput(
              proto.Relation.newBuilder().setSql(proto.SQL.newBuilder().setQuery("select 1")))),
      proto.Command
        .newBuilder()
        .setGetResourcesCommand(proto.GetResourcesCommand.newBuilder()),
      proto.Command
        .newBuilder()
        .setExtension(
          protobuf.Any.pack(
            proto.ExamplePluginCommand
              .newBuilder()
              .setCustomField("SPARK-43923")
              .build())),
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
            .putOptions("checkpointLocation", s"${UUID.randomUUID}")
            .setPath("test-path")
            .build()),
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
      proto.Command
        .newBuilder()
        .setStreamingQueryManagerCommand(proto.StreamingQueryManagerCommand
          .newBuilder()
          .setListListeners(true)),
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
                .build())))) { command =>
    withCommandTest { verifyEvents =>
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
        .setSessionId("s1")
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
      verifyEvents.onCompleted()
      // The current implementation is expected to be blocking.
      // This is here to make sure it is.
      assert(done)

      // Result + Metrics
      if (responses.size > 1) {
        assert(responses.size == 2)

        // Make sure the first response result only
        val head = responses.head
        assert(head.hasSqlCommandResult && !head.hasMetrics)

        // Make sure the last response is metrics only
        val last = responses.last
        assert(last.hasMetrics && !last.hasSqlCommandResult)
      }
    }
  }

  test("SPARK-43923: canceled request send events") {
    withEvents { verifyEvents =>
      val instance = new SparkConnectService(false)

      // Add an always crashing UDF
      val session = SparkConnectService.getOrCreateIsolatedSession("c1", "session").session
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
        .setSessionId("session")
        .build()

      new Thread {
        override def run: Unit = {
          verifyEvents.listener.semaphoreStarted.acquire()
          instance.interrupt(
            proto.InterruptRequest
              .newBuilder()
              .setSessionId("session")
              .setUserContext(context)
              .setInterruptType(proto.InterruptRequest.InterruptType.INTERRUPT_TYPE_ALL)
              .build(),
            new StreamObserver[proto.InterruptResponse] {
              override def onNext(v: proto.InterruptResponse): Unit = {}

              override def onError(throwable: Throwable): Unit = {}

              override def onCompleted(): Unit = {}
            })
        }
      }.start()
      // The observer is executed inside this thread. So
      // we can perform the checks inside the observer.
      instance.executePlan(
        request,
        new StreamObserver[proto.ExecutePlanResponse] {
          override def onNext(v: proto.ExecutePlanResponse): Unit = {
            logInfo(s"$v")
          }

          override def onError(throwable: Throwable): Unit = {
            verifyEvents.onCanceled
          }

          override def onCompleted(): Unit = {
            fail("this should not complete")
          }
        })
      verifyEvents.onCompleted()
    }
  }

  test("SPARK-41165: failures in the arrow collect path should not cause hangs") {
    withEvents { verifyEvents =>
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
            verifyEvents.onError(throwable)
          }

          override def onCompleted(): Unit = {
            fail("this should not complete")
          }
        })
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
    // TODO(SPARK-44121) Renable Arrow-based connect tests in Java 21
    assume(SystemUtils.isJavaVersionAtMost(JavaVersion.JAVA_17))
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

  protected def withCommandTest(f: VerifyEvents => Unit): Unit = {
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
              SparkConnectService.getOrCreateIsolatedSession("c1", "s1"),
              restartedQuery)
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
      SparkConnectService.invalidateAllSessions()
      verifyEvents.onSessionClosed()
    }) {
      verifyEvents.waitUntilEmpty()
      spark.sparkContext.removeSparkListener(verifyEvents.listener)
      SparkConnectService.invalidateAllSessions()
      SparkConnectPluginRegistry.reset()
    }
  }

  protected def gridTest[A](testNamePrefix: String, testTags: Tag*)(params: Seq[A])(
      testFun: A => Unit): Unit = {
    for (param <- params) {
      test(testNamePrefix + s" ($param)", testTags: _*)(testFun(param))
    }
  }

  sealed abstract class Status(value: Int)

  object Status {
    case object Pending extends Status(0)
    case object SessionStarted extends Status(1)
    case object Started extends Status(2)
    case object Analyzed extends Status(3)
    case object ReadyForExecution extends Status(4)
    case object Finished extends Status(5)
    case object Failed extends Status(6)
    case object Canceled extends Status(7)
    case object Closed extends Status(8)
    case object SessionClosed extends Status(9)
  }

  class VerifyEvents(val sparkContext: SparkContext) {
    val listener: MockSparkListener = new MockSparkListener()
    val listenerBus = sparkContext.listenerBus
    val LISTENER_BUS_TIMEOUT = 30000
    def onNext(v: proto.ExecutePlanResponse): Unit = {
      if (v.hasSchema) {
        waitUntilEmpty()
        assert(listener.status == Status.Analyzed)
      }
      if (v.hasMetrics) {
        waitUntilEmpty()
        assert(listener.status == Status.Finished)
      }
      assertStatusExpected()
    }
    def onError(throwable: Throwable): Unit = {
      waitUntilEmpty()
      assert(listener.canceled.isEmpty)
      assert(listener.queryError.isDefined)
      assertStatusExpected()
    }
    def onCompleted(): Unit = {
      waitUntilEmpty()
      assert(listener.status == Status.Closed)
      assertStatusExpected()
    }
    def onCanceled(): Unit = {
      waitUntilEmpty()
      assert(listener.canceled.contains(true))
      assert(listener.queryError.isEmpty)
      assertStatusExpected()
    }
    def onSessionClosed(): Unit = {
      waitUntilEmpty()
      assert(listener.status == Status.SessionClosed)
      assertStatusExpected()
    }
    def onSessionStarted(): Unit = {
      waitUntilEmpty()
      assert(listener.status == Status.SessionStarted)
      assertStatusExpected()
    }
    def waitUntilEmpty(): Unit = {
      listenerBus.waitUntilEmpty(LISTENER_BUS_TIMEOUT)
    }
    def assertStatusExpected(): Unit = {
      if (listener.statusError.isDefined) {
        throw listener.statusError.get
      }
    }
  }

  class MockSparkListener() extends SparkListener {
    var status: Status = Status.Pending
    var statusError: Option[Throwable] = None
    var queryError: Option[String] = None
    var canceled: Option[Boolean] = None
    val semaphoreStarted = new Semaphore(0)
    override def onOtherEvent(event: SparkListenerEvent): Unit = {
      try {
        _onOtherEvent(event)
      } catch {
        case e: Throwable =>
          logError("Failed MockSparkListener assertion", e)
          statusError = Some(e)
      }
    }
    def _onOtherEvent(event: SparkListenerEvent): Unit = {
      val prevStatus = status
      event match {
        case e: SparkListenerConnectSessionStarted =>
          logInfo(s"$e")
          status = Status.SessionStarted
          semaphoreStarted.release()
          assert(prevStatus == Status.Pending, s"$e")
        case e: SparkListenerConnectOperationStarted =>
          logInfo(s"$e")
          status = Status.Started
          assert(prevStatus == Status.SessionStarted, s"$e")
        case e: SparkListenerConnectOperationAnalyzed =>
          logInfo(s"$e")
          status = Status.Analyzed
          assert(
            List(Status.Started, Status.Analyzed)
              .find(s => s == prevStatus)
              .isDefined,
            s"$e")
        case e: SparkListenerConnectOperationReadyForExecution =>
          logInfo(s"$e")
          status = Status.ReadyForExecution
          assert(prevStatus == Status.Analyzed, s"$e")
        case e: SparkListenerConnectOperationFinished =>
          logInfo(s"$e")
          status = Status.Finished
          assert(
            List(Status.Started, Status.ReadyForExecution)
              .find(s => s == prevStatus)
              .isDefined,
            s"$e")
        case e: SparkListenerConnectOperationFailed =>
          logInfo(s"$e")
          status = Status.Failed
          queryError = Some(e.errorMessage)
          assert(
            List(Status.Started, Status.Analyzed, Status.ReadyForExecution, Status.Finished)
              .find(s => s == prevStatus)
              .isDefined,
            s"$e")
        case e: SparkListenerConnectOperationCanceled =>
          logInfo(s"$e")
          status = Status.Canceled
          canceled = Some(true)
          assert(
            List(
              Status.Started,
              Status.Analyzed,
              Status.ReadyForExecution,
              Status.Finished,
              Status.Failed)
              .find(s => s == prevStatus)
              .isDefined,
            s"$e")
        case e: SparkListenerConnectOperationClosed =>
          logInfo(s"$e")
          status = Status.Closed
          assert(
            List(Status.Finished, Status.Failed, Status.Canceled)
              .find(s => s == prevStatus)
              .isDefined,
            s"$e")
        case e: SparkListenerConnectSessionClosed =>
          logInfo(s"$e")
          status = Status.SessionClosed
          assert(
            List(Status.Failed, Status.Canceled, Status.Closed)
              .find(s => s == prevStatus)
              .isDefined,
            s"$e")
        case _ =>
      }
    }
  }
}
