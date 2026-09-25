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

package org.apache.spark.sql.execution.externalUDF

import java.io.IOException
import java.nio.channels.Channels
import java.nio.charset.StandardCharsets
import java.util.Properties

import scala.jdk.CollectionConverters._

import com.google.protobuf.ByteString
import org.apache.arrow.vector.ipc.ReadChannel
import org.apache.arrow.vector.ipc.message.MessageSerializer
import org.apache.arrow.vector.types.pojo.Schema
import org.json4s.{Formats, NoTypeHints}
import org.json4s.jackson.Serialization

import org.apache.spark.{BarrierTaskContext, SparkException, SparkUnsupportedOperationException,
  TaskContext, TaskContextImpl}
import org.apache.spark.api.python.PythonEvalType
import org.apache.spark.resource.{CpuAmount, ResourceInformation}
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, Expression,
  ExternalUserDefinedFunction, IsNull}
import org.apache.spark.sql.execution.{ProjectExec, SparkPlan}
import org.apache.spark.sql.execution.arrow.ArrowConverters
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{DataType, LongType, StructField, StructType}
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.udf.worker.{Cancel, DataRequest, DataResponse, Finish, FinishResponse, Init,
  InitResponse, UDFWorkerDataFormat, UDFWorkerSpecification}
import org.apache.spark.udf.worker.core.{Termination, WorkerHandle, WorkerLogger,
  WorkerSecurityScope, WorkerSession}
import org.apache.spark.util.{LongAccumulator, Utils}

object ExecuteExternalUDFExecSuite {
  private val TEST_PAYLOAD = "identity".getBytes(StandardCharsets.UTF_8)
  private val RESPONSE_ALLOCATOR_NAME = "externalUdfArrowResponse"

  private implicit val jsonFormats: Formats = Serialization.formats(NoTypeHints)

  private final case class SerializedResource(name: String, addresses: Seq[String])

  private sealed trait ResponseBehavior extends Serializable
  private case object EchoResponses extends ResponseBehavior
  private case object DropLastResponse extends ResponseBehavior
  private case object DuplicateFirstResponse extends ResponseBehavior
  private case object MalformedResponse extends ResponseBehavior
  private case object CombineLongArguments extends ResponseBehavior
  private case object ConstantLongResponses extends ResponseBehavior

  private final case class TestExecution(
      plan: SparkPlan,
      requestCount: LongAccumulator,
      closeCount: LongAccumulator)

  private final class TestWorkerHandle extends WorkerHandle {
    override def id: String = "test-worker"
    override def markInvalid(): Unit = ()
    override def releaseSession(): Unit = ()
  }

  private final class TestWorkerSession(
      behavior: ResponseBehavior,
      expectedInputSchema: StructType,
      expectedOutputSchema: StructType,
      expectedTimeZone: String,
      expectedLargeVarTypes: Boolean,
      requestCount: LongAccumulator,
      closeCount: LongAccumulator)
    extends WorkerSession(new TestWorkerHandle, WorkerLogger.NoOp) {

    override protected def doInit(message: Init): InitResponse = {
      require(message.getProtocolVersion == 1, "unexpected protocol version")
      require(message.getDataFormat == UDFWorkerDataFormat.ARROW, "unexpected data format")
      require(message.getUdf.getPayload.toByteArray.sameElements(TEST_PAYLOAD),
        "unexpected UDF payload")
      require(message.getUdf.getFormat == "pyspark-udf-experimental",
        "unexpected UDF format")
      require(message.getUdf.getEvalType == PythonEvalType.SQL_ARROW_BATCHED_UDF.toString,
        "unexpected Python evaluation type")
      require(message.getUdf.getName == "identity", "unexpected UDF name")
      require(deserializeArrowSchema(message.getInputSchema) == ArrowUtils.toArrowSchema(
        expectedInputSchema,
        expectedTimeZone,
        true,
        expectedLargeVarTypes),
        "unexpected input schema")
      require(deserializeArrowSchema(message.getOutputSchema) == ArrowUtils.toArrowSchema(
        expectedOutputSchema,
        expectedTimeZone,
        true,
        expectedLargeVarTypes),
        "unexpected output schema")
      require(message.getTimezone == expectedTimeZone, "unexpected session timezone")
      validateTaskContext(message, TaskContext.get())

      val sessionConf = message.getSessionConfMap
      require(sessionConf.get("input_type") == expectedInputSchema.json, "unexpected input type")
      require(sessionConf.containsKey(SQLConf.PYSPARK_BINARY_AS_BYTES.key),
        "Python binary representation setting is missing")
      require(!sessionConf.containsKey(SQLConf.SESSION_LOCAL_TIMEZONE.key),
        "timezone should use the typed Init field")
      InitResponse.getDefaultInstance
    }

    override protected def doProcess(
        input: Iterator[DataRequest],
        finish: () => Finish): Iterator[DataResponse] = behavior match {
      case EchoResponses => echoResponses(input, finish)
      case DropLastResponse => eagerResponses(input, finish).dropRight(1).iterator
      case DuplicateFirstResponse =>
        val responses = eagerResponses(input, finish)
        (responses ++ responses.take(1)).iterator
      case MalformedResponse =>
        eagerResponses(input, finish)
        Iterator.single(DataResponse.newBuilder()
          .setData(ByteString.copyFromUtf8("not an Arrow record batch"))
          .build())
      case CombineLongArguments => transformToLong(input, finish) { row =>
        row.getLong(0) * 100L + row.getLong(1)
      }
      case ConstantLongResponses => transformToLong(input, finish)(_ => 42L)
    }

    override protected def doClose(cancel: () => Cancel): Termination = {
      closeCount.add(1L)
      completeTerminal(Termination.Finished(FinishResponse.getDefaultInstance))
      settledTermination
    }

    private def echoResponses(
        input: Iterator[DataRequest],
        finish: () => Finish): Iterator[DataResponse] = new Iterator[DataResponse] {
      private var finished = false

      override def hasNext: Boolean = {
        val hasNextRequest = input.hasNext
        if (!hasNextRequest && !finished) {
          finish()
          finished = true
        }
        hasNextRequest
      }

      override def next(): DataResponse = {
        if (!hasNext) Iterator.empty.next()
        requestCount.add(1L)
        response(input.next())
      }
    }

    private def eagerResponses(
        input: Iterator[DataRequest],
        finish: () => Finish): Vector[DataResponse] = {
      val requests = input.toVector
      requestCount.add(requests.length.toLong)
      finish()
      requests.map(response)
    }

    private def response(request: DataRequest): DataResponse = {
      DataResponse.newBuilder().setData(request.getData).build()
    }

    private def transformToLong(
        input: Iterator[DataRequest],
        finish: () => Finish)(
        evaluate: InternalRow => Long): Iterator[DataResponse] = {
      val requests = input.toVector
      requestCount.add(requests.length.toLong)
      finish()
      val context = TaskContext.get()
      requests.iterator.flatMap { request =>
        val inputRows = ArrowConverters.fromBatchIterator(
          Iterator.single(request.getData.toByteArray),
          expectedInputSchema,
          expectedTimeZone,
          true,
          expectedLargeVarTypes,
          context)
        val outputRows = inputRows.map { row =>
          InternalRow(evaluate(row))
        }
        ArrowConverters.toBatchIterator(
          outputRows,
          expectedOutputSchema,
          0,
          expectedTimeZone,
          true,
          expectedLargeVarTypes,
          context).map { batch =>
          DataResponse.newBuilder().setData(ByteString.copyFrom(batch)).build()
        }
      }
    }

    private def deserializeArrowSchema(schema: ByteString): Schema = {
      MessageSerializer.deserializeSchema(
        new ReadChannel(Channels.newChannel(schema.newInput())))
    }
  }

  private def validateTaskContext(message: Init, context: TaskContext): Unit = {
    val taskContext = message.getTaskContextMap.asScala.toMap
    val expectedValues = Map(
      "isBarrier" -> context.isInstanceOf[BarrierTaskContext].toString,
      "stageId" -> context.stageId().toString,
      "partitionId" -> context.partitionId().toString,
      "attemptNumber" -> context.attemptNumber().toString,
      "taskAttemptId" -> context.taskAttemptId().toString,
      "cpus" -> context.cpuAmount()
        .setScale(0, BigDecimal.RoundingMode.CEILING).intValue.toString,
      "cpuAmount" -> CpuAmount.toDisplayString(context.cpuAmount()))
    val expectedKeys = expectedValues.keySet ++ Set("resources", "localProperties")
    require(taskContext.keySet == expectedKeys,
      s"unexpected task context keys: ${taskContext.keySet}")
    expectedValues.foreach { case (key, value) =>
      require(taskContext(key) == value,
        s"unexpected task context value for $key: ${taskContext(key)}")
    }

    val expectedResources = context.resources().map { case (name, resource) =>
      name -> SerializedResource(resource.name, resource.addresses.toSeq)
    }
    require(
      Serialization.read[Map[String, SerializedResource]](taskContext("resources")) ==
        expectedResources,
      "unexpected task resources")
    require(
      Serialization.read[Map[String, String]](taskContext("localProperties")) ==
        context.getLocalProperties.asScala.toMap,
      "unexpected task local properties")
  }

  private final class TestExecuteExternalUDFExec(
      udf: ExternalUserDefinedFunction,
      resultAttr: Attribute,
      child: SparkPlan,
      behavior: ResponseBehavior,
      expectedInputSchema: StructType,
      expectedOutputSchema: StructType,
      expectedTimeZone: String,
      expectedLargeVarTypes: Boolean,
      requestCount: LongAccumulator,
      closeCount: LongAccumulator)
    extends ExecuteExternalUDFExec(udf, resultAttr, child) {

    override protected def withUDFWorkerSession(
        taskContext: TaskContext,
        securityScope: Option[WorkerSecurityScope])(
        f: WorkerSession => Iterator[InternalRow]): Iterator[InternalRow] = {
      require(securityScope.isEmpty, "scalar external UDF execution must not request a sandbox")
      val session = new TestWorkerSession(
        behavior,
        expectedInputSchema,
        expectedOutputSchema,
        expectedTimeZone,
        expectedLargeVarTypes,
        requestCount,
        closeCount)
      registerWorkerSessionCompletionListener(taskContext, session)
      f(session)
    }
  }
}

class ExecuteExternalUDFExecSuite extends QueryTest with SharedSparkSession {
  import ExecuteExternalUDFExecSuite._

  private def testExecution(
      behavior: ResponseBehavior,
      rowCount: Long): TestExecution = {
    val child = spark.range(0L, rowCount, 1L, 1).queryExecution.executedPlan
    testExecution(
      behavior,
      child,
      Seq(child.output.head),
      LongType,
      udfNullable = false)
  }

  private def testExecution(
      behavior: ResponseBehavior,
      child: SparkPlan,
      udfChildren: Seq[Expression],
      udfDataType: DataType,
      udfNullable: Boolean): TestExecution = {
    val udf = ExternalUserDefinedFunction(
      name = Some("identity"),
      workerSpec = UDFWorkerSpecification.getDefaultInstance,
      payload = TEST_PAYLOAD,
      dataType = udfDataType,
      children = udfChildren,
      udfDeterministic = true,
      udfNullable = udfNullable)
    val resultAttr = AttributeReference("externalUDF", udfDataType, udf.nullable)()
    val requestCount = spark.sparkContext.longAccumulator("external UDF request count")
    val closeCount = new LongAccumulator
    closeCount.register(
      spark.sparkContext,
      name = Some("external UDF close count"),
      countFailedValues = true)
    val inputSchema = StructType(udfChildren.zipWithIndex.map {
      case (expression, index) =>
        StructField(s"_$index", expression.dataType, expression.nullable)
    })
    val outputSchema = StructType(Seq(StructField("_0", udfDataType, nullable = true)))
    val conf = spark.sessionState.conf
    TestExecution(
      new TestExecuteExternalUDFExec(
        udf,
        resultAttr,
        child,
        behavior,
        inputSchema,
        outputSchema,
        conf.sessionLocalTimeZone,
        conf.arrowUseLargeVarTypes,
        requestCount,
        closeCount),
      requestCount,
      closeCount)
  }

  private def checkResultRowsMismatch(
      error: SparkException,
      outputLength: String,
      inputLength: Long): Unit = {
    checkError(
      exception = Utils.getRootCause(error).asInstanceOf[SparkException],
      condition = "RESULT_ROWS_MISMATCH",
      parameters = Map(
        "output_length" -> outputLength,
        "input_length" -> inputLength.toString))
  }

  private def checkFailedExecutionCleanup(execution: TestExecution): Unit = {
    assert(execution.closeCount.value === 1L)
    val openResponseAllocators = ArrowUtils.rootAllocator.getChildAllocators.asScala
      .filter(_.getName == RESPONSE_ALLOCATOR_NAME)
      .toSeq
    assert(openResponseAllocators.isEmpty,
      s"Arrow response allocators remain open: $openResponseAllocators")
  }

  test("Python Init adapter serializes the complete task context") {
    val localProperties = new Properties
    localProperties.setProperty("externalUdfTestProperty", "test-value")
    val context = new TaskContextImpl(
      stageId = 7,
      stageAttemptNumber = 3,
      partitionId = 5,
      taskAttemptId = 1234L,
      attemptNumber = 2,
      numPartitions = 10,
      taskMemoryManager = null,
      localProperties = localProperties,
      metricsSystem = null,
      cpuAmount = BigDecimal("1.5"),
      resources = Map(
        "gpu" -> new ResourceInformation("gpu", Array("0", "2"))))

    val init = PythonInitAdapter.build(
      PythonInitAdapter.PreparedInit(Init.getDefaultInstance),
      context)
    validateTaskContext(init, context)
  }

  test("scalar external UDF rejects barrier task contexts before worker creation") {
    val context = new TaskContextImpl(
      stageId = 0,
      stageAttemptNumber = 0,
      partitionId = 0,
      taskAttemptId = 0L,
      attemptNumber = 0,
      numPartitions = 1,
      taskMemoryManager = null,
      localProperties = new Properties,
      metricsSystem = null)
    val barrierContext = new BarrierTaskContext(context)

    val error = intercept[SparkUnsupportedOperationException] {
      ExecuteExternalUDFExec.ensureTaskContextSupported(barrierContext)
    }
    checkError(
      exception = error,
      condition = "UNSUPPORTED_FEATURE.EXTERNAL_UDF_IN_BARRIER_TASK",
      parameters = Map.empty)
  }

  test("scalar external UDF exchanges multiple Arrow batches through a worker session") {
    withSQLConf(
        SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC",
        SQLConf.ARROW_EXECUTION_MAX_RECORDS_PER_BATCH.key -> "2") {
      val execution = testExecution(EchoResponses, rowCount = 5L)
      val rows = execution.plan.executeCollect()

      assert(rows.map(row => (row.getLong(0), row.getLong(1))).toSeq === Seq(
        (0L, 0L),
        (1L, 1L),
        (2L, 2L),
        (3L, 3L),
        (4L, 4L)))
      assert(execution.requestCount.value === 3L)
      assert(execution.closeCount.value === 1L)
    }
  }

  test("scalar external UDF finishes an empty input partition") {
    val child = spark.range(0L, 0L, 1L, 1).coalesce(1).queryExecution.executedPlan
    val execution = testExecution(
      EchoResponses,
      child,
      Seq(child.output.head),
      LongType,
      udfNullable = false)

    assert(execution.plan.executeCollect().isEmpty)
    assert(execution.requestCount.value === 0L)
    assert(execution.closeCount.value === 1L)
  }

  test("scalar external UDF preserves row count for zero-argument functions") {
    withSQLConf(SQLConf.ARROW_EXECUTION_MAX_RECORDS_PER_BATCH.key -> "2") {
      val child = spark.range(0L, 5L, 1L, 1).queryExecution.executedPlan
      val execution = testExecution(
        ConstantLongResponses,
        child,
        Seq.empty,
        LongType,
        udfNullable = false)

      val rows = execution.plan.executeCollect()
      assert(rows.map(row => (row.getLong(0), row.getLong(1))).toSeq ===
        (0L until 5L).map(value => (value, 42L)))
      assert(execution.requestCount.value === 3L)
      assert(execution.closeCount.value === 1L)
    }
  }

  test("scalar external UDF round-trips ordered multi-argument Arrow data") {
    val child = spark.range(0L, 3L, 1L, 1)
      .selectExpr("id AS left", "id + 10 AS right")
      .queryExecution.executedPlan
    val execution = testExecution(
      CombineLongArguments,
      child,
      child.output,
      LongType,
      udfNullable = false)

    val rows = execution.plan.executeCollect()
    assert(rows.map(row => (row.getLong(0), row.getLong(1), row.getLong(2))).toSeq === Seq(
      (0L, 10L, 10L),
      (1L, 11L, 111L),
      (2L, 12L, 212L)))
    assert(execution.requestCount.value === 1L)
    assert(execution.closeCount.value === 1L)
  }

  test("scalar external UDF closes sessions for multiple partitions") {
    val child = spark.range(0L, 6L, 1L, 3).queryExecution.executedPlan
    val execution = testExecution(
      EchoResponses,
      child,
      Seq(child.output.head),
      LongType,
      udfNullable = false)

    val rows = execution.plan.executeCollect()

    assert(rows.map(row => (row.getLong(0), row.getLong(1))).toSeq ===
      (0L until 6L).map(value => (value, value)))
    assert(execution.requestCount.value === 3L)
    assert(execution.closeCount.value === 3L)
  }

  test("scalar external UDF exposes nullable output to downstream consumers") {
    val child = spark.range(0L, 2L, 1L, 1)
      .selectExpr("IF(id = 0, CAST(NULL AS BIGINT), id) AS value")
      .queryExecution.executedPlan
    val input = child.output.head
    val execution = testExecution(
      EchoResponses,
      child,
      Seq(input),
      LongType,
      udfNullable = false)

    val resultAttr = execution.plan.output.last
    assert(resultAttr.nullable)
    val downstream = ProjectExec(
      Seq(resultAttr, Alias(IsNull(resultAttr), "resultIsNull")()),
      execution.plan)
    val rows = downstream.executeCollect()
    assert(rows.length === 2)
    assert(rows.head.isNullAt(0))
    assert(rows.head.getBoolean(1))
    assert(rows(1).getLong(0) === 1L)
    assert(!rows(1).getBoolean(1))
    assert(execution.requestCount.value === 1L)
    assert(execution.closeCount.value === 1L)
  }

  test("scalar external UDF limits Arrow input batches by byte size") {
    withSQLConf(
        SQLConf.ARROW_EXECUTION_MAX_RECORDS_PER_BATCH.key -> "100",
        SQLConf.ARROW_EXECUTION_MAX_BYTES_PER_BATCH.key -> "512") {
      val child = spark.range(0L, 3L, 1L, 1)
        .selectExpr("repeat('x', 1024) AS value")
        .queryExecution.executedPlan
      val input = child.output.head
      val execution = testExecution(
        EchoResponses,
        child,
        Seq(input),
        input.dataType,
        input.nullable)
      val rows = execution.plan.executeCollect()

      assert(rows.length === 3)
      rows.foreach { row =>
        assert(row.getUTF8String(0) === row.getUTF8String(1))
      }
      assert(execution.requestCount.value === 3L)
      assert(execution.closeCount.value === 1L)
    }
  }

  test("scalar external UDF uses large variable Arrow types for requests and responses") {
    withSQLConf(SQLConf.ARROW_EXECUTION_USE_LARGE_VAR_TYPES.key -> "true") {
      val child = spark.range(0L, 3L, 1L, 1)
        .selectExpr("concat('value-', id) AS value")
        .queryExecution.executedPlan
      val input = child.output.head
      val execution = testExecution(
        EchoResponses,
        child,
        Seq(input),
        input.dataType,
        input.nullable)
      val rows = execution.plan.executeCollect()

      assert(rows.length === 3)
      rows.foreach { row =>
        assert(row.getUTF8String(0) === row.getUTF8String(1))
      }
      assert(execution.requestCount.value === 1L)
      assert(execution.closeCount.value === 1L)
    }
  }

  test("scalar external UDF decodes compressed Arrow responses") {
    withSQLConf(
        SQLConf.ARROW_EXECUTION_COMPRESSION_CODEC.key -> "lz4",
        SQLConf.ARROW_EXECUTION_MAX_RECORDS_PER_BATCH.key -> "2") {
      val child = spark.range(0L, 5L, 1L, 1)
        .selectExpr("repeat(CAST(id AS STRING), 1024) AS value")
        .queryExecution.executedPlan
      val input = child.output.head
      val execution = testExecution(
        EchoResponses,
        child,
        Seq(input),
        input.dataType,
        input.nullable)
      val rows = execution.plan.executeCollect()

      assert(rows.length === 5)
      rows.foreach { row =>
        assert(row.getUTF8String(0) === row.getUTF8String(1))
      }
      assert(execution.requestCount.value === 3L)
      assert(execution.closeCount.value === 1L)
    }
  }

  test("scalar external UDF rejects fewer output rows than input rows") {
    withSQLConf(SQLConf.ARROW_EXECUTION_MAX_RECORDS_PER_BATCH.key -> "1") {
      val execution = testExecution(DropLastResponse, rowCount = 3L)
      val error = intercept[SparkException] {
        execution.plan.executeCollect()
      }

      checkResultRowsMismatch(error, outputLength = "2", inputLength = 3L)
      checkFailedExecutionCleanup(execution)
    }
  }

  test("scalar external UDF reports buffered extra output rows as a lower bound") {
    withSQLConf(SQLConf.ARROW_EXECUTION_MAX_RECORDS_PER_BATCH.key -> "3") {
      val execution = testExecution(DuplicateFirstResponse, rowCount = 3L)
      val error = intercept[SparkException] {
        execution.plan.executeCollect()
      }

      checkResultRowsMismatch(error, outputLength = "at least 6", inputLength = 3L)
      checkFailedExecutionCleanup(execution)
    }
  }

  test("scalar external UDF rejects a malformed Arrow response") {
    val execution = testExecution(MalformedResponse, rowCount = 3L)
    val error = intercept[SparkException] {
      execution.plan.executeCollect()
    }

    assert(Utils.getRootCause(error).isInstanceOf[IOException])
    checkFailedExecutionCleanup(execution)
  }

  test("scalar external UDF closes its worker session when output consumption stops early") {
    withSQLConf(SQLConf.ARROW_EXECUTION_MAX_RECORDS_PER_BATCH.key -> "2") {
      val execution = testExecution(EchoResponses, rowCount = 5L)
      val rows = execution.plan.executeTake(1)

      assert(rows.length === 1)
      assert(rows.head.getLong(0) === rows.head.getLong(1))
      assert(execution.requestCount.value === 1L)
      assert(execution.closeCount.value === 1L)
    }
  }
}
