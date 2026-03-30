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
package org.apache.spark.sql.connect.messages

import com.google.protobuf.ByteString

import org.apache.spark.SparkFunSuite
import org.apache.spark.connect.proto._

/**
 * Test suite for SPARK-55278 UDF Protocol protobuf messages.
 * Verifies serialization/deserialization round-trip for all message types.
 */
class UdfProtocolMessagesSuite extends SparkFunSuite {

  test("RegisterWorker round-trip with full capabilities") {
    // Build RegisterWorkerRequest with all fields
    val capabilities = WorkerCapabilities
      .newBuilder()
      .setLanguage("python")
      .setLanguageVersion("3.11.5")
      .setSdkVersion("pyspark-udf-1.0.0")
      .setProtocolVersion(1)
      .addSupportedModes(UdfMode.SCALAR)
      .addSupportedModes(UdfMode.VECTORIZED)
      .setSupportsArrowIpc(true)
      .setMaxBatchBytes(67108864L) // 64MB
      .build()

    val request = RegisterWorkerRequest
      .newBuilder()
      .setWorkerId("worker-uuid-12345")
      .setAuthToken("secret-auth-token-xyz")
      .setCapabilities(capabilities)
      .build()

    // Serialize and parse back
    val serialized = request.toByteArray
    val parsed = RegisterWorkerRequest.parseFrom(serialized)

    // Verify all fields match
    assert(parsed.getWorkerId == "worker-uuid-12345")
    assert(parsed.getAuthToken == "secret-auth-token-xyz")
    assert(parsed.hasCapabilities)

    val parsedCaps = parsed.getCapabilities
    assert(parsedCaps.getLanguage == "python")
    assert(parsedCaps.getLanguageVersion == "3.11.5")
    assert(parsedCaps.getSdkVersion == "pyspark-udf-1.0.0")
    assert(parsedCaps.getProtocolVersion == 1)
    assert(parsedCaps.getSupportedModesCount == 2)
    assert(parsedCaps.getSupportedModes(0) == UdfMode.SCALAR)
    assert(parsedCaps.getSupportedModes(1) == UdfMode.VECTORIZED)
    assert(parsedCaps.getSupportsArrowIpc == true)
    assert(parsedCaps.getMaxBatchBytes == 67108864L)
  }

  test("WorkerCapabilities with all UDF modes") {
    val capabilities = WorkerCapabilities
      .newBuilder()
      .setLanguage("go")
      .setLanguageVersion("1.21")
      .addSupportedModes(UdfMode.SCALAR)
      .addSupportedModes(UdfMode.VECTORIZED)
      .addSupportedModes(UdfMode.GROUPED_MAP)
      .addSupportedModes(UdfMode.TABLE)
      .build()

    val serialized = capabilities.toByteArray
    val parsed = WorkerCapabilities.parseFrom(serialized)

    assert(parsed.getSupportedModesCount == 4)
    assert(parsed.getSupportedModes(0) == UdfMode.SCALAR)
    assert(parsed.getSupportedModes(1) == UdfMode.VECTORIZED)
    assert(parsed.getSupportedModes(2) == UdfMode.GROUPED_MAP)
    assert(parsed.getSupportedModes(3) == UdfMode.TABLE)
  }

  test("RegisterWorkerResponse with JWT session token") {
    val response = RegisterWorkerResponse
      .newBuilder()
      .setStatus(WorkerStatus.ACCEPTED)
      .setSessionToken("eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ3b3JrZXIifQ")
      .build()

    val serialized = response.toByteArray
    val parsed = RegisterWorkerResponse.parseFrom(serialized)

    assert(parsed.getStatus == WorkerStatus.ACCEPTED)
    assert(parsed.getSessionToken == "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ3b3JrZXIifQ")
    assert(parsed.getErrorMessage.isEmpty)
  }

  test("RegisterWorkerResponse with rejection") {
    val response = RegisterWorkerResponse
      .newBuilder()
      .setStatus(WorkerStatus.REJECTED)
      .setErrorMessage("Too many workers registered")
      .build()

    val serialized = response.toByteArray
    val parsed = RegisterWorkerResponse.parseFrom(serialized)

    assert(parsed.getStatus == WorkerStatus.REJECTED)
    assert(parsed.getErrorMessage == "Too many workers registered")
    assert(parsed.getSessionToken.isEmpty)
  }

  test("InitializeUdfRequest with serialized_function payload") {
    val payload = UdfPayload
      .newBuilder()
      .setSerializedFunction(ByteString.copyFrom("cloudpickle-bytes".getBytes))
      .setPayloadFormat("cloudpickle")
      .setPayloadHash(ByteString.copyFrom("hash123".getBytes))
      .build()

    val request = InitializeUdfRequest
      .newBuilder()
      .setUdfId("udf-001")
      .setUdfName("my_scalar_udf")
      .setMode(UdfMode.SCALAR)
      .setInputSchema(ByteString.copyFrom("arrow-input-schema".getBytes))
      .setOutputSchema(ByteString.copyFrom("arrow-output-schema".getBytes))
      .setPayload(payload)
      .setPayloadIntegrityHash(ByteString.copyFrom("sha256-hash".getBytes))
      .build()

    val serialized = request.toByteArray
    val parsed = InitializeUdfRequest.parseFrom(serialized)

    assert(parsed.getUdfId == "udf-001")
    assert(parsed.getUdfName == "my_scalar_udf")
    assert(parsed.getMode == UdfMode.SCALAR)
    assert(parsed.getInputSchema.toStringUtf8 == "arrow-input-schema")
    assert(parsed.getOutputSchema.toStringUtf8 == "arrow-output-schema")
    assert(parsed.hasPayload)

    val parsedPayload = parsed.getPayload
    assert(parsedPayload.getPayloadTypeCase == UdfPayload.PayloadTypeCase.SERIALIZED_FUNCTION)
    assert(parsedPayload.getSerializedFunction.toStringUtf8 == "cloudpickle-bytes")
    assert(parsedPayload.getPayloadFormat == "cloudpickle")
    assert(parsed.getPayloadIntegrityHash.toStringUtf8 == "sha256-hash")
  }

  test("InitializeUdfRequest with source_code payload") {
    val payload = UdfPayload
      .newBuilder()
      .setSourceCode("def my_udf(x): return x * 2")
      .setPayloadFormat("python-source")
      .build()

    val request = InitializeUdfRequest
      .newBuilder()
      .setUdfId("udf-002")
      .setUdfName("double_udf")
      .setMode(UdfMode.VECTORIZED)
      .setPayload(payload)
      .build()

    val serialized = request.toByteArray
    val parsed = InitializeUdfRequest.parseFrom(serialized)

    val parsedPayload = parsed.getPayload
    assert(parsedPayload.getPayloadTypeCase == UdfPayload.PayloadTypeCase.SOURCE_CODE)
    assert(parsedPayload.getSourceCode == "def my_udf(x): return x * 2")
    assert(parsedPayload.getPayloadFormat == "python-source")
  }

  test("InitializeUdfRequest with entry_point payload") {
    val payload = UdfPayload
      .newBuilder()
      .setEntryPoint("mypackage.mymodule.my_function")
      .setPayloadFormat("python-entry-point")
      .build()

    val request = InitializeUdfRequest
      .newBuilder()
      .setUdfId("udf-003")
      .setMode(UdfMode.GROUPED_MAP)
      .setPayload(payload)
      .build()

    val serialized = request.toByteArray
    val parsed = InitializeUdfRequest.parseFrom(serialized)

    val parsedPayload = parsed.getPayload
    assert(parsedPayload.getPayloadTypeCase == UdfPayload.PayloadTypeCase.ENTRY_POINT)
    assert(parsedPayload.getEntryPoint == "mypackage.mymodule.my_function")
  }

  test("InitializeUdfRequest with compiled_artifact payload") {
    val payload = UdfPayload
      .newBuilder()
      .setCompiledArtifact(ByteString.copyFrom("wasm-binary-data".getBytes))
      .setPayloadFormat("wasm")
      .build()

    val request = InitializeUdfRequest
      .newBuilder()
      .setUdfId("udf-004")
      .setMode(UdfMode.TABLE)
      .setPayload(payload)
      .build()

    val serialized = request.toByteArray
    val parsed = InitializeUdfRequest.parseFrom(serialized)

    val parsedPayload = parsed.getPayload
    assert(parsedPayload.getPayloadTypeCase == UdfPayload.PayloadTypeCase.COMPILED_ARTIFACT)
    assert(parsedPayload.getCompiledArtifact.toStringUtf8 == "wasm-binary-data")
    assert(parsedPayload.getPayloadFormat == "wasm")
  }

  test("InitializeUdfRequest with dependencies and integrity hash") {
    val payload = UdfPayload
      .newBuilder()
      .setSerializedFunction(ByteString.copyFrom("function-bytes".getBytes))
      .setPayloadFormat("cloudpickle")
      .setPayloadHash(ByteString.copyFrom("content-hash".getBytes))
      .setPayloadSignature(ByteString.copyFrom("digital-signature".getBytes))
      .putDependencies("numpy", ByteString.copyFrom("numpy-bytes".getBytes))
      .putDependencies("pandas", ByteString.copyFrom("pandas-bytes".getBytes))
      .build()

    val request = InitializeUdfRequest
      .newBuilder()
      .setUdfId("udf-005")
      .setPayload(payload)
      .setPayloadIntegrityHash(ByteString.copyFrom("sha256-integrity".getBytes))
      .build()

    val serialized = request.toByteArray
    val parsed = InitializeUdfRequest.parseFrom(serialized)

    val parsedPayload = parsed.getPayload
    assert(parsedPayload.getPayloadHash.toStringUtf8 == "content-hash")
    assert(parsedPayload.getPayloadSignature.toStringUtf8 == "digital-signature")
    assert(parsedPayload.getDependenciesCount == 2)
    assert(parsedPayload.getDependenciesOrThrow("numpy").toStringUtf8 == "numpy-bytes")
    assert(parsedPayload.getDependenciesOrThrow("pandas").toStringUtf8 == "pandas-bytes")
    assert(parsed.getPayloadIntegrityHash.toStringUtf8 == "sha256-integrity")
  }

  test("InitializeUdfRequest with config and resource hints") {
    val resourceHints = UdfResourceHints
      .newBuilder()
      .setEstimatedMemoryBytes(1073741824L) // 1GB
      .setEstimatedCpuFraction(0.75)
      .setRequiresGpu(true)
      .setEstimatedOutputRowMultiplier(2)
      .build()

    val request = InitializeUdfRequest
      .newBuilder()
      .setUdfId("udf-006")
      .setMode(UdfMode.TABLE)
      .putConfig("batch_size", "1000")
      .putConfig("timeout_ms", "30000")
      .setResourceHints(resourceHints)
      .build()

    val serialized = request.toByteArray
    val parsed = InitializeUdfRequest.parseFrom(serialized)

    assert(parsed.getConfigCount == 2)
    assert(parsed.getConfigOrThrow("batch_size") == "1000")
    assert(parsed.getConfigOrThrow("timeout_ms") == "30000")

    val parsedHints = parsed.getResourceHints
    assert(parsedHints.getEstimatedMemoryBytes == 1073741824L)
    assert(parsedHints.getEstimatedCpuFraction == 0.75)
    assert(parsedHints.getRequiresGpu == true)
    assert(parsedHints.getEstimatedOutputRowMultiplier == 2)
  }

  test("InitializeUdfResponse with OK status") {
    val response = InitializeUdfResponse
      .newBuilder()
      .setStatus(InitializeUdfStatus.OK)
      .setActualOutputSchema(ByteString.copyFrom("actual-schema".getBytes))
      .build()

    val serialized = response.toByteArray
    val parsed = InitializeUdfResponse.parseFrom(serialized)

    assert(parsed.getStatus == InitializeUdfStatus.OK)
    assert(parsed.getActualOutputSchema.toStringUtf8 == "actual-schema")
    assert(parsed.getErrorMessage.isEmpty)
  }

  test("InitializeUdfResponse with error") {
    val response = InitializeUdfResponse
      .newBuilder()
      .setStatus(InitializeUdfStatus.SCHEMA_MISMATCH)
      .setErrorMessage("Input schema validation failed")
      .setErrorDetail("Expected INT, got STRING in column 'age'")
      .build()

    val serialized = response.toByteArray
    val parsed = InitializeUdfResponse.parseFrom(serialized)

    assert(parsed.getStatus == InitializeUdfStatus.SCHEMA_MISMATCH)
    assert(parsed.getErrorMessage == "Input schema validation failed")
    assert(parsed.getErrorDetail == "Expected INT, got STRING in column 'age'")
  }

  test("ExecuteUdfRequest with DataBatch") {
    val dataBatch = DataBatch
      .newBuilder()
      .setBatchId(12345L)
      .setArrowIpcBytes(ByteString.copyFrom("arrow-record-batch-bytes".getBytes))
      .setNumRows(1000)
      .setNumColumns(5)
      .build()

    val request = ExecuteUdfRequest
      .newBuilder()
      .setUdfId("udf-001")
      .setMessageId(1L)
      .setDataBatch(dataBatch)
      .build()

    val serialized = request.toByteArray
    val parsed = ExecuteUdfRequest.parseFrom(serialized)

    assert(parsed.getUdfId == "udf-001")
    assert(parsed.getMessageId == 1L)
    assert(parsed.getRequestTypeCase == ExecuteUdfRequest.RequestTypeCase.DATA_BATCH)

    val parsedBatch = parsed.getDataBatch
    assert(parsedBatch.getBatchId == 12345L)
    assert(parsedBatch.getArrowIpcBytes.toStringUtf8 == "arrow-record-batch-bytes")
    assert(parsedBatch.getNumRows == 1000)
    assert(parsedBatch.getNumColumns == 5)
    assert(!parsedBatch.hasGroupKey)
  }

  test("ExecuteUdfRequest with DataBatch and GroupKey") {
    val groupKey = GroupKey
      .newBuilder()
      .addKeyColumns("department")
      .addKeyColumns("region")
      .setKeyValuesArrow(ByteString.copyFrom("group-key-values".getBytes))
      .build()

    val dataBatch = DataBatch
      .newBuilder()
      .setBatchId(67890L)
      .setArrowIpcBytes(ByteString.copyFrom("grouped-data".getBytes))
      .setNumRows(500)
      .setNumColumns(3)
      .setGroupKey(groupKey)
      .build()

    val request = ExecuteUdfRequest
      .newBuilder()
      .setUdfId("udf-002")
      .setMessageId(2L)
      .setDataBatch(dataBatch)
      .build()

    val serialized = request.toByteArray
    val parsed = ExecuteUdfRequest.parseFrom(serialized)

    val parsedBatch = parsed.getDataBatch
    assert(parsedBatch.hasGroupKey)

    val parsedGroupKey = parsedBatch.getGroupKey
    assert(parsedGroupKey.getKeyColumnsCount == 2)
    assert(parsedGroupKey.getKeyColumns(0) == "department")
    assert(parsedGroupKey.getKeyColumns(1) == "region")
    assert(parsedGroupKey.getKeyValuesArrow.toStringUtf8 == "group-key-values")
  }

  test("ExecuteUdfRequest with EndOfData") {
    val endOfData = EndOfData
      .newBuilder()
      .setTotalBatchesSent(100)
      .setTotalRowsSent(50000)
      .build()

    val request = ExecuteUdfRequest
      .newBuilder()
      .setUdfId("udf-003")
      .setMessageId(101L)
      .setEndOfData(endOfData)
      .build()

    val serialized = request.toByteArray
    val parsed = ExecuteUdfRequest.parseFrom(serialized)

    assert(parsed.getRequestTypeCase == ExecuteUdfRequest.RequestTypeCase.END_OF_DATA)

    val parsedEndOfData = parsed.getEndOfData
    assert(parsedEndOfData.getTotalBatchesSent == 100)
    assert(parsedEndOfData.getTotalRowsSent == 50000)
  }

  test("ExecuteUdfRequest with CancelExecution") {
    val cancel = CancelExecution
      .newBuilder()
      .setReason("user interrupt")
      .build()

    val request = ExecuteUdfRequest
      .newBuilder()
      .setUdfId("udf-004")
      .setMessageId(50L)
      .setCancel(cancel)
      .build()

    val serialized = request.toByteArray
    val parsed = ExecuteUdfRequest.parseFrom(serialized)

    assert(parsed.getRequestTypeCase == ExecuteUdfRequest.RequestTypeCase.CANCEL)
    assert(parsed.getCancel.getReason == "user interrupt")
  }

  test("ExecuteUdfResponse with DataBatchResult") {
    val result = DataBatchResult
      .newBuilder()
      .setBatchId(12345L)
      .setArrowIpcBytes(ByteString.copyFrom("output-arrow-bytes".getBytes))
      .setNumRows(1000)
      .setProcessingTimeNs(123456789L)
      .build()

    val response = ExecuteUdfResponse
      .newBuilder()
      .setUdfId("udf-001")
      .setMessageId(1L)
      .setDataBatchResult(result)
      .build()

    val serialized = response.toByteArray
    val parsed = ExecuteUdfResponse.parseFrom(serialized)

    assert(parsed.getUdfId == "udf-001")
    assert(parsed.getMessageId == 1L)
    assert(parsed.getResponseTypeCase == ExecuteUdfResponse.ResponseTypeCase.DATA_BATCH_RESULT)

    val parsedResult = parsed.getDataBatchResult
    assert(parsedResult.getBatchId == 12345L)
    assert(parsedResult.getArrowIpcBytes.toStringUtf8 == "output-arrow-bytes")
    assert(parsedResult.getNumRows == 1000)
    assert(parsedResult.getProcessingTimeNs == 123456789L)
  }

  test("ExecuteUdfResponse with BackpressureSignal") {
    val backpressure = BackpressureSignal
      .newBuilder()
      .setBackoffMs(500)
      .setReason("high memory usage")
      .build()

    val response = ExecuteUdfResponse
      .newBuilder()
      .setUdfId("udf-002")
      .setMessageId(5L)
      .setBackpressure(backpressure)
      .build()

    val serialized = response.toByteArray
    val parsed = ExecuteUdfResponse.parseFrom(serialized)

    assert(parsed.getResponseTypeCase == ExecuteUdfResponse.ResponseTypeCase.BACKPRESSURE)

    val parsedBackpressure = parsed.getBackpressure
    assert(parsedBackpressure.getBackoffMs == 500)
    assert(parsedBackpressure.getReason == "high memory usage")
  }

  test("ExecuteUdfResponse with UdfError - all severity levels") {
    // Test RETRYABLE error
    val retryableError = UdfError
      .newBuilder()
      .setSeverity(ErrorSeverity.RETRYABLE)
      .setCategory(ErrorCategory.RESOURCE_LIMIT)
      .setMessage("Temporary network timeout")
      .setStackTrace("at line 42\n  at module.func")
      .setBatchId(100L)
      .setRowOffset(50)
      .putMetadata("retry_count", "3")
      .build()

    var response = ExecuteUdfResponse
      .newBuilder()
      .setUdfId("udf-003")
      .setMessageId(10L)
      .setError(retryableError)
      .build()

    var serialized = response.toByteArray
    var parsed = ExecuteUdfResponse.parseFrom(serialized)
    var parsedError = parsed.getError

    assert(parsedError.getSeverity == ErrorSeverity.RETRYABLE)
    assert(parsedError.getCategory == ErrorCategory.RESOURCE_LIMIT)
    assert(parsedError.getMessage == "Temporary network timeout")
    assert(parsedError.getStackTrace == "at line 42\n  at module.func")
    assert(parsedError.getBatchId == 100L)
    assert(parsedError.getRowOffset == 50)
    assert(parsedError.getMetadataOrThrow("retry_count") == "3")

    // Test FATAL error
    val fatalError = UdfError
      .newBuilder()
      .setSeverity(ErrorSeverity.FATAL)
      .setCategory(ErrorCategory.UDF_EXECUTION)
      .setMessage("Division by zero")
      .build()

    response = ExecuteUdfResponse
      .newBuilder()
      .setUdfId("udf-004")
      .setMessageId(11L)
      .setError(fatalError)
      .build()

    serialized = response.toByteArray
    parsed = ExecuteUdfResponse.parseFrom(serialized)
    parsedError = parsed.getError

    assert(parsedError.getSeverity == ErrorSeverity.FATAL)
    assert(parsedError.getCategory == ErrorCategory.UDF_EXECUTION)

    // Test WORKER_CRASHED error
    val crashedError = UdfError
      .newBuilder()
      .setSeverity(ErrorSeverity.WORKER_CRASHED)
      .setCategory(ErrorCategory.INTERNAL)
      .setMessage("Worker process terminated unexpectedly")
      .build()

    response = ExecuteUdfResponse
      .newBuilder()
      .setUdfId("udf-005")
      .setMessageId(12L)
      .setError(crashedError)
      .build()

    serialized = response.toByteArray
    parsed = ExecuteUdfResponse.parseFrom(serialized)
    parsedError = parsed.getError

    assert(parsedError.getSeverity == ErrorSeverity.WORKER_CRASHED)
    assert(parsedError.getCategory == ErrorCategory.INTERNAL)
  }

  test("ExecuteUdfResponse with UdfError - all error categories") {
    val categories = Seq(
      ErrorCategory.UDF_EXECUTION,
      ErrorCategory.SERIALIZATION,
      ErrorCategory.RESOURCE_LIMIT,
      ErrorCategory.PROTOCOL,
      ErrorCategory.INTERNAL
    )

    categories.foreach { category =>
      val error = UdfError
        .newBuilder()
        .setSeverity(ErrorSeverity.FATAL)
        .setCategory(category)
        .setMessage(s"Error in category $category")
        .build()

      val response = ExecuteUdfResponse
        .newBuilder()
        .setUdfId("udf-test")
        .setMessageId(1L)
        .setError(error)
        .build()

      val serialized = response.toByteArray
      val parsed = ExecuteUdfResponse.parseFrom(serialized)

      assert(parsed.getError.getCategory == category)
    }
  }

  test("ExecuteUdfResponse with EndOfDataResult") {
    val endResult = EndOfDataResult
      .newBuilder()
      .setTotalBatchesProcessed(100)
      .setTotalRowsProcessed(50000)
      .setTotalProcessingTimeNs(987654321000L)
      .putMetrics("cache_hits", "750")
      .putMetrics("model_inferences", "50000")
      .build()

    val response = ExecuteUdfResponse
      .newBuilder()
      .setUdfId("udf-006")
      .setMessageId(101L)
      .setEndOfDataResult(endResult)
      .build()

    val serialized = response.toByteArray
    val parsed = ExecuteUdfResponse.parseFrom(serialized)

    assert(parsed.getResponseTypeCase == ExecuteUdfResponse.ResponseTypeCase.END_OF_DATA_RESULT)

    val parsedResult = parsed.getEndOfDataResult
    assert(parsedResult.getTotalBatchesProcessed == 100)
    assert(parsedResult.getTotalRowsProcessed == 50000)
    assert(parsedResult.getTotalProcessingTimeNs == 987654321000L)
    assert(parsedResult.getMetricsCount == 2)
    assert(parsedResult.getMetricsOrThrow("cache_hits") == "750")
    assert(parsedResult.getMetricsOrThrow("model_inferences") == "50000")
  }

  test("HeartbeatRequest round-trip") {
    val request = HeartbeatRequest
      .newBuilder()
      .setTimestampNs(1234567890123456789L)
      .build()

    val serialized = request.toByteArray
    val parsed = HeartbeatRequest.parseFrom(serialized)

    assert(parsed.getTimestampNs == 1234567890123456789L)
  }

  test("HeartbeatResponse with WorkerMetrics") {
    val metrics = WorkerMetrics
      .newBuilder()
      .setHeapUsedBytes(2147483648L) // 2GB
      .setHeapTotalBytes(4294967296L) // 4GB
      .setCpuUsagePercent(75.5)
      .setTotalBatchesProcessed(1000)
      .setTotalRowsProcessed(500000)
      .setActiveUdfCount(3)
      .build()

    val response = HeartbeatResponse
      .newBuilder()
      .setStatus(HealthStatus.HEALTHY)
      .setMetrics(metrics)
      .build()

    val serialized = response.toByteArray
    val parsed = HeartbeatResponse.parseFrom(serialized)

    assert(parsed.getStatus == HealthStatus.HEALTHY)
    assert(parsed.hasMetrics)

    val parsedMetrics = parsed.getMetrics
    assert(parsedMetrics.getHeapUsedBytes == 2147483648L)
    assert(parsedMetrics.getHeapTotalBytes == 4294967296L)
    assert(parsedMetrics.getCpuUsagePercent == 75.5)
    assert(parsedMetrics.getTotalBatchesProcessed == 1000)
    assert(parsedMetrics.getTotalRowsProcessed == 500000)
    assert(parsedMetrics.getActiveUdfCount == 3)
  }

  test("HeartbeatResponse with different health statuses") {
    val healthStatuses = Seq(
      HealthStatus.HEALTHY,
      HealthStatus.DEGRADED,
      HealthStatus.UNHEALTHY
    )

    healthStatuses.foreach { status =>
      val response = HeartbeatResponse
        .newBuilder()
        .setStatus(status)
        .build()

      val serialized = response.toByteArray
      val parsed = HeartbeatResponse.parseFrom(serialized)

      assert(parsed.getStatus == status)
    }
  }

  test("CleanupWorkerRequest round-trip") {
    val request = CleanupWorkerRequest
      .newBuilder()
      .setUdfId("udf-007")
      .setForceGc(true)
      .setClearGlobalState(true)
      .build()

    val serialized = request.toByteArray
    val parsed = CleanupWorkerRequest.parseFrom(serialized)

    assert(parsed.getUdfId == "udf-007")
    assert(parsed.getForceGc == true)
    assert(parsed.getClearGlobalState == true)
  }

  test("CleanupWorkerRequest for all UDFs") {
    val request = CleanupWorkerRequest
      .newBuilder()
      .setUdfId("") // Empty means clean up all
      .setForceGc(true)
      .setClearGlobalState(false)
      .build()

    val serialized = request.toByteArray
    val parsed = CleanupWorkerRequest.parseFrom(serialized)

    assert(parsed.getUdfId.isEmpty)
    assert(parsed.getForceGc == true)
    assert(parsed.getClearGlobalState == false)
  }

  test("CleanupWorkerResponse round-trip") {
    val response = CleanupWorkerResponse
      .newBuilder()
      .setSuccess(true)
      .setHeapBytesFreed(536870912L) // 512MB
      .build()

    val serialized = response.toByteArray
    val parsed = CleanupWorkerResponse.parseFrom(serialized)

    assert(parsed.getSuccess == true)
    assert(parsed.getHeapBytesFreed == 536870912L)
    assert(parsed.getErrorMessage.isEmpty)
  }

  test("CleanupWorkerResponse with error") {
    val response = CleanupWorkerResponse
      .newBuilder()
      .setSuccess(false)
      .setHeapBytesFreed(0L)
      .setErrorMessage("Failed to cleanup: UDF still in use")
      .build()

    val serialized = response.toByteArray
    val parsed = CleanupWorkerResponse.parseFrom(serialized)

    assert(parsed.getSuccess == false)
    assert(parsed.getHeapBytesFreed == 0L)
    assert(parsed.getErrorMessage == "Failed to cleanup: UDF still in use")
  }

  test("ShutdownRequest with all shutdown reasons") {
    val shutdownReasons = Seq(
      ShutdownReason.IDLE_TIMEOUT,
      ShutdownReason.EXECUTOR_SHUTTING_DOWN,
      ShutdownReason.RESOURCE_PRESSURE,
      ShutdownReason.ADMIN_REQUEST
    )

    shutdownReasons.foreach { reason =>
      val request = ShutdownRequest
        .newBuilder()
        .setReason(reason)
        .setGracePeriodMs(5000)
        .build()

      val serialized = request.toByteArray
      val parsed = ShutdownRequest.parseFrom(serialized)

      assert(parsed.getReason == reason)
      assert(parsed.getGracePeriodMs == 5000)
    }
  }

  test("ShutdownResponse with final metrics") {
    val finalMetrics = WorkerMetrics
      .newBuilder()
      .setHeapUsedBytes(1073741824L)
      .setHeapTotalBytes(4294967296L)
      .setCpuUsagePercent(15.0)
      .setTotalBatchesProcessed(5000)
      .setTotalRowsProcessed(2500000)
      .setActiveUdfCount(0)
      .build()

    val response = ShutdownResponse
      .newBuilder()
      .setAcknowledged(true)
      .setFinalMetrics(finalMetrics)
      .build()

    val serialized = response.toByteArray
    val parsed = ShutdownResponse.parseFrom(serialized)

    assert(parsed.getAcknowledged == true)
    assert(parsed.hasFinalMetrics)

    val parsedMetrics = parsed.getFinalMetrics
    assert(parsedMetrics.getTotalBatchesProcessed == 5000)
    assert(parsedMetrics.getTotalRowsProcessed == 2500000)
    assert(parsedMetrics.getActiveUdfCount == 0)
  }

  test("All UdfMode enum values") {
    val modes = Seq(
      UdfMode.UDF_MODE_UNSPECIFIED,
      UdfMode.SCALAR,
      UdfMode.VECTORIZED,
      UdfMode.GROUPED_MAP,
      UdfMode.TABLE
    )

    modes.foreach { mode =>
      val capabilities = WorkerCapabilities
        .newBuilder()
        .addSupportedModes(mode)
        .build()

      val serialized = capabilities.toByteArray
      val parsed = WorkerCapabilities.parseFrom(serialized)

      assert(parsed.getSupportedModes(0) == mode)
    }
  }

  test("All WorkerStatus enum values") {
    val statuses = Seq(
      WorkerStatus.WORKER_STATUS_UNSPECIFIED,
      WorkerStatus.ACCEPTED,
      WorkerStatus.REJECTED,
      WorkerStatus.VERSION_MISMATCH
    )

    statuses.foreach { status =>
      val response = RegisterWorkerResponse
        .newBuilder()
        .setStatus(status)
        .build()

      val serialized = response.toByteArray
      val parsed = RegisterWorkerResponse.parseFrom(serialized)

      assert(parsed.getStatus == status)
    }
  }

  test("All InitializeUdfStatus enum values") {
    val statuses = Seq(
      InitializeUdfStatus.INITIALIZE_UDF_STATUS_UNSPECIFIED,
      InitializeUdfStatus.OK,
      InitializeUdfStatus.SCHEMA_MISMATCH,
      InitializeUdfStatus.PAYLOAD_ERROR,
      InitializeUdfStatus.UNSUPPORTED_MODE,
      InitializeUdfStatus.RESOURCE_EXCEEDED
    )

    statuses.foreach { status =>
      val response = InitializeUdfResponse
        .newBuilder()
        .setStatus(status)
        .build()

      val serialized = response.toByteArray
      val parsed = InitializeUdfResponse.parseFrom(serialized)

      assert(parsed.getStatus == status)
    }
  }

  test("All ErrorSeverity enum values") {
    val severities = Seq(
      ErrorSeverity.ERROR_SEVERITY_UNSPECIFIED,
      ErrorSeverity.RETRYABLE,
      ErrorSeverity.FATAL,
      ErrorSeverity.WORKER_CRASHED
    )

    severities.foreach { severity =>
      val error = UdfError
        .newBuilder()
        .setSeverity(severity)
        .setCategory(ErrorCategory.UDF_EXECUTION)
        .setMessage("Test error")
        .build()

      val response = ExecuteUdfResponse
        .newBuilder()
        .setUdfId("test")
        .setMessageId(1L)
        .setError(error)
        .build()

      val serialized = response.toByteArray
      val parsed = ExecuteUdfResponse.parseFrom(serialized)

      assert(parsed.getError.getSeverity == severity)
    }
  }

  test("All ErrorCategory enum values") {
    val categories = Seq(
      ErrorCategory.ERROR_CATEGORY_UNSPECIFIED,
      ErrorCategory.UDF_EXECUTION,
      ErrorCategory.SERIALIZATION,
      ErrorCategory.RESOURCE_LIMIT,
      ErrorCategory.PROTOCOL,
      ErrorCategory.INTERNAL
    )

    categories.foreach { category =>
      val error = UdfError
        .newBuilder()
        .setSeverity(ErrorSeverity.FATAL)
        .setCategory(category)
        .setMessage("Test error")
        .build()

      val response = ExecuteUdfResponse
        .newBuilder()
        .setUdfId("test")
        .setMessageId(1L)
        .setError(error)
        .build()

      val serialized = response.toByteArray
      val parsed = ExecuteUdfResponse.parseFrom(serialized)

      assert(parsed.getError.getCategory == category)
    }
  }

  test("All HealthStatus enum values") {
    val healthStatuses = Seq(
      HealthStatus.HEALTH_STATUS_UNSPECIFIED,
      HealthStatus.HEALTHY,
      HealthStatus.DEGRADED,
      HealthStatus.UNHEALTHY
    )

    healthStatuses.foreach { status =>
      val response = HeartbeatResponse
        .newBuilder()
        .setStatus(status)
        .build()

      val serialized = response.toByteArray
      val parsed = HeartbeatResponse.parseFrom(serialized)

      assert(parsed.getStatus == status)
    }
  }

  test("All ShutdownReason enum values") {
    val shutdownReasons = Seq(
      ShutdownReason.SHUTDOWN_REASON_UNSPECIFIED,
      ShutdownReason.IDLE_TIMEOUT,
      ShutdownReason.EXECUTOR_SHUTTING_DOWN,
      ShutdownReason.RESOURCE_PRESSURE,
      ShutdownReason.ADMIN_REQUEST
    )

    shutdownReasons.foreach { reason =>
      val request = ShutdownRequest
        .newBuilder()
        .setReason(reason)
        .build()

      val serialized = request.toByteArray
      val parsed = ShutdownRequest.parseFrom(serialized)

      assert(parsed.getReason == reason)
    }
  }
}
