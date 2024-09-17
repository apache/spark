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

package org.apache.spark.util

import java.util.Properties

import scala.jdk.CollectionConverters._
import scala.language.implicitConversions

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.databind.node.{ObjectNode, TextNode}
import org.json4s.JsonAST.{JArray, JInt, JString, JValue}
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import org.scalatest.Assertions
import org.scalatest.exceptions.TestFailedException

import org.apache.spark._
import org.apache.spark.executor._
import org.apache.spark.metrics.ExecutorMetricType
import org.apache.spark.rdd.{DeterministicLevel, RDDOperationScope}
import org.apache.spark.resource._
import org.apache.spark.resource.ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.cluster.ExecutorInfo
import org.apache.spark.shuffle.MetadataFetchFailedException
import org.apache.spark.storage._

class JsonProtocolSuite extends SparkFunSuite {
  import JsonProtocol._
  import JsonProtocolSuite._

  test("SparkListenerEvent") {
    val stageSubmitted =
      SparkListenerStageSubmitted(
        makeStageInfo(100, 200, 300, 400L, 500L, includeAccumulables = false), properties)
    val stageSubmittedWithNullProperties =
      SparkListenerStageSubmitted(
        makeStageInfo(100, 200, 300, 400L, 500L, includeAccumulables = false), properties = null)
    val stageCompleted = SparkListenerStageCompleted(makeStageInfo(101, 201, 301, 401L, 501L))
    val taskStart =
      SparkListenerTaskStart(
        111,
        0,
        makeTaskInfo(222L, 333, 1, 333, 444L, speculative = false, includeAccumulables = false))
    val taskGettingResult = SparkListenerTaskGettingResult(
        makeTaskInfo(1000L, 2000, 5, 2000, 3000L, speculative = true, includeAccumulables = false))
    val taskEnd = SparkListenerTaskEnd(1, 0, "ShuffleMapTask", Success,
      makeTaskInfo(123L, 234, 67, 234, 345L, false),
      new ExecutorMetrics(Array(543L, 123456L, 12345L, 1234L, 123L, 12L, 432L,
        321L, 654L, 765L, 256912L, 123456L, 123456L, 61728L, 30364L, 15182L,
        0, 0, 0, 0, 80001L, 3, 3)),
      makeTaskMetrics(300L, 400L, 500L, 600L, 700, 800, 0,
        hasHadoopInput = false, hasOutput = false))
    val taskEndWithHadoopInput = SparkListenerTaskEnd(1, 0, "ShuffleMapTask", Success,
      makeTaskInfo(123L, 234, 67, 234, 345L, false),
      new ExecutorMetrics(Array(543L, 123456L, 12345L, 1234L, 123L, 12L, 432L,
        321L, 654L, 765L, 256912L, 123456L, 123456L, 61728L, 30364L, 15182L,
        0, 0, 0, 0, 80001L, 3, 3)),
      makeTaskMetrics(300L, 400L, 500L, 600L, 700, 800, 0,
        hasHadoopInput = true, hasOutput = false))
    val taskEndWithOutput = SparkListenerTaskEnd(1, 0, "ResultTask", Success,
      makeTaskInfo(123L, 234, 67, 234, 345L, false),
      new ExecutorMetrics(Array(543L, 123456L, 12345L, 1234L, 123L, 12L, 432L,
        321L, 654L, 765L, 256912L, 123456L, 123456L, 61728L, 30364L, 15182L,
        0, 0, 0, 0, 80001L, 3, 3)),
      makeTaskMetrics(300L, 400L, 500L, 600L, 700, 800, 0,
        hasHadoopInput = true, hasOutput = true))
    val jobStart = {
      val stageIds = Seq[Int](1, 2, 3, 4)
      val stageInfos = stageIds.map(x =>
        makeStageInfo(x, x * 200, x * 300, x * 400L, x * 500L))
      SparkListenerJobStart(10, jobSubmissionTime, stageInfos, properties)
    }
    val jobStartWithNullProperties = {
      SparkListenerJobStart(10, jobSubmissionTime, stageInfos = Seq.empty, properties = null)
    }
    val jobEnd = SparkListenerJobEnd(20, jobCompletionTime, JobSucceeded)
    val environmentUpdate = SparkListenerEnvironmentUpdate(Map[String, Seq[(String, String)]](
      "JVM Information" -> Seq(("GC speed", "9999 objects/s"), ("Java home", "Land of coffee")),
      "Spark Properties" -> Seq(("Job throughput", "80000 jobs/s, regardless of job type")),
      "Hadoop Properties" -> Seq(("hadoop.tmp.dir", "/usr/local/hadoop/tmp")),
      "System Properties" -> Seq(("Username", "guest"), ("Password", "guest")),
      "Metrics Properties" ->
        Seq(("*.sink.servlet.class", "org.apache.spark.metrics.sink.MetricsServlet")),
      "Classpath Entries" -> Seq(("Super library", "/tmp/super_library"))
    ))
    val blockManagerAdded = SparkListenerBlockManagerAdded(1L,
      BlockManagerId("Stars", "In your multitude...", 300), 500)
    val blockManagerRemoved = SparkListenerBlockManagerRemoved(2L,
      BlockManagerId("Scarce", "to be counted...", 100))
    val unpersistRdd = SparkListenerUnpersistRDD(12345)
    val logUrlMap = Map("stderr" -> "mystderr", "stdout" -> "mystdout")
    val attributes = Map("ContainerId" -> "ct1", "User" -> "spark")
    val resources = Map(ResourceUtils.GPU ->
      new ResourceInformation(ResourceUtils.GPU, Array("0", "1")))
    val applicationStart = SparkListenerApplicationStart("The winner of all", Some("appId"),
      42L, "Garfield", Some("appAttempt"))
    val applicationStartWithLogs = SparkListenerApplicationStart("The winner of all", Some("appId"),
      42L, "Garfield", Some("appAttempt"), Some(logUrlMap))
    val applicationEnd = SparkListenerApplicationEnd(42L)
    val executorAdded = SparkListenerExecutorAdded(executorAddedTime, "exec1",
      new ExecutorInfo("Hostee.awesome.com", 11, logUrlMap, attributes, resources, 4))
    val executorAddedWithTime = SparkListenerExecutorAdded(executorAddedTime, "exec1",
      new ExecutorInfo("Hostee.awesome.com", 11, logUrlMap, attributes, resources, 4,
        Some(1), Some(0)))
    val executorRemoved = SparkListenerExecutorRemoved(executorRemovedTime, "exec2", "test reason")
    val executorBlacklisted = SparkListenerExecutorBlacklisted(executorExcludedTime, "exec1", 22)
    val executorUnblacklisted =
      SparkListenerExecutorUnblacklisted(executorUnexcludedTime, "exec1")
    val nodeBlacklisted = SparkListenerNodeBlacklisted(nodeExcludedTime, "node1", 33)
    val executorExcluded = SparkListenerExecutorExcluded(executorExcludedTime, "exec1", 22)
    val executorUnexcluded =
      SparkListenerExecutorUnexcluded(executorUnexcludedTime, "exec1")
    val nodeExcluded = SparkListenerNodeExcluded(nodeExcludedTime, "node1", 33)
    val nodeUnblacklisted =
      SparkListenerNodeUnblacklisted(nodeUnexcludedTime, "node1")
    val nodeUnexcluded =
      SparkListenerNodeUnexcluded(nodeUnexcludedTime, "node1")
    val executorMetricsUpdate = {
      // Use custom accum ID for determinism
      val accumUpdates =
        makeTaskMetrics(300L, 400L, 500L, 600L, 700, 800, 0,
          hasHadoopInput = true, hasOutput = true)
          .accumulators().map(AccumulatorSuite.makeInfo)
          .zipWithIndex.map { case (a, i) => a.copy(id = i) }
      val executorUpdates = new ExecutorMetrics(
        Array(543L, 123456L, 12345L, 1234L, 123L, 12L, 432L,
          321L, 654L, 765L, 256912L, 123456L, 123456L, 61728L,
          30364L, 15182L, 10L, 90L, 2L, 20L, 80001L, 3, 3))
      SparkListenerExecutorMetricsUpdate("exec3", Seq((1L, 2, 3, accumUpdates)),
        Map((0, 0) -> executorUpdates))
    }
    val blockUpdated =
      SparkListenerBlockUpdated(BlockUpdatedInfo(BlockManagerId("Stars",
        "In your multitude...", 300), RDDBlockId(0, 0), StorageLevel.MEMORY_ONLY, 100L, 0L))
    val stageExecutorMetrics =
      SparkListenerStageExecutorMetrics("1", 2, 3,
        new ExecutorMetrics(Array(543L, 123456L, 12345L, 1234L, 123L, 12L, 432L,
          321L, 654L, 765L, 256912L, 123456L, 123456L, 61728L,
          30364L, 15182L, 10L, 90L, 2L, 20L, 80001L, 3, 3)))
    val rprofBuilder = new ResourceProfileBuilder()
    val taskReq = new TaskResourceRequests()
      .cpus(1)
      .resource("gpu", 1)
      .resource("fgpa", 0.5)
    val execReq: ExecutorResourceRequests = new ExecutorResourceRequests()
      .cores(2)
      .resource("gpu", 2, "myscript")
      .resource("myCustomResource", amount = Int.MaxValue + 1L, discoveryScript = "myscript2")
    rprofBuilder.require(taskReq).require(execReq)
    val resourceProfile = rprofBuilder.build()
    resourceProfile.setResourceProfileId(21)
    val resourceProfileAdded = SparkListenerResourceProfileAdded(resourceProfile)
    testEvent(stageSubmitted, stageSubmittedJsonString)
    testEvent(stageSubmittedWithNullProperties, stageSubmittedWithNullPropertiesJsonString)
    testEvent(stageCompleted, stageCompletedJsonString)
    testEvent(taskStart, taskStartJsonString)
    testEvent(taskGettingResult, taskGettingResultJsonString)
    testEvent(taskEnd, taskEndJsonString)
    testEvent(taskEndWithHadoopInput, taskEndWithHadoopInputJsonString)
    testEvent(taskEndWithOutput, taskEndWithOutputJsonString)
    testEvent(jobStart, jobStartJsonString)
    testEvent(jobStartWithNullProperties, jobStartWithNullPropertiesJsonString)
    testEvent(jobEnd, jobEndJsonString)
    testEvent(environmentUpdate, environmentUpdateJsonString)
    testEvent(blockManagerAdded, blockManagerAddedJsonString)
    testEvent(blockManagerRemoved, blockManagerRemovedJsonString)
    testEvent(unpersistRdd, unpersistRDDJsonString)
    testEvent(applicationStart, applicationStartJsonString)
    testEvent(applicationStartWithLogs, applicationStartJsonWithLogUrlsString)
    testEvent(applicationEnd, applicationEndJsonString)
    testEvent(executorAdded, executorAddedJsonString)
    testEvent(executorAddedWithTime, executorAddedWithTimeJsonString)
    testEvent(executorRemoved, executorRemovedJsonString)
    testEvent(executorBlacklisted, executorBlacklistedJsonString)
    testEvent(executorUnblacklisted, executorUnblacklistedJsonString)
    testEvent(executorExcluded, executorExcludedJsonString)
    testEvent(executorUnexcluded, executorUnexcludedJsonString)
    testEvent(nodeBlacklisted, nodeBlacklistedJsonString)
    testEvent(nodeUnblacklisted, nodeUnblacklistedJsonString)
    testEvent(nodeExcluded, nodeExcludedJsonString)
    testEvent(nodeUnexcluded, nodeUnexcludedJsonString)
    testEvent(executorMetricsUpdate, executorMetricsUpdateJsonString)
    testEvent(blockUpdated, blockUpdatedJsonString)
    testEvent(stageExecutorMetrics, stageExecutorMetricsJsonString)
    testEvent(resourceProfileAdded, resourceProfileJsonString)
  }

  test("Dependent Classes") {
    val logUrlMap = Map("stderr" -> "mystderr", "stdout" -> "mystdout")
    val attributes = Map("ContainerId" -> "ct1", "User" -> "spark")
    val rinfo = Map[String, ResourceInformation]()
    testRDDInfo(makeRddInfo(2, 3, 4, 5L, 6L, DeterministicLevel.DETERMINATE))
    testStageInfo(makeStageInfo(10, 20, 30, 40L, 50L))
    testTaskInfo(makeTaskInfo(999L, 888, 55, 888, 777L, false))
    testTaskMetrics(makeTaskMetrics(
      33333L, 44444L, 55555L, 66666L, 7, 8, 0, hasHadoopInput = false, hasOutput = false))
    testBlockManagerId(BlockManagerId("Hong", "Kong", 500))
    testExecutorInfo(new ExecutorInfo("host", 43, logUrlMap, attributes))
    testExecutorInfo(new ExecutorInfo("host", 43, logUrlMap, attributes,
      rinfo, 1, Some(1), Some(0)))

    // StorageLevel
    testStorageLevel(StorageLevel.NONE)
    testStorageLevel(StorageLevel.DISK_ONLY)
    testStorageLevel(StorageLevel.DISK_ONLY_2)
    testStorageLevel(StorageLevel.DISK_ONLY_3)
    testStorageLevel(StorageLevel.MEMORY_ONLY)
    testStorageLevel(StorageLevel.MEMORY_ONLY_2)
    testStorageLevel(StorageLevel.MEMORY_ONLY_SER)
    testStorageLevel(StorageLevel.MEMORY_ONLY_SER_2)
    testStorageLevel(StorageLevel.MEMORY_AND_DISK)
    testStorageLevel(StorageLevel.MEMORY_AND_DISK_2)
    testStorageLevel(StorageLevel.MEMORY_AND_DISK_SER)
    testStorageLevel(StorageLevel.MEMORY_AND_DISK_SER_2)
    testStorageLevel(StorageLevel.OFF_HEAP)

    // JobResult
    val exception = new Exception("Out of Memory! Please restock film.")
    exception.setStackTrace(stackTrace)
    val jobFailed = JobFailed(exception)
    testJobResult(JobSucceeded)
    testJobResult(jobFailed)

    // TaskEndReason
    val fetchFailed = FetchFailed(BlockManagerId("With or", "without you", 15), 17, 16L, 18, 19,
      "Some exception")
    val fetchMetadataFailed = new MetadataFetchFailedException(17,
      19, "metadata Fetch failed exception").toTaskFailedReason
    val exceptionFailure = new ExceptionFailure(exception, Seq.empty[AccumulableInfo])
    testTaskEndReason(Success)
    testTaskEndReason(Resubmitted)
    testTaskEndReason(fetchFailed)
    testTaskEndReason(fetchMetadataFailed)
    testTaskEndReason(exceptionFailure)
    testTaskEndReason(TaskResultLost)
    testTaskEndReason(TaskKilled("test"))
    testTaskEndReason(TaskCommitDenied(2, 3, 4))
    testTaskEndReason(ExecutorLostFailure("100", true, Some("Induced failure")))
    testTaskEndReason(UnknownReason)

    // BlockId
    testBlockId(RDDBlockId(1, 2))
    testBlockId(ShuffleBlockId(1, 2, 3))
    testBlockId(BroadcastBlockId(1L, "insert_words_of_wisdom_here"))
    testBlockId(TaskResultBlockId(1L))
    testBlockId(StreamBlockId(1, 2L))
  }

  /* ============================== *
   |  Backward compatibility tests  |
   * ============================== */

  test("ExceptionFailure backward compatibility: full stack trace") {
    val exceptionFailure = ExceptionFailure("To be", "or not to be", stackTrace, null, None)
    val oldEvent = toJsonString(JsonProtocol.taskEndReasonToJson(exceptionFailure, _))
      .removeField("Full Stack Trace")
    assertEquals(exceptionFailure, JsonProtocol.taskEndReasonFromJson(oldEvent))
  }

  test("StageInfo backward compatibility (details, accumulables)") {
    val info = makeStageInfo(1, 2, 3, 4L, 5L)
    val newJson = toJsonString(JsonProtocol.stageInfoToJson(info, _, includeAccumulables = true))

    // Fields added after 1.0.0.
    assert(info.details.nonEmpty)
    assert(info.accumulables.nonEmpty)
    val oldJson = newJson
      .removeField("Details")
      .removeField("Accumulables")

    val newInfo = JsonProtocol.stageInfoFromJson(oldJson)

    assert(info.name === newInfo.name)
    assert("" === newInfo.details)
    assert(0 === newInfo.accumulables.size)
  }

  test("StageInfo resourceProfileId") {
    val info = makeStageInfo(1, 2, 3, 4L, 5L, 5)
    val json = toJsonString(JsonProtocol.stageInfoToJson(info, _, includeAccumulables = true))

    // Fields added after 1.0.0.
    assert(info.details.nonEmpty)
    assert(info.resourceProfileId === 5)

    val newInfo = JsonProtocol.stageInfoFromJson(json)

    assert(info.name === newInfo.name)
    assert(5 === newInfo.resourceProfileId)
  }

  test("InputMetrics backward compatibility") {
    // InputMetrics were added after 1.0.1.
    val metrics = makeTaskMetrics(1L, 2L, 3L, 4L, 5, 6, 0, hasHadoopInput = true, hasOutput = false)
    val newJson = toJsonString(JsonProtocol.taskMetricsToJson(metrics, _))
    val oldJson = newJson.removeField("Input Metrics")
    val newMetrics = JsonProtocol.taskMetricsFromJson(oldJson)
    assert(newMetrics.inputMetrics.recordsRead == 0)
    assert(newMetrics.inputMetrics.bytesRead == 0)
  }

  test("Input/Output records backwards compatibility") {
    // records read were added after 1.2
    val metrics = makeTaskMetrics(1L, 2L, 3L, 4L, 5, 6, 0,
      hasHadoopInput = true, hasOutput = true, hasRecords = false)
    val newJson = toJsonString(JsonProtocol.taskMetricsToJson(metrics, _))
    val oldJson = newJson
      .removeField("Records Read")
      .removeField("Records Written")
    val newMetrics = JsonProtocol.taskMetricsFromJson(oldJson)
    assert(newMetrics.inputMetrics.recordsRead == 0)
    assert(newMetrics.outputMetrics.recordsWritten == 0)
  }

  test("Shuffle Read/Write records backwards compatibility") {
    // records read were added after 1.2
    // "Remote Bytes Read To Disk" was added in 2.3.0
    val metrics = makeTaskMetrics(1L, 2L, 3L, 4L, 5, 6, 0,
      hasHadoopInput = false, hasOutput = false, hasRecords = false)
    val newJson = toJsonString(JsonProtocol.taskMetricsToJson(metrics, _))
    val oldJson = newJson
      .removeField("Total Records Read")
      .removeField("Shuffle Records Written")
      .removeField("Remote Bytes Read To Disk")
    val newMetrics = JsonProtocol.taskMetricsFromJson(oldJson)
    assert(newMetrics.shuffleReadMetrics.recordsRead == 0)
    assert(newMetrics.shuffleReadMetrics.remoteBytesReadToDisk == 0)
    assert(newMetrics.shuffleWriteMetrics.recordsWritten == 0)
  }

  test("OutputMetrics backward compatibility") {
    // OutputMetrics were added after 1.1
    val metrics = makeTaskMetrics(1L, 2L, 3L, 4L, 5, 6, 0, hasHadoopInput = false, hasOutput = true)
    val newJson = toJsonString(JsonProtocol.taskMetricsToJson(metrics, _))
    val oldJson = newJson.removeField("Output Metrics")
    val newMetrics = JsonProtocol.taskMetricsFromJson(oldJson)
    assert(newMetrics.outputMetrics.recordsWritten == 0)
    assert(newMetrics.outputMetrics.bytesWritten == 0)
  }

  test("TaskMetrics backward compatibility") {
    // "Executor Deserialize CPU Time" and "Executor CPU Time" were introduced in Spark 2.1.0
    // "Peak Execution Memory" was introduced in Spark 3.0.0
    // "Peak On Heap Execution Memory" and "Peak Off Heap Execution Memory" were introduced in
    // Spark 3.5.0
    val metrics = makeTaskMetrics(1L, 2L, 3L, 4L, 5, 6, 0, hasHadoopInput = false, hasOutput = true)
    metrics.setExecutorDeserializeCpuTime(100L)
    metrics.setExecutorCpuTime(100L)
    metrics.setPeakExecutionMemory(100L)
    val newJson = toJsonString(JsonProtocol.taskMetricsToJson(metrics, _))
    val oldJson = newJson
      .removeField("Executor Deserialize CPU Time")
      .removeField("Executor CPU Time")
      .removeField("Peak Execution Memory")
      .removeField("Peak On Heap Execution Memory")
      .removeField("Peak Off Heap Execution Memory")
    val newMetrics = JsonProtocol.taskMetricsFromJson(oldJson)
    assert(newMetrics.executorDeserializeCpuTime == 0)
    assert(newMetrics.executorCpuTime == 0)
    assert(newMetrics.peakExecutionMemory == 0)
    assert(newMetrics.peakOnHeapExecutionMemory == 0)
    assert(newMetrics.peakOffHeapExecutionMemory == 0)
  }

  test("StorageLevel backward compatibility") {
    // "Use Off Heap" was added in Spark 3.4.0
    val level = StorageLevel(
      useDisk = false,
      useMemory = true,
      useOffHeap = true,
      deserialized = false,
      replication = 1
    )
    val newJson = toJsonString(JsonProtocol.storageLevelToJson(level, _))
    val oldJson = newJson.removeField("Use Off Heap")
    val newLevel = JsonProtocol.storageLevelFromJson(oldJson)
    assert(newLevel.useOffHeap === false)
  }

  test("BlockManager events backward compatibility") {
    // SparkListenerBlockManagerAdded/Removed in Spark 1.0.0 do not have a "time" property.
    val blockManagerAdded = SparkListenerBlockManagerAdded(1L,
      BlockManagerId("Stars", "In your multitude...", 300), 500)
    val blockManagerRemoved = SparkListenerBlockManagerRemoved(2L,
      BlockManagerId("Scarce", "to be counted...", 100))

    val oldBmAdded = toJsonString(JsonProtocol.blockManagerAddedToJson(blockManagerAdded, _))
      .removeField("Timestamp")

    val deserializedBmAdded = JsonProtocol.blockManagerAddedFromJson(oldBmAdded)
    assert(SparkListenerBlockManagerAdded(-1L, blockManagerAdded.blockManagerId,
      blockManagerAdded.maxMem) === deserializedBmAdded)

    val oldBmRemoved = toJsonString(JsonProtocol.blockManagerRemovedToJson(blockManagerRemoved, _))
      .removeField("Timestamp")

    val deserializedBmRemoved = JsonProtocol.blockManagerRemovedFromJson(oldBmRemoved)
    assert(SparkListenerBlockManagerRemoved(-1L, blockManagerRemoved.blockManagerId) ===
      deserializedBmRemoved)
  }

  test("FetchFailed backwards compatibility") {
    // FetchFailed in Spark 1.1.0 does not have a "Message" property.
    val fetchFailed = FetchFailed(BlockManagerId("With or", "without you", 15), 17, 16L, 18, 19,
      "ignored")
    val oldEvent = toJsonString(JsonProtocol.taskEndReasonToJson(fetchFailed, _))
      .removeField("Message")
    val expectedFetchFailed = FetchFailed(BlockManagerId("With or", "without you", 15), 17, 16L,
      18, 19, "Unknown reason")
    assert(expectedFetchFailed === JsonProtocol.taskEndReasonFromJson(oldEvent))
  }

  test("SPARK-32124: FetchFailed Map Index backwards compatibility") {
    // FetchFailed in Spark 2.4.0 does not have "Map Index" property.
    val fetchFailed = FetchFailed(BlockManagerId("With or", "without you", 15), 17, 16L, 18, 19,
      "ignored")
    val oldEvent = toJsonString(JsonProtocol.taskEndReasonToJson(fetchFailed, _))
      .removeField("Map Index")
    val expectedFetchFailed = FetchFailed(BlockManagerId("With or", "without you", 15), 17, 16L,
      Int.MinValue, 19, "ignored")
    assert(expectedFetchFailed === JsonProtocol.taskEndReasonFromJson(oldEvent))
  }

  test("ShuffleReadMetrics: Local bytes read backwards compatibility") {
    // Metrics about local shuffle bytes read were added in 1.3.1.
    val metrics = makeTaskMetrics(1L, 2L, 3L, 4L, 5, 6, 0,
      hasHadoopInput = false, hasOutput = false, hasRecords = false)
    val newJson = toJsonString(JsonProtocol.taskMetricsToJson(metrics, _))
    val oldJson = newJson.removeField("Local Bytes Read")
    val newMetrics = JsonProtocol.taskMetricsFromJson(oldJson)
    assert(newMetrics.shuffleReadMetrics.localBytesRead == 0)
  }

  test("SparkListenerApplicationStart backwards compatibility") {
    // SparkListenerApplicationStart in Spark 1.0.0 do not have an "appId" property.
    // SparkListenerApplicationStart pre-Spark 1.4 does not have "appAttemptId".
    // SparkListenerApplicationStart pre-Spark 1.5 does not have "driverLogs
    val applicationStart = SparkListenerApplicationStart("test", None, 1L, "user", None, None)
    val oldEvent = toJsonString(JsonProtocol.applicationStartToJson(applicationStart, _))
      .removeField("App ID")
      .removeField("App Attempt ID")
      .removeField( "Driver Logs")
    assert(applicationStart === JsonProtocol.applicationStartFromJson(oldEvent))
  }

  test("ExecutorLostFailure backward compatibility") {
    // ExecutorLostFailure in Spark 1.1.0 does not have an "Executor ID" property.
    val executorLostFailure = ExecutorLostFailure("100", true, Some("Induced failure"))
    val oldEvent = toJsonString(JsonProtocol.taskEndReasonToJson(executorLostFailure, _))
      .removeField("Executor ID")
    val expectedExecutorLostFailure = ExecutorLostFailure("Unknown", true, Some("Induced failure"))
    assert(expectedExecutorLostFailure === JsonProtocol.taskEndReasonFromJson(oldEvent))
  }

  test("SparkListenerJobStart backward compatibility") {
    // Prior to Spark 1.2.0, SparkListenerJobStart did not have a "Stage Infos" property.
    val stageIds = Seq[Int](1, 2, 3, 4)
    val stageInfos = stageIds.map(x => makeStageInfo(x, x * 200, x * 300, x * 400L, x * 500L))
    val dummyStageInfos =
      stageIds.map(id => new StageInfo(id, 0, "unknown", 0, Seq.empty, Seq.empty, "unknown",
        resourceProfileId = ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID))
    val jobStart = SparkListenerJobStart(10, jobSubmissionTime, stageInfos, properties)
    val oldEvent = toJsonString(JsonProtocol.jobStartToJson(jobStart, _)).removeField("Stage Infos")
    val expectedJobStart =
      SparkListenerJobStart(10, jobSubmissionTime, dummyStageInfos, properties)
    assertEquals(expectedJobStart, JsonProtocol.jobStartFromJson(oldEvent))
  }

  test("SparkListenerJobStart and SparkListenerJobEnd backward compatibility") {
    // Prior to Spark 1.3.0, SparkListenerJobStart did not have a "Submission Time" property.
    // Also, SparkListenerJobEnd did not have a "Completion Time" property.
    val stageIds = Seq[Int](1, 2, 3, 4)
    val stageInfos = stageIds.map(x => makeStageInfo(x * 10, x * 20, x * 30, x * 40L, x * 50L))
    val jobStart = SparkListenerJobStart(11, jobSubmissionTime, stageInfos, properties)
    val oldStartEvent = toJsonString(JsonProtocol.jobStartToJson(jobStart, _))
      .removeField("Submission Time")
    val expectedJobStart = SparkListenerJobStart(11, -1, stageInfos, properties)
    assertEquals(expectedJobStart, JsonProtocol.jobStartFromJson(oldStartEvent))

    val jobEnd = SparkListenerJobEnd(11, jobCompletionTime, JobSucceeded)
    val oldEndEvent = toJsonString(JsonProtocol.jobEndToJson(jobEnd, _))
      .removeField("Completion Time")
    val expectedJobEnd = SparkListenerJobEnd(11, -1, JobSucceeded)
    assertEquals(expectedJobEnd, JsonProtocol.jobEndFromJson(oldEndEvent))
  }

  test("RDDInfo backward compatibility") {
    // "Scope" and "Parent IDs" were introduced in Spark 1.4.0
    // "Callsite" was introduced in Spark 1.6.0
    // "Barrier" was introduced in Spark 3.0.0
    // "DeterministicLevel" was introduced in Spark 3.2.0
    val rddInfo = new RDDInfo(1, "one", 100, StorageLevel.NONE, true, Seq(1, 6, 8),
      "callsite", Some(new RDDOperationScope("fable")), DeterministicLevel.INDETERMINATE)
    val oldRddInfoJson = toJsonString(JsonProtocol.rddInfoToJson(rddInfo, _))
      .removeField("Parent IDs")
      .removeField("Scope")
      .removeField("Callsite")
      .removeField("Barrier")
      .removeField("DeterministicLevel")
    val expectedRddInfo = new RDDInfo(
      1, "one", 100, StorageLevel.NONE, false, Seq.empty, "", scope = None,
      outputDeterministicLevel = DeterministicLevel.INDETERMINATE)
    assertEquals(expectedRddInfo, JsonProtocol.rddInfoFromJson(oldRddInfoJson))
  }

  test("StageInfo backward compatibility (parent IDs)") {
    // Prior to Spark 1.4.0, StageInfo did not have the "Parent IDs" property
    val stageInfo = new StageInfo(1, 1, "me-stage", 1, Seq.empty, Seq(1, 2, 3), "details",
      resourceProfileId = ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID)
    val oldStageInfo =
      toJsonString(JsonProtocol.stageInfoToJson(stageInfo, _, includeAccumulables = true))
        .removeField("Parent IDs")
    val expectedStageInfo = new StageInfo(1, 1, "me-stage", 1, Seq.empty, Seq.empty, "details",
      resourceProfileId = ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID)
    assertEquals(expectedStageInfo, JsonProtocol.stageInfoFromJson(oldStageInfo))
  }

  // `TaskCommitDenied` was added in 1.3.0 but JSON de/serialization logic was added in 1.5.1
  test("TaskCommitDenied backward compatibility") {
    val denied = TaskCommitDenied(1, 2, 3)
    val oldDenied = toJsonString(JsonProtocol.taskEndReasonToJson(denied, _))
      .removeField("Job ID")
      .removeField("Partition ID")
      .removeField("Attempt Number")
    val expectedDenied = TaskCommitDenied(-1, -1, -1)
    assertEquals(expectedDenied, JsonProtocol.taskEndReasonFromJson(oldDenied))
  }

  test("AccumulableInfo backward compatibility") {
    // "Internal" property of AccumulableInfo was added in 1.5.1
    val accumulableInfo = makeAccumulableInfo(1, internal = true, countFailedValues = true)
    val accumulableInfoJson = toJsonString(JsonProtocol.accumulableInfoToJson(accumulableInfo, _))
    val oldJson = accumulableInfoJson.removeField("Internal")
    val oldInfo = JsonProtocol.accumulableInfoFromJson(oldJson)
    assert(!oldInfo.internal)
    // "Count Failed Values" property of AccumulableInfo was added in 2.0.0
    val oldJson2 = accumulableInfoJson.removeField("Count Failed Values")
    val oldInfo2 = JsonProtocol.accumulableInfoFromJson(oldJson2)
    assert(!oldInfo2.countFailedValues)
    // "Metadata" property of AccumulableInfo was added in 2.0.0
    val oldJson3 = accumulableInfoJson.removeField("Metadata")
    val oldInfo3 = JsonProtocol.accumulableInfoFromJson(oldJson3)
    assert(oldInfo3.metadata.isEmpty)
  }

  test("ExceptionFailure backward compatibility: accumulator updates") {
    // "Task Metrics" was replaced with "Accumulator Updates" in 2.0.0. For older event logs,
    // we should still be able to fallback to constructing the accumulator updates from the
    // "Task Metrics" field, if it exists.
    val tm = makeTaskMetrics(1L, 2L, 3L, 4L, 5, 6, 0, hasHadoopInput = true, hasOutput = true)
    val tmJson = toJsonString(JsonProtocol.taskMetricsToJson(tm, _))
    val accumUpdates = tm.accumulators().map(AccumulatorSuite.makeInfo)
    val exception = new SparkException("sentimental")
    val exceptionFailure = new ExceptionFailure(exception, accumUpdates)
    val exceptionFailureJson = toJsonString(JsonProtocol.taskEndReasonToJson(exceptionFailure, _))
    val oldExceptionFailureJson =
      exceptionFailureJson
        .removeField("Accumulator Updates")
        .addStringField("Task Metrics", tmJson)
    val oldExceptionFailure =
      JsonProtocol.taskEndReasonFromJson(oldExceptionFailureJson).asInstanceOf[ExceptionFailure]
    assert(exceptionFailure.className === oldExceptionFailure.className)
    assert(exceptionFailure.description === oldExceptionFailure.description)
    assertSeqEquals[StackTraceElement](
      exceptionFailure.stackTrace, oldExceptionFailure.stackTrace, assertStackTraceElementEquals)
    assert(exceptionFailure.fullStackTrace === oldExceptionFailure.fullStackTrace)
    assertSeqEquals[AccumulableInfo](
      exceptionFailure.accumUpdates, oldExceptionFailure.accumUpdates, (x, y) => x == y)
  }

  test("TaskKilled backward compatibility") {
    // The "Kill Reason" field was added in Spark 2.2.0
    // The "Accumulator Updates" field was added in Spark 2.4.0
    val tm = makeTaskMetrics(1L, 2L, 3L, 4L, 5, 6, 0, hasHadoopInput = true, hasOutput = true)
    val accumUpdates = tm.accumulators().map(AccumulatorSuite.makeInfo)
    val taskKilled = TaskKilled(reason = "test", accumUpdates)
    val taskKilledJson = toJsonString(JsonProtocol.taskEndReasonToJson(taskKilled, _))
    val oldExceptionFailureJson =
      taskKilledJson
        .removeField("Kill Reason")
        .removeField("Accumulator Updates")
    val oldTaskKilled =
      JsonProtocol.taskEndReasonFromJson(oldExceptionFailureJson).asInstanceOf[TaskKilled]
    assert(oldTaskKilled.reason === "unknown reason")
    assert(oldTaskKilled.accums.isEmpty)
    assert(oldTaskKilled.accumUpdates.isEmpty)
  }

  test("ExecutorMetricsUpdate backward compatibility: executor metrics update") {
    // executorMetricsUpdate was added in 2.4.0.
    val executorMetricsUpdate = makeExecutorMetricsUpdate("1", true, true)
    val oldExecutorMetricsUpdateJson =
      toJsonString(JsonProtocol.executorMetricsUpdateToJson(executorMetricsUpdate, _))
        .removeField("Executor Metrics Updated")
    val expectedExecutorMetricsUpdate = makeExecutorMetricsUpdate("1", true, false)
    assertEquals(expectedExecutorMetricsUpdate,
      JsonProtocol.executorMetricsUpdateFromJson(oldExecutorMetricsUpdateJson))
  }

  test("executorMetricsFromJson backward compatibility: handle missing metrics") {
    // any missing metrics should be set to 0
    val executorMetrics = new ExecutorMetrics(Array(12L, 23L, 45L, 67L, 78L, 89L,
      90L, 123L, 456L, 789L, 40L, 20L, 20L, 10L, 20L, 10L, 301L))
    val oldExecutorMetricsJson =
      toJsonString(JsonProtocol.executorMetricsToJson(executorMetrics, _))
        .removeField("MappedPoolMemory")
    val expectedExecutorMetrics = new ExecutorMetrics(Array(12L, 23L, 45L, 67L,
      78L, 89L, 90L, 123L, 456L, 0L, 40L, 20L, 20L, 10L, 20L, 10L, 301L))
    assertEquals(expectedExecutorMetrics,
      JsonProtocol.executorMetricsFromJson(oldExecutorMetricsJson))
  }

  test("EnvironmentUpdate backward compatibility: handle missing metrics properties") {
    // The "Metrics Properties" field was added in Spark 3.4.0:
    val expectedEvent: SparkListenerEnvironmentUpdate = {
      val e = JsonProtocol.environmentUpdateFromJson(environmentUpdateJsonString)
      e.copy(environmentDetails =
        e.environmentDetails ++ Map("Metrics Properties" -> Seq.empty[(String, String)]))
    }
    val oldEnvironmentUpdateJson = environmentUpdateJsonString
      .removeField("Metrics Properties")
    assertEquals(expectedEvent, JsonProtocol.environmentUpdateFromJson(oldEnvironmentUpdateJson))
  }

  test("ExecutorInfo backward compatibility") {
    // The "Attributes" and "Resources" fields were added in Spark 3.0.0
    // The "Resource Profile Id", "Registration Time", and "Request Time"
    // fields were added in Spark 3.4.0
    val resourcesInfo = Map(ResourceUtils.GPU ->
      new ResourceInformation(ResourceUtils.GPU, Array("0", "1")))
    val attributes = Map("ContainerId" -> "ct1", "User" -> "spark")
    val executorInfo =
      new ExecutorInfo(
        "Hostee.awesome.com",
        11,
        logUrlMap = Map.empty[String, String],
        attributes = attributes,
        resourcesInfo = resourcesInfo,
        resourceProfileId = 123,
        registrationTime = Some(2L),
        requestTime = Some(1L))
    val oldExecutorInfoJson = toJsonString(JsonProtocol.executorInfoToJson(executorInfo, _))
      .removeField("Attributes")
      .removeField("Resources")
      .removeField("Resource Profile Id")
      .removeField("Registration Time")
      .removeField("Request Time")
    val oldEvent = JsonProtocol.executorInfoFromJson(oldExecutorInfoJson)
    assert(oldEvent.attributes.isEmpty)
    assert(oldEvent.resourcesInfo.isEmpty)
    assert(oldEvent.resourceProfileId == DEFAULT_RESOURCE_PROFILE_ID)
    assert(oldEvent.registrationTime.isEmpty)
    assert(oldEvent.requestTime.isEmpty)
  }

  test("TaskInfo backward compatibility: handle missing partition ID field") {
    // The "Partition ID" field was added in Spark 3.3.0:
    val newJson =
      """
        |{
        |  "Task ID": 222,
        |  "Index": 333,
        |  "Attempt": 1,
        |  "Partition ID": 333,
        |  "Launch Time": 444,
        |  "Executor ID": "executor",
        |  "Host": "your kind sir",
        |  "Locality": "NODE_LOCAL",
        |  "Speculative": false,
        |  "Getting Result Time": 0,
        |  "Finish Time": 0,
        |  "Failed": false,
        |  "Killed": false,
        |  "Accumulables": [
        |    {
        |      "ID": 1,
        |      "Name": "Accumulable1",
        |      "Update": "delta1",
        |      "Value": "val1",
        |      "Internal": false,
        |      "Count Failed Values": false
        |    }
        |  ]
        |}
    """.stripMargin
    val oldJson = newJson.removeField("Partition ID")
    assert(JsonProtocol.taskInfoFromJson(oldJson).partitionId === -1)
  }

  test("AccumulableInfo value de/serialization") {
    import InternalAccumulator._
    val blocks = Seq[(BlockId, BlockStatus)](
      (TestBlockId("meebo"), BlockStatus(StorageLevel.MEMORY_ONLY, 1L, 2L)),
      (TestBlockId("feebo"), BlockStatus(StorageLevel.DISK_ONLY, 3L, 4L)))
    val blocksJson = JArray(blocks.toList.map { case (id, status) =>
      ("Block ID" -> id.toString) ~
      ("Status" -> parse(toJsonString(JsonProtocol.blockStatusToJson(status, _))))
    })
    testAccumValue(Some(RESULT_SIZE), 3L, JInt(3))
    testAccumValue(Some(shuffleRead.REMOTE_BLOCKS_FETCHED), 2, JInt(2))
    testAccumValue(Some(UPDATED_BLOCK_STATUSES), blocks.asJava, blocksJson)
    // For anything else, we just cast the value to a string
    testAccumValue(Some("anything"), blocks, JString(blocks.toString))
    testAccumValue(Some("anything"), 123, JString("123"))
  }

  /** Create an AccumulableInfo and verify we can serialize and deserialize it. */
  private def testAccumulableInfo(
      name: String,
      value: Option[Any],
      expectedValue: Option[Any]): Unit = {
    val isInternal = name.startsWith(InternalAccumulator.METRICS_PREFIX)
    val accum = AccumulableInfo(
      123L,
      Some(name),
      update = value,
      value = value,
      internal = isInternal,
      countFailedValues = false)
    val json = toJsonString(JsonProtocol.accumulableInfoToJson(accum, _))
    val newAccum = JsonProtocol.accumulableInfoFromJson(json)
    assert(newAccum == accum.copy(update = expectedValue, value = expectedValue))
  }

  test("SPARK-31923: unexpected value type of internal accumulator") {
    // Because a user may use `METRICS_PREFIX` in an accumulator name, we should test unexpected
    // types to make sure we don't crash.
    import InternalAccumulator.METRICS_PREFIX
    testAccumulableInfo(
      METRICS_PREFIX + "fooString",
      value = Some("foo"),
      expectedValue = None)
    testAccumulableInfo(
      METRICS_PREFIX + "fooList",
      value = Some(java.util.Arrays.asList("string")),
      expectedValue = Some(java.util.Collections.emptyList())
    )
    val blocks = Seq(
      (TestBlockId("block1"), BlockStatus(StorageLevel.MEMORY_ONLY, 1L, 2L)),
      (TestBlockId("block2"), BlockStatus(StorageLevel.DISK_ONLY, 3L, 4L)))
    testAccumulableInfo(
      METRICS_PREFIX + "fooList",
      value = Some(java.util.Arrays.asList(
        "string",
        blocks(0),
        blocks(1))),
      expectedValue = Some(blocks.asJava)
    )
    testAccumulableInfo(
      METRICS_PREFIX + "fooSet",
      value = Some(Set("foo")),
      expectedValue = None)
  }

  test("SPARK-30936: forwards compatibility - ignore unknown fields") {
    val expected = TestListenerEvent("foo", 123)
    val unknownFieldsJson =
      """{
        |  "Event" : "org.apache.spark.util.TestListenerEvent",
        |  "foo" : "foo",
        |  "bar" : 123,
        |  "unknown" : "unknown"
        |}""".stripMargin
    assert(JsonProtocol.sparkEventFromJson(unknownFieldsJson) === expected)
  }

  test("SPARK-30936: backwards compatibility - set default values for missing fields") {
    val expected = TestListenerEvent("foo", 0)
    val unknownFieldsJson =
      """{
        |  "Event" : "org.apache.spark.util.TestListenerEvent",
        |  "foo" : "foo"
        |}""".stripMargin
    assert(JsonProtocol.sparkEventFromJson(unknownFieldsJson) === expected)
  }

  test("SPARK-42403: properly handle null string values") {
    // Null string values can appear in a few different event types,
    // so we test multiple known cases here:
    val stackTraceJson =
      """
        |[
        |  {
        |    "Declaring Class": "someClass",
        |    "Method Name": "someMethod",
        |    "File Name": null,
        |    "Line Number": -1
        |  }
        |]
        |""".stripMargin
    val stackTrace = JsonProtocol.stackTraceFromJson(stackTraceJson)
    assert(stackTrace === Array(new StackTraceElement("someClass", "someMethod", null, -1)))

    val exceptionFailureJson =
      """
        |{
        |  "Reason": "ExceptionFailure",
        |  "Class Name": "java.lang.Exception",
        |  "Description": null,
        |  "Stack Trace": [],
        |  "Accumulator Updates": []
        |}
        |""".stripMargin
    val exceptionFailure =
      JsonProtocol.taskEndReasonFromJson(exceptionFailureJson).asInstanceOf[ExceptionFailure]
    assert(exceptionFailure.description == null)
  }

  test("SPARK-43237: Handle null exception message in event log") {
    val exception = new Exception()
    testException(exception)
  }

  test("SPARK-43052: Handle stackTrace with null file name") {
    val ex = new Exception()
    ex.setStackTrace(Array(new StackTraceElement("class", "method", null, -1)))
    testException(ex)
  }

  test("SPARK-43340: Handle missing Stack Trace in event log") {
    val exNoStackJson =
      """
        |{
        |  "Message": "Job aborted"
        |}
        |""".stripMargin
    val exNoStack = JsonProtocol.exceptionFromJson(exNoStackJson)
    assert(exNoStack.getStackTrace.isEmpty)

    val exEmptyStackJson =
      """
        |{
        |  "Message": "Job aborted",
        |  "Stack Trace": []
        |}
        |""".stripMargin
    val exEmptyStack = JsonProtocol.exceptionFromJson(exEmptyStackJson)
    assert(exEmptyStack.getStackTrace.isEmpty)

    // test entire job failure event is equivalent
    val exJobFailureNoStackJson =
      """
        |{
        |   "Event": "SparkListenerJobEnd",
        |   "Job ID": 31,
        |   "Completion Time": 1616171909785,
        |   "Job Result":{
        |      "Result": "JobFailed",
        |      "Exception": {
        |         "Message": "Job aborted"
        |      }
        |   }
        |}
        |""".stripMargin
    val exJobFailureExpectedJson =
      """
        |{
        |   "Event": "SparkListenerJobEnd",
        |   "Job ID": 31,
        |   "Completion Time": 1616171909785,
        |   "Job Result": {
        |      "Result": "JobFailed",
        |      "Exception": {
        |         "Message": "Job aborted",
        |         "Stack Trace": []
        |      }
        |   }
        |}
        |""".stripMargin
    val jobFailedEvent = JsonProtocol.sparkEventFromJson(exJobFailureNoStackJson)
    testEvent(jobFailedEvent, exJobFailureExpectedJson)
  }

  test("SPARK-42205: don't log accumulables in start and getting result events") {
    // Simulate case where a job / stage / task completes before the event logging
    // listener logs the event. In this case, the TaskInfo / StageInfo will have
    // accumulables from the finished task / stage, but we want to skip logging
    // them because they are redundant with the accumulables in the end event and
    // the history server only uses the value from the end event because start
    // events normally will not contain accumulable values.
    val stageInfo = makeStageInfo(1, 200, 300, 400, 500)
    assert(stageInfo.accumulables.nonEmpty)
    val taskInfo = makeTaskInfo(1, 200, 300, 400, 500, false)
    assert(taskInfo.accumulables.nonEmpty)

    val stageSubmitted = SparkListenerStageSubmitted(stageInfo)
    val taskStart = SparkListenerTaskStart(1, 0, taskInfo)
    val jobStart = SparkListenerJobStart(10, jobSubmissionTime, Seq(stageInfo), properties)
    val gettingResult = SparkListenerTaskGettingResult(taskInfo)

    assert(
      stageSubmittedFromJson(sparkEventToJsonString(stageSubmitted)).stageInfo.accumulables.isEmpty)
    assert(
      taskStartFromJson(sparkEventToJsonString(taskStart)).taskInfo.accumulables.isEmpty)
    assert(
      taskGettingResultFromJson(sparkEventToJsonString(gettingResult))
        .taskInfo.accumulables.isEmpty)

    // Deliberately not fixed for job starts because a job might legitimately reference
    // stages that have completed even before the job start event is emitted.
    testEvent(jobStart, sparkEventToJsonString(jobStart))
  }
}


private[spark] object JsonProtocolSuite extends Assertions {
  import InternalAccumulator._
  import JsonProtocol._

  private val mapper = new ObjectMapper()

  private implicit class JsonStringImplicits(json: String) {
    def removeField(field: String): String = {
      val tree = mapper.readTree(json)
      Option(tree.asInstanceOf[ObjectNode].findParent(field)).foreach(_.remove(field))
      tree.toString
    }

    def addStringField(k: String, v: String): String = {
      val tree = mapper.readTree(json)
      tree.asInstanceOf[ObjectNode].set(k, new TextNode(v))
      tree.toString
    }
  }

  private implicit def toJsonNode(json: String): JsonNode = {
    mapper.readTree(json)
  }

  private val jobSubmissionTime = 1421191042750L
  private val jobCompletionTime = 1421191296660L
  private val executorAddedTime = 1421458410000L
  private val executorRemovedTime = 1421458922000L
  private val executorExcludedTime = 1421458932000L
  private val executorUnexcludedTime = 1421458942000L
  private val nodeExcludedTime = 1421458952000L
  private val nodeUnexcludedTime = 1421458962000L

  implicit def jValueToJsonNode(value: JValue): JsonNode = {
    mapper.readTree(pretty(value))
  }

  private def testEvent(event: SparkListenerEvent, jsonString: String): Unit = {
    val actualJsonString = JsonProtocol.sparkEventToJsonString(event)
    val newEvent = JsonProtocol.sparkEventFromJson(actualJsonString)
    assertJsonStringEquals(jsonString, actualJsonString, event.getClass.getSimpleName)
    assertEquals(event, newEvent)
  }

  private def testRDDInfo(info: RDDInfo): Unit = {
    val newInfo = JsonProtocol.rddInfoFromJson(
      toJsonString(JsonProtocol.rddInfoToJson(info, _)))
    assertEquals(info, newInfo)
  }

  private def testStageInfo(info: StageInfo): Unit = {
    val newInfo = JsonProtocol.stageInfoFromJson(
      toJsonString(JsonProtocol.stageInfoToJson(info, _, includeAccumulables = true)))
    assertEquals(info, newInfo)
  }

  private def testStorageLevel(level: StorageLevel): Unit = {
    val newLevel = JsonProtocol.storageLevelFromJson(
      toJsonString(JsonProtocol.storageLevelToJson(level, _)))
    assertEquals(level, newLevel)
  }

  private def testTaskMetrics(metrics: TaskMetrics): Unit = {
    val newMetrics = JsonProtocol.taskMetricsFromJson(
      toJsonString(JsonProtocol.taskMetricsToJson(metrics, _)))
    assertEquals(metrics, newMetrics)
  }

  private def testBlockManagerId(id: BlockManagerId): Unit = {
    val newId = JsonProtocol.blockManagerIdFromJson(
      toJsonString(JsonProtocol.blockManagerIdToJson(id, _)))
    assert(id === newId)
  }

  private def testTaskInfo(info: TaskInfo): Unit = {
    val newInfo = JsonProtocol.taskInfoFromJson(
      toJsonString(JsonProtocol.taskInfoToJson(info, _, includeAccumulables = true)))
    assertEquals(info, newInfo)
  }

  private def testJobResult(result: JobResult): Unit = {
    val newResult = JsonProtocol.jobResultFromJson(
      toJsonString(JsonProtocol.jobResultToJson(result, _)))
    assertEquals(result, newResult)
  }

  private def testTaskEndReason(reason: TaskEndReason): Unit = {
    val newReason = JsonProtocol.taskEndReasonFromJson(
      toJsonString(JsonProtocol.taskEndReasonToJson(reason, _)))
    assertEquals(reason, newReason)
  }

  private def testBlockId(blockId: BlockId): Unit = {
    val newBlockId = BlockId(blockId.toString)
    assert(blockId === newBlockId)
  }

  private def testExecutorInfo(info: ExecutorInfo): Unit = {
    val newInfo = JsonProtocol.executorInfoFromJson(
      toJsonString(JsonProtocol.executorInfoToJson(info, _)))
    assertEquals(info, newInfo)
  }

  private def testAccumValue(name: Option[String], value: Any, expectedJson: JValue): Unit = {
    val json = parse(toJsonString(JsonProtocol.accumValueToJson(name, value, _)))
    assert(json === expectedJson)
    val newValue = JsonProtocol.accumValueFromJson(name, json)
    val expectedValue = if (name.exists(_.startsWith(METRICS_PREFIX))) value else value.toString
    assert(newValue === expectedValue)
  }

  private def testException(exception: Exception): Unit = {
    val newException = JsonProtocol.exceptionFromJson(
      toJsonString(JsonProtocol.exceptionToJson(exception, _)))
    assertEquals(exception, newException)
  }

  /** -------------------------------- *
   | Util methods for comparing events |
   * --------------------------------- */

  private[spark] def assertEquals(event1: SparkListenerEvent, event2: SparkListenerEvent): Unit = {
    (event1, event2) match {
      case (e1: SparkListenerStageSubmitted, e2: SparkListenerStageSubmitted) =>
        assert(e1.properties === e2.properties)
        assertEquals(e1.stageInfo, e2.stageInfo)
      case (e1: SparkListenerStageCompleted, e2: SparkListenerStageCompleted) =>
        assertEquals(e1.stageInfo, e2.stageInfo)
      case (e1: SparkListenerTaskStart, e2: SparkListenerTaskStart) =>
        assert(e1.stageId === e2.stageId)
        assertEquals(e1.taskInfo, e2.taskInfo)
      case (e1: SparkListenerTaskGettingResult, e2: SparkListenerTaskGettingResult) =>
        assertEquals(e1.taskInfo, e2.taskInfo)
      case (e1: SparkListenerTaskEnd, e2: SparkListenerTaskEnd) =>
        assert(e1.stageId === e2.stageId)
        assert(e1.stageAttemptId === e2.stageAttemptId)
        assert(e1.taskType === e2.taskType)
        assertEquals(e1.reason, e2.reason)
        assertEquals(e1.taskInfo, e2.taskInfo)
        assertEquals(e1.taskExecutorMetrics, e2.taskExecutorMetrics)
        assertEquals(e1.taskMetrics, e2.taskMetrics)
      case (e1: SparkListenerJobStart, e2: SparkListenerJobStart) =>
        assert(e1.jobId === e2.jobId)
        assert(e1.properties === e2.properties)
        assert(e1.stageIds === e2.stageIds)
      case (e1: SparkListenerJobEnd, e2: SparkListenerJobEnd) =>
        assert(e1.jobId === e2.jobId)
        assertEquals(e1.jobResult, e2.jobResult)
      case (e1: SparkListenerEnvironmentUpdate, e2: SparkListenerEnvironmentUpdate) =>
        assertEquals(e1.environmentDetails.toMap, e2.environmentDetails.toMap)
      case (e1: SparkListenerExecutorAdded, e2: SparkListenerExecutorAdded) =>
        assert(e1.executorId === e2.executorId)
        assertEquals(e1.executorInfo, e2.executorInfo)
      case (e1: SparkListenerExecutorRemoved, e2: SparkListenerExecutorRemoved) =>
        assert(e1.executorId === e2.executorId)
      case (e1: SparkListenerExecutorMetricsUpdate, e2: SparkListenerExecutorMetricsUpdate) =>
        assert(e1.execId === e2.execId)
        assertSeqEquals[(Long, Int, Int, Seq[AccumulableInfo])](
          e1.accumUpdates,
          e2.accumUpdates,
          (a, b) => {
            val (taskId1, stageId1, stageAttemptId1, updates1) = a
            val (taskId2, stageId2, stageAttemptId2, updates2) = b
            assert(taskId1 === taskId2)
            assert(stageId1 === stageId2)
            assert(stageAttemptId1 === stageAttemptId2)
            assertSeqEquals[AccumulableInfo](updates1, updates2, (a, b) => a.equals(b))
          })
        assertSeqEquals[((Int, Int), ExecutorMetrics)](
          e1.executorUpdates.toSeq.sortBy(_._1),
          e2.executorUpdates.toSeq.sortBy(_._1),
          (a, b) => {
            val (k1, v1) = a
            val (k2, v2) = b
            assert(k1 === k2)
            assertEquals(v1, v2)
          }
        )
      case (e1: SparkListenerStageExecutorMetrics, e2: SparkListenerStageExecutorMetrics) =>
        assert(e1.execId === e2.execId)
        assert(e1.stageId === e2.stageId)
        assert(e1.stageAttemptId === e2.stageAttemptId)
        assertEquals(e1.executorMetrics, e2.executorMetrics)
      case (e1, e2) =>
        assert(e1 === e2)
      case _ => fail("Events don't match in types!")
    }
  }

  private def assertEquals(info1: StageInfo, info2: StageInfo): Unit = {
    assert(info1.stageId === info2.stageId)
    assert(info1.name === info2.name)
    assert(info1.numTasks === info2.numTasks)
    assert(info1.submissionTime === info2.submissionTime)
    assert(info1.completionTime === info2.completionTime)
    assert(info1.rddInfos.size === info2.rddInfos.size)
    info1.rddInfos.indices.foreach { i =>
      assertEquals(info1.rddInfos(i), info2.rddInfos(i))
    }
    assert(info1.accumulables === info2.accumulables)
    assert(info1.details === info2.details)
  }

  private def assertEquals(info1: RDDInfo, info2: RDDInfo): Unit = {
    assert(info1.id === info2.id)
    assert(info1.name === info2.name)
    assert(info1.numPartitions === info2.numPartitions)
    assert(info1.numCachedPartitions === info2.numCachedPartitions)
    assert(info1.memSize === info2.memSize)
    assert(info1.diskSize === info2.diskSize)
    assertEquals(info1.storageLevel, info2.storageLevel)
  }

  private def assertEquals(level1: StorageLevel, level2: StorageLevel): Unit = {
    assert(level1.useDisk === level2.useDisk)
    assert(level1.useMemory === level2.useMemory)
    assert(level1.deserialized === level2.deserialized)
    assert(level1.replication === level2.replication)
  }

  private def assertEquals(info1: TaskInfo, info2: TaskInfo): Unit = {
    assert(info1.taskId === info2.taskId)
    assert(info1.index === info2.index)
    assert(info1.attemptNumber === info2.attemptNumber)
    // The "Partition ID" field was added in Spark 3.3.0
    assert(info1.partitionId === info2.partitionId)
    assert(info1.launchTime === info2.launchTime)
    assert(info1.executorId === info2.executorId)
    assert(info1.host === info2.host)
    assert(info1.taskLocality === info2.taskLocality)
    assert(info1.speculative === info2.speculative)
    assert(info1.gettingResultTime === info2.gettingResultTime)
    assert(info1.finishTime === info2.finishTime)
    assert(info1.failed === info2.failed)
    assert(info1.accumulables === info2.accumulables)
  }

  private def assertEquals(info1: ExecutorInfo, info2: ExecutorInfo): Unit = {
    assert(info1.executorHost == info2.executorHost)
    assert(info1.totalCores == info2.totalCores)
  }

  private def assertEquals(metrics1: TaskMetrics, metrics2: TaskMetrics): Unit = {
    assert(metrics1.executorDeserializeTime === metrics2.executorDeserializeTime)
    assert(metrics1.executorDeserializeCpuTime === metrics2.executorDeserializeCpuTime)
    assert(metrics1.executorRunTime === metrics2.executorRunTime)
    assert(metrics1.executorCpuTime === metrics2.executorCpuTime)
    assert(metrics1.resultSize === metrics2.resultSize)
    assert(metrics1.jvmGCTime === metrics2.jvmGCTime)
    assert(metrics1.resultSerializationTime === metrics2.resultSerializationTime)
    assert(metrics1.memoryBytesSpilled === metrics2.memoryBytesSpilled)
    assert(metrics1.diskBytesSpilled === metrics2.diskBytesSpilled)
    assertEquals(metrics1.shuffleReadMetrics, metrics2.shuffleReadMetrics)
    assertEquals(metrics1.shuffleWriteMetrics, metrics2.shuffleWriteMetrics)
    assertEquals(metrics1.inputMetrics, metrics2.inputMetrics)
    assertBlocksEquals(metrics1.updatedBlockStatuses, metrics2.updatedBlockStatuses)
  }

  private def assertEquals(metrics1: ShuffleReadMetrics, metrics2: ShuffleReadMetrics): Unit = {
    assert(metrics1.remoteBlocksFetched === metrics2.remoteBlocksFetched)
    assert(metrics1.localBlocksFetched === metrics2.localBlocksFetched)
    assert(metrics1.fetchWaitTime === metrics2.fetchWaitTime)
    assert(metrics1.remoteBytesRead === metrics2.remoteBytesRead)
  }

  private def assertEquals(metrics1: ShuffleWriteMetrics, metrics2: ShuffleWriteMetrics): Unit = {
    assert(metrics1.bytesWritten === metrics2.bytesWritten)
    assert(metrics1.writeTime === metrics2.writeTime)
  }

  private def assertEquals(metrics1: InputMetrics, metrics2: InputMetrics): Unit = {
    assert(metrics1.bytesRead === metrics2.bytesRead)
  }

  private def assertEquals(result1: JobResult, result2: JobResult): Unit = {
    (result1, result2) match {
      case (JobSucceeded, JobSucceeded) =>
      case (r1: JobFailed, r2: JobFailed) =>
        assertEquals(r1.exception, r2.exception)
      case _ => fail("Job results don't match in types!")
    }
  }

  private def assertEquals(reason1: TaskEndReason, reason2: TaskEndReason): Unit = {
    (reason1, reason2) match {
      case (Success, Success) =>
      case (Resubmitted, Resubmitted) =>
      case (r1: FetchFailed, r2: FetchFailed) =>
        assert(r1.shuffleId === r2.shuffleId)
        assert(r1.mapId === r2.mapId)
        assert(r1.mapIndex === r2.mapIndex)
        assert(r1.reduceId === r2.reduceId)
        assert(r1.bmAddress === r2.bmAddress)
        assert(r1.message === r2.message)
      case (r1: ExceptionFailure, r2: ExceptionFailure) =>
        assert(r1.className === r2.className)
        assert(r1.description === r2.description)
        assertSeqEquals(r1.stackTrace, r2.stackTrace, assertStackTraceElementEquals)
        assert(r1.fullStackTrace === r2.fullStackTrace)
        assertSeqEquals[AccumulableInfo](r1.accumUpdates, r2.accumUpdates, (a, b) => a.equals(b))
      case (TaskResultLost, TaskResultLost) =>
      case (r1: TaskKilled, r2: TaskKilled) =>
        assert(r1.reason == r2.reason)
      case (TaskCommitDenied(jobId1, partitionId1, attemptNumber1),
          TaskCommitDenied(jobId2, partitionId2, attemptNumber2)) =>
        assert(jobId1 === jobId2)
        assert(partitionId1 === partitionId2)
        assert(attemptNumber1 === attemptNumber2)
      case (ExecutorLostFailure(execId1, exit1CausedByApp, reason1),
          ExecutorLostFailure(execId2, exit2CausedByApp, reason2)) =>
        assert(execId1 === execId2)
        assert(exit1CausedByApp === exit2CausedByApp)
        assert(reason1 === reason2)
      case (UnknownReason, UnknownReason) =>
      case _ => fail("Task end reasons don't match in types!")
    }
  }

  private def assertEquals(
      details1: Map[String, scala.collection.Seq[(String, String)]],
      details2: Map[String, scala.collection.Seq[(String, String)]]): Unit = {
    details1.zip(details2).foreach {
      case ((key1, values1: scala.collection.Seq[(String, String)]),
        (key2, values2: scala.collection.Seq[(String, String)])) =>
        assert(key1 === key2)
        values1.zip(values2).foreach { case (v1, v2) => assert(v1 === v2) }
    }
  }

  private def assertEquals(exception1: Exception, exception2: Exception): Unit = {
    assert(exception1.getMessage === exception2.getMessage)
    assertSeqEquals(
      exception1.getStackTrace,
      exception2.getStackTrace,
      assertStackTraceElementEquals)
  }

  private def assertEquals(metrics1: ExecutorMetrics, metrics2: ExecutorMetrics): Unit = {
    ExecutorMetricType.metricToOffset.foreach { metric =>
      assert(metrics1.getMetricValue(metric._1) === metrics2.getMetricValue(metric._1))
    }
  }

  private def prettyString(json: JsonNode): String = {
    mapper.writerWithDefaultPrettyPrinter().writeValueAsString(json)
  }

  private def assertJsonStringEquals(expected: String, actual: String, metadata: String): Unit = {
    val expectedJson = mapper.readTree(expected)
    val actualJson = mapper.readTree(actual)
    if (expectedJson != actualJson) {
      // scalastyle:off
      // This prints something useful if the JSON strings don't match
      println(s"=== EXPECTED ===\n${prettyString(expectedJson)}\n")
      println(s"=== ACTUAL ===\n${prettyString(actualJson)}\n")
      // scalastyle:on
      throw new TestFailedException(s"$metadata JSON did not equal", 1)
    }
  }

  private def assertSeqEquals[T](
      seq1: scala.collection.Seq[T],
      seq2: scala.collection.Seq[T],
      assertEquals: (T, T) => Unit): Unit = {
    assert(seq1.length === seq2.length)
    seq1.zip(seq2).foreach { case (t1, t2) =>
      assertEquals(t1, t2)
    }
  }

  private def assertOptionEquals[T](
      opt1: Option[T],
      opt2: Option[T],
      assertEquals: (T, T) => Unit): Unit = {
    if (opt1.isDefined) {
      assert(opt2.isDefined)
      assertEquals(opt1.get, opt2.get)
    } else {
      assert(opt2.isEmpty)
    }
  }

  /**
   * Use different names for methods we pass in to assertSeqEquals or assertOptionEquals
   */

  private def assertBlocksEquals(
      blocks1: Seq[(BlockId, BlockStatus)],
      blocks2: Seq[(BlockId, BlockStatus)]) = {
    assertSeqEquals(blocks1, blocks2, assertBlockEquals)
  }

  private def assertBlockEquals(b1: (BlockId, BlockStatus), b2: (BlockId, BlockStatus)): Unit = {
    assert(b1 === b2)
  }

  private def assertStackTraceElementEquals(ste1: StackTraceElement,
      ste2: StackTraceElement): Unit = {
    // This mimics the equals() method from Java 8 and earlier. Java 9 adds checks for
    // class loader and module, which will cause them to be not equal, when we don't
    // care about those
    assert(ste1.getClassName === ste2.getClassName)
    assert(ste1.getMethodName === ste2.getMethodName)
    assert(ste1.getLineNumber === ste2.getLineNumber)
    assert(ste1.getFileName === ste2.getFileName)
  }

  private def assertEquals(rp1: ResourceProfile, rp2: ResourceProfile): Unit = {
    assert(rp1 === rp2)
  }

  /** ----------------------------------- *
   | Util methods for constructing events |
   * ------------------------------------ */

  private val properties = {
    val p = new Properties
    p.setProperty("Ukraine", "Kiev")
    p.setProperty("Russia", "Moscow")
    p.setProperty("France", "Paris")
    p.setProperty("Germany", "Berlin")
    p
  }

  private val stackTrace = {
    Array[StackTraceElement](
      new StackTraceElement("Apollo", "Venus", "Mercury", 42),
      new StackTraceElement("Afollo", "Vemus", "Mercurry", 420), /* odd spellings intentional */
      new StackTraceElement("Ayollo", "Vesus", "Blackberry", 4200) /* odd spellings intentional */
    )
  }

  private def makeRddInfo(a: Int, b: Int, c: Int, d: Long, e: Long,
      deterministic: DeterministicLevel.Value) = {
    val r =
      new RDDInfo(a, "mayor", b, StorageLevel.MEMORY_AND_DISK, false, Seq(1, 4, 7), a.toString,
        outputDeterministicLevel = deterministic)
    r.numCachedPartitions = c
    r.memSize = d
    r.diskSize = e
    r
  }

  private def makeStageInfo(
      a: Int,
      b: Int,
      c: Int,
      d: Long,
      e: Long,
      rpId: Int = ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID,
      includeAccumulables: Boolean = true) = {
    val rddInfos = (0 until a % 5).map { i =>
      if (i == (a % 5) - 1) {
        makeRddInfo(a + i, b + i, c + i, d + i, e + i, DeterministicLevel.INDETERMINATE)
      } else {
        makeRddInfo(a + i, b + i, c + i, d + i, e + i, DeterministicLevel.DETERMINATE)
      }
    }
    val stageInfo = new StageInfo(a, 0, "greetings", b, rddInfos, Seq(100, 200, 300), "details",
      resourceProfileId = rpId)
    if (includeAccumulables) {
      val (acc1, acc2) = (makeAccumulableInfo(1), makeAccumulableInfo(2))
      stageInfo.accumulables(acc1.id) = acc1
      stageInfo.accumulables(acc2.id) = acc2
    }
    stageInfo
  }

  private def makeTaskInfo(
      a: Long,
      b: Int,
      c: Int,
      d: Int,
      e: Long,
      speculative: Boolean,
      includeAccumulables: Boolean = true) = {
    val taskInfo = new TaskInfo(a, b, c, d, e,
      "executor", "your kind sir", TaskLocality.NODE_LOCAL, speculative)
    if (includeAccumulables) {
      taskInfo.setAccumulables(List(
        makeAccumulableInfo(1),
        makeAccumulableInfo(2),
        makeAccumulableInfo(3, internal = true)))
    }
    taskInfo
  }

  private def makeAccumulableInfo(
      id: Int,
      internal: Boolean = false,
      countFailedValues: Boolean = false,
      metadata: Option[String] = None): AccumulableInfo =
    new AccumulableInfo(id, Some(s"Accumulable$id"), Some(s"delta$id"), Some(s"val$id"),
      internal, countFailedValues, metadata)

  /** Creates an SparkListenerExecutorMetricsUpdate event */
  private def makeExecutorMetricsUpdate(
      execId: String,
      includeTaskMetrics: Boolean,
      includeExecutorMetrics: Boolean): SparkListenerExecutorMetricsUpdate = {
    val taskMetrics =
      if (includeTaskMetrics) {
        Seq((1L, 1, 1, Seq(makeAccumulableInfo(1, false, false, None),
          makeAccumulableInfo(2, false, false, None))))
       } else {
        Seq()
      }
    val executorMetricsUpdate: Map[(Int, Int), ExecutorMetrics] =
      if (includeExecutorMetrics) {
        Map((0, 0) -> new ExecutorMetrics(Array(123456L, 543L, 0L, 0L, 0L, 0L, 0L,
          0L, 0L, 0L, 256912L, 123456L, 123456L, 61728L, 30364L, 15182L, 10L, 90L, 2L, 20L, 301L)))
      } else {
        Map.empty
      }
    SparkListenerExecutorMetricsUpdate(execId, taskMetrics, executorMetricsUpdate)
  }

  /**
   * Creates a TaskMetrics object describing a task that read data from Hadoop (if hasHadoopInput is
   * set to true) or read data from a shuffle otherwise.
   */
  private def makeTaskMetrics(
      a: Long,
      b: Long,
      c: Long,
      d: Long,
      e: Int,
      f: Int,
      g: Int,
      hasHadoopInput: Boolean,
      hasOutput: Boolean,
      hasRecords: Boolean = true) = {
    val t = TaskMetrics.registered
    // Set CPU times same as wall times for testing purpose
    t.setExecutorDeserializeTime(a)
    t.setExecutorDeserializeCpuTime(a)
    t.setExecutorRunTime(b)
    t.setExecutorCpuTime(b)
    t.setPeakExecutionMemory(c)
    t.setPeakOnHeapExecutionMemory(c)
    t.setPeakOffHeapExecutionMemory(c)
    t.setResultSize(c)
    t.setJvmGCTime(d)
    t.setResultSerializationTime(a + b)
    t.incMemoryBytesSpilled(a + c)

    if (hasHadoopInput) {
      val inputMetrics = t.inputMetrics
      inputMetrics.setBytesRead(d + e + f)
      inputMetrics.incRecordsRead(if (hasRecords) (d + e + f) / 100 else -1)
    } else {
      val sr = t.createTempShuffleReadMetrics()
      sr.incRemoteBytesRead(b + d)
      sr.incRemoteBytesReadToDisk(b)
      sr.incLocalBlocksFetched(e)
      sr.incFetchWaitTime(a + d)
      sr.incRemoteBlocksFetched(f)
      sr.incRecordsRead(if (hasRecords) (b + d) / 100 else -1)
      sr.incLocalBytesRead(a + f)
      sr.incCorruptMergedBlockChunks(if (f > e) f - e else e - f)
      sr.incMergedFetchFallbackCount(if (f > e) f - e else e - f)
      sr.incRemoteReqsDuration(a + d)
      sr.incRemoteMergedReqsDuration(g)
      t.mergeShuffleReadMetrics()
    }
    if (hasOutput) {
      t.outputMetrics.setBytesWritten(a + b + c)
      t.outputMetrics.setRecordsWritten(if (hasRecords) (a + b + c) / 100 else -1)
    } else {
      val sw = t.shuffleWriteMetrics
      sw.incBytesWritten(a + b + c)
      sw.incWriteTime(b + c + d)
      sw.incRecordsWritten(if (hasRecords) (a + b + c) / 100 else -1)
    }
    // Make at most 6 blocks
    t.setUpdatedBlockStatuses((1 to (e % 5 + 1)).map { i =>
      (RDDBlockId(e % i, f % i), BlockStatus(StorageLevel.MEMORY_AND_DISK_SER_2, a % i, b % i))
    }.toSeq)
    t
  }


  /** --------------------------------------- *
   | JSON string representation of each event |
   * ---------------------------------------- */

  private val stageSubmittedJsonString =
    """
      |{
      |  "Event": "SparkListenerStageSubmitted",
      |  "Stage Info": {
      |    "Stage ID": 100,
      |    "Stage Attempt ID": 0,
      |    "Stage Name": "greetings",
      |    "Number of Tasks": 200,
      |    "RDD Info": [],
      |    "Parent IDs" : [100, 200, 300],
      |    "Details": "details",
      |    "Accumulables": [],
      |    "Resource Profile Id" : 0,
      |    "Shuffle Push Enabled" : false,
      |    "Shuffle Push Mergers Count" : 0
      |  },
      |  "Properties": {
      |    "France": "Paris",
      |    "Germany": "Berlin",
      |    "Russia": "Moscow",
      |    "Ukraine": "Kiev"
      |  }
      |}
    """.stripMargin

  private val stageSubmittedWithNullPropertiesJsonString =
    """
      |{
      |  "Event": "SparkListenerStageSubmitted",
      |  "Stage Info": {
      |    "Stage ID": 100,
      |    "Stage Attempt ID": 0,
      |    "Stage Name": "greetings",
      |    "Number of Tasks": 200,
      |    "RDD Info": [],
      |    "Parent IDs" : [100, 200, 300],
      |    "Details": "details",
      |    "Accumulables": [],
      |    "Resource Profile Id" : 0,
      |    "Shuffle Push Enabled" : false,
      |    "Shuffle Push Mergers Count" : 0
      |  }
      |}
    """.stripMargin

  private val stageCompletedJsonString =
    """
      |{
      |  "Event": "SparkListenerStageCompleted",
      |  "Stage Info": {
      |    "Stage ID": 101,
      |    "Stage Attempt ID": 0,
      |    "Stage Name": "greetings",
      |    "Number of Tasks": 201,
      |    "RDD Info": [
      |      {
      |        "RDD ID": 101,
      |        "Name": "mayor",
      |        "Callsite": "101",
      |        "Parent IDs": [1, 4, 7],
      |        "Storage Level": {
      |          "Use Disk": true,
      |          "Use Memory": true,
      |          "Use Off Heap": false,
      |          "Deserialized": true,
      |          "Replication": 1
      |        },
      |        "Barrier" : false,
      |        "DeterministicLevel" : "INDETERMINATE",
      |        "Number of Partitions": 201,
      |        "Number of Cached Partitions": 301,
      |        "Memory Size": 401,
      |        "Disk Size": 501
      |      }
      |    ],
      |    "Parent IDs" : [100, 200, 300],
      |    "Details": "details",
      |    "Accumulables": [
      |      {
      |        "ID": 1,
      |        "Name": "Accumulable1",
      |        "Update": "delta1",
      |        "Value": "val1",
      |        "Internal": false,
      |        "Count Failed Values": false
      |      },
      |      {
      |        "ID": 2,
      |        "Name": "Accumulable2",
      |        "Update": "delta2",
      |        "Value": "val2",
      |        "Internal": false,
      |        "Count Failed Values": false
      |      }
      |    ],
      |    "Resource Profile Id" : 0,
      |    "Shuffle Push Enabled" : false,
      |    "Shuffle Push Mergers Count" : 0
      |  }
      |}
    """.stripMargin

  private val taskStartJsonString =
    """
      |{
      |  "Event": "SparkListenerTaskStart",
      |  "Stage ID": 111,
      |  "Stage Attempt ID": 0,
      |  "Task Info": {
      |    "Task ID": 222,
      |    "Index": 333,
      |    "Attempt": 1,
      |    "Partition ID": 333,
      |    "Launch Time": 444,
      |    "Executor ID": "executor",
      |    "Host": "your kind sir",
      |    "Locality": "NODE_LOCAL",
      |    "Speculative": false,
      |    "Getting Result Time": 0,
      |    "Finish Time": 0,
      |    "Failed": false,
      |    "Killed": false,
      |    "Accumulables": []
      |  }
      |}
    """.stripMargin

  private val taskGettingResultJsonString =
    """
      |{
      |  "Event": "SparkListenerTaskGettingResult",
      |  "Task Info": {
      |    "Task ID": 1000,
      |    "Index": 2000,
      |    "Attempt": 5,
      |    "Partition ID": 2000,
      |    "Launch Time": 3000,
      |    "Executor ID": "executor",
      |    "Host": "your kind sir",
      |    "Locality": "NODE_LOCAL",
      |    "Speculative": true,
      |    "Getting Result Time": 0,
      |    "Finish Time": 0,
      |    "Failed": false,
      |    "Killed": false,
      |    "Accumulables": []
      |  }
      |}
    """.stripMargin

  private val taskEndJsonString =
    """
      |{
      |  "Event": "SparkListenerTaskEnd",
      |  "Stage ID": 1,
      |  "Stage Attempt ID": 0,
      |  "Task Type": "ShuffleMapTask",
      |  "Task End Reason": {
      |    "Reason": "Success"
      |  },
      |  "Task Info": {
      |    "Task ID": 123,
      |    "Index": 234,
      |    "Attempt": 67,
      |    "Partition ID": 234,
      |    "Launch Time": 345,
      |    "Executor ID": "executor",
      |    "Host": "your kind sir",
      |    "Locality": "NODE_LOCAL",
      |    "Speculative": false,
      |    "Getting Result Time": 0,
      |    "Finish Time": 0,
      |    "Failed": false,
      |    "Killed": false,
      |    "Accumulables": [
      |      {
      |        "ID": 1,
      |        "Name": "Accumulable1",
      |        "Update": "delta1",
      |        "Value": "val1",
      |        "Internal": false,
      |        "Count Failed Values": false
      |      },
      |      {
      |        "ID": 2,
      |        "Name": "Accumulable2",
      |        "Update": "delta2",
      |        "Value": "val2",
      |        "Internal": false,
      |        "Count Failed Values": false
      |      },
      |      {
      |        "ID": 3,
      |        "Name": "Accumulable3",
      |        "Update": "delta3",
      |        "Value": "val3",
      |        "Internal": true,
      |        "Count Failed Values": false
      |      }
      |    ]
      |  },
      |  "Task Executor Metrics" : {
      |    "JVMHeapMemory" : 543,
      |    "JVMOffHeapMemory" : 123456,
      |    "OnHeapExecutionMemory" : 12345,
      |    "OffHeapExecutionMemory" : 1234,
      |    "OnHeapStorageMemory" : 123,
      |    "OffHeapStorageMemory" : 12,
      |    "OnHeapUnifiedMemory" : 432,
      |    "OffHeapUnifiedMemory" : 321,
      |    "DirectPoolMemory" : 654,
      |    "MappedPoolMemory" : 765,
      |    "ProcessTreeJVMVMemory": 256912,
      |    "ProcessTreeJVMRSSMemory": 123456,
      |    "ProcessTreePythonVMemory": 123456,
      |    "ProcessTreePythonRSSMemory": 61728,
      |    "ProcessTreeOtherVMemory": 30364,
      |    "ProcessTreeOtherRSSMemory": 15182,
      |    "MinorGCCount" : 0,
      |    "MinorGCTime" : 0,
      |    "MajorGCCount" : 0,
      |    "MajorGCTime" : 0,
      |    "TotalGCTime": 80001,
      |    "ConcurrentGCCount" : 3,
      |    "ConcurrentGCTime" : 3
      |  },
      |  "Task Metrics": {
      |    "Executor Deserialize Time": 300,
      |    "Executor Deserialize CPU Time": 300,
      |    "Executor Run Time": 400,
      |    "Executor CPU Time": 400,
      |    "Peak Execution Memory": 500,
      |    "Peak On Heap Execution Memory": 500,
      |    "Peak Off Heap Execution Memory": 500,
      |    "Result Size": 500,
      |    "JVM GC Time": 600,
      |    "Result Serialization Time": 700,
      |    "Memory Bytes Spilled": 800,
      |    "Disk Bytes Spilled": 0,
      |    "Shuffle Read Metrics": {
      |      "Remote Blocks Fetched": 800,
      |      "Local Blocks Fetched": 700,
      |      "Fetch Wait Time": 900,
      |      "Remote Bytes Read": 1000,
      |      "Remote Bytes Read To Disk": 400,
      |      "Local Bytes Read": 1100,
      |      "Total Records Read": 10,
      |      "Remote Requests Duration": 900,
      |      "Push Based Shuffle": {
      |         "Corrupt Merged Block Chunks" : 100,
      |         "Merged Fetch Fallback Count" : 100,
      |         "Merged Remote Blocks Fetched" : 0,
      |         "Merged Local Blocks Fetched" : 0,
      |         "Merged Remote Chunks Fetched" : 0,
      |         "Merged Local Chunks Fetched" : 0,
      |         "Merged Remote Bytes Read" : 0,
      |         "Merged Local Bytes Read" : 0,
      |         "Merged Remote Requests Duration": 0
      |      }
      |    },
      |    "Shuffle Write Metrics": {
      |      "Shuffle Bytes Written": 1200,
      |      "Shuffle Write Time": 1500,
      |      "Shuffle Records Written": 12
      |    },
      |    "Input Metrics" : {
      |      "Bytes Read" : 0,
      |      "Records Read" : 0
      |    },
      |    "Output Metrics" : {
      |      "Bytes Written" : 0,
      |      "Records Written" : 0
      |    },
      |    "Updated Blocks": [
      |      {
      |        "Block ID": "rdd_0_0",
      |        "Status": {
      |          "Storage Level": {
      |            "Use Disk": true,
      |            "Use Memory": true,
      |            "Use Off Heap": false,
      |            "Deserialized": false,
      |            "Replication": 2
      |          },
      |          "Memory Size": 0,
      |          "Disk Size": 0
      |        }
      |      }
      |    ]
      |  }
      |}
    """.stripMargin

  private val taskEndWithHadoopInputJsonString =
    """
      |{
      |  "Event": "SparkListenerTaskEnd",
      |  "Stage ID": 1,
      |  "Stage Attempt ID": 0,
      |  "Task Type": "ShuffleMapTask",
      |  "Task End Reason": {
      |    "Reason": "Success"
      |  },
      |  "Task Info": {
      |    "Task ID": 123,
      |    "Index": 234,
      |    "Attempt": 67,
      |    "Partition ID": 234,
      |    "Launch Time": 345,
      |    "Executor ID": "executor",
      |    "Host": "your kind sir",
      |    "Locality": "NODE_LOCAL",
      |    "Speculative": false,
      |    "Getting Result Time": 0,
      |    "Finish Time": 0,
      |    "Failed": false,
      |    "Killed": false,
      |    "Accumulables": [
      |      {
      |        "ID": 1,
      |        "Name": "Accumulable1",
      |        "Update": "delta1",
      |        "Value": "val1",
      |        "Internal": false,
      |        "Count Failed Values": false
      |      },
      |      {
      |        "ID": 2,
      |        "Name": "Accumulable2",
      |        "Update": "delta2",
      |        "Value": "val2",
      |        "Internal": false,
      |        "Count Failed Values": false
      |      },
      |      {
      |        "ID": 3,
      |        "Name": "Accumulable3",
      |        "Update": "delta3",
      |        "Value": "val3",
      |        "Internal": true,
      |        "Count Failed Values": false
      |      }
      |    ]
      |  },
      |  "Task Executor Metrics" : {
      |    "JVMHeapMemory" : 543,
      |    "JVMOffHeapMemory" : 123456,
      |    "OnHeapExecutionMemory" : 12345,
      |    "OffHeapExecutionMemory" : 1234,
      |    "OnHeapStorageMemory" : 123,
      |    "OffHeapStorageMemory" : 12,
      |    "OnHeapUnifiedMemory" : 432,
      |    "OffHeapUnifiedMemory" : 321,
      |    "DirectPoolMemory" : 654,
      |    "MappedPoolMemory" : 765,
      |    "ProcessTreeJVMVMemory": 256912,
      |    "ProcessTreeJVMRSSMemory": 123456,
      |    "ProcessTreePythonVMemory": 123456,
      |    "ProcessTreePythonRSSMemory": 61728,
      |    "ProcessTreeOtherVMemory": 30364,
      |    "ProcessTreeOtherRSSMemory": 15182,
      |    "MinorGCCount" : 0,
      |    "MinorGCTime" : 0,
      |    "MajorGCCount" : 0,
      |    "MajorGCTime" : 0,
      |    "TotalGCTime": 80001,
      |    "ConcurrentGCCount" : 3,
      |    "ConcurrentGCTime" : 3
      |  },
      |  "Task Metrics": {
      |    "Executor Deserialize Time": 300,
      |    "Executor Deserialize CPU Time": 300,
      |    "Executor Run Time": 400,
      |    "Executor CPU Time": 400,
      |    "Peak Execution Memory": 500,
      |    "Peak On Heap Execution Memory": 500,
      |    "Peak Off Heap Execution Memory": 500,
      |    "Result Size": 500,
      |    "JVM GC Time": 600,
      |    "Result Serialization Time": 700,
      |    "Memory Bytes Spilled": 800,
      |    "Disk Bytes Spilled": 0,
      |    "Shuffle Read Metrics" : {
      |      "Remote Blocks Fetched" : 0,
      |      "Local Blocks Fetched" : 0,
      |      "Fetch Wait Time" : 0,
      |      "Remote Bytes Read" : 0,
      |      "Remote Bytes Read To Disk" : 0,
      |      "Local Bytes Read" : 0,
      |      "Total Records Read" : 0,
      |      "Remote Requests Duration": 0,
      |      "Push Based Shuffle": {
      |         "Corrupt Merged Block Chunks" : 0,
      |         "Merged Fetch Fallback Count" : 0,
      |         "Merged Remote Blocks Fetched" : 0,
      |         "Merged Local Blocks Fetched" : 0,
      |         "Merged Remote Chunks Fetched" : 0,
      |         "Merged Local Chunks Fetched" : 0,
      |         "Merged Remote Bytes Read" : 0,
      |         "Merged Local Bytes Read" : 0,
      |         "Merged Remote Requests Duration": 0
      |      }
      |    },
      |    "Shuffle Write Metrics": {
      |      "Shuffle Bytes Written": 1200,
      |      "Shuffle Write Time": 1500,
      |      "Shuffle Records Written": 12
      |    },
      |    "Input Metrics": {
      |      "Bytes Read": 2100,
      |      "Records Read": 21
      |    },
      |     "Output Metrics" : {
      |      "Bytes Written" : 0,
      |      "Records Written" : 0
      |    },
      |    "Updated Blocks": [
      |      {
      |        "Block ID": "rdd_0_0",
      |        "Status": {
      |          "Storage Level": {
      |            "Use Disk": true,
      |            "Use Memory": true,
      |            "Use Off Heap": false,
      |            "Deserialized": false,
      |            "Replication": 2
      |          },
      |          "Memory Size": 0,
      |          "Disk Size": 0
      |        }
      |      }
      |    ]
      |  }
      |}
    """.stripMargin

  private val taskEndWithOutputJsonString =
    """
      |{
      |  "Event": "SparkListenerTaskEnd",
      |  "Stage ID": 1,
      |  "Stage Attempt ID": 0,
      |  "Task Type": "ResultTask",
      |  "Task End Reason": {
      |    "Reason": "Success"
      |  },
      |  "Task Info": {
      |    "Task ID": 123,
      |    "Index": 234,
      |    "Attempt": 67,
      |    "Partition ID": 234,
      |    "Launch Time": 345,
      |    "Executor ID": "executor",
      |    "Host": "your kind sir",
      |    "Locality": "NODE_LOCAL",
      |    "Speculative": false,
      |    "Getting Result Time": 0,
      |    "Finish Time": 0,
      |    "Failed": false,
      |    "Killed": false,
      |    "Accumulables": [
      |      {
      |        "ID": 1,
      |        "Name": "Accumulable1",
      |        "Update": "delta1",
      |        "Value": "val1",
      |        "Internal": false,
      |        "Count Failed Values": false
      |      },
      |      {
      |        "ID": 2,
      |        "Name": "Accumulable2",
      |        "Update": "delta2",
      |        "Value": "val2",
      |        "Internal": false,
      |        "Count Failed Values": false
      |      },
      |      {
      |        "ID": 3,
      |        "Name": "Accumulable3",
      |        "Update": "delta3",
      |        "Value": "val3",
      |        "Internal": true,
      |        "Count Failed Values": false
      |      }
      |    ]
      |  },
      |  "Task Executor Metrics" : {
      |    "JVMHeapMemory" : 543,
      |    "JVMOffHeapMemory" : 123456,
      |    "OnHeapExecutionMemory" : 12345,
      |    "OffHeapExecutionMemory" : 1234,
      |    "OnHeapStorageMemory" : 123,
      |    "OffHeapStorageMemory" : 12,
      |    "OnHeapUnifiedMemory" : 432,
      |    "OffHeapUnifiedMemory" : 321,
      |    "DirectPoolMemory" : 654,
      |    "MappedPoolMemory" : 765,
      |    "ProcessTreeJVMVMemory": 256912,
      |    "ProcessTreeJVMRSSMemory": 123456,
      |    "ProcessTreePythonVMemory": 123456,
      |    "ProcessTreePythonRSSMemory": 61728,
      |    "ProcessTreeOtherVMemory": 30364,
      |    "ProcessTreeOtherRSSMemory": 15182,
      |    "MinorGCCount" : 0,
      |    "MinorGCTime" : 0,
      |    "MajorGCCount" : 0,
      |    "MajorGCTime" : 0,
      |    "TotalGCTime": 80001,
      |    "ConcurrentGCCount" : 3,
      |    "ConcurrentGCTime" : 3
      |  },
      |  "Task Metrics": {
      |    "Executor Deserialize Time": 300,
      |    "Executor Deserialize CPU Time": 300,
      |    "Executor Run Time": 400,
      |    "Executor CPU Time": 400,
      |    "Peak Execution Memory": 500,
      |    "Peak On Heap Execution Memory": 500,
      |    "Peak Off Heap Execution Memory": 500,
      |    "Result Size": 500,
      |    "JVM GC Time": 600,
      |    "Result Serialization Time": 700,
      |    "Memory Bytes Spilled": 800,
      |    "Disk Bytes Spilled": 0,
      |    "Shuffle Read Metrics" : {
      |      "Remote Blocks Fetched" : 0,
      |      "Local Blocks Fetched" : 0,
      |      "Fetch Wait Time" : 0,
      |      "Remote Bytes Read" : 0,
      |      "Remote Bytes Read To Disk" : 0,
      |      "Local Bytes Read" : 0,
      |      "Total Records Read" : 0,
      |      "Remote Requests Duration": 0,
      |      "Push Based Shuffle": {
      |         "Corrupt Merged Block Chunks" : 0,
      |         "Merged Fetch Fallback Count" : 0,
      |         "Merged Remote Blocks Fetched" : 0,
      |         "Merged Local Blocks Fetched" : 0,
      |         "Merged Remote Chunks Fetched" : 0,
      |         "Merged Local Chunks Fetched" : 0,
      |         "Merged Remote Bytes Read" : 0,
      |         "Merged Local Bytes Read" : 0,
      |         "Merged Remote Requests Duration": 0
      |      }
      |    },
      |    "Shuffle Write Metrics": {
      |      "Shuffle Bytes Written" : 0,
      |      "Shuffle Write Time" : 0,
      |      "Shuffle Records Written" : 0
      |    },
      |    "Input Metrics": {
      |      "Bytes Read": 2100,
      |      "Records Read": 21
      |    },
      |    "Output Metrics": {
      |      "Bytes Written": 1200,
      |      "Records Written": 12
      |    },
      |    "Updated Blocks": [
      |      {
      |        "Block ID": "rdd_0_0",
      |        "Status": {
      |          "Storage Level": {
      |            "Use Disk": true,
      |            "Use Memory": true,
      |            "Use Off Heap": false,
      |            "Deserialized": false,
      |            "Replication": 2
      |          },
      |          "Memory Size": 0,
      |          "Disk Size": 0
      |        }
      |      }
      |    ]
      |  }
      |}
    """.stripMargin

  private val jobStartJsonString =
    """
      |{
      |  "Event": "SparkListenerJobStart",
      |  "Job ID": 10,
      |  "Submission Time": 1421191042750,
      |  "Stage Infos": [
      |    {
      |      "Stage ID": 1,
      |      "Stage Attempt ID": 0,
      |      "Stage Name": "greetings",
      |      "Number of Tasks": 200,
      |      "RDD Info": [
      |        {
      |          "RDD ID": 1,
      |          "Name": "mayor",
      |          "Callsite": "1",
      |          "Parent IDs": [1, 4, 7],
      |          "Storage Level": {
      |            "Use Disk": true,
      |            "Use Memory": true,
      |            "Use Off Heap": false,
      |            "Deserialized": true,
      |            "Replication": 1
      |          },
      |          "Barrier" : false,
      |          "DeterministicLevel" : "INDETERMINATE",
      |          "Number of Partitions": 200,
      |          "Number of Cached Partitions": 300,
      |          "Memory Size": 400,
      |          "Disk Size": 500
      |        }
      |      ],
      |      "Parent IDs" : [100, 200, 300],
      |      "Details": "details",
      |      "Accumulables": [
      |        {
      |          "ID": 1,
      |          "Name": "Accumulable1",
      |          "Update": "delta1",
      |          "Value": "val1",
      |          "Internal": false,
      |          "Count Failed Values": false
      |        },
      |        {
      |          "ID": 2,
      |          "Name": "Accumulable2",
      |          "Update": "delta2",
      |          "Value": "val2",
      |          "Internal": false,
      |          "Count Failed Values": false
      |        }
      |      ],
      |      "Resource Profile Id" : 0,
      |      "Shuffle Push Enabled" : false,
      |      "Shuffle Push Mergers Count" : 0
      |    },
      |    {
      |      "Stage ID": 2,
      |      "Stage Attempt ID": 0,
      |      "Stage Name": "greetings",
      |      "Number of Tasks": 400,
      |      "RDD Info": [
      |        {
      |          "RDD ID": 2,
      |          "Name": "mayor",
      |          "Callsite": "2",
      |          "Parent IDs": [1, 4, 7],
      |          "Storage Level": {
      |            "Use Disk": true,
      |            "Use Memory": true,
      |            "Use Off Heap": false,
      |            "Deserialized": true,
      |            "Replication": 1
      |          },
      |          "Barrier" : false,
      |          "DeterministicLevel" : "DETERMINATE",
      |          "Number of Partitions": 400,
      |          "Number of Cached Partitions": 600,
      |          "Memory Size": 800,
      |          "Disk Size": 1000
      |        },
      |        {
      |          "RDD ID": 3,
      |          "Name": "mayor",
      |          "Callsite": "3",
      |          "Parent IDs": [1, 4, 7],
      |          "Storage Level": {
      |            "Use Disk": true,
      |            "Use Memory": true,
      |            "Use Off Heap": false,
      |            "Deserialized": true,
      |            "Replication": 1
      |          },
      |          "Barrier" : false,
      |          "DeterministicLevel" : "INDETERMINATE",
      |          "Number of Partitions": 401,
      |          "Number of Cached Partitions": 601,
      |          "Memory Size": 801,
      |          "Disk Size": 1001
      |        }
      |      ],
      |      "Parent IDs" : [100, 200, 300],
      |      "Details": "details",
      |      "Accumulables": [
      |        {
      |          "ID": 1,
      |          "Name": "Accumulable1",
      |          "Update": "delta1",
      |          "Value": "val1",
      |          "Internal": false,
      |          "Count Failed Values": false
      |        },
      |        {
      |          "ID": 2,
      |          "Name": "Accumulable2",
      |          "Update": "delta2",
      |          "Value": "val2",
      |          "Internal": false,
      |          "Count Failed Values": false
      |        }
      |      ],
      |      "Resource Profile Id" : 0,
      |      "Shuffle Push Enabled" : false,
      |      "Shuffle Push Mergers Count" : 0
      |    },
      |    {
      |      "Stage ID": 3,
      |      "Stage Attempt ID": 0,
      |      "Stage Name": "greetings",
      |      "Number of Tasks": 600,
      |      "RDD Info": [
      |        {
      |          "RDD ID": 3,
      |          "Name": "mayor",
      |          "Callsite": "3",
      |          "Parent IDs": [1, 4, 7],
      |          "Storage Level": {
      |            "Use Disk": true,
      |            "Use Memory": true,
      |            "Use Off Heap": false,
      |            "Deserialized": true,
      |            "Replication": 1
      |          },
      |          "Barrier" : false,
      |          "DeterministicLevel" : "DETERMINATE",
      |          "Number of Partitions": 600,
      |          "Number of Cached Partitions": 900,
      |          "Memory Size": 1200,
      |          "Disk Size": 1500
      |        },
      |        {
      |          "RDD ID": 4,
      |          "Name": "mayor",
      |          "Callsite": "4",
      |          "Parent IDs": [1, 4, 7],
      |          "Storage Level": {
      |            "Use Disk": true,
      |            "Use Memory": true,
      |            "Use Off Heap": false,
      |            "Deserialized": true,
      |            "Replication": 1
      |          },
      |          "Barrier" : false,
      |          "DeterministicLevel" : "DETERMINATE",
      |          "Number of Partitions": 601,
      |          "Number of Cached Partitions": 901,
      |          "Memory Size": 1201,
      |          "Disk Size": 1501
      |        },
      |        {
      |          "RDD ID": 5,
      |          "Name": "mayor",
      |          "Callsite": "5",
      |          "Parent IDs": [1, 4, 7],
      |          "Storage Level": {
      |            "Use Disk": true,
      |            "Use Memory": true,
      |            "Use Off Heap": false,
      |            "Deserialized": true,
      |            "Replication": 1
      |          },
      |          "Barrier" : false,
      |          "DeterministicLevel" : "INDETERMINATE",
      |          "Number of Partitions": 602,
      |          "Number of Cached Partitions": 902,
      |          "Memory Size": 1202,
      |          "Disk Size": 1502
      |        }
      |      ],
      |      "Parent IDs" : [100, 200, 300],
      |      "Details": "details",
      |      "Accumulables": [
      |        {
      |          "ID": 1,
      |          "Name": "Accumulable1",
      |          "Update": "delta1",
      |          "Value": "val1",
      |          "Internal": false,
      |          "Count Failed Values": false
      |        },
      |        {
      |          "ID": 2,
      |          "Name": "Accumulable2",
      |          "Update": "delta2",
      |          "Value": "val2",
      |          "Internal": false,
      |          "Count Failed Values": false
      |        }
      |      ],
      |      "Resource Profile Id" : 0,
      |      "Shuffle Push Enabled" : false,
      |      "Shuffle Push Mergers Count" : 0
      |    },
      |    {
      |      "Stage ID": 4,
      |      "Stage Attempt ID": 0,
      |      "Stage Name": "greetings",
      |      "Number of Tasks": 800,
      |      "RDD Info": [
      |        {
      |          "RDD ID": 4,
      |          "Name": "mayor",
      |          "Callsite": "4",
      |          "Parent IDs": [1, 4, 7],
      |          "Storage Level": {
      |            "Use Disk": true,
      |            "Use Memory": true,
      |            "Use Off Heap": false,
      |            "Deserialized": true,
      |            "Replication": 1
      |          },
      |          "Barrier" : false,
      |          "DeterministicLevel" : "DETERMINATE",
      |          "Number of Partitions": 800,
      |          "Number of Cached Partitions": 1200,
      |          "Memory Size": 1600,
      |          "Disk Size": 2000
      |        },
      |        {
      |          "RDD ID": 5,
      |          "Name": "mayor",
      |          "Callsite": "5",
      |          "Parent IDs": [1, 4, 7],
      |          "Storage Level": {
      |            "Use Disk": true,
      |            "Use Memory": true,
      |            "Use Off Heap": false,
      |            "Deserialized": true,
      |            "Replication": 1
      |          },
      |          "Barrier" : false,
      |          "DeterministicLevel" : "DETERMINATE",
      |          "Number of Partitions": 801,
      |          "Number of Cached Partitions": 1201,
      |          "Memory Size": 1601,
      |          "Disk Size": 2001
      |        },
      |        {
      |          "RDD ID": 6,
      |          "Name": "mayor",
      |          "Callsite": "6",
      |          "Parent IDs": [1, 4, 7],
      |          "Storage Level": {
      |            "Use Disk": true,
      |            "Use Memory": true,
      |            "Use Off Heap": false,
      |            "Deserialized": true,
      |            "Replication": 1
      |          },
      |          "Barrier" : false,
      |          "DeterministicLevel" : "DETERMINATE",
      |          "Number of Partitions": 802,
      |          "Number of Cached Partitions": 1202,
      |          "Memory Size": 1602,
      |          "Disk Size": 2002
      |        },
      |        {
      |          "RDD ID": 7,
      |          "Name": "mayor",
      |          "Callsite": "7",
      |          "Parent IDs": [1, 4, 7],
      |          "Storage Level": {
      |            "Use Disk": true,
      |            "Use Memory": true,
      |            "Use Off Heap": false,
      |            "Deserialized": true,
      |            "Replication": 1
      |          },
      |          "Barrier" : false,
      |          "DeterministicLevel" : "INDETERMINATE",
      |          "Number of Partitions": 803,
      |          "Number of Cached Partitions": 1203,
      |          "Memory Size": 1603,
      |          "Disk Size": 2003
      |        }
      |      ],
      |      "Parent IDs" : [100, 200, 300],
      |      "Details": "details",
      |      "Accumulables": [
      |        {
      |          "ID": 1,
      |          "Name": "Accumulable1",
      |          "Update": "delta1",
      |          "Value": "val1",
      |          "Internal": false,
      |          "Count Failed Values": false
      |        },
      |        {
      |          "ID": 2,
      |          "Name": "Accumulable2",
      |          "Update": "delta2",
      |          "Value": "val2",
      |          "Internal": false,
      |          "Count Failed Values": false
      |        }
      |      ],
      |      "Resource Profile Id" : 0,
      |      "Shuffle Push Enabled" : false,
      |      "Shuffle Push Mergers Count" : 0
      |    }
      |  ],
      |  "Stage IDs": [
      |    1,
      |    2,
      |    3,
      |    4
      |  ],
      |  "Properties": {
      |    "France": "Paris",
      |    "Germany": "Berlin",
      |    "Russia": "Moscow",
      |    "Ukraine": "Kiev"
      |  }
      |}
    """.stripMargin

  private val jobStartWithNullPropertiesJsonString =
    """
      |{
      |  "Event": "SparkListenerJobStart",
      |  "Job ID": 10,
      |  "Submission Time": 1421191042750,
      |  "Stage Infos": [],
      |  "Stage IDs": []
      |}
    """.stripMargin

  private val jobEndJsonString =
    """
      |{
      |  "Event": "SparkListenerJobEnd",
      |  "Job ID": 20,
      |  "Completion Time": 1421191296660,
      |  "Job Result": {
      |    "Result": "JobSucceeded"
      |  }
      |}
    """.stripMargin

  private val environmentUpdateJsonString =
    """
      |{
      |  "Event": "SparkListenerEnvironmentUpdate",
      |  "JVM Information": {
      |    "GC speed": "9999 objects/s",
      |    "Java home": "Land of coffee"
      |  },
      |  "Spark Properties": {
      |    "Job throughput": "80000 jobs/s, regardless of job type"
      |  },
      |  "Hadoop Properties": {
      |    "hadoop.tmp.dir": "/usr/local/hadoop/tmp"
      |  },
      |  "System Properties": {
      |    "Username": "guest",
      |    "Password": "guest"
      |  },
      |  "Metrics Properties": {
      |    "*.sink.servlet.class": "org.apache.spark.metrics.sink.MetricsServlet"
      |  },
      |  "Classpath Entries": {
      |    "Super library": "/tmp/super_library"
      |  }
      |}
    """.stripMargin

  private val blockManagerAddedJsonString =
    """
      |{
      |  "Event": "SparkListenerBlockManagerAdded",
      |  "Block Manager ID": {
      |    "Executor ID": "Stars",
      |    "Host": "In your multitude...",
      |    "Port": 300
      |  },
      |  "Maximum Memory": 500,
      |  "Timestamp": 1
      |}
    """.stripMargin

  private val blockManagerRemovedJsonString =
    """
      |{
      |  "Event": "SparkListenerBlockManagerRemoved",
      |  "Block Manager ID": {
      |    "Executor ID": "Scarce",
      |    "Host": "to be counted...",
      |    "Port": 100
      |  },
      |  "Timestamp": 2
      |}
    """.stripMargin

  private val unpersistRDDJsonString =
    """
      |{
      |  "Event": "SparkListenerUnpersistRDD",
      |  "RDD ID": 12345
      |}
    """.stripMargin

  private val applicationStartJsonString =
    """
      |{
      |  "Event": "SparkListenerApplicationStart",
      |  "App Name": "The winner of all",
      |  "App ID": "appId",
      |  "Timestamp": 42,
      |  "User": "Garfield",
      |  "App Attempt ID": "appAttempt"
      |}
    """.stripMargin

  private val applicationStartJsonWithLogUrlsString =
    """
      |{
      |  "Event": "SparkListenerApplicationStart",
      |  "App Name": "The winner of all",
      |  "App ID": "appId",
      |  "Timestamp": 42,
      |  "User": "Garfield",
      |  "App Attempt ID": "appAttempt",
      |  "Driver Logs" : {
      |      "stderr" : "mystderr",
      |      "stdout" : "mystdout"
      |  }
      |}
    """.stripMargin

  private val applicationEndJsonString =
    """
      |{
      |  "Event": "SparkListenerApplicationEnd",
      |  "Timestamp": 42
      |}
    """.stripMargin

  private val executorAddedJsonString =
    s"""
      |{
      |  "Event": "SparkListenerExecutorAdded",
      |  "Timestamp": ${executorAddedTime},
      |  "Executor ID": "exec1",
      |  "Executor Info": {
      |    "Host": "Hostee.awesome.com",
      |    "Total Cores": 11,
      |    "Log Urls" : {
      |      "stderr" : "mystderr",
      |      "stdout" : "mystdout"
      |    },
      |    "Attributes" : {
      |      "ContainerId" : "ct1",
      |      "User" : "spark"
      |    },
      |    "Resources" : {
      |      "gpu" : {
      |        "name" : "gpu",
      |        "addresses" : [ "0", "1" ]
      |      }
      |    },
      |    "Resource Profile Id": 4
      |  }
      |}
    """.stripMargin

  private val executorAddedWithTimeJsonString =
    s"""
      |{
      |  "Event": "SparkListenerExecutorAdded",
      |  "Timestamp": ${executorAddedTime},
      |  "Executor ID": "exec1",
      |  "Executor Info": {
      |    "Host": "Hostee.awesome.com",
      |    "Total Cores": 11,
      |    "Log Urls" : {
      |      "stderr" : "mystderr",
      |      "stdout" : "mystdout"
      |    },
      |    "Attributes" : {
      |      "ContainerId" : "ct1",
      |      "User" : "spark"
      |    },
      |    "Resources" : {
      |      "gpu" : {
      |        "name" : "gpu",
      |        "addresses" : [ "0", "1" ]
      |      }
      |    },
      |    "Resource Profile Id": 4,
      |    "Registration Time" : 1,
      |    "Request Time" : 0
      |  }
      |
      |}
    """.stripMargin

  private val executorRemovedJsonString =
    s"""
      |{
      |  "Event": "SparkListenerExecutorRemoved",
      |  "Timestamp": ${executorRemovedTime},
      |  "Executor ID": "exec2",
      |  "Removed Reason": "test reason"
      |}
    """.stripMargin

  private val executorMetricsUpdateJsonString =
    s"""
      |{
      |  "Event": "SparkListenerExecutorMetricsUpdate",
      |  "Executor ID": "exec3",
      |  "Metrics Updated": [
      |    {
      |      "Task ID": 1,
      |      "Stage ID": 2,
      |      "Stage Attempt ID": 3,
      |      "Accumulator Updates": [
      |        {
      |          "ID": 0,
      |          "Name": "$EXECUTOR_DESERIALIZE_TIME",
      |          "Update": 300,
      |          "Internal": true,
      |          "Count Failed Values": true
      |        },
      |        {
      |          "ID": 1,
      |          "Name": "$EXECUTOR_DESERIALIZE_CPU_TIME",
      |          "Update": 300,
      |          "Internal": true,
      |          "Count Failed Values": true
      |        },
      |
      |        {
      |          "ID": 2,
      |          "Name": "$EXECUTOR_RUN_TIME",
      |          "Update": 400,
      |          "Internal": true,
      |          "Count Failed Values": true
      |        },
      |        {
      |          "ID": 3,
      |          "Name": "$EXECUTOR_CPU_TIME",
      |          "Update": 400,
      |          "Internal": true,
      |          "Count Failed Values": true
      |        },
      |        {
      |          "ID": 4,
      |          "Name": "$RESULT_SIZE",
      |          "Update": 500,
      |          "Internal": true,
      |          "Count Failed Values": true
      |        },
      |        {
      |          "ID": 5,
      |          "Name": "$JVM_GC_TIME",
      |          "Update": 600,
      |          "Internal": true,
      |          "Count Failed Values": true
      |        },
      |        {
      |          "ID": 6,
      |          "Name": "$RESULT_SERIALIZATION_TIME",
      |          "Update": 700,
      |          "Internal": true,
      |          "Count Failed Values": true
      |        },
      |        {
      |          "ID": 7,
      |          "Name": "$MEMORY_BYTES_SPILLED",
      |          "Update": 800,
      |          "Internal": true,
      |          "Count Failed Values": true
      |        },
      |        {
      |          "ID": 8,
      |          "Name": "$DISK_BYTES_SPILLED",
      |          "Update": 0,
      |          "Internal": true,
      |          "Count Failed Values": true
      |        },
      |        {
      |          "ID": 9,
      |          "Name": "$PEAK_EXECUTION_MEMORY",
      |          "Update": 500,
      |          "Internal": true,
      |          "Count Failed Values": true
      |        },
      |        {
      |          "ID": 10,
      |          "Name": "$PEAK_ON_HEAP_EXECUTION_MEMORY",
      |          "Update": 500,
      |          "Internal": true,
      |          "Count Failed Values": true
      |        },
      |        {
      |          "ID": 11,
      |          "Name": "$PEAK_OFF_HEAP_EXECUTION_MEMORY",
      |          "Update": 500,
      |          "Internal": true,
      |          "Count Failed Values": true
      |        },
      |        {
      |          "ID": 12,
      |          "Name": "$UPDATED_BLOCK_STATUSES",
      |          "Update": [
      |            {
      |              "Block ID": "rdd_0_0",
      |              "Status": {
      |                "Storage Level": {
      |                  "Use Disk": true,
      |                  "Use Memory": true,
      |                  "Use Off Heap": false,
      |                  "Deserialized": false,
      |                  "Replication": 2
      |                },
      |                "Memory Size": 0,
      |                "Disk Size": 0
      |              }
      |            }
      |          ],
      |          "Internal": true,
      |          "Count Failed Values": true
      |        },
      |        {
      |          "ID": 13,
      |          "Name": "${shuffleRead.REMOTE_BLOCKS_FETCHED}",
      |          "Update": 0,
      |          "Internal": true,
      |          "Count Failed Values": true
      |        },
      |        {
      |          "ID": 14,
      |          "Name": "${shuffleRead.LOCAL_BLOCKS_FETCHED}",
      |          "Update": 0,
      |          "Internal": true,
      |          "Count Failed Values": true
      |        },
      |        {
      |          "ID": 15,
      |          "Name": "${shuffleRead.REMOTE_BYTES_READ}",
      |          "Update": 0,
      |          "Internal": true,
      |          "Count Failed Values": true
      |        },
      |        {
      |          "ID": 16,
      |          "Name": "${shuffleRead.REMOTE_BYTES_READ_TO_DISK}",
      |          "Update": 0,
      |          "Internal": true,
      |          "Count Failed Values": true
      |        },
      |        {
      |          "ID": 17,
      |          "Name": "${shuffleRead.LOCAL_BYTES_READ}",
      |          "Update": 0,
      |          "Internal": true,
      |          "Count Failed Values": true
      |        },
      |        {
      |          "ID": 18,
      |          "Name": "${shuffleRead.FETCH_WAIT_TIME}",
      |          "Update": 0,
      |          "Internal": true,
      |          "Count Failed Values": true
      |        },
      |        {
      |          "ID": 19,
      |          "Name": "${shuffleRead.RECORDS_READ}",
      |          "Update": 0,
      |          "Internal": true,
      |          "Count Failed Values": true
      |        },
      |        {
      |          "ID": 20,
      |          "Name": "${shuffleRead.CORRUPT_MERGED_BLOCK_CHUNKS}",
      |          "Update": 0,
      |          "Internal": true,
      |          "Count Failed Values": true
      |        },
      |        {
      |          "ID": 21,
      |          "Name": "${shuffleRead.MERGED_FETCH_FALLBACK_COUNT}",
      |          "Update": 0,
      |          "Internal": true,
      |          "Count Failed Values": true
      |        },
      |        {
      |          "ID" : 22,
      |          "Name" : "${shuffleRead.REMOTE_MERGED_BLOCKS_FETCHED}",
      |          "Update" : 0,
      |          "Internal" : true,
      |          "Count Failed Values" : true
      |        },
      |        {
      |          "ID" : 23,
      |          "Name" : "${shuffleRead.LOCAL_MERGED_BLOCKS_FETCHED}",
      |          "Update" : 0,
      |          "Internal" : true,
      |          "Count Failed Values" : true
      |        },
      |        {
      |          "ID" : 24,
      |          "Name" : "${shuffleRead.REMOTE_MERGED_CHUNKS_FETCHED}",
      |          "Update" : 0,
      |          "Internal" : true,
      |          "Count Failed Values" : true
      |        },
      |        {
      |          "ID" : 25,
      |          "Name" : "${shuffleRead.LOCAL_MERGED_CHUNKS_FETCHED}",
      |          "Update" : 0,
      |          "Internal" : true,
      |          "Count Failed Values" : true
      |        },
      |        {
      |          "ID" : 26,
      |          "Name" : "${shuffleRead.REMOTE_MERGED_BYTES_READ}",
      |          "Update" : 0,
      |          "Internal" : true,
      |          "Count Failed Values" : true
      |        },
      |        {
      |          "ID" : 27,
      |          "Name" : "${shuffleRead.LOCAL_MERGED_BYTES_READ}",
      |          "Update" : 0,
      |          "Internal" : true,
      |          "Count Failed Values" : true
      |        },
      |        {
      |          "ID" : 28,
      |          "Name" : "${shuffleRead.REMOTE_REQS_DURATION}",
      |          "Update" : 0,
      |          "Internal" : true,
      |          "Count Failed Values" : true
      |        },
      |        {
      |          "ID" : 29,
      |          "Name" : "${shuffleRead.REMOTE_MERGED_REQS_DURATION}",
      |          "Update" : 0,
      |          "Internal" : true,
      |          "Count Failed Values" : true
      |        },
      |        {
      |          "ID": 30,
      |          "Name": "${shuffleWrite.BYTES_WRITTEN}",
      |          "Update": 0,
      |          "Internal": true,
      |          "Count Failed Values": true
      |        },
      |        {
      |          "ID": 31,
      |          "Name": "${shuffleWrite.RECORDS_WRITTEN}",
      |          "Update": 0,
      |          "Internal": true,
      |          "Count Failed Values": true
      |        },
      |        {
      |          "ID": 32,
      |          "Name": "${shuffleWrite.WRITE_TIME}",
      |          "Update": 0,
      |          "Internal": true,
      |          "Count Failed Values": true
      |        },
      |        {
      |          "ID": 33,
      |          "Name": "${input.BYTES_READ}",
      |          "Update": 2100,
      |          "Internal": true,
      |          "Count Failed Values": true
      |        },
      |        {
      |          "ID": 34,
      |          "Name": "${input.RECORDS_READ}",
      |          "Update": 21,
      |          "Internal": true,
      |          "Count Failed Values": true
      |        },
      |        {
      |          "ID": 35,
      |          "Name": "${output.BYTES_WRITTEN}",
      |          "Update": 1200,
      |          "Internal": true,
      |          "Count Failed Values": true
      |        },
      |        {
      |          "ID": 36,
      |          "Name": "${output.RECORDS_WRITTEN}",
      |          "Update": 12,
      |          "Internal": true,
      |          "Count Failed Values": true
      |        },
      |        {
      |          "ID": 37,
      |          "Name": "$TEST_ACCUM",
      |          "Update": 0,
      |          "Internal": true,
      |          "Count Failed Values": true
      |        }
      |      ]
      |    }
      |  ],
      |  "Executor Metrics Updated" : [
      |    {
      |      "Stage ID" : 0,
      |      "Stage Attempt ID" : 0,
      |      "Executor Metrics" : {
      |        "JVMHeapMemory" : 543,
      |        "JVMOffHeapMemory" : 123456,
      |        "OnHeapExecutionMemory" : 12345,
      |        "OffHeapExecutionMemory" : 1234,
      |        "OnHeapStorageMemory" : 123,
      |        "OffHeapStorageMemory" : 12,
      |        "OnHeapUnifiedMemory" : 432,
      |        "OffHeapUnifiedMemory" : 321,
      |        "DirectPoolMemory" : 654,
      |        "MappedPoolMemory" : 765,
      |        "ProcessTreeJVMVMemory": 256912,
      |        "ProcessTreeJVMRSSMemory": 123456,
      |        "ProcessTreePythonVMemory": 123456,
      |        "ProcessTreePythonRSSMemory": 61728,
      |        "ProcessTreeOtherVMemory": 30364,
      |        "ProcessTreeOtherRSSMemory": 15182,
      |        "MinorGCCount": 10,
      |        "MinorGCTime": 90,
      |        "MajorGCCount": 2,
      |        "MajorGCTime": 20,
      |        "TotalGCTime": 80001,
      |        "ConcurrentGCCount" : 3,
      |        "ConcurrentGCTime" : 3
      |      }
      |    }
      |  ]
      |}
    """.stripMargin

  private val stageExecutorMetricsJsonString =
    """
      |{
      |  "Event": "SparkListenerStageExecutorMetrics",
      |  "Executor ID": "1",
      |  "Stage ID": 2,
      |  "Stage Attempt ID": 3,
      |  "Executor Metrics" : {
      |    "JVMHeapMemory" : 543,
      |    "JVMOffHeapMemory" : 123456,
      |    "OnHeapExecutionMemory" : 12345,
      |    "OffHeapExecutionMemory" : 1234,
      |    "OnHeapStorageMemory" : 123,
      |    "OffHeapStorageMemory" : 12,
      |    "OnHeapUnifiedMemory" : 432,
      |    "OffHeapUnifiedMemory" : 321,
      |    "DirectPoolMemory" : 654,
      |    "MappedPoolMemory" : 765,
      |    "ProcessTreeJVMVMemory": 256912,
      |    "ProcessTreeJVMRSSMemory": 123456,
      |    "ProcessTreePythonVMemory": 123456,
      |    "ProcessTreePythonRSSMemory": 61728,
      |    "ProcessTreeOtherVMemory": 30364,
      |    "ProcessTreeOtherRSSMemory": 15182,
      |    "MinorGCCount": 10,
      |    "MinorGCTime": 90,
      |    "MajorGCCount": 2,
      |    "MajorGCTime": 20,
      |    "TotalGCTime": 80001,
      |    "ConcurrentGCCount" : 3,
      |    "ConcurrentGCTime" : 3
      |  }
      |}
    """.stripMargin

  private val blockUpdatedJsonString =
    """
      |{
      |  "Event": "SparkListenerBlockUpdated",
      |  "Block Updated Info": {
      |    "Block Manager ID": {
      |      "Executor ID": "Stars",
      |      "Host": "In your multitude...",
      |      "Port": 300
      |    },
      |    "Block ID": "rdd_0_0",
      |    "Storage Level": {
      |      "Use Disk": false,
      |      "Use Memory": true,
      |      "Use Off Heap": false,
      |      "Deserialized": true,
      |      "Replication": 1
      |    },
      |    "Memory Size": 100,
      |    "Disk Size": 0
      |  }
      |}
    """.stripMargin

  private val executorBlacklistedJsonString =
    s"""
      |{
      |  "Event" : "org.apache.spark.scheduler.SparkListenerExecutorBlacklisted",
      |  "time" : ${executorExcludedTime},
      |  "executorId" : "exec1",
      |  "taskFailures" : 22
      |}
    """.stripMargin
  private val executorExcludedJsonString =
    s"""
       |{
       |  "Event" : "org.apache.spark.scheduler.SparkListenerExecutorExcluded",
       |  "time" : ${executorExcludedTime},
       |  "executorId" : "exec1",
       |  "taskFailures" : 22
       |}
    """.stripMargin
  private val executorUnblacklistedJsonString =
    s"""
      |{
      |  "Event" : "org.apache.spark.scheduler.SparkListenerExecutorUnblacklisted",
      |  "time" : ${executorUnexcludedTime},
      |  "executorId" : "exec1"
      |}
    """.stripMargin
  private val executorUnexcludedJsonString =
    s"""
       |{
       |  "Event" : "org.apache.spark.scheduler.SparkListenerExecutorUnexcluded",
       |  "time" : ${executorUnexcludedTime},
       |  "executorId" : "exec1"
       |}
    """.stripMargin
  private val nodeBlacklistedJsonString =
    s"""
      |{
      |  "Event" : "org.apache.spark.scheduler.SparkListenerNodeBlacklisted",
      |  "time" : ${nodeExcludedTime},
      |  "hostId" : "node1",
      |  "executorFailures" : 33
      |}
    """.stripMargin
  private val nodeExcludedJsonString =
    s"""
       |{
       |  "Event" : "org.apache.spark.scheduler.SparkListenerNodeExcluded",
       |  "time" : ${nodeExcludedTime},
       |  "hostId" : "node1",
       |  "executorFailures" : 33
       |}
    """.stripMargin
  private val nodeUnblacklistedJsonString =
    s"""
      |{
      |  "Event" : "org.apache.spark.scheduler.SparkListenerNodeUnblacklisted",
      |  "time" : ${nodeUnexcludedTime},
      |  "hostId" : "node1"
      |}
    """.stripMargin
  private val nodeUnexcludedJsonString =
    s"""
       |{
       |  "Event" : "org.apache.spark.scheduler.SparkListenerNodeUnexcluded",
       |  "time" : ${nodeUnexcludedTime},
       |  "hostId" : "node1"
       |}
    """.stripMargin
  private val resourceProfileJsonString =
    """
      |{
      |  "Event":"SparkListenerResourceProfileAdded",
      |  "Resource Profile Id":21,
      |  "Executor Resource Requests":{
      |    "cores" : {
      |      "Resource Name":"cores",
      |      "Amount":2,
      |      "Discovery Script":"",
      |      "Vendor":""
      |    },
      |    "myCustomResource":{
      |      "Resource Name":"myCustomResource",
      |      "Amount": 2147483648,
      |      "Discovery Script": "myscript2",
      |      "Vendor" : ""
      |    },
      |    "gpu":{
      |      "Resource Name":"gpu",
      |      "Amount":2,
      |      "Discovery Script":"myscript",
      |      "Vendor":""
      |    }
      |  },
      |  "Task Resource Requests":{
      |    "cpus":{
      |      "Resource Name":"cpus",
      |      "Amount":1.0
      |    },
      |    "gpu":{
      |      "Resource Name":"gpu",
      |      "Amount":1.0
      |    },
      |    "fgpa":{
      |      "Resource Name":"fgpa",
      |      "Amount":0.5
      |    }
      |  }
      |}
    """.stripMargin

}

case class TestListenerEvent(foo: String, bar: Int) extends SparkListenerEvent
