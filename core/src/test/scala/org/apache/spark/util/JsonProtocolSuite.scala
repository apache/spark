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

import scala.collection.JavaConverters._
import scala.collection.Map

import org.json4s.jackson.JsonMethods._
import org.json4s.JsonAST.{JArray, JInt, JString, JValue}
import org.json4s.JsonDSL._
import org.scalatest.Assertions
import org.scalatest.exceptions.TestFailedException

import org.apache.spark._
import org.apache.spark.executor._
import org.apache.spark.rdd.RDDOperationScope
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.cluster.ExecutorInfo
import org.apache.spark.shuffle.MetadataFetchFailedException
import org.apache.spark.storage._

class JsonProtocolSuite extends SparkFunSuite {
  import JsonProtocolSuite._

  test("SparkListenerEvent") {
    val stageSubmitted =
      SparkListenerStageSubmitted(makeStageInfo(100, 200, 300, 400L, 500L), properties)
    val stageCompleted = SparkListenerStageCompleted(makeStageInfo(101, 201, 301, 401L, 501L))
    val taskStart = SparkListenerTaskStart(111, 0, makeTaskInfo(222L, 333, 1, 444L, false))
    val taskGettingResult =
      SparkListenerTaskGettingResult(makeTaskInfo(1000L, 2000, 5, 3000L, true))
    val taskEnd = SparkListenerTaskEnd(1, 0, "ShuffleMapTask", Success,
      makeTaskInfo(123L, 234, 67, 345L, false),
      makeTaskMetrics(300L, 400L, 500L, 600L, 700, 800, hasHadoopInput = false, hasOutput = false))
    val taskEndWithHadoopInput = SparkListenerTaskEnd(1, 0, "ShuffleMapTask", Success,
      makeTaskInfo(123L, 234, 67, 345L, false),
      makeTaskMetrics(300L, 400L, 500L, 600L, 700, 800, hasHadoopInput = true, hasOutput = false))
    val taskEndWithOutput = SparkListenerTaskEnd(1, 0, "ResultTask", Success,
      makeTaskInfo(123L, 234, 67, 345L, false),
      makeTaskMetrics(300L, 400L, 500L, 600L, 700, 800, hasHadoopInput = true, hasOutput = true))
    val jobStart = {
      val stageIds = Seq[Int](1, 2, 3, 4)
      val stageInfos = stageIds.map(x =>
        makeStageInfo(x, x * 200, x * 300, x * 400L, x * 500L))
      SparkListenerJobStart(10, jobSubmissionTime, stageInfos, properties)
    }
    val jobEnd = SparkListenerJobEnd(20, jobCompletionTime, JobSucceeded)
    val environmentUpdate = SparkListenerEnvironmentUpdate(Map[String, Seq[(String, String)]](
      "JVM Information" -> Seq(("GC speed", "9999 objects/s"), ("Java home", "Land of coffee")),
      "Spark Properties" -> Seq(("Job throughput", "80000 jobs/s, regardless of job type")),
      "System Properties" -> Seq(("Username", "guest"), ("Password", "guest")),
      "Classpath Entries" -> Seq(("Super library", "/tmp/super_library"))
    ))
    val blockManagerAdded = SparkListenerBlockManagerAdded(1L,
      BlockManagerId("Stars", "In your multitude...", 300), 500)
    val blockManagerRemoved = SparkListenerBlockManagerRemoved(2L,
      BlockManagerId("Scarce", "to be counted...", 100))
    val unpersistRdd = SparkListenerUnpersistRDD(12345)
    val logUrlMap = Map("stderr" -> "mystderr", "stdout" -> "mystdout").toMap
    val applicationStart = SparkListenerApplicationStart("The winner of all", Some("appId"),
      42L, "Garfield", Some("appAttempt"))
    val applicationStartWithLogs = SparkListenerApplicationStart("The winner of all", Some("appId"),
      42L, "Garfield", Some("appAttempt"), Some(logUrlMap))
    val applicationEnd = SparkListenerApplicationEnd(42L)
    val executorAdded = SparkListenerExecutorAdded(executorAddedTime, "exec1",
      new ExecutorInfo("Hostee.awesome.com", 11, logUrlMap))
    val executorRemoved = SparkListenerExecutorRemoved(executorRemovedTime, "exec2", "test reason")
    val executorMetricsUpdate = {
      // Use custom accum ID for determinism
      val accumUpdates =
        makeTaskMetrics(300L, 400L, 500L, 600L, 700, 800, hasHadoopInput = true, hasOutput = true)
          .accumulators().map(AccumulatorSuite.makeInfo)
          .zipWithIndex.map { case (a, i) => a.copy(id = i) }
      SparkListenerExecutorMetricsUpdate("exec3", Seq((1L, 2, 3, accumUpdates)))
    }

    testEvent(stageSubmitted, stageSubmittedJsonString)
    testEvent(stageCompleted, stageCompletedJsonString)
    testEvent(taskStart, taskStartJsonString)
    testEvent(taskGettingResult, taskGettingResultJsonString)
    testEvent(taskEnd, taskEndJsonString)
    testEvent(taskEndWithHadoopInput, taskEndWithHadoopInputJsonString)
    testEvent(taskEndWithOutput, taskEndWithOutputJsonString)
    testEvent(jobStart, jobStartJsonString)
    testEvent(jobEnd, jobEndJsonString)
    testEvent(environmentUpdate, environmentUpdateJsonString)
    testEvent(blockManagerAdded, blockManagerAddedJsonString)
    testEvent(blockManagerRemoved, blockManagerRemovedJsonString)
    testEvent(unpersistRdd, unpersistRDDJsonString)
    testEvent(applicationStart, applicationStartJsonString)
    testEvent(applicationStartWithLogs, applicationStartJsonWithLogUrlsString)
    testEvent(applicationEnd, applicationEndJsonString)
    testEvent(executorAdded, executorAddedJsonString)
    testEvent(executorRemoved, executorRemovedJsonString)
    testEvent(executorMetricsUpdate, executorMetricsUpdateJsonString)
  }

  test("Dependent Classes") {
    val logUrlMap = Map("stderr" -> "mystderr", "stdout" -> "mystdout").toMap
    testRDDInfo(makeRddInfo(2, 3, 4, 5L, 6L))
    testStageInfo(makeStageInfo(10, 20, 30, 40L, 50L))
    testTaskInfo(makeTaskInfo(999L, 888, 55, 777L, false))
    testTaskMetrics(makeTaskMetrics(
      33333L, 44444L, 55555L, 66666L, 7, 8, hasHadoopInput = false, hasOutput = false))
    testBlockManagerId(BlockManagerId("Hong", "Kong", 500))
    testExecutorInfo(new ExecutorInfo("host", 43, logUrlMap))

    // StorageLevel
    testStorageLevel(StorageLevel.NONE)
    testStorageLevel(StorageLevel.DISK_ONLY)
    testStorageLevel(StorageLevel.DISK_ONLY_2)
    testStorageLevel(StorageLevel.MEMORY_ONLY)
    testStorageLevel(StorageLevel.MEMORY_ONLY_2)
    testStorageLevel(StorageLevel.MEMORY_ONLY_SER)
    testStorageLevel(StorageLevel.MEMORY_ONLY_SER_2)
    testStorageLevel(StorageLevel.MEMORY_AND_DISK)
    testStorageLevel(StorageLevel.MEMORY_AND_DISK_2)
    testStorageLevel(StorageLevel.MEMORY_AND_DISK_SER)
    testStorageLevel(StorageLevel.MEMORY_AND_DISK_SER_2)

    // JobResult
    val exception = new Exception("Out of Memory! Please restock film.")
    exception.setStackTrace(stackTrace)
    val jobFailed = JobFailed(exception)
    testJobResult(JobSucceeded)
    testJobResult(jobFailed)

    // TaskEndReason
    val fetchFailed = FetchFailed(BlockManagerId("With or", "without you", 15), 17, 18, 19,
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
    testTaskEndReason(TaskKilled)
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
    val oldEvent = JsonProtocol.taskEndReasonToJson(exceptionFailure)
      .removeField({ _._1 == "Full Stack Trace" })
    assertEquals(exceptionFailure, JsonProtocol.taskEndReasonFromJson(oldEvent))
  }

  test("StageInfo backward compatibility (details, accumulables)") {
    val info = makeStageInfo(1, 2, 3, 4L, 5L)
    val newJson = JsonProtocol.stageInfoToJson(info)

    // Fields added after 1.0.0.
    assert(info.details.nonEmpty)
    assert(info.accumulables.nonEmpty)
    val oldJson = newJson
      .removeField { case (field, _) => field == "Details" }
      .removeField { case (field, _) => field == "Accumulables" }

    val newInfo = JsonProtocol.stageInfoFromJson(oldJson)

    assert(info.name === newInfo.name)
    assert("" === newInfo.details)
    assert(0 === newInfo.accumulables.size)
  }

  test("InputMetrics backward compatibility") {
    // InputMetrics were added after 1.0.1.
    val metrics = makeTaskMetrics(1L, 2L, 3L, 4L, 5, 6, hasHadoopInput = true, hasOutput = false)
    val newJson = JsonProtocol.taskMetricsToJson(metrics)
    val oldJson = newJson.removeField { case (field, _) => field == "Input Metrics" }
    val newMetrics = JsonProtocol.taskMetricsFromJson(oldJson)
  }

  test("Input/Output records backwards compatibility") {
    // records read were added after 1.2
    val metrics = makeTaskMetrics(1L, 2L, 3L, 4L, 5, 6,
      hasHadoopInput = true, hasOutput = true, hasRecords = false)
    val newJson = JsonProtocol.taskMetricsToJson(metrics)
    val oldJson = newJson.removeField { case (field, _) => field == "Records Read" }
                         .removeField { case (field, _) => field == "Records Written" }
    val newMetrics = JsonProtocol.taskMetricsFromJson(oldJson)
    assert(newMetrics.inputMetrics.recordsRead == 0)
    assert(newMetrics.outputMetrics.recordsWritten == 0)
  }

  test("Shuffle Read/Write records backwards compatibility") {
    // records read were added after 1.2
    val metrics = makeTaskMetrics(1L, 2L, 3L, 4L, 5, 6,
      hasHadoopInput = false, hasOutput = false, hasRecords = false)
    val newJson = JsonProtocol.taskMetricsToJson(metrics)
    val oldJson = newJson.removeField { case (field, _) => field == "Total Records Read" }
                         .removeField { case (field, _) => field == "Shuffle Records Written" }
    val newMetrics = JsonProtocol.taskMetricsFromJson(oldJson)
    assert(newMetrics.shuffleReadMetrics.recordsRead == 0)
    assert(newMetrics.shuffleWriteMetrics.recordsWritten == 0)
  }

  test("OutputMetrics backward compatibility") {
    // OutputMetrics were added after 1.1
    val metrics = makeTaskMetrics(1L, 2L, 3L, 4L, 5, 6, hasHadoopInput = false, hasOutput = true)
    val newJson = JsonProtocol.taskMetricsToJson(metrics)
    val oldJson = newJson.removeField { case (field, _) => field == "Output Metrics" }
    val newMetrics = JsonProtocol.taskMetricsFromJson(oldJson)
  }

  test("BlockManager events backward compatibility") {
    // SparkListenerBlockManagerAdded/Removed in Spark 1.0.0 do not have a "time" property.
    val blockManagerAdded = SparkListenerBlockManagerAdded(1L,
      BlockManagerId("Stars", "In your multitude...", 300), 500)
    val blockManagerRemoved = SparkListenerBlockManagerRemoved(2L,
      BlockManagerId("Scarce", "to be counted...", 100))

    val oldBmAdded = JsonProtocol.blockManagerAddedToJson(blockManagerAdded)
      .removeField({ _._1 == "Timestamp" })

    val deserializedBmAdded = JsonProtocol.blockManagerAddedFromJson(oldBmAdded)
    assert(SparkListenerBlockManagerAdded(-1L, blockManagerAdded.blockManagerId,
      blockManagerAdded.maxMem) === deserializedBmAdded)

    val oldBmRemoved = JsonProtocol.blockManagerRemovedToJson(blockManagerRemoved)
      .removeField({ _._1 == "Timestamp" })

    val deserializedBmRemoved = JsonProtocol.blockManagerRemovedFromJson(oldBmRemoved)
    assert(SparkListenerBlockManagerRemoved(-1L, blockManagerRemoved.blockManagerId) ===
      deserializedBmRemoved)
  }

  test("FetchFailed backwards compatibility") {
    // FetchFailed in Spark 1.1.0 does not have a "Message" property.
    val fetchFailed = FetchFailed(BlockManagerId("With or", "without you", 15), 17, 18, 19,
      "ignored")
    val oldEvent = JsonProtocol.taskEndReasonToJson(fetchFailed)
      .removeField({ _._1 == "Message" })
    val expectedFetchFailed = FetchFailed(BlockManagerId("With or", "without you", 15), 17, 18, 19,
      "Unknown reason")
    assert(expectedFetchFailed === JsonProtocol.taskEndReasonFromJson(oldEvent))
  }

  test("ShuffleReadMetrics: Local bytes read backwards compatibility") {
    // Metrics about local shuffle bytes read were added in 1.3.1.
    val metrics = makeTaskMetrics(1L, 2L, 3L, 4L, 5, 6,
      hasHadoopInput = false, hasOutput = false, hasRecords = false)
    val newJson = JsonProtocol.taskMetricsToJson(metrics)
    val oldJson = newJson.removeField { case (field, _) => field == "Local Bytes Read" }
    val newMetrics = JsonProtocol.taskMetricsFromJson(oldJson)
    assert(newMetrics.shuffleReadMetrics.localBytesRead == 0)
  }

  test("SparkListenerApplicationStart backwards compatibility") {
    // SparkListenerApplicationStart in Spark 1.0.0 do not have an "appId" property.
    // SparkListenerApplicationStart pre-Spark 1.4 does not have "appAttemptId".
    // SparkListenerApplicationStart pre-Spark 1.5 does not have "driverLogs
    val applicationStart = SparkListenerApplicationStart("test", None, 1L, "user", None, None)
    val oldEvent = JsonProtocol.applicationStartToJson(applicationStart)
      .removeField({ _._1 == "App ID" })
      .removeField({ _._1 == "App Attempt ID" })
      .removeField({ _._1 == "Driver Logs"})
    assert(applicationStart === JsonProtocol.applicationStartFromJson(oldEvent))
  }

  test("ExecutorLostFailure backward compatibility") {
    // ExecutorLostFailure in Spark 1.1.0 does not have an "Executor ID" property.
    val executorLostFailure = ExecutorLostFailure("100", true, Some("Induced failure"))
    val oldEvent = JsonProtocol.taskEndReasonToJson(executorLostFailure)
      .removeField({ _._1 == "Executor ID" })
    val expectedExecutorLostFailure = ExecutorLostFailure("Unknown", true, Some("Induced failure"))
    assert(expectedExecutorLostFailure === JsonProtocol.taskEndReasonFromJson(oldEvent))
  }

  test("SparkListenerJobStart backward compatibility") {
    // Prior to Spark 1.2.0, SparkListenerJobStart did not have a "Stage Infos" property.
    val stageIds = Seq[Int](1, 2, 3, 4)
    val stageInfos = stageIds.map(x => makeStageInfo(x, x * 200, x * 300, x * 400, x * 500))
    val dummyStageInfos =
      stageIds.map(id => new StageInfo(id, 0, "unknown", 0, Seq.empty, Seq.empty, "unknown"))
    val jobStart = SparkListenerJobStart(10, jobSubmissionTime, stageInfos, properties)
    val oldEvent = JsonProtocol.jobStartToJson(jobStart).removeField({_._1 == "Stage Infos"})
    val expectedJobStart =
      SparkListenerJobStart(10, jobSubmissionTime, dummyStageInfos, properties)
    assertEquals(expectedJobStart, JsonProtocol.jobStartFromJson(oldEvent))
  }

  test("SparkListenerJobStart and SparkListenerJobEnd backward compatibility") {
    // Prior to Spark 1.3.0, SparkListenerJobStart did not have a "Submission Time" property.
    // Also, SparkListenerJobEnd did not have a "Completion Time" property.
    val stageIds = Seq[Int](1, 2, 3, 4)
    val stageInfos = stageIds.map(x => makeStageInfo(x * 10, x * 20, x * 30, x * 40, x * 50))
    val jobStart = SparkListenerJobStart(11, jobSubmissionTime, stageInfos, properties)
    val oldStartEvent = JsonProtocol.jobStartToJson(jobStart)
      .removeField({ _._1 == "Submission Time"})
    val expectedJobStart = SparkListenerJobStart(11, -1, stageInfos, properties)
    assertEquals(expectedJobStart, JsonProtocol.jobStartFromJson(oldStartEvent))

    val jobEnd = SparkListenerJobEnd(11, jobCompletionTime, JobSucceeded)
    val oldEndEvent = JsonProtocol.jobEndToJson(jobEnd)
      .removeField({ _._1 == "Completion Time"})
    val expectedJobEnd = SparkListenerJobEnd(11, -1, JobSucceeded)
    assertEquals(expectedJobEnd, JsonProtocol.jobEndFromJson(oldEndEvent))
  }

  test("RDDInfo backward compatibility (scope, parent IDs, callsite)") {
    // "Scope" and "Parent IDs" were introduced in Spark 1.4.0
    // "Callsite" was introduced in Spark 1.6.0
    val rddInfo = new RDDInfo(1, "one", 100, StorageLevel.NONE, Seq(1, 6, 8),
      "callsite", Some(new RDDOperationScope("fable")))
    val oldRddInfoJson = JsonProtocol.rddInfoToJson(rddInfo)
      .removeField({ _._1 == "Parent IDs"})
      .removeField({ _._1 == "Scope"})
      .removeField({ _._1 == "Callsite"})
    val expectedRddInfo = new RDDInfo(
      1, "one", 100, StorageLevel.NONE, Seq.empty, "", scope = None)
    assertEquals(expectedRddInfo, JsonProtocol.rddInfoFromJson(oldRddInfoJson))
  }

  test("StageInfo backward compatibility (parent IDs)") {
    // Prior to Spark 1.4.0, StageInfo did not have the "Parent IDs" property
    val stageInfo = new StageInfo(1, 1, "me-stage", 1, Seq.empty, Seq(1, 2, 3), "details")
    val oldStageInfo = JsonProtocol.stageInfoToJson(stageInfo).removeField({ _._1 == "Parent IDs"})
    val expectedStageInfo = new StageInfo(1, 1, "me-stage", 1, Seq.empty, Seq.empty, "details")
    assertEquals(expectedStageInfo, JsonProtocol.stageInfoFromJson(oldStageInfo))
  }

  // `TaskCommitDenied` was added in 1.3.0 but JSON de/serialization logic was added in 1.5.1
  test("TaskCommitDenied backward compatibility") {
    val denied = TaskCommitDenied(1, 2, 3)
    val oldDenied = JsonProtocol.taskEndReasonToJson(denied)
      .removeField({ _._1 == "Job ID" })
      .removeField({ _._1 == "Partition ID" })
      .removeField({ _._1 == "Attempt Number" })
    val expectedDenied = TaskCommitDenied(-1, -1, -1)
    assertEquals(expectedDenied, JsonProtocol.taskEndReasonFromJson(oldDenied))
  }

  test("AccumulableInfo backward compatibility") {
    // "Internal" property of AccumulableInfo was added in 1.5.1
    val accumulableInfo = makeAccumulableInfo(1, internal = true, countFailedValues = true)
    val accumulableInfoJson = JsonProtocol.accumulableInfoToJson(accumulableInfo)
    val oldJson = accumulableInfoJson.removeField({ _._1 == "Internal" })
    val oldInfo = JsonProtocol.accumulableInfoFromJson(oldJson)
    assert(!oldInfo.internal)
    // "Count Failed Values" property of AccumulableInfo was added in 2.0.0
    val oldJson2 = accumulableInfoJson.removeField({ _._1 == "Count Failed Values" })
    val oldInfo2 = JsonProtocol.accumulableInfoFromJson(oldJson2)
    assert(!oldInfo2.countFailedValues)
    // "Metadata" property of AccumulableInfo was added in 2.0.0
    val oldJson3 = accumulableInfoJson.removeField({ _._1 == "Metadata" })
    val oldInfo3 = JsonProtocol.accumulableInfoFromJson(oldJson3)
    assert(oldInfo3.metadata.isEmpty)
  }

  test("ExceptionFailure backward compatibility: accumulator updates") {
    // "Task Metrics" was replaced with "Accumulator Updates" in 2.0.0. For older event logs,
    // we should still be able to fallback to constructing the accumulator updates from the
    // "Task Metrics" field, if it exists.
    val tm = makeTaskMetrics(1L, 2L, 3L, 4L, 5, 6, hasHadoopInput = true, hasOutput = true)
    val tmJson = JsonProtocol.taskMetricsToJson(tm)
    val accumUpdates = tm.accumulators().map(AccumulatorSuite.makeInfo)
    val exception = new SparkException("sentimental")
    val exceptionFailure = new ExceptionFailure(exception, accumUpdates)
    val exceptionFailureJson = JsonProtocol.taskEndReasonToJson(exceptionFailure)
    val tmFieldJson: JValue = "Task Metrics" -> tmJson
    val oldExceptionFailureJson: JValue =
      exceptionFailureJson.removeField { _._1 == "Accumulator Updates" }.merge(tmFieldJson)
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

  test("AccumulableInfo value de/serialization") {
    import InternalAccumulator._
    val blocks = Seq[(BlockId, BlockStatus)](
      (TestBlockId("meebo"), BlockStatus(StorageLevel.MEMORY_ONLY, 1L, 2L)),
      (TestBlockId("feebo"), BlockStatus(StorageLevel.DISK_ONLY, 3L, 4L)))
    val blocksJson = JArray(blocks.toList.map { case (id, status) =>
      ("Block ID" -> id.toString) ~
      ("Status" -> JsonProtocol.blockStatusToJson(status))
    })
    testAccumValue(Some(RESULT_SIZE), 3L, JInt(3))
    testAccumValue(Some(shuffleRead.REMOTE_BLOCKS_FETCHED), 2, JInt(2))
    testAccumValue(Some(UPDATED_BLOCK_STATUSES), blocks.asJava, blocksJson)
    // For anything else, we just cast the value to a string
    testAccumValue(Some("anything"), blocks, JString(blocks.toString))
    testAccumValue(Some("anything"), 123, JString("123"))
  }

}


private[spark] object JsonProtocolSuite extends Assertions {
  import InternalAccumulator._

  private val jobSubmissionTime = 1421191042750L
  private val jobCompletionTime = 1421191296660L
  private val executorAddedTime = 1421458410000L
  private val executorRemovedTime = 1421458922000L

  private def testEvent(event: SparkListenerEvent, jsonString: String) {
    val actualJsonString = compact(render(JsonProtocol.sparkEventToJson(event)))
    val newEvent = JsonProtocol.sparkEventFromJson(parse(actualJsonString))
    assertJsonStringEquals(jsonString, actualJsonString, event.getClass.getSimpleName)
    assertEquals(event, newEvent)
  }

  private def testRDDInfo(info: RDDInfo) {
    val newInfo = JsonProtocol.rddInfoFromJson(JsonProtocol.rddInfoToJson(info))
    assertEquals(info, newInfo)
  }

  private def testStageInfo(info: StageInfo) {
    val newInfo = JsonProtocol.stageInfoFromJson(JsonProtocol.stageInfoToJson(info))
    assertEquals(info, newInfo)
  }

  private def testStorageLevel(level: StorageLevel) {
    val newLevel = JsonProtocol.storageLevelFromJson(JsonProtocol.storageLevelToJson(level))
    assertEquals(level, newLevel)
  }

  private def testTaskMetrics(metrics: TaskMetrics) {
    val newMetrics = JsonProtocol.taskMetricsFromJson(JsonProtocol.taskMetricsToJson(metrics))
    assertEquals(metrics, newMetrics)
  }

  private def testBlockManagerId(id: BlockManagerId) {
    val newId = JsonProtocol.blockManagerIdFromJson(JsonProtocol.blockManagerIdToJson(id))
    assert(id === newId)
  }

  private def testTaskInfo(info: TaskInfo) {
    val newInfo = JsonProtocol.taskInfoFromJson(JsonProtocol.taskInfoToJson(info))
    assertEquals(info, newInfo)
  }

  private def testJobResult(result: JobResult) {
    val newResult = JsonProtocol.jobResultFromJson(JsonProtocol.jobResultToJson(result))
    assertEquals(result, newResult)
  }

  private def testTaskEndReason(reason: TaskEndReason) {
    val newReason = JsonProtocol.taskEndReasonFromJson(JsonProtocol.taskEndReasonToJson(reason))
    assertEquals(reason, newReason)
  }

  private def testBlockId(blockId: BlockId) {
    val newBlockId = BlockId(blockId.toString)
    assert(blockId === newBlockId)
  }

  private def testExecutorInfo(info: ExecutorInfo) {
    val newInfo = JsonProtocol.executorInfoFromJson(JsonProtocol.executorInfoToJson(info))
    assertEquals(info, newInfo)
  }

  private def testAccumValue(name: Option[String], value: Any, expectedJson: JValue): Unit = {
    val json = JsonProtocol.accumValueToJson(name, value)
    assert(json === expectedJson)
    val newValue = JsonProtocol.accumValueFromJson(name, json)
    val expectedValue = if (name.exists(_.startsWith(METRICS_PREFIX))) value else value.toString
    assert(newValue === expectedValue)
  }

  /** -------------------------------- *
   | Util methods for comparing events |
   * --------------------------------- */

  private[spark] def assertEquals(event1: SparkListenerEvent, event2: SparkListenerEvent) {
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
        assert(e1.taskType === e2.taskType)
        assertEquals(e1.reason, e2.reason)
        assertEquals(e1.taskInfo, e2.taskInfo)
        assertEquals(e1.taskMetrics, e2.taskMetrics)
      case (e1: SparkListenerJobStart, e2: SparkListenerJobStart) =>
        assert(e1.jobId === e2.jobId)
        assert(e1.properties === e2.properties)
        assert(e1.stageIds === e2.stageIds)
      case (e1: SparkListenerJobEnd, e2: SparkListenerJobEnd) =>
        assert(e1.jobId === e2.jobId)
        assertEquals(e1.jobResult, e2.jobResult)
      case (e1: SparkListenerEnvironmentUpdate, e2: SparkListenerEnvironmentUpdate) =>
        assertEquals(e1.environmentDetails, e2.environmentDetails)
      case (e1: SparkListenerExecutorAdded, e2: SparkListenerExecutorAdded) =>
        assert(e1.executorId === e1.executorId)
        assertEquals(e1.executorInfo, e2.executorInfo)
      case (e1: SparkListenerExecutorRemoved, e2: SparkListenerExecutorRemoved) =>
        assert(e1.executorId === e1.executorId)
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
      case (e1, e2) =>
        assert(e1 === e2)
      case _ => fail("Events don't match in types!")
    }
  }

  private def assertEquals(info1: StageInfo, info2: StageInfo) {
    assert(info1.stageId === info2.stageId)
    assert(info1.name === info2.name)
    assert(info1.numTasks === info2.numTasks)
    assert(info1.submissionTime === info2.submissionTime)
    assert(info1.completionTime === info2.completionTime)
    assert(info1.rddInfos.size === info2.rddInfos.size)
    (0 until info1.rddInfos.size).foreach { i =>
      assertEquals(info1.rddInfos(i), info2.rddInfos(i))
    }
    assert(info1.accumulables === info2.accumulables)
    assert(info1.details === info2.details)
  }

  private def assertEquals(info1: RDDInfo, info2: RDDInfo) {
    assert(info1.id === info2.id)
    assert(info1.name === info2.name)
    assert(info1.numPartitions === info2.numPartitions)
    assert(info1.numCachedPartitions === info2.numCachedPartitions)
    assert(info1.memSize === info2.memSize)
    assert(info1.diskSize === info2.diskSize)
    assertEquals(info1.storageLevel, info2.storageLevel)
  }

  private def assertEquals(level1: StorageLevel, level2: StorageLevel) {
    assert(level1.useDisk === level2.useDisk)
    assert(level1.useMemory === level2.useMemory)
    assert(level1.deserialized === level2.deserialized)
    assert(level1.replication === level2.replication)
  }

  private def assertEquals(info1: TaskInfo, info2: TaskInfo) {
    assert(info1.taskId === info2.taskId)
    assert(info1.index === info2.index)
    assert(info1.attemptNumber === info2.attemptNumber)
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

  private def assertEquals(info1: ExecutorInfo, info2: ExecutorInfo) {
    assert(info1.executorHost == info2.executorHost)
    assert(info1.totalCores == info2.totalCores)
  }

  private def assertEquals(metrics1: TaskMetrics, metrics2: TaskMetrics) {
    assert(metrics1.executorDeserializeTime === metrics2.executorDeserializeTime)
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

  private def assertEquals(metrics1: ShuffleReadMetrics, metrics2: ShuffleReadMetrics) {
    assert(metrics1.remoteBlocksFetched === metrics2.remoteBlocksFetched)
    assert(metrics1.localBlocksFetched === metrics2.localBlocksFetched)
    assert(metrics1.fetchWaitTime === metrics2.fetchWaitTime)
    assert(metrics1.remoteBytesRead === metrics2.remoteBytesRead)
  }

  private def assertEquals(metrics1: ShuffleWriteMetrics, metrics2: ShuffleWriteMetrics) {
    assert(metrics1.bytesWritten === metrics2.bytesWritten)
    assert(metrics1.writeTime === metrics2.writeTime)
  }

  private def assertEquals(metrics1: InputMetrics, metrics2: InputMetrics) {
    assert(metrics1.bytesRead === metrics2.bytesRead)
  }

  private def assertEquals(result1: JobResult, result2: JobResult) {
    (result1, result2) match {
      case (JobSucceeded, JobSucceeded) =>
      case (r1: JobFailed, r2: JobFailed) =>
        assertEquals(r1.exception, r2.exception)
      case _ => fail("Job results don't match in types!")
    }
  }

  private def assertEquals(reason1: TaskEndReason, reason2: TaskEndReason) {
    (reason1, reason2) match {
      case (Success, Success) =>
      case (Resubmitted, Resubmitted) =>
      case (r1: FetchFailed, r2: FetchFailed) =>
        assert(r1.shuffleId === r2.shuffleId)
        assert(r1.mapId === r2.mapId)
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
      case (TaskKilled, TaskKilled) =>
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
      details1: Map[String, Seq[(String, String)]],
      details2: Map[String, Seq[(String, String)]]) {
    details1.zip(details2).foreach {
      case ((key1, values1: Seq[(String, String)]), (key2, values2: Seq[(String, String)])) =>
        assert(key1 === key2)
        values1.zip(values2).foreach { case (v1, v2) => assert(v1 === v2) }
    }
  }

  private def assertEquals(exception1: Exception, exception2: Exception) {
    assert(exception1.getMessage === exception2.getMessage)
    assertSeqEquals(
      exception1.getStackTrace,
      exception2.getStackTrace,
      assertStackTraceElementEquals)
  }

  private def assertJsonStringEquals(expected: String, actual: String, metadata: String) {
    val expectedJson = pretty(parse(expected))
    val actualJson = pretty(parse(actual))
    if (expectedJson != actualJson) {
      // scalastyle:off
      // This prints something useful if the JSON strings don't match
      println("=== EXPECTED ===\n" + expectedJson + "\n")
      println("=== ACTUAL ===\n" + actualJson + "\n")
      // scalastyle:on
      throw new TestFailedException(s"$metadata JSON did not equal", 1)
    }
  }

  private def assertSeqEquals[T](seq1: Seq[T], seq2: Seq[T], assertEquals: (T, T) => Unit) {
    assert(seq1.length === seq2.length)
    seq1.zip(seq2).foreach { case (t1, t2) =>
      assertEquals(t1, t2)
    }
  }

  private def assertOptionEquals[T](
      opt1: Option[T],
      opt2: Option[T],
      assertEquals: (T, T) => Unit) {
    if (opt1.isDefined) {
      assert(opt2.isDefined)
      assertEquals(opt1.get, opt2.get)
    } else {
      assert(!opt2.isDefined)
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

  private def assertBlockEquals(b1: (BlockId, BlockStatus), b2: (BlockId, BlockStatus)) {
    assert(b1 === b2)
  }

  private def assertStackTraceElementEquals(ste1: StackTraceElement, ste2: StackTraceElement) {
    assert(ste1 === ste2)
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
      new StackTraceElement("Afollo", "Vemus", "Mercurry", 420),
      new StackTraceElement("Ayollo", "Vesus", "Blackberry", 4200)
    )
  }

  private def makeRddInfo(a: Int, b: Int, c: Int, d: Long, e: Long) = {
    val r = new RDDInfo(a, "mayor", b, StorageLevel.MEMORY_AND_DISK, Seq(1, 4, 7), a.toString)
    r.numCachedPartitions = c
    r.memSize = d
    r.diskSize = e
    r
  }

  private def makeStageInfo(a: Int, b: Int, c: Int, d: Long, e: Long) = {
    val rddInfos = (0 until a % 5).map { i => makeRddInfo(a + i, b + i, c + i, d + i, e + i) }
    val stageInfo = new StageInfo(a, 0, "greetings", b, rddInfos, Seq(100, 200, 300), "details")
    val (acc1, acc2) = (makeAccumulableInfo(1), makeAccumulableInfo(2))
    stageInfo.accumulables(acc1.id) = acc1
    stageInfo.accumulables(acc2.id) = acc2
    stageInfo
  }

  private def makeTaskInfo(a: Long, b: Int, c: Int, d: Long, speculative: Boolean) = {
    val taskInfo = new TaskInfo(a, b, c, d, "executor", "your kind sir", TaskLocality.NODE_LOCAL,
      speculative)
    val (acc1, acc2, acc3) =
      (makeAccumulableInfo(1), makeAccumulableInfo(2), makeAccumulableInfo(3, internal = true))
    taskInfo.accumulables += acc1
    taskInfo.accumulables += acc2
    taskInfo.accumulables += acc3
    taskInfo
  }

  private def makeAccumulableInfo(
      id: Int,
      internal: Boolean = false,
      countFailedValues: Boolean = false,
      metadata: Option[String] = None): AccumulableInfo =
    new AccumulableInfo(id, Some(s"Accumulable$id"), Some(s"delta$id"), Some(s"val$id"),
      internal, countFailedValues, metadata)

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
      hasHadoopInput: Boolean,
      hasOutput: Boolean,
      hasRecords: Boolean = true) = {
    val t = TaskMetrics.empty
    t.setExecutorDeserializeTime(a)
    t.setExecutorRunTime(b)
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
      sr.incLocalBlocksFetched(e)
      sr.incFetchWaitTime(a + d)
      sr.incRemoteBlocksFetched(f)
      sr.incRecordsRead(if (hasRecords) (b + d) / 100 else -1)
      sr.incLocalBytesRead(a + f)
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
      |    "Accumulables": [
      |      {
      |        "ID": 2,
      |        "Name": "Accumulable2",
      |        "Update": "delta2",
      |        "Value": "val2",
      |        "Internal": false,
      |        "Count Failed Values": false
      |      },
      |      {
      |        "ID": 1,
      |        "Name": "Accumulable1",
      |        "Update": "delta1",
      |        "Value": "val1",
      |        "Internal": false,
      |        "Count Failed Values": false
      |      }
      |    ]
      |  },
      |  "Properties": {
      |    "France": "Paris",
      |    "Germany": "Berlin",
      |    "Russia": "Moscow",
      |    "Ukraine": "Kiev"
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
      |          "Deserialized": true,
      |          "Replication": 1
      |        },
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
      |        "ID": 2,
      |        "Name": "Accumulable2",
      |        "Update": "delta2",
      |        "Value": "val2",
      |        "Internal": false,
      |        "Count Failed Values": false
      |      },
      |      {
      |        "ID": 1,
      |        "Name": "Accumulable1",
      |        "Update": "delta1",
      |        "Value": "val1",
      |        "Internal": false,
      |        "Count Failed Values": false
      |      }
      |    ]
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
      |    "Launch Time": 444,
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
      |    "Launch Time": 3000,
      |    "Executor ID": "executor",
      |    "Host": "your kind sir",
      |    "Locality": "NODE_LOCAL",
      |    "Speculative": true,
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
      |  "Task Metrics": {
      |    "Executor Deserialize Time": 300,
      |    "Executor Run Time": 400,
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
      |      "Local Bytes Read": 1100,
      |      "Total Records Read": 10
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
      |  "Task Metrics": {
      |    "Executor Deserialize Time": 300,
      |    "Executor Run Time": 400,
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
      |      "Local Bytes Read" : 0,
      |      "Total Records Read" : 0
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
      |  "Task Metrics": {
      |    "Executor Deserialize Time": 300,
      |    "Executor Run Time": 400,
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
      |      "Local Bytes Read" : 0,
      |      "Total Records Read" : 0
      |    },
      |    "Shuffle Write Metrics" : {
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
      |            "Deserialized": true,
      |            "Replication": 1
      |          },
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
      |          "ID": 2,
      |          "Name": "Accumulable2",
      |          "Update": "delta2",
      |          "Value": "val2",
      |          "Internal": false,
      |          "Count Failed Values": false
      |        },
      |        {
      |          "ID": 1,
      |          "Name": "Accumulable1",
      |          "Update": "delta1",
      |          "Value": "val1",
      |          "Internal": false,
      |          "Count Failed Values": false
      |        }
      |      ]
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
      |            "Deserialized": true,
      |            "Replication": 1
      |          },
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
      |            "Deserialized": true,
      |            "Replication": 1
      |          },
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
      |          "ID": 2,
      |          "Name": "Accumulable2",
      |          "Update": "delta2",
      |          "Value": "val2",
      |          "Internal": false,
      |          "Count Failed Values": false
      |        },
      |        {
      |          "ID": 1,
      |          "Name": "Accumulable1",
      |          "Update": "delta1",
      |          "Value": "val1",
      |          "Internal": false,
      |          "Count Failed Values": false
      |        }
      |      ]
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
      |            "Deserialized": true,
      |            "Replication": 1
      |          },
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
      |            "Deserialized": true,
      |            "Replication": 1
      |          },
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
      |            "Deserialized": true,
      |            "Replication": 1
      |          },
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
      |          "ID": 2,
      |          "Name": "Accumulable2",
      |          "Update": "delta2",
      |          "Value": "val2",
      |          "Internal": false,
      |          "Count Failed Values": false
      |        },
      |        {
      |          "ID": 1,
      |          "Name": "Accumulable1",
      |          "Update": "delta1",
      |          "Value": "val1",
      |          "Internal": false,
      |          "Count Failed Values": false
      |        }
      |      ]
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
      |            "Deserialized": true,
      |            "Replication": 1
      |          },
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
      |            "Deserialized": true,
      |            "Replication": 1
      |          },
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
      |            "Deserialized": true,
      |            "Replication": 1
      |          },
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
      |            "Deserialized": true,
      |            "Replication": 1
      |          },
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
      |          "ID": 2,
      |          "Name": "Accumulable2",
      |          "Update": "delta2",
      |          "Value": "val2",
      |          "Internal": false,
      |          "Count Failed Values": false
      |        },
      |        {
      |          "ID": 1,
      |          "Name": "Accumulable1",
      |          "Update": "delta1",
      |          "Value": "val1",
      |          "Internal": false,
      |          "Count Failed Values": false
      |        }
      |      ]
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
      |  "System Properties": {
      |    "Username": "guest",
      |    "Password": "guest"
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
      |    }
      |  }
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
      |          "Name": "$EXECUTOR_RUN_TIME",
      |          "Update": 400,
      |          "Internal": true,
      |          "Count Failed Values": true
      |        },
      |        {
      |          "ID": 2,
      |          "Name": "$RESULT_SIZE",
      |          "Update": 500,
      |          "Internal": true,
      |          "Count Failed Values": true
      |        },
      |        {
      |          "ID": 3,
      |          "Name": "$JVM_GC_TIME",
      |          "Update": 600,
      |          "Internal": true,
      |          "Count Failed Values": true
      |        },
      |        {
      |          "ID": 4,
      |          "Name": "$RESULT_SERIALIZATION_TIME",
      |          "Update": 700,
      |          "Internal": true,
      |          "Count Failed Values": true
      |        },
      |        {
      |          "ID": 5,
      |          "Name": "$MEMORY_BYTES_SPILLED",
      |          "Update": 800,
      |          "Internal": true,
      |          "Count Failed Values": true
      |        },
      |        {
      |          "ID": 6,
      |          "Name": "$DISK_BYTES_SPILLED",
      |          "Update": 0,
      |          "Internal": true,
      |          "Count Failed Values": true
      |        },
      |        {
      |          "ID": 7,
      |          "Name": "$PEAK_EXECUTION_MEMORY",
      |          "Update": 0,
      |          "Internal": true,
      |          "Count Failed Values": true
      |        },
      |        {
      |          "ID": 8,
      |          "Name": "$UPDATED_BLOCK_STATUSES",
      |          "Update": [
      |            {
      |              "Block ID": "rdd_0_0",
      |              "Status": {
      |                "Storage Level": {
      |                  "Use Disk": true,
      |                  "Use Memory": true,
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
      |          "ID": 9,
      |          "Name": "${shuffleRead.REMOTE_BLOCKS_FETCHED}",
      |          "Update": 0,
      |          "Internal": true,
      |          "Count Failed Values": true
      |        },
      |        {
      |          "ID": 10,
      |          "Name": "${shuffleRead.LOCAL_BLOCKS_FETCHED}",
      |          "Update": 0,
      |          "Internal": true,
      |          "Count Failed Values": true
      |        },
      |        {
      |          "ID": 11,
      |          "Name": "${shuffleRead.REMOTE_BYTES_READ}",
      |          "Update": 0,
      |          "Internal": true,
      |          "Count Failed Values": true
      |        },
      |        {
      |          "ID": 12,
      |          "Name": "${shuffleRead.LOCAL_BYTES_READ}",
      |          "Update": 0,
      |          "Internal": true,
      |          "Count Failed Values": true
      |        },
      |        {
      |          "ID": 13,
      |          "Name": "${shuffleRead.FETCH_WAIT_TIME}",
      |          "Update": 0,
      |          "Internal": true,
      |          "Count Failed Values": true
      |        },
      |        {
      |          "ID": 14,
      |          "Name": "${shuffleRead.RECORDS_READ}",
      |          "Update": 0,
      |          "Internal": true,
      |          "Count Failed Values": true
      |        },
      |        {
      |          "ID": 15,
      |          "Name": "${shuffleWrite.BYTES_WRITTEN}",
      |          "Update": 0,
      |          "Internal": true,
      |          "Count Failed Values": true
      |        },
      |        {
      |          "ID": 16,
      |          "Name": "${shuffleWrite.RECORDS_WRITTEN}",
      |          "Update": 0,
      |          "Internal": true,
      |          "Count Failed Values": true
      |        },
      |        {
      |          "ID": 17,
      |          "Name": "${shuffleWrite.WRITE_TIME}",
      |          "Update": 0,
      |          "Internal": true,
      |          "Count Failed Values": true
      |        },
      |        {
      |          "ID": 18,
      |          "Name": "${input.BYTES_READ}",
      |          "Update": 2100,
      |          "Internal": true,
      |          "Count Failed Values": true
      |        },
      |        {
      |          "ID": 19,
      |          "Name": "${input.RECORDS_READ}",
      |          "Update": 21,
      |          "Internal": true,
      |          "Count Failed Values": true
      |        },
      |        {
      |          "ID": 20,
      |          "Name": "${output.BYTES_WRITTEN}",
      |          "Update": 1200,
      |          "Internal": true,
      |          "Count Failed Values": true
      |        },
      |        {
      |          "ID": 21,
      |          "Name": "${output.RECORDS_WRITTEN}",
      |          "Update": 12,
      |          "Internal": true,
      |          "Count Failed Values": true
      |        },
      |        {
      |          "ID": 22,
      |          "Name": "$TEST_ACCUM",
      |          "Update": 0,
      |          "Internal": true,
      |          "Count Failed Values": true
      |        }
      |      ]
      |    }
      |  ]
      |}
    """.stripMargin
}
