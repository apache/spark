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

import scala.collection.Map

import org.json4s.DefaultFormats
import org.json4s.JsonDSL._
import org.json4s.JsonAST._
import org.json4s.jackson.JsonMethods._
import org.scalatest.FunSuite

import org.apache.spark._
import org.apache.spark.executor._
import org.apache.spark.scheduler._
import org.apache.spark.storage._

class JsonProtocolSuite extends FunSuite {

  test("SparkListenerEvent") {
    val stageSubmitted =
      SparkListenerStageSubmitted(makeStageInfo(100, 200, 300, 400L, 500L), properties)
    val stageCompleted = SparkListenerStageCompleted(makeStageInfo(101, 201, 301, 401L, 501L))
    val taskStart = SparkListenerTaskStart(111, 0, makeTaskInfo(222L, 333, 1, 444L, false))
    val taskGettingResult =
      SparkListenerTaskGettingResult(makeTaskInfo(1000L, 2000, 5, 3000L, true))
    val taskEnd = SparkListenerTaskEnd(1, 0, "ShuffleMapTask", Success,
      makeTaskInfo(123L, 234, 67, 345L, false),
      makeTaskMetrics(300L, 400L, 500L, 600L, 700, 800, hasHadoopInput = false))
    val taskEndWithHadoopInput = SparkListenerTaskEnd(1, 0, "ShuffleMapTask", Success,
      makeTaskInfo(123L, 234, 67, 345L, false),
      makeTaskMetrics(300L, 400L, 500L, 600L, 700, 800, hasHadoopInput = true))
    val jobStart = SparkListenerJobStart(10, Seq[Int](1, 2, 3, 4), properties)
    val jobEnd = SparkListenerJobEnd(20, JobSucceeded)
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
    val applicationStart = SparkListenerApplicationStart("The winner of all", None, 42L, "Garfield")
    val applicationEnd = SparkListenerApplicationEnd(42L)

    testEvent(stageSubmitted, stageSubmittedJsonString)
    testEvent(stageCompleted, stageCompletedJsonString)
    testEvent(taskStart, taskStartJsonString)
    testEvent(taskGettingResult, taskGettingResultJsonString)
    testEvent(taskEnd, taskEndJsonString)
    testEvent(taskEndWithHadoopInput, taskEndWithHadoopInputJsonString)
    testEvent(jobStart, jobStartJsonString)
    testEvent(jobEnd, jobEndJsonString)
    testEvent(environmentUpdate, environmentUpdateJsonString)
    testEvent(blockManagerAdded, blockManagerAddedJsonString)
    testEvent(blockManagerRemoved, blockManagerRemovedJsonString)
    testEvent(unpersistRdd, unpersistRDDJsonString)
    testEvent(applicationStart, applicationStartJsonString)
    testEvent(applicationEnd, applicationEndJsonString)
  }

  test("Dependent Classes") {
    testRDDInfo(makeRddInfo(2, 3, 4, 5L, 6L))
    testStageInfo(makeStageInfo(10, 20, 30, 40L, 50L))
    testTaskInfo(makeTaskInfo(999L, 888, 55, 777L, false))
    testTaskMetrics(makeTaskMetrics(33333L, 44444L, 55555L, 66666L, 7, 8, hasHadoopInput = false))
    testBlockManagerId(BlockManagerId("Hong", "Kong", 500))

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
    val exceptionFailure = ExceptionFailure("To be", "or not to be", stackTrace, None)
    testTaskEndReason(Success)
    testTaskEndReason(Resubmitted)
    testTaskEndReason(fetchFailed)
    testTaskEndReason(exceptionFailure)
    testTaskEndReason(TaskResultLost)
    testTaskEndReason(TaskKilled)
    testTaskEndReason(ExecutorLostFailure("100"))
    testTaskEndReason(UnknownReason)

    // BlockId
    testBlockId(RDDBlockId(1, 2))
    testBlockId(ShuffleBlockId(1, 2, 3))
    testBlockId(BroadcastBlockId(1L, "insert_words_of_wisdom_here"))
    testBlockId(TaskResultBlockId(1L))
    testBlockId(StreamBlockId(1, 2L))
  }

  test("StageInfo backward compatibility") {
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
    val metrics = makeTaskMetrics(1L, 2L, 3L, 4L, 5, 6, hasHadoopInput = true)
    assert(metrics.inputMetrics.nonEmpty)
    val newJson = JsonProtocol.taskMetricsToJson(metrics)
    val oldJson = newJson.removeField { case (field, _) => field == "Input Metrics" }
    val newMetrics = JsonProtocol.taskMetricsFromJson(oldJson)
    assert(newMetrics.inputMetrics.isEmpty)
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
    // FetchFailed in Spark 1.1.0 does not have an "Message" property.
    val fetchFailed = FetchFailed(BlockManagerId("With or", "without you", 15), 17, 18, 19,
      "ignored")
    val oldEvent = JsonProtocol.taskEndReasonToJson(fetchFailed)
      .removeField({ _._1 == "Message" })
    val expectedFetchFailed = FetchFailed(BlockManagerId("With or", "without you", 15), 17, 18, 19,
      "Unknown reason")
    assert(expectedFetchFailed === JsonProtocol.taskEndReasonFromJson(oldEvent))
  }

  test("SparkListenerApplicationStart backwards compatibility") {
    // SparkListenerApplicationStart in Spark 1.0.0 do not have an "appId" property.
    val applicationStart = SparkListenerApplicationStart("test", None, 1L, "user")
    val oldEvent = JsonProtocol.applicationStartToJson(applicationStart)
      .removeField({ _._1 == "App ID" })
    assert(applicationStart === JsonProtocol.applicationStartFromJson(oldEvent))
  }

  test("ExecutorLostFailure backward compatibility") {
    // ExecutorLostFailure in Spark 1.1.0 does not have an "Executor ID" property.
    val executorLostFailure = ExecutorLostFailure("100")
    val oldEvent = JsonProtocol.taskEndReasonToJson(executorLostFailure)
      .removeField({ _._1 == "Executor ID" })
    val expectedExecutorLostFailure = ExecutorLostFailure("Unknown")
    assert(expectedExecutorLostFailure === JsonProtocol.taskEndReasonFromJson(oldEvent))
  }

  /** -------------------------- *
   | Helper test running methods |
   * --------------------------- */

  private def testEvent(event: SparkListenerEvent, jsonString: String) {
    val actualJsonString = compact(render(JsonProtocol.sparkEventToJson(event)))
    val newEvent = JsonProtocol.sparkEventFromJson(parse(actualJsonString))
    assertJsonStringEquals(jsonString, actualJsonString)
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
    assertEquals(id, newId)
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


  /** -------------------------------- *
   | Util methods for comparing events |
   * --------------------------------- */

  private def assertEquals(event1: SparkListenerEvent, event2: SparkListenerEvent) {
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
        assertSeqEquals(e1.stageIds, e2.stageIds, (i1: Int, i2: Int) => assert(i1 === i2))
      case (e1: SparkListenerJobEnd, e2: SparkListenerJobEnd) =>
        assert(e1.jobId === e2.jobId)
        assertEquals(e1.jobResult, e2.jobResult)
      case (e1: SparkListenerEnvironmentUpdate, e2: SparkListenerEnvironmentUpdate) =>
        assertEquals(e1.environmentDetails, e2.environmentDetails)
      case (e1: SparkListenerBlockManagerAdded, e2: SparkListenerBlockManagerAdded) =>
        assert(e1.maxMem === e2.maxMem)
        assert(e1.time === e2.time)
        assertEquals(e1.blockManagerId, e2.blockManagerId)
      case (e1: SparkListenerBlockManagerRemoved, e2: SparkListenerBlockManagerRemoved) =>
        assert(e1.time === e2.time)
        assertEquals(e1.blockManagerId, e2.blockManagerId)
      case (e1: SparkListenerUnpersistRDD, e2: SparkListenerUnpersistRDD) =>
        assert(e1.rddId == e2.rddId)
      case (e1: SparkListenerApplicationStart, e2: SparkListenerApplicationStart) =>
        assert(e1.appName == e2.appName)
        assert(e1.time == e2.time)
        assert(e1.sparkUser == e2.sparkUser)
      case (e1: SparkListenerApplicationEnd, e2: SparkListenerApplicationEnd) =>
        assert(e1.time == e2.time)
      case (SparkListenerShutdown, SparkListenerShutdown) =>
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
    assert(info1.attempt === info2.attempt)
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

  private def assertEquals(metrics1: TaskMetrics, metrics2: TaskMetrics) {
    assert(metrics1.hostname === metrics2.hostname)
    assert(metrics1.executorDeserializeTime === metrics2.executorDeserializeTime)
    assert(metrics1.resultSize === metrics2.resultSize)
    assert(metrics1.jvmGCTime === metrics2.jvmGCTime)
    assert(metrics1.resultSerializationTime === metrics2.resultSerializationTime)
    assert(metrics1.memoryBytesSpilled === metrics2.memoryBytesSpilled)
    assert(metrics1.diskBytesSpilled === metrics2.diskBytesSpilled)
    assertOptionEquals(
      metrics1.shuffleReadMetrics, metrics2.shuffleReadMetrics, assertShuffleReadEquals)
    assertOptionEquals(
      metrics1.shuffleWriteMetrics, metrics2.shuffleWriteMetrics, assertShuffleWriteEquals)
    assertOptionEquals(
      metrics1.inputMetrics, metrics2.inputMetrics, assertInputMetricsEquals)
    assertOptionEquals(metrics1.updatedBlocks, metrics2.updatedBlocks, assertBlocksEquals)
  }

  private def assertEquals(metrics1: ShuffleReadMetrics, metrics2: ShuffleReadMetrics) {
    assert(metrics1.remoteBlocksFetched === metrics2.remoteBlocksFetched)
    assert(metrics1.localBlocksFetched === metrics2.localBlocksFetched)
    assert(metrics1.fetchWaitTime === metrics2.fetchWaitTime)
    assert(metrics1.remoteBytesRead === metrics2.remoteBytesRead)
  }

  private def assertEquals(metrics1: ShuffleWriteMetrics, metrics2: ShuffleWriteMetrics) {
    assert(metrics1.shuffleBytesWritten === metrics2.shuffleBytesWritten)
    assert(metrics1.shuffleWriteTime === metrics2.shuffleWriteTime)
  }

  private def assertEquals(metrics1: InputMetrics, metrics2: InputMetrics) {
    assert(metrics1.readMethod === metrics2.readMethod)
    assert(metrics1.bytesRead === metrics2.bytesRead)
  }

  private def assertEquals(bm1: BlockManagerId, bm2: BlockManagerId) {
    assert(bm1.executorId === bm2.executorId)
    assert(bm1.host === bm2.host)
    assert(bm1.port === bm2.port)
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
        assertEquals(r1.bmAddress, r2.bmAddress)
        assert(r1.message === r2.message)
      case (r1: ExceptionFailure, r2: ExceptionFailure) =>
        assert(r1.className === r2.className)
        assert(r1.description === r2.description)
        assertSeqEquals(r1.stackTrace, r2.stackTrace, assertStackTraceElementEquals)
        assertOptionEquals(r1.metrics, r2.metrics, assertTaskMetricsEquals)
      case (TaskResultLost, TaskResultLost) =>
      case (TaskKilled, TaskKilled) =>
      case (ExecutorLostFailure(execId1), ExecutorLostFailure(execId2)) =>
        assert(execId1 === execId2)
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

  private def assertJsonStringEquals(json1: String, json2: String) {
    val formatJsonString = (json: String) => json.replaceAll("[\\s|]", "")
    assert(formatJsonString(json1) === formatJsonString(json2),
      s"input ${formatJsonString(json1)} got ${formatJsonString(json2)}")
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

  private def assertShuffleReadEquals(r1: ShuffleReadMetrics, r2: ShuffleReadMetrics) {
    assertEquals(r1, r2)
  }

  private def assertShuffleWriteEquals(w1: ShuffleWriteMetrics, w2: ShuffleWriteMetrics) {
    assertEquals(w1, w2)
  }

  private def assertInputMetricsEquals(i1: InputMetrics, i2: InputMetrics) {
    assertEquals(i1, i2)
  }

  private def assertTaskMetricsEquals(t1: TaskMetrics, t2: TaskMetrics) {
    assertEquals(t1, t2)
  }

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
    val r = new RDDInfo(a, "mayor", b, StorageLevel.MEMORY_AND_DISK)
    r.numCachedPartitions = c
    r.memSize = d
    r.diskSize = e
    r
  }

  private def makeStageInfo(a: Int, b: Int, c: Int, d: Long, e: Long) = {
    val rddInfos = (0 until a % 5).map { i => makeRddInfo(a + i, b + i, c + i, d + i, e + i) }
    val stageInfo = new StageInfo(a, 0, "greetings", b, rddInfos, "details")
    val (acc1, acc2) = (makeAccumulableInfo(1), makeAccumulableInfo(2))
    stageInfo.accumulables(acc1.id) = acc1
    stageInfo.accumulables(acc2.id) = acc2
    stageInfo
  }

  private def makeTaskInfo(a: Long, b: Int, c: Int, d: Long, speculative: Boolean) = {
    val taskInfo = new TaskInfo(a, b, c, d, "executor", "your kind sir", TaskLocality.NODE_LOCAL,
      speculative)
    val (acc1, acc2, acc3) =
      (makeAccumulableInfo(1), makeAccumulableInfo(2), makeAccumulableInfo(3))
    taskInfo.accumulables += acc1
    taskInfo.accumulables += acc2
    taskInfo.accumulables += acc3
    taskInfo
  }

  private def makeAccumulableInfo(id: Int): AccumulableInfo =
    AccumulableInfo(id, " Accumulable " + id, Some("delta" + id), "val" + id)

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
      hasHadoopInput: Boolean) = {
    val t = new TaskMetrics
    val sw = new ShuffleWriteMetrics
    t.hostname = "localhost"
    t.executorDeserializeTime = a
    t.executorRunTime = b
    t.resultSize = c
    t.jvmGCTime = d
    t.resultSerializationTime = a + b
    t.memoryBytesSpilled = a + c

    if (hasHadoopInput) {
      val inputMetrics = new InputMetrics(DataReadMethod.Hadoop)
      inputMetrics.bytesRead = d + e + f
      t.inputMetrics = Some(inputMetrics)
    } else {
      val sr = new ShuffleReadMetrics
      sr.remoteBytesRead = b + d
      sr.localBlocksFetched = e
      sr.fetchWaitTime = a + d
      sr.remoteBlocksFetched = f
      t.setShuffleReadMetrics(Some(sr))
    }
    sw.shuffleBytesWritten = a + b + c
    sw.shuffleWriteTime = b + c + d
    t.shuffleWriteMetrics = Some(sw)
    // Make at most 6 blocks
    t.updatedBlocks = Some((1 to (e % 5 + 1)).map { i =>
      (RDDBlockId(e % i, f % i), BlockStatus(StorageLevel.MEMORY_AND_DISK_SER_2, a % i, b % i, c%i))
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
      |    "Details": "details",
      |    "Accumulables": [
      |      {
      |        "ID": 2,
      |        "Name": "Accumulable2",
      |        "Update": "delta2",
      |        "Value": "val2"
      |      },
      |      {
      |        "ID": 1,
      |        "Name": "Accumulable1",
      |        "Update": "delta1",
      |        "Value": "val1"
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
    """

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
      |        "Storage Level": {
      |          "Use Disk": true,
      |          "Use Memory": true,
      |          "Use Tachyon": false,
      |          "Deserialized": true,
      |          "Replication": 1
      |        },
      |        "Number of Partitions": 201,
      |        "Number of Cached Partitions": 301,
      |        "Memory Size": 401,
      |        "Tachyon Size": 0,
      |        "Disk Size": 501
      |      }
      |    ],
      |    "Details": "details",
      |    "Accumulables": [
      |      {
      |        "ID": 2,
      |        "Name": "Accumulable2",
      |        "Update": "delta2",
      |        "Value": "val2"
      |      },
      |      {
      |        "ID": 1,
      |        "Name": "Accumulable1",
      |        "Update": "delta1",
      |        "Value": "val1"
      |      }
      |    ]
      |  }
      |}
    """

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
      |    "Accumulables": [
      |      {
      |        "ID": 1,
      |        "Name": "Accumulable1",
      |        "Update": "delta1",
      |        "Value": "val1"
      |      },
      |      {
      |        "ID": 2,
      |        "Name": "Accumulable2",
      |        "Update": "delta2",
      |        "Value": "val2"
      |      },
      |      {
      |        "ID": 3,
      |        "Name": "Accumulable3",
      |        "Update": "delta3",
      |        "Value": "val3"
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
      |    "Accumulables": [
      |      {
      |        "ID": 1,
      |        "Name": "Accumulable1",
      |        "Update": "delta1",
      |        "Value": "val1"
      |      },
      |      {
      |        "ID": 2,
      |        "Name": "Accumulable2",
      |        "Update": "delta2",
      |        "Value": "val2"
      |      },
      |      {
      |        "ID": 3,
      |        "Name": "Accumulable3",
      |        "Update": "delta3",
      |        "Value": "val3"
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
      |    "Accumulables": [
      |      {
      |        "ID": 1,
      |        "Name": "Accumulable1",
      |        "Update": "delta1",
      |        "Value": "val1"
      |      },
      |      {
      |        "ID": 2,
      |        "Name": "Accumulable2",
      |        "Update": "delta2",
      |        "Value": "val2"
      |      },
      |      {
      |        "ID": 3,
      |        "Name": "Accumulable3",
      |        "Update": "delta3",
      |        "Value": "val3"
      |      }
      |    ]
      |  },
      |  "Task Metrics": {
      |    "Host Name": "localhost",
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
      |      "Remote Bytes Read": 1000
      |    },
      |    "Shuffle Write Metrics": {
      |      "Shuffle Bytes Written": 1200,
      |      "Shuffle Write Time": 1500
      |    },
      |    "Updated Blocks": [
      |      {
      |        "Block ID": "rdd_0_0",
      |        "Status": {
      |          "Storage Level": {
      |            "Use Disk": true,
      |            "Use Memory": true,
      |            "Use Tachyon": false,
      |            "Deserialized": false,
      |            "Replication": 2
      |          },
      |          "Memory Size": 0,
      |          "Tachyon Size": 0,
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
      |    "Accumulables": [
      |      {
      |        "ID": 1,
      |        "Name": "Accumulable1",
      |        "Update": "delta1",
      |        "Value": "val1"
      |      },
      |      {
      |        "ID": 2,
      |        "Name": "Accumulable2",
      |        "Update": "delta2",
      |        "Value": "val2"
      |      },
      |      {
      |        "ID": 3,
      |        "Name": "Accumulable3",
      |        "Update": "delta3",
      |        "Value": "val3"
      |      }
      |    ]
      |  },
      |  "Task Metrics": {
      |    "Host Name": "localhost",
      |    "Executor Deserialize Time": 300,
      |    "Executor Run Time": 400,
      |    "Result Size": 500,
      |    "JVM GC Time": 600,
      |    "Result Serialization Time": 700,
      |    "Memory Bytes Spilled": 800,
      |    "Disk Bytes Spilled": 0,
      |    "Shuffle Write Metrics": {
      |      "Shuffle Bytes Written": 1200,
      |      "Shuffle Write Time": 1500
      |    },
      |    "Input Metrics": {
      |      "Data Read Method": "Hadoop",
      |      "Bytes Read": 2100
      |    },
      |    "Updated Blocks": [
      |      {
      |        "Block ID": "rdd_0_0",
      |        "Status": {
      |          "Storage Level": {
      |            "Use Disk": true,
      |            "Use Memory": true,
      |            "Use Tachyon": false,
      |            "Deserialized": false,
      |            "Replication": 2
      |          },
      |          "Memory Size": 0,
      |          "Tachyon Size": 0,
      |          "Disk Size": 0
      |        }
      |      }
      |    ]
      |  }
      |}
    """

  private val jobStartJsonString =
    """
      |{
      |  "Event": "SparkListenerJobStart",
      |  "Job ID": 10,
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
    """

  private val jobEndJsonString =
    """
      |{
      |  "Event": "SparkListenerJobEnd",
      |  "Job ID": 20,
      |  "Job Result": {
      |    "Result": "JobSucceeded"
      |  }
      |}
    """

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
    """

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
    """

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
    """

  private val unpersistRDDJsonString =
    """
      |{
      |  "Event": "SparkListenerUnpersistRDD",
      |  "RDD ID": 12345
      |}
    """

  private val applicationStartJsonString =
    """
      |{
      |  "Event": "SparkListenerApplicationStart",
      |  "App Name": "The winner of all",
      |  "Timestamp": 42,
      |  "User": "Garfield"
      |}
    """

  private val applicationEndJsonString =
    """
      |{
      |  "Event": "SparkListenerApplicationEnd",
      |  "Timestamp": 42
      |}
    """
}
