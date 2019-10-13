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

package org.apache.spark.sql.execution.ui

import java.util.Properties

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

import org.json4s.jackson.JsonMethods._
import org.scalatest.BeforeAndAfter

import org.apache.spark._
import org.apache.spark.LocalSparkContext._
import org.apache.spark.executor.ExecutorMetrics
import org.apache.spark.internal.config
import org.apache.spark.internal.config.Status._
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.catalyst.util.quietly
import org.apache.spark.sql.execution.{LeafExecNode, QueryExecution, SparkPlanInfo, SQLExecution}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.internal.StaticSQLConf.UI_RETAINED_EXECUTIONS
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.status.ElementTrackingStore
import org.apache.spark.util.{AccumulatorMetadata, JsonProtocol, LongAccumulator}
import org.apache.spark.util.kvstore.InMemoryStore


class SQLAppStatusListenerSuite extends SharedSparkSession with JsonTestUtils
  with BeforeAndAfter {

  import testImplicits._

  override protected def sparkConf = {
    super.sparkConf.set(LIVE_ENTITY_UPDATE_PERIOD, 0L).set(ASYNC_TRACKING_ENABLED, false)
  }

  private var kvstore: ElementTrackingStore = _

  after {
    if (kvstore != null) {
      kvstore.close()
      kvstore = null
    }
  }

  private def createTestDataFrame: DataFrame = {
    Seq(
      (1, 1),
      (2, 2)
    ).toDF().filter("_1 > 1")
  }

  private def createProperties(executionId: Long): Properties = {
    val properties = new Properties()
    properties.setProperty(SQLExecution.EXECUTION_ID_KEY, executionId.toString)
    properties
  }

  private def createStageInfo(stageId: Int, attemptId: Int): StageInfo = {
    new StageInfo(stageId = stageId,
      attemptId = attemptId,
      // The following fields are not used in tests
      name = "",
      numTasks = 0,
      rddInfos = Nil,
      parentIds = Nil,
      details = "")
  }

  private def createTaskInfo(
      taskId: Int,
      attemptNumber: Int,
      accums: Map[Long, Long] = Map.empty): TaskInfo = {
    val info = new TaskInfo(
      taskId = taskId,
      attemptNumber = attemptNumber,
      // The following fields are not used in tests
      index = 0,
      launchTime = 0,
      executorId = "",
      host = "",
      taskLocality = null,
      speculative = false)
    info.markFinished(TaskState.FINISHED, 1L)
    info.setAccumulables(createAccumulatorInfos(accums))
    info
  }

  private def createAccumulatorInfos(accumulatorUpdates: Map[Long, Long]): Seq[AccumulableInfo] = {
    accumulatorUpdates.map { case (id, value) =>
      val acc = new LongAccumulator
      acc.metadata = AccumulatorMetadata(id, None, false)
      acc.toInfo(Some(value), None)
    }.toSeq
  }

  private def assertJobs(
      exec: Option[SQLExecutionUIData],
      running: Seq[Int] = Nil,
      completed: Seq[Int] = Nil,
      failed: Seq[Int] = Nil): Unit = {
    val actualRunning = new ListBuffer[Int]()
    val actualCompleted = new ListBuffer[Int]()
    val actualFailed = new ListBuffer[Int]()

    exec.get.jobs.foreach { case (jobId, jobStatus) =>
      jobStatus match {
        case JobExecutionStatus.RUNNING => actualRunning += jobId
        case JobExecutionStatus.SUCCEEDED => actualCompleted += jobId
        case JobExecutionStatus.FAILED => actualFailed += jobId
        case _ => fail(s"Unexpected status $jobStatus")
      }
    }

    assert(actualRunning.sorted === running)
    assert(actualCompleted.sorted === completed)
    assert(actualFailed.sorted === failed)
  }

  private def createStatusStore(): SQLAppStatusStore = {
    val conf = sparkContext.conf
    kvstore = new ElementTrackingStore(new InMemoryStore, conf)
    val listener = new SQLAppStatusListener(conf, kvstore, live = true)
    new SQLAppStatusStore(kvstore, Some(listener))
  }

  test("basic") {
    def checkAnswer(actual: Map[Long, String], expected: Map[Long, Long]): Unit = {
      assert(actual.size == expected.size)
      expected.foreach { case (id, value) =>
        // The values in actual can be SQL metrics meaning that they contain additional formatting
        // when converted to string. Verify that they start with the expected value.
        // TODO: this is brittle. There is no requirement that the actual string needs to start
        // with the accumulator value.
        assert(actual.contains(id))
        val v = actual(id).trim
        assert(v.startsWith(value.toString), s"Wrong value for accumulator $id")
      }
    }

    val statusStore = createStatusStore()
    val listener = statusStore.listener.get

    val executionId = 0
    val df = createTestDataFrame
    val accumulatorIds =
      SparkPlanGraph(SparkPlanInfo.fromSparkPlan(df.queryExecution.executedPlan))
        .allNodes.flatMap(_.metrics.map(_.accumulatorId))
    // Assume all accumulators are long
    var accumulatorValue = 0L
    val accumulatorUpdates = accumulatorIds.map { id =>
      accumulatorValue += 1L
      (id, accumulatorValue)
    }.toMap

    listener.onOtherEvent(SparkListenerSQLExecutionStart(
      executionId,
      "test",
      "test",
      df.queryExecution.toString,
      SparkPlanInfo.fromSparkPlan(df.queryExecution.executedPlan),
      System.currentTimeMillis()))

    listener.onJobStart(SparkListenerJobStart(
      jobId = 0,
      time = System.currentTimeMillis(),
      stageInfos = Seq(
        createStageInfo(0, 0),
        createStageInfo(1, 0)
      ),
      createProperties(executionId)))
    listener.onStageSubmitted(SparkListenerStageSubmitted(createStageInfo(0, 0)))

    assert(statusStore.executionMetrics(executionId).isEmpty)

    listener.onExecutorMetricsUpdate(SparkListenerExecutorMetricsUpdate("", Seq(
      // (task id, stage id, stage attempt, accum updates)
      (0L, 0, 0, createAccumulatorInfos(accumulatorUpdates)),
      (1L, 0, 0, createAccumulatorInfos(accumulatorUpdates))
    )))

    checkAnswer(statusStore.executionMetrics(executionId), accumulatorUpdates.mapValues(_ * 2))

    // Driver accumulator updates don't belong to this execution should be filtered and no
    // exception will be thrown.
    listener.onOtherEvent(SparkListenerDriverAccumUpdates(0, Seq((999L, 2L))))

    checkAnswer(statusStore.executionMetrics(executionId), accumulatorUpdates.mapValues(_ * 2))

    listener.onExecutorMetricsUpdate(SparkListenerExecutorMetricsUpdate("", Seq(
      // (task id, stage id, stage attempt, accum updates)
      (0L, 0, 0, createAccumulatorInfos(accumulatorUpdates)),
      (1L, 0, 0, createAccumulatorInfos(accumulatorUpdates.mapValues(_ * 2)))
    )))

    checkAnswer(statusStore.executionMetrics(executionId), accumulatorUpdates.mapValues(_ * 3))

    // Retrying a stage should reset the metrics
    listener.onStageSubmitted(SparkListenerStageSubmitted(createStageInfo(0, 1)))

    listener.onExecutorMetricsUpdate(SparkListenerExecutorMetricsUpdate("", Seq(
      // (task id, stage id, stage attempt, accum updates)
      (0L, 0, 1, createAccumulatorInfos(accumulatorUpdates)),
      (1L, 0, 1, createAccumulatorInfos(accumulatorUpdates))
    )))

    checkAnswer(statusStore.executionMetrics(executionId), accumulatorUpdates.mapValues(_ * 2))

    // Ignore the task end for the first attempt
    listener.onTaskEnd(SparkListenerTaskEnd(
      stageId = 0,
      stageAttemptId = 0,
      taskType = "",
      reason = null,
      createTaskInfo(0, 0, accums = accumulatorUpdates.mapValues(_ * 100)),
      new ExecutorMetrics,
      null))

    checkAnswer(statusStore.executionMetrics(executionId), accumulatorUpdates.mapValues(_ * 2))

    // Finish two tasks
    listener.onTaskEnd(SparkListenerTaskEnd(
      stageId = 0,
      stageAttemptId = 1,
      taskType = "",
      reason = null,
      createTaskInfo(0, 0, accums = accumulatorUpdates.mapValues(_ * 2)),
      new ExecutorMetrics,
      null))
    listener.onTaskEnd(SparkListenerTaskEnd(
      stageId = 0,
      stageAttemptId = 1,
      taskType = "",
      reason = null,
      createTaskInfo(1, 0, accums = accumulatorUpdates.mapValues(_ * 3)),
      new ExecutorMetrics,
      null))

    checkAnswer(statusStore.executionMetrics(executionId), accumulatorUpdates.mapValues(_ * 5))

    // Summit a new stage
    listener.onStageSubmitted(SparkListenerStageSubmitted(createStageInfo(1, 0)))

    listener.onExecutorMetricsUpdate(SparkListenerExecutorMetricsUpdate("", Seq(
      // (task id, stage id, stage attempt, accum updates)
      (0L, 1, 0, createAccumulatorInfos(accumulatorUpdates)),
      (1L, 1, 0, createAccumulatorInfos(accumulatorUpdates))
    )))

    checkAnswer(statusStore.executionMetrics(executionId), accumulatorUpdates.mapValues(_ * 7))

    // Finish two tasks
    listener.onTaskEnd(SparkListenerTaskEnd(
      stageId = 1,
      stageAttemptId = 0,
      taskType = "",
      reason = null,
      createTaskInfo(0, 0, accums = accumulatorUpdates.mapValues(_ * 3)),
      new ExecutorMetrics,
      null))
    listener.onTaskEnd(SparkListenerTaskEnd(
      stageId = 1,
      stageAttemptId = 0,
      taskType = "",
      reason = null,
      createTaskInfo(1, 0, accums = accumulatorUpdates.mapValues(_ * 3)),
      new ExecutorMetrics,
      null))

    checkAnswer(statusStore.executionMetrics(executionId), accumulatorUpdates.mapValues(_ * 11))

    assertJobs(statusStore.execution(executionId), running = Seq(0))

    listener.onJobEnd(SparkListenerJobEnd(
      jobId = 0,
      time = System.currentTimeMillis(),
      JobSucceeded
    ))
    listener.onOtherEvent(SparkListenerSQLExecutionEnd(
      executionId, System.currentTimeMillis()))

    assertJobs(statusStore.execution(executionId), completed = Seq(0))

    checkAnswer(statusStore.executionMetrics(executionId), accumulatorUpdates.mapValues(_ * 11))
  }

  test("onExecutionEnd happens before onJobEnd(JobSucceeded)") {
    val statusStore = createStatusStore()
    val listener = statusStore.listener.get

    val executionId = 0
    val df = createTestDataFrame
    listener.onOtherEvent(SparkListenerSQLExecutionStart(
      executionId,
      "test",
      "test",
      df.queryExecution.toString,
      SparkPlanInfo.fromSparkPlan(df.queryExecution.executedPlan),
      System.currentTimeMillis()))
    listener.onJobStart(SparkListenerJobStart(
      jobId = 0,
      time = System.currentTimeMillis(),
      stageInfos = Nil,
      createProperties(executionId)))
    listener.onOtherEvent(SparkListenerSQLExecutionEnd(
      executionId, System.currentTimeMillis()))
    listener.onJobEnd(SparkListenerJobEnd(
      jobId = 0,
      time = System.currentTimeMillis(),
      JobSucceeded
    ))

    assertJobs(statusStore.execution(executionId), completed = Seq(0))
  }

  test("onExecutionEnd happens before multiple onJobEnd(JobSucceeded)s") {
    val statusStore = createStatusStore()
    val listener = statusStore.listener.get

    val executionId = 0
    val df = createTestDataFrame
    listener.onOtherEvent(SparkListenerSQLExecutionStart(
      executionId,
      "test",
      "test",
      df.queryExecution.toString,
      SparkPlanInfo.fromSparkPlan(df.queryExecution.executedPlan),
      System.currentTimeMillis()))
    listener.onJobStart(SparkListenerJobStart(
      jobId = 0,
      time = System.currentTimeMillis(),
      stageInfos = Nil,
      createProperties(executionId)))
    listener.onJobEnd(SparkListenerJobEnd(
        jobId = 0,
        time = System.currentTimeMillis(),
        JobSucceeded
    ))

    listener.onJobStart(SparkListenerJobStart(
      jobId = 1,
      time = System.currentTimeMillis(),
      stageInfos = Nil,
      createProperties(executionId)))
    listener.onOtherEvent(SparkListenerSQLExecutionEnd(
      executionId, System.currentTimeMillis()))
    listener.onJobEnd(SparkListenerJobEnd(
      jobId = 1,
      time = System.currentTimeMillis(),
      JobSucceeded
    ))

    assertJobs(statusStore.execution(executionId), completed = Seq(0, 1))
  }

  test("onExecutionEnd happens before onJobEnd(JobFailed)") {
    val statusStore = createStatusStore()
    val listener = statusStore.listener.get

    val executionId = 0
    val df = createTestDataFrame
    listener.onOtherEvent(SparkListenerSQLExecutionStart(
      executionId,
      "test",
      "test",
      df.queryExecution.toString,
      SparkPlanInfo.fromSparkPlan(df.queryExecution.executedPlan),
      System.currentTimeMillis()))
    listener.onJobStart(SparkListenerJobStart(
      jobId = 0,
      time = System.currentTimeMillis(),
      stageInfos = Seq.empty,
      createProperties(executionId)))
    listener.onOtherEvent(SparkListenerSQLExecutionEnd(
      executionId, System.currentTimeMillis()))
    listener.onJobEnd(SparkListenerJobEnd(
      jobId = 0,
      time = System.currentTimeMillis(),
      JobFailed(new RuntimeException("Oops"))
    ))

    assertJobs(statusStore.execution(executionId), failed = Seq(0))
  }

  test("onJobStart happens after onExecutionEnd shouldn't overwrite kvstore") {
    val statusStore = createStatusStore()
    val listener = statusStore.listener.get

    val executionId = 0
    val df = createTestDataFrame
    listener.onOtherEvent(SparkListenerSQLExecutionStart(
      executionId,
      "test",
      "test",
      df.queryExecution.toString,
      SparkPlanInfo.fromSparkPlan(df.queryExecution.executedPlan),
      System.currentTimeMillis()))
    listener.onOtherEvent(SparkListenerSQLExecutionEnd(
      executionId, System.currentTimeMillis()))
    listener.onJobStart(SparkListenerJobStart(
      jobId = 0,
      time = System.currentTimeMillis(),
      stageInfos = Seq(createStageInfo(0, 0)),
      createProperties(executionId)))
    listener.onStageSubmitted(SparkListenerStageSubmitted(createStageInfo(0, 0)))
    listener.onJobEnd(SparkListenerJobEnd(
      jobId = 0,
      time = System.currentTimeMillis(),
      JobFailed(new RuntimeException("Oops"))))

    assert(listener.noLiveData())
    assert(statusStore.execution(executionId).get.completionTime.nonEmpty)
  }

  test("handle one execution with multiple jobs") {
    val statusStore = createStatusStore()
    val listener = statusStore.listener.get

    val executionId = 0
    val df = createTestDataFrame
    listener.onOtherEvent(SparkListenerSQLExecutionStart(
      executionId,
      "test",
      "test",
      df.queryExecution.toString,
      SparkPlanInfo.fromSparkPlan(df.queryExecution.executedPlan),
      System.currentTimeMillis()))

    var stageId = 0
    def twoStageJob(jobId: Int): Unit = {
      val stages = Seq(stageId, stageId + 1).map { id => createStageInfo(id, 0)}
      stageId += 2
      listener.onJobStart(SparkListenerJobStart(
        jobId = jobId,
        time = System.currentTimeMillis(),
        stageInfos = stages,
        createProperties(executionId)))
      stages.foreach { s =>
        listener.onStageSubmitted(SparkListenerStageSubmitted(s))
        listener.onStageCompleted(SparkListenerStageCompleted(s))
      }
      listener.onJobEnd(SparkListenerJobEnd(
        jobId = jobId,
        time = System.currentTimeMillis(),
        JobSucceeded
      ))
    }
    // submit two jobs with the same executionId
    twoStageJob(0)
    twoStageJob(1)
    listener.onOtherEvent(SparkListenerSQLExecutionEnd(
      executionId, System.currentTimeMillis()))

    assertJobs(statusStore.execution(0), completed = 0 to 1)
    assert(statusStore.execution(0).get.stages === (0 to 3).toSet)
  }

  test("SPARK-11126: no memory leak when running non SQL jobs") {
    val listener = spark.sharedState.statusStore.listener.get
    // At the beginning of this test case, there should be no live data in the listener.
    assert(listener.noLiveData())
    spark.sparkContext.parallelize(1 to 10).foreach(i => ())
    spark.sparkContext.listenerBus.waitUntilEmpty()
    // Listener should ignore the non-SQL stages, as the stage data are only removed when SQL
    // execution ends, which will not be triggered for non-SQL jobs.
    assert(listener.noLiveData())
  }

  test("driver side SQL metrics") {
    val statusStore = spark.sharedState.statusStore
    val oldCount = statusStore.executionsList().size

    val expectedAccumValue = 12345
    val expectedAccumValue2 = 54321
    val physicalPlan = MyPlan(sqlContext.sparkContext, expectedAccumValue, expectedAccumValue2)
    val dummyQueryExecution = new QueryExecution(spark, LocalRelation()) {
      override lazy val sparkPlan = physicalPlan
      override lazy val executedPlan = physicalPlan
    }

    SQLExecution.withNewExecutionId(spark, dummyQueryExecution) {
      physicalPlan.execute().collect()
    }

    // Wait until the new execution is started and being tracked.
    while (statusStore.executionsCount() < oldCount) {
      Thread.sleep(100)
    }

    // Wait for listener to finish computing the metrics for the execution.
    while (statusStore.executionsList().isEmpty ||
        statusStore.executionsList().last.metricValues == null) {
      Thread.sleep(100)
    }

    val execId = statusStore.executionsList().last.executionId
    val metrics = statusStore.executionMetrics(execId)
    val driverMetric = physicalPlan.metrics("dummy")
    val driverMetric2 = physicalPlan.metrics("dummy2")
    val expectedValue = SQLMetrics.stringValue(driverMetric.metricType, Seq(expectedAccumValue))
    val expectedValue2 = SQLMetrics.stringValue(driverMetric2.metricType, Seq(expectedAccumValue2))

    assert(metrics.contains(driverMetric.id))
    assert(metrics(driverMetric.id) === expectedValue)
    assert(metrics.contains(driverMetric2.id))
    assert(metrics(driverMetric2.id) === expectedValue2)
  }

  test("roundtripping SparkListenerDriverAccumUpdates through JsonProtocol (SPARK-18462)") {
    val event = SparkListenerDriverAccumUpdates(1L, Seq((2L, 3L)))
    val json = JsonProtocol.sparkEventToJson(event)
    assertValidDataInJson(json,
      parse("""
        |{
        |  "Event": "org.apache.spark.sql.execution.ui.SparkListenerDriverAccumUpdates",
        |  "executionId": 1,
        |  "accumUpdates": [[2,3]]
        |}
      """.stripMargin))
    JsonProtocol.sparkEventFromJson(json) match {
      case SparkListenerDriverAccumUpdates(executionId, accums) =>
        assert(executionId == 1L)
        accums.foreach { case (a, b) =>
          assert(a == 2L)
          assert(b == 3L)
        }
    }

    // Test a case where the numbers in the JSON can only fit in longs:
    val longJson = parse(
      """
        |{
        |  "Event": "org.apache.spark.sql.execution.ui.SparkListenerDriverAccumUpdates",
        |  "executionId": 4294967294,
        |  "accumUpdates": [[4294967294,3]]
        |}
      """.stripMargin)
    JsonProtocol.sparkEventFromJson(longJson) match {
      case SparkListenerDriverAccumUpdates(executionId, accums) =>
        assert(executionId == 4294967294L)
        accums.foreach { case (a, b) =>
          assert(a == 4294967294L)
          assert(b == 3L)
        }
    }
  }

  test("eviction should respect execution completion time") {
    val conf = sparkContext.conf.clone().set(UI_RETAINED_EXECUTIONS.key, "2")
    kvstore = new ElementTrackingStore(new InMemoryStore, conf)
    val listener = new SQLAppStatusListener(conf, kvstore, live = true)
    val statusStore = new SQLAppStatusStore(kvstore, Some(listener))

    var time = 0
    val df = createTestDataFrame
    // Start execution 1 and execution 2
    time += 1
    listener.onOtherEvent(SparkListenerSQLExecutionStart(
      1,
      "test",
      "test",
      df.queryExecution.toString,
      SparkPlanInfo.fromSparkPlan(df.queryExecution.executedPlan),
      time))
    time += 1
    listener.onOtherEvent(SparkListenerSQLExecutionStart(
      2,
      "test",
      "test",
      df.queryExecution.toString,
      SparkPlanInfo.fromSparkPlan(df.queryExecution.executedPlan),
      time))

    // Stop execution 2 before execution 1
    time += 1
    listener.onOtherEvent(SparkListenerSQLExecutionEnd(2, time))
    time += 1
    listener.onOtherEvent(SparkListenerSQLExecutionEnd(1, time))

    // Start execution 3 and execution 2 should be evicted.
    time += 1
    listener.onOtherEvent(SparkListenerSQLExecutionStart(
      3,
      "test",
      "test",
      df.queryExecution.toString,
      SparkPlanInfo.fromSparkPlan(df.queryExecution.executedPlan),
      time))
    assert(statusStore.executionsCount === 2)
    assert(statusStore.execution(2) === None)
  }

  test("recover live entities from KVStore") {
    def assertSQLListenerEquals(live: SQLAppStatusListener, nonLive: SQLAppStatusListener)
    : Unit = {
      // ensures all live entities are wrote into KVStore
      live.flush()
      nonLive.clearLiveEntities()
      nonLive.recoverLiveEntities()
      assertSQLLiveEntityEquals(live, nonLive)
    }
    val conf = sparkContext.conf
    kvstore = new ElementTrackingStore(new InMemoryStore, conf)
    val liveListener = new SQLAppStatusListener(conf, kvstore, live = true)
    val nonLiveListener = new SQLAppStatusListener(conf, kvstore, live = false)
    var time = 1

    liveListener.onOtherEvent(SparkListenerSQLExecutionStart(
      0, "desc", "details", "planDesc",
      new SparkPlanInfo("node", "test", Nil, null, Nil), time
    ))
    assert(liveListener.liveExecutions.size() === 1)
    assertSQLListenerEquals(liveListener, nonLiveListener)
    time += 1

    val stage0 = createStageInfo(stageId = 0, attemptId = 0)
    val stage1 = createStageInfo(stageId = 1, attemptId = 0)
    val properties = createProperties(executionId = 0)
    liveListener.onJobStart(SparkListenerJobStart(
      0, time, Seq(stage0, stage1), properties))
    assert(liveListener.stageMetrics.size() === 2)
    assertSQLListenerEquals(liveListener, nonLiveListener)
    time += 1

    liveListener.onStageSubmitted(SparkListenerStageSubmitted(stage0))
    liveListener.onStageSubmitted(SparkListenerStageSubmitted(stage1))
    liveListener.onExecutorMetricsUpdate(SparkListenerExecutorMetricsUpdate(
      "execId-0", Seq((0, 0, 0, createAccumulatorInfos(Map((0, 0), (1, 1)))))))
    liveListener.onExecutorMetricsUpdate(SparkListenerExecutorMetricsUpdate(
      "execId-0", Seq((0, 1, 1, createAccumulatorInfos(Map((0, 0), (1, 1)))))))
    assertSQLListenerEquals(liveListener, nonLiveListener)

    liveListener.onJobEnd(SparkListenerJobEnd(0, time, JobSucceeded))
    assertSQLListenerEquals(liveListener, nonLiveListener)
    time += 1

    liveListener.onOtherEvent(SparkListenerSQLExecutionEnd(0, time))
    assert(liveListener.liveExecutions.size() === 0)
    assertSQLListenerEquals(liveListener, nonLiveListener)

    // SQLStageMetricsWrapper should be clean up after SQL executions end
    assert(kvstore.count(classOf[SQLStageMetricsWrapper]) === 0)
  }

  private def assertSQLLiveEntityEquals(src: SQLAppStatusListener, dest: SQLAppStatusListener)
    : Unit = {
    val srcExecutions = src.liveExecutions
    val destExecutions = dest.liveExecutions
    assert(srcExecutions.size() === destExecutions.size())
    srcExecutions.keys().asScala.foreach { id =>
      val srcExec = srcExecutions.get(id)
      val destExec = destExecutions.get(id)
      assert(srcExec.executionId === destExec.executionId)
      assert(srcExec.description === destExec.description)
      assert(srcExec.details === destExec.details)
      assert(srcExec.physicalPlanDescription === destExec.physicalPlanDescription)
      assert(srcExec.metrics === destExec.metrics)
      assert(srcExec.submissionTime === destExec.submissionTime)
      assert(srcExec.completionTime === destExec.completionTime)
      assert(srcExec.jobs === destExec.jobs)
      assert(srcExec.stages === destExec.stages)
      assert(srcExec.driverAccumUpdates === destExec.driverAccumUpdates)
      assert(srcExec.metricsValues === destExec.metricsValues)
      assert(srcExec.endEvents === destExec.endEvents)
    }
    val srcStageMetrics = src.stageMetrics
    val destStageMetrics = dest.stageMetrics
    assert(srcStageMetrics.size() === destStageMetrics.size())
    srcStageMetrics.keys().asScala.foreach { id =>
      val srcStageM = srcStageMetrics.get(id)
      val destStageM = destStageMetrics.get(id)
      assert(srcStageM.stageId === destStageM.stageId)
      assert(srcStageM.attemptId === destStageM.attemptId)
      assert(srcStageM.accumulatorIds === destStageM.accumulatorIds)
      val srcTaskMetrics = srcStageM.taskMetrics
      val destTaskMetrics = destStageM.taskMetrics
      assert(srcTaskMetrics.size() === destTaskMetrics.size())
      srcTaskMetrics.keys().asScala.foreach { id =>
        val srcTaskM = srcTaskMetrics.get(id)
        val destTaskM = destTaskMetrics.get(id)
        assert(srcTaskM.ids === destTaskM.ids)
        assert(srcTaskM.values === destTaskM.values)
        assert(srcTaskM.succeeded === destTaskM.succeeded)
      }
    }
  }
}


/**
 * A dummy [[org.apache.spark.sql.execution.SparkPlan]] that updates a [[SQLMetrics]]
 * on the driver.
 */
private case class MyPlan(sc: SparkContext, expectedValue: Long, expectedValue2: Long)
  extends LeafExecNode {

  override def sparkContext: SparkContext = sc
  override def output: Seq[Attribute] = Seq()

  override val metrics: Map[String, SQLMetric] = Map(
    "dummy" -> SQLMetrics.createMetric(sc, "dummy"),
    "dummy2" -> SQLMetrics.createMetric(sc, "dummy2"))

  override def doExecute(): RDD[InternalRow] = {
    longMetric("dummy") += expectedValue
    longMetric("dummy2") += expectedValue2

    // postDriverMetricUpdates may happen multiple time in a query.
    // (normally from different operators, but for the sake of testing, from one operator)
    SQLMetrics.postDriverMetricUpdates(
      sc,
      sc.getLocalProperty(SQLExecution.EXECUTION_ID_KEY),
      Seq(metrics("dummy")))

    SQLMetrics.postDriverMetricUpdates(
      sc,
      sc.getLocalProperty(SQLExecution.EXECUTION_ID_KEY),
      Seq(metrics("dummy2")))
    sc.emptyRDD
  }
}


class SQLAppStatusListenerMemoryLeakSuite extends SparkFunSuite {

  test("no memory leak") {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("test")
      .set(config.TASK_MAX_FAILURES, 1) // Don't retry the tasks to run this test quickly
      .set(UI_RETAINED_EXECUTIONS.key, "50") // Set it to 50 to run this test quickly
      .set(ASYNC_TRACKING_ENABLED, false)
    withSpark(new SparkContext(conf)) { sc =>
      quietly {
        val spark = new SparkSession(sc)
        import spark.implicits._
        // Run 100 successful executions and 100 failed executions.
        // Each execution only has one job and one stage.
        for (i <- 0 until 100) {
          val df = Seq(
            (1, 1),
            (2, 2)
          ).toDF()
          df.collect()
          try {
            df.foreach(_ => throw new RuntimeException("Oops"))
          } catch {
            case e: SparkException => // This is expected for a failed job
          }
        }
        sc.listenerBus.waitUntilEmpty()
        val statusStore = spark.sharedState.statusStore
        assert(statusStore.executionsCount() <= 50)
        assert(statusStore.planGraphCount() <= 50)
        // No live data should be left behind after all executions end.
        assert(statusStore.listener.get.noLiveData())
      }
    }
  }
}
