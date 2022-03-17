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

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.json4s.jackson.JsonMethods._
import org.scalatest.BeforeAndAfter
import org.scalatest.time.SpanSugar._

import org.apache.spark._
import org.apache.spark.LocalSparkContext._
import org.apache.spark.executor.ExecutorMetrics
import org.apache.spark.internal.config
import org.apache.spark.internal.config.Status._
import org.apache.spark.rdd.RDD
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.scheduler._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.catalyst.util.quietly
import org.apache.spark.sql.connector.{CSVDataWriter, CSVDataWriterFactory, RangeInputPartition, SimpleScanBuilder, SimpleWritableDataSource}
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.metric.{CustomMetric, CustomTaskMetric}
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.connector.write.{BatchWrite, DataWriter, DataWriterFactory, LogicalWriteInfo, PhysicalWriteInfo, Write, WriteBuilder}
import org.apache.spark.sql.execution.{LeafExecNode, QueryExecution, SparkPlanInfo, SQLExecution}
import org.apache.spark.sql.execution.adaptive.DisableAdaptiveExecution
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.StaticSQLConf.UI_RETAINED_EXECUTIONS
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.status.ElementTrackingStore
import org.apache.spark.util.{AccumulatorMetadata, JsonProtocol, LongAccumulator, SerializableConfiguration}
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
      numTasks = 8,
      // The following fields are not used in tests
      name = "",
      rddInfos = Nil,
      parentIds = Nil,
      details = "",
      resourceProfileId = ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID)
  }

  private def createTaskInfo(
      taskId: Int,
      attemptNumber: Int,
      accums: Map[Long, Long] = Map.empty): TaskInfo = {
    val info = new TaskInfo(
      taskId = taskId,
      attemptNumber = attemptNumber,
      index = taskId.toInt,
      // The following fields are not used in tests
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
        assert(actual.contains(id))
        val v = actual(id).trim
        if (v.contains("\n")) {
          // The actual value can be "total (max, ...)\n6 ms (5 ms, ...)".
          assert(v.split("\n")(1).startsWith(value.toString), s"Wrong value for accumulator $id")
        } else {
          assert(v.startsWith(value.toString), s"Wrong value for accumulator $id")
        }
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
      System.currentTimeMillis(),
      Map.empty))

    listener.onJobStart(SparkListenerJobStart(
      jobId = 0,
      time = System.currentTimeMillis(),
      stageInfos = Seq(
        createStageInfo(0, 0),
        createStageInfo(1, 0)
      ),
      createProperties(executionId)))
    listener.onStageSubmitted(SparkListenerStageSubmitted(createStageInfo(0, 0)))
    listener.onTaskStart(SparkListenerTaskStart(0, 0, createTaskInfo(0, 0)))
    listener.onTaskStart(SparkListenerTaskStart(0, 0, createTaskInfo(1, 0)))

    assert(statusStore.executionMetrics(executionId).isEmpty)

    listener.onExecutorMetricsUpdate(SparkListenerExecutorMetricsUpdate("", Seq(
      // (task id, stage id, stage attempt, accum updates)
      (0L, 0, 0, createAccumulatorInfos(accumulatorUpdates)),
      (1L, 0, 0, createAccumulatorInfos(accumulatorUpdates))
    )))

    checkAnswer(statusStore.executionMetrics(executionId),
      accumulatorUpdates.mapValues(_ * 2).toMap)

    // Driver accumulator updates don't belong to this execution should be filtered and no
    // exception will be thrown.
    listener.onOtherEvent(SparkListenerDriverAccumUpdates(0, Seq((999L, 2L))))

    checkAnswer(statusStore.executionMetrics(executionId),
      accumulatorUpdates.mapValues(_ * 2).toMap)

    listener.onExecutorMetricsUpdate(SparkListenerExecutorMetricsUpdate("", Seq(
      // (task id, stage id, stage attempt, accum updates)
      (0L, 0, 0, createAccumulatorInfos(accumulatorUpdates)),
      (1L, 0, 0, createAccumulatorInfos(accumulatorUpdates.mapValues(_ * 2).toMap))
    )))

    checkAnswer(statusStore.executionMetrics(executionId),
      accumulatorUpdates.mapValues(_ * 3).toMap)

    // Retrying a stage should reset the metrics
    listener.onStageSubmitted(SparkListenerStageSubmitted(createStageInfo(0, 1)))
    listener.onTaskStart(SparkListenerTaskStart(0, 1, createTaskInfo(0, 0)))
    listener.onTaskStart(SparkListenerTaskStart(0, 1, createTaskInfo(1, 0)))

    listener.onExecutorMetricsUpdate(SparkListenerExecutorMetricsUpdate("", Seq(
      // (task id, stage id, stage attempt, accum updates)
      (0L, 0, 1, createAccumulatorInfos(accumulatorUpdates)),
      (1L, 0, 1, createAccumulatorInfos(accumulatorUpdates))
    )))

    checkAnswer(statusStore.executionMetrics(executionId),
      accumulatorUpdates.mapValues(_ * 2).toMap)

    // Ignore the task end for the first attempt
    listener.onTaskEnd(SparkListenerTaskEnd(
      stageId = 0,
      stageAttemptId = 0,
      taskType = "",
      reason = null,
      createTaskInfo(0, 0, accums = accumulatorUpdates.mapValues(_ * 100).toMap),
      new ExecutorMetrics,
      null))

    checkAnswer(statusStore.executionMetrics(executionId),
      accumulatorUpdates.mapValues(_ * 2).toMap)

    // Finish two tasks
    listener.onTaskEnd(SparkListenerTaskEnd(
      stageId = 0,
      stageAttemptId = 1,
      taskType = "",
      reason = null,
      createTaskInfo(0, 0, accums = accumulatorUpdates.mapValues(_ * 2).toMap),
      new ExecutorMetrics,
      null))
    listener.onTaskEnd(SparkListenerTaskEnd(
      stageId = 0,
      stageAttemptId = 1,
      taskType = "",
      reason = null,
      createTaskInfo(1, 0, accums = accumulatorUpdates.mapValues(_ * 3).toMap),
      new ExecutorMetrics,
      null))

    checkAnswer(statusStore.executionMetrics(executionId),
      accumulatorUpdates.mapValues(_ * 5).toMap)

    // Summit a new stage
    listener.onStageSubmitted(SparkListenerStageSubmitted(createStageInfo(1, 0)))
    listener.onTaskStart(SparkListenerTaskStart(1, 0, createTaskInfo(0, 0)))
    listener.onTaskStart(SparkListenerTaskStart(1, 0, createTaskInfo(1, 0)))

    listener.onExecutorMetricsUpdate(SparkListenerExecutorMetricsUpdate("", Seq(
      // (task id, stage id, stage attempt, accum updates)
      (0L, 1, 0, createAccumulatorInfos(accumulatorUpdates)),
      (1L, 1, 0, createAccumulatorInfos(accumulatorUpdates))
    )))

    checkAnswer(statusStore.executionMetrics(executionId),
      accumulatorUpdates.mapValues(_ * 7).toMap)

    // Finish two tasks
    listener.onTaskEnd(SparkListenerTaskEnd(
      stageId = 1,
      stageAttemptId = 0,
      taskType = "",
      reason = null,
      createTaskInfo(0, 0, accums = accumulatorUpdates.mapValues(_ * 3).toMap),
      new ExecutorMetrics,
      null))
    listener.onTaskEnd(SparkListenerTaskEnd(
      stageId = 1,
      stageAttemptId = 0,
      taskType = "",
      reason = null,
      createTaskInfo(1, 0, accums = accumulatorUpdates.mapValues(_ * 3).toMap),
      new ExecutorMetrics,
      null))

    checkAnswer(statusStore.executionMetrics(executionId),
      accumulatorUpdates.mapValues(_ * 11).toMap)

    assertJobs(statusStore.execution(executionId), running = Seq(0))

    listener.onJobEnd(SparkListenerJobEnd(
      jobId = 0,
      time = System.currentTimeMillis(),
      JobSucceeded
    ))
    listener.onOtherEvent(SparkListenerSQLExecutionEnd(
      executionId, System.currentTimeMillis()))

    assertJobs(statusStore.execution(executionId), completed = Seq(0))

    checkAnswer(statusStore.executionMetrics(executionId),
      accumulatorUpdates.mapValues(_ * 11).toMap)
  }

  test("control a plan explain mode in listeners via SQLConf") {

    def checkPlanDescription(mode: String, expected: Seq[String]): Unit = {
      var checkDone = false
      val listener = new SparkListener {
        override def onOtherEvent(event: SparkListenerEvent): Unit = {
          event match {
            case SparkListenerSQLExecutionStart(_, _, _, planDescription, _, _, _) =>
              assert(expected.forall(planDescription.contains))
              checkDone = true
            case _ => // ignore other events
          }
        }
      }
      spark.sparkContext.addSparkListener(listener)
      withSQLConf(SQLConf.UI_EXPLAIN_MODE.key -> mode) {
        createTestDataFrame.collect()
        try {
          spark.sparkContext.listenerBus.waitUntilEmpty()
          assert(checkDone)
        } finally {
          spark.sparkContext.removeSparkListener(listener)
        }
      }
    }

    Seq(("simple", Seq("== Physical Plan ==")),
        ("extended", Seq("== Parsed Logical Plan ==", "== Analyzed Logical Plan ==",
          "== Optimized Logical Plan ==", "== Physical Plan ==")),
        ("codegen", Seq("WholeStageCodegen subtrees")),
        ("cost", Seq("== Optimized Logical Plan ==", "Statistics(sizeInBytes")),
        ("formatted", Seq("== Physical Plan ==", "Output", "Arguments"))).foreach {
      case (mode, expected) =>
        checkPlanDescription(mode, expected)
    }
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
      System.currentTimeMillis(),
      Map.empty))
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
      System.currentTimeMillis(),
      Map.empty))
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
      System.currentTimeMillis(),
      Map.empty))
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
      System.currentTimeMillis(),
      Map.empty))
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
      System.currentTimeMillis(),
      Map.empty))

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

    val expectedAccumValue = 12345L
    val expectedAccumValue2 = 54321L
    val physicalPlan = MyPlan(sqlContext.sparkContext, expectedAccumValue, expectedAccumValue2)
    val dummyQueryExecution = new QueryExecution(spark, LocalRelation()) {
      override lazy val sparkPlan = physicalPlan
      override lazy val executedPlan = physicalPlan
    }

    SQLExecution.withNewExecutionId(dummyQueryExecution) {
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
    val expectedValue = SQLMetrics.stringValue(driverMetric.metricType,
      Array(expectedAccumValue), Array.empty[Long])
    val expectedValue2 = SQLMetrics.stringValue(driverMetric2.metricType,
      Array(expectedAccumValue2), Array.empty[Long])

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
      time,
      Map.empty))
    time += 1
    listener.onOtherEvent(SparkListenerSQLExecutionStart(
      2,
      "test",
      "test",
      df.queryExecution.toString,
      SparkPlanInfo.fromSparkPlan(df.queryExecution.executedPlan),
      time,
      Map.empty))

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
      time,
      Map.empty))
    assert(statusStore.executionsCount === 2)
    assert(statusStore.execution(2) === None)
  }

  test("SPARK-29894 test Codegen Stage Id in SparkPlanInfo",
    DisableAdaptiveExecution("WSCG rule is applied later in AQE")) {
    // with AQE on, the WholeStageCodegen rule is applied when running QueryStageExec.
    val df = createTestDataFrame.select(count("*"))
    val sparkPlanInfo = SparkPlanInfo.fromSparkPlan(df.queryExecution.executedPlan)
    assert(sparkPlanInfo.nodeName === "WholeStageCodegen (2)")
  }

  test("SPARK-32615,SPARK-33016: SQLMetrics validation after sparkPlanInfo updated in AQE") {
    val statusStore = createStatusStore()
    val listener = statusStore.listener.get

    val executionId = 0
    val df = createTestDataFrame

    // oldPlan SQLMetrics
    // SQLPlanMetric(duration,0,timing)
    // SQLPlanMetric(number of output rows,1,sum)
    // SQLPlanMetric(number of output rows,2,sum)
    val oldPlan = SparkPlanInfo.fromSparkPlan(df.queryExecution.executedPlan)
    val oldAccumulatorIds =
      SparkPlanGraph(oldPlan)
        .allNodes.flatMap(_.metrics.map(_.accumulatorId))

    listener.onOtherEvent(SparkListenerSQLExecutionStart(
      executionId,
      "test",
      "test",
      df.queryExecution.toString,
      oldPlan,
      System.currentTimeMillis(),
      Map.empty))

    listener.onJobStart(SparkListenerJobStart(
      jobId = 0,
      time = System.currentTimeMillis(),
      stageInfos = Seq(createStageInfo(0, 0)),
      createProperties(executionId)))

    listener.onStageSubmitted(SparkListenerStageSubmitted(createStageInfo(0, 0)))
    listener.onTaskStart(SparkListenerTaskStart(0, 0, createTaskInfo(0, 0)))

    assert(statusStore.executionMetrics(executionId).isEmpty)

    // update old metrics with Id 1 & 2, since 0 is timing metrics,
    // timing metrics has a complicated string presentation so we don't test it here.
    val oldMetricsValueMap = oldAccumulatorIds.sorted.tail.map(id => (id, 100L)).toMap
    listener.onExecutorMetricsUpdate(SparkListenerExecutorMetricsUpdate("", Seq(
      (0L, 0, 0, createAccumulatorInfos(oldMetricsValueMap))
    )))

    assert(statusStore.executionMetrics(executionId).size == 2)
    statusStore.executionMetrics(executionId).foreach { m =>
      assert(m._2 == "100")
    }

    listener.onTaskEnd(SparkListenerTaskEnd(
      stageId = 0,
      stageAttemptId = 0,
      taskType = "",
      reason = null,
      createTaskInfo(0, 0),
      new ExecutorMetrics,
      null))

    listener.onStageCompleted(SparkListenerStageCompleted(createStageInfo(0, 0)))

    listener.onJobEnd(SparkListenerJobEnd(
      jobId = 0,
      time = System.currentTimeMillis(),
      JobSucceeded
    ))

    val df2 = createTestDataFrame.filter("_2 > 2")
    // newPlan SQLMetrics
    // SQLPlanMetric(duration,3,timing)
    // SQLPlanMetric(number of output rows,4,sum)
    // SQLPlanMetric(number of output rows,5,sum)
    val newPlan = SparkPlanInfo.fromSparkPlan(df2.queryExecution.executedPlan)
    val newAccumulatorIds =
      SparkPlanGraph(newPlan)
        .allNodes.flatMap(_.metrics.map(_.accumulatorId))

    // Assume that AQE update sparkPlanInfo with newPlan
    // ExecutionMetrics will be appended using newPlan's SQLMetrics
    listener.onOtherEvent(SparkListenerSQLAdaptiveExecutionUpdate(
      executionId,
      "test",
      newPlan))

    listener.onJobStart(SparkListenerJobStart(
      jobId = 1,
      time = System.currentTimeMillis(),
      stageInfos = Seq(createStageInfo(1, 0)),
      createProperties(executionId)))

    listener.onStageSubmitted(SparkListenerStageSubmitted(createStageInfo(1, 0)))
    listener.onTaskStart(SparkListenerTaskStart(1, 0, createTaskInfo(0, 0)))

    // historical metrics will be kept despite of the newPlan updated.
    assert(statusStore.executionMetrics(executionId).size == 2)

    // update new metrics with Id 4 & 5, since 3 is timing metrics,
    // timing metrics has a complicated string presentation so we don't test it here.
    val newMetricsValueMap = newAccumulatorIds.sorted.tail.map(id => (id, 500L)).toMap
    listener.onExecutorMetricsUpdate(SparkListenerExecutorMetricsUpdate("", Seq(
      (0L, 1, 0, createAccumulatorInfos(newMetricsValueMap))
    )))

    assert(statusStore.executionMetrics(executionId).size == 4)
    statusStore.executionMetrics(executionId).foreach { m =>
      assert(m._2 == "100" || m._2 == "500")
    }

    listener.onTaskEnd(SparkListenerTaskEnd(
      stageId = 1,
      stageAttemptId = 0,
      taskType = "",
      reason = null,
      createTaskInfo(0, 0),
      new ExecutorMetrics,
      null))

    listener.onStageCompleted(SparkListenerStageCompleted(createStageInfo(1, 0)))

    listener.onJobEnd(SparkListenerJobEnd(
      jobId = 1,
      time = System.currentTimeMillis(),
      JobSucceeded
    ))

    // aggregateMetrics should contains all metrics from job 0 and job 1
    val aggregateMetrics = listener.liveExecutionMetrics(executionId)
    if (aggregateMetrics.isDefined) {
      assert(aggregateMetrics.get.keySet.size == 4)
    }

    listener.onOtherEvent(SparkListenerSQLExecutionEnd(
      executionId, System.currentTimeMillis()))
  }


  test("SPARK-34338: Report metrics from Datasource v2 scan") {
    val statusStore = spark.sharedState.statusStore
    val oldCount = statusStore.executionsList().size

    val schema = new StructType().add("i", "int").add("j", "int")
    val physicalPlan = BatchScanExec(schema.toAttributes, new CustomMetricScanBuilder(), Seq.empty)
    val dummyQueryExecution = new QueryExecution(spark, LocalRelation()) {
      override lazy val sparkPlan = physicalPlan
      override lazy val executedPlan = physicalPlan
    }

    SQLExecution.withNewExecutionId(dummyQueryExecution) {
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
    val expectedMetric = physicalPlan.metrics("custom_metric")
    val expectedValue = "custom_metric: 12345, 12345"
    val innerMetric = physicalPlan.metrics("inner_metric")
    val expectedInnerValue = "inner_metric: 54321, 54321"
    assert(metrics.contains(expectedMetric.id))
    assert(metrics(expectedMetric.id) === expectedValue)
    assert(metrics.contains(innerMetric.id))
    assert(metrics(innerMetric.id) === expectedInnerValue)
  }

  test("SPARK-36030: Report metrics from Datasource v2 write") {
    withTempDir { dir =>
      val statusStore = spark.sharedState.statusStore
      val oldCount = statusStore.executionsList().size

      val cls = classOf[CustomMetricsDataSource].getName
      spark.range(10).select(Symbol("id") as Symbol("i"), -Symbol("id") as Symbol("j"))
        .write.format(cls)
        .option("path", dir.getCanonicalPath).mode("append").save()

      // Wait until the new execution is started and being tracked.
      eventually(timeout(10.seconds), interval(10.milliseconds)) {
        assert(statusStore.executionsCount() >= oldCount)
      }

      // Wait for listener to finish computing the metrics for the execution.
      eventually(timeout(10.seconds), interval(10.milliseconds)) {
        assert(statusStore.executionsList().nonEmpty &&
          statusStore.executionsList().last.metricValues != null)
      }

      val execId = statusStore.executionsList().last.executionId
      val metrics = statusStore.executionMetrics(execId)
      val customMetric = metrics.find(_._2 == "custom_metric: 12345, 12345")
      val innerMetric = metrics.find(_._2 == "inner_metric: 54321, 54321")
      assert(customMetric.isDefined)
      assert(innerMetric.isDefined)
    }
  }

  test("SPARK-37578: Update output metrics from Datasource v2") {
    withTempDir { dir =>
      val statusStore = spark.sharedState.statusStore
      val oldCount = statusStore.executionsCount()

      val bytesWritten = new ArrayBuffer[Long]()
      val recordsWritten = new ArrayBuffer[Long]()

      val bytesWrittenListener = new SparkListener() {
        override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
          bytesWritten += taskEnd.taskMetrics.outputMetrics.bytesWritten
          recordsWritten += taskEnd.taskMetrics.outputMetrics.recordsWritten
        }
      }
      spark.sparkContext.addSparkListener(bytesWrittenListener)

      try {
        val cls = classOf[CustomMetricsDataSource].getName
        spark.range(0, 10, 1, 2).select(Symbol("id") as Symbol("i"), -'id as Symbol("j"))
          .write.format(cls)
          .option("path", dir.getCanonicalPath).mode("append").save()

        // Wait until the new execution is started and being tracked.
        eventually(timeout(10.seconds), interval(10.milliseconds)) {
          assert(statusStore.executionsCount() > oldCount)
        }

        // Wait for listener to finish computing the metrics for the execution.
        eventually(timeout(10.seconds), interval(10.milliseconds)) {
          assert(statusStore.executionsList().nonEmpty &&
            statusStore.executionsList().last.metricValues != null)
        }

        spark.sparkContext.listenerBus.waitUntilEmpty()
        assert(bytesWritten.sum == 246)
        assert(recordsWritten.sum == 20)
      } finally {
        spark.sparkContext.removeSparkListener(bytesWrittenListener)
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

object Outer {
  class InnerCustomMetric extends CustomMetric {
    override def name(): String = "inner_metric"
    override def description(): String = "a simple custom metric in an inner class"
    override def aggregateTaskMetrics(taskMetrics: Array[Long]): String = {
      s"inner_metric: ${taskMetrics.mkString(", ")}"
    }
  }
}

class SimpleCustomMetric extends CustomMetric {
  override def name(): String = "custom_metric"
  override def description(): String = "a simple custom metric"
  override def aggregateTaskMetrics(taskMetrics: Array[Long]): String = {
    s"custom_metric: ${taskMetrics.mkString(", ")}"
  }
}

class BytesWrittenCustomMetric extends CustomMetric {
  override def name(): String = "bytesWritten"
  override def description(): String = "bytesWritten metric"
  override def aggregateTaskMetrics(taskMetrics: Array[Long]): String = {
    s"bytesWritten: ${taskMetrics.mkString(", ")}"
  }
}

class RecordsWrittenCustomMetric extends CustomMetric {
  override def name(): String = "recordsWritten"
  override def description(): String = "recordsWritten metric"
  override def aggregateTaskMetrics(taskMetrics: Array[Long]): String = {
    s"recordsWritten: ${taskMetrics.mkString(", ")}"
  }
}

// The followings are for custom metrics of V2 data source.
object CustomMetricReaderFactory extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    val RangeInputPartition(start, end) = partition
    new PartitionReader[InternalRow] {
      private var current = start - 1

      override def next(): Boolean = {
        current += 1
        current < end
      }

      override def get(): InternalRow = InternalRow(current, -current)

      override def close(): Unit = {}

      override def currentMetricsValues(): Array[CustomTaskMetric] = {
        val metric = new CustomTaskMetric {
          override def name(): String = "custom_metric"
          override def value(): Long = 12345
        }
        val innerMetric = new CustomTaskMetric {
          override def name(): String = "inner_metric"
          override def value(): Long = 54321;
        }
        Array(metric, innerMetric)
      }
    }
  }
}

class CustomMetricScanBuilder extends SimpleScanBuilder {
  override def planInputPartitions(): Array[InputPartition] = {
    Array(RangeInputPartition(0, 5), RangeInputPartition(5, 10))
  }

  override def supportedCustomMetrics(): Array[CustomMetric] = {
    Array(new SimpleCustomMetric, new Outer.InnerCustomMetric)
  }

  override def createReaderFactory(): PartitionReaderFactory = CustomMetricReaderFactory
}

class CustomMetricsCSVDataWriter(fs: FileSystem, file: Path) extends CSVDataWriter(fs, file) {
  override def currentMetricsValues(): Array[CustomTaskMetric] = {
    val metric = new CustomTaskMetric {
      override def name(): String = "custom_metric"
      override def value(): Long = 12345
    }
    val innerMetric = new CustomTaskMetric {
      override def name(): String = "inner_metric"
      override def value(): Long = 54321;
    }
    val bytesWrittenMetric = new CustomTaskMetric {
      override def name(): String = "bytesWritten"
      override def value(): Long = 123;
    }
    val recordsWrittenMetric = new CustomTaskMetric {
      override def name(): String = "recordsWritten"
      override def value(): Long = 10;
    }
    Array(metric, innerMetric, bytesWrittenMetric, recordsWrittenMetric)
  }
}

class CustomMetricsWriterFactory(path: String, jobId: String, conf: SerializableConfiguration)
  extends CSVDataWriterFactory(path, jobId, conf) {

  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] = {
    val jobPath = new Path(new Path(path, "_temporary"), jobId)
    val filePath = new Path(jobPath, s"$jobId-$partitionId-$taskId")
    val fs = filePath.getFileSystem(conf.value)
    new CustomMetricsCSVDataWriter(fs, filePath)
  }
}

class CustomMetricsDataSource extends SimpleWritableDataSource {

  class CustomMetricBatchWrite(queryId: String, path: String, conf: Configuration)
      extends MyBatchWrite(queryId, path, conf) {
    override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory = {
      new CustomMetricsWriterFactory(path, queryId, new SerializableConfiguration(conf))
    }
  }

  class CustomMetricWriteBuilder(path: String, info: LogicalWriteInfo)
      extends MyWriteBuilder(path, info) {
    override def build(): Write = {
      new Write {
        override def toBatch: BatchWrite = {
          val hadoopPath = new Path(path)
          val hadoopConf = SparkContext.getActive.get.hadoopConfiguration
          val fs = hadoopPath.getFileSystem(hadoopConf)

          if (needTruncate) {
            fs.delete(hadoopPath, true)
          }

          val pathStr = hadoopPath.toUri.toString
          new CustomMetricBatchWrite(queryId, pathStr, hadoopConf)
        }

        override def supportedCustomMetrics(): Array[CustomMetric] = {
          Array(new SimpleCustomMetric, new Outer.InnerCustomMetric,
            new BytesWrittenCustomMetric, new RecordsWrittenCustomMetric)
        }
      }
    }
  }

  class CustomMetricTable(options: CaseInsensitiveStringMap) extends MyTable(options) {
    override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
      new CustomMetricWriteBuilder(path, info)
    }
  }

  override def getTable(options: CaseInsensitiveStringMap): Table = {
    new CustomMetricTable(options)
  }
}
