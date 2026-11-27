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
import org.scalatest.concurrent.Eventually._
import org.scalatest.time.SpanSugar._

import org.apache.spark._
import org.apache.spark.LocalSparkContext._
import org.apache.spark.executor.ExecutorMetrics
import org.apache.spark.internal.config
import org.apache.spark.internal.config.Status._
import org.apache.spark.rdd.RDD
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.scheduler._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, DynamicPruningExpression, Literal, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.catalyst.types.DataTypeUtils.toAttributes
import org.apache.spark.sql.catalyst.util.quietly
import org.apache.spark.sql.classic.{DataFrame, SparkSession}
import org.apache.spark.sql.connector.{CSVDataWriter, CSVDataWriterFactory, RangeInputPartition, SimpleScanBuilder, SimpleWritableDataSource, TestLocalScanTable}
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.metric.{CustomMetric, CustomTaskMetric}
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.connector.write.{BatchWrite, DataWriter, DataWriterFactory, LogicalWriteInfo, PhysicalWriteInfo, Write, WriteBuilder}
import org.apache.spark.sql.execution.{FilterExec, InSubqueryExec, LeafExecNode, QueryExecution, SparkPlanInfo, SQLExecution, SubqueryExec, UnionExec, WholeStageCodegenExec}
import org.apache.spark.sql.execution.adaptive.DisableAdaptiveExecution
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetricInfo, SQLMetrics}
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.StaticSQLConf.UI_RETAINED_EXECUTIONS
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.status.{AppStatusStore, ElementTrackingStore}
import org.apache.spark.util.{AccumulatorMetadata, JsonProtocol, LongAccumulator, MetricUtils, SerializableConfiguration, Utils}
import org.apache.spark.util.kvstore.InMemoryStore


abstract class SQLAppStatusListenerSuite extends SharedSparkSession with JsonTestUtils
  with BeforeAndAfter {

  import testImplicits._

  override protected def sparkConf = {
    super.sparkConf.set(LIVE_ENTITY_UPDATE_PERIOD, 0L).set(ASYNC_TRACKING_ENABLED, false)
  }

  protected var kvstore: ElementTrackingStore = _

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
      partitionId = taskId.toInt,
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

  protected def createStatusStore(): SQLAppStatusStore

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
      Some(executionId),
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
      accumulatorUpdates.transform((_, v) => v * 2))

    // Driver accumulator updates don't belong to this execution should be filtered and no
    // exception will be thrown.
    listener.onOtherEvent(SparkListenerDriverAccumUpdates(0, Seq((999L, 2L))))

    checkAnswer(statusStore.executionMetrics(executionId),
      accumulatorUpdates.transform((_, v) => v * 2))

    listener.onExecutorMetricsUpdate(SparkListenerExecutorMetricsUpdate("", Seq(
      // (task id, stage id, stage attempt, accum updates)
      (0L, 0, 0, createAccumulatorInfos(accumulatorUpdates)),
      (1L, 0, 0, createAccumulatorInfos(accumulatorUpdates.transform((_, v) => v * 2)))
    )))

    checkAnswer(statusStore.executionMetrics(executionId),
      accumulatorUpdates.transform((_, v) => v * 3))

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
      accumulatorUpdates.transform((_, v) => v * 2))

    // Ignore the task end for the first attempt
    listener.onTaskEnd(SparkListenerTaskEnd(
      stageId = 0,
      stageAttemptId = 0,
      taskType = "",
      reason = null,
      createTaskInfo(0, 0, accums = accumulatorUpdates.transform((_, v) => v * 100)),
      new ExecutorMetrics,
      null))

    checkAnswer(statusStore.executionMetrics(executionId),
      accumulatorUpdates.transform((_, v) => v * 2))

    // Finish two tasks
    listener.onTaskEnd(SparkListenerTaskEnd(
      stageId = 0,
      stageAttemptId = 1,
      taskType = "",
      reason = null,
      createTaskInfo(0, 0, accums = accumulatorUpdates.transform((_, v) => v * 2)),
      new ExecutorMetrics,
      null))
    listener.onTaskEnd(SparkListenerTaskEnd(
      stageId = 0,
      stageAttemptId = 1,
      taskType = "",
      reason = null,
      createTaskInfo(1, 0, accums = accumulatorUpdates.transform((_, v) => v * 3)),
      new ExecutorMetrics,
      null))

    checkAnswer(statusStore.executionMetrics(executionId),
      accumulatorUpdates.transform((_, v) => v * 5))

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
      accumulatorUpdates.transform((_, v) => v * 7))

    // Finish two tasks
    listener.onTaskEnd(SparkListenerTaskEnd(
      stageId = 1,
      stageAttemptId = 0,
      taskType = "",
      reason = null,
      createTaskInfo(0, 0, accums = accumulatorUpdates.transform((_, v) => v * 3)),
      new ExecutorMetrics,
      null))
    listener.onTaskEnd(SparkListenerTaskEnd(
      stageId = 1,
      stageAttemptId = 0,
      taskType = "",
      reason = null,
      createTaskInfo(1, 0, accums = accumulatorUpdates.transform((_, v) => v * 3)),
      new ExecutorMetrics,
      null))

    checkAnswer(statusStore.executionMetrics(executionId),
      accumulatorUpdates.transform((_, v) => v * 11))

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
      accumulatorUpdates.transform((_, v) => v * 11))
  }

  test("control a plan explain mode in listeners via SQLConf") {

    def checkPlanDescription(mode: String, expected: Seq[String]): Unit = {
      var checkDone = false
      val listener = new SparkListener {
        override def onOtherEvent(event: SparkListenerEvent): Unit = {
          event match {
            case SparkListenerSQLExecutionStart(_, _, _, _, planDescription, _, _, _, _, _, _) =>
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
        ("formatted", Seq("== Physical Plan ==", "Output", "Arguments")),
        ("none", Seq(SQLExecution.NO_PLAN_DESCRIPTION))).foreach {
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
      Some(executionId),
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
      Some(executionId),
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
      Some(executionId),
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
      Some(executionId),
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
      Some(executionId),
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

    val expectedAccumValue = 12345L
    val expectedAccumValue2 = 54321L
    val physicalPlan = MyPlan(sqlContext.sparkContext, expectedAccumValue, expectedAccumValue2)
    val dummyQueryExecution = new QueryExecution(spark, LocalRelation()) {
      override lazy val sparkPlan = physicalPlan
      override lazy val executedPlan = physicalPlan
    }

    val execId = runAndWaitForExecution {
      SQLExecution.withNewExecutionId(dummyQueryExecution) {
        physicalPlan.execute().collect()
      }
    }

    val metrics = statusStore.executionMetrics(execId)
    val driverMetric = physicalPlan.metrics("dummy")
    val driverMetric2 = physicalPlan.metrics("dummy2")
    val expectedValue = MetricUtils.stringValue(driverMetric.metricType,
      Array(expectedAccumValue), Array.empty[Long])
    val expectedValue2 = MetricUtils.stringValue(driverMetric2.metricType,
      Array(expectedAccumValue2), Array.empty[Long])

    assert(metrics.contains(driverMetric.id))
    assert(metrics(driverMetric.id) === expectedValue)
    assert(metrics.contains(driverMetric2.id))
    assert(metrics(driverMetric2.id) === expectedValue2)
  }

  test("SPARK-60759: driver updates for an accumulator that tasks also report") {
    val statusStore = createStatusStore()
    val listener = statusStore.listener.get

    val accumId = 1000L
    val planInfo = new SparkPlanInfo("TestNode", "TestNode", Nil, Map.empty,
      Seq(new SQLMetricInfo("size", accumId, MetricUtils.SIZE_METRIC)))
    // Task 1 reports the largest task value. The stage has 8 tasks, and the ones that have not
    // reported yet contribute 0.
    val taskValues = Seq(100L, 200L)
    val allTaskValues = taskValues.padTo(8, 0L)

    def render(executionId: Int, driverValues: Seq[Long]): String = {
      val stageId = executionId
      listener.onOtherEvent(SparkListenerSQLExecutionStart(
        executionId,
        Some(executionId),
        "test",
        "test",
        "",
        planInfo,
        System.currentTimeMillis(),
        Map.empty))
      listener.onJobStart(SparkListenerJobStart(
        jobId = executionId,
        time = System.currentTimeMillis(),
        stageInfos = Seq(createStageInfo(stageId, 0)),
        createProperties(executionId)))
      listener.onStageSubmitted(SparkListenerStageSubmitted(createStageInfo(stageId, 0)))
      taskValues.indices.foreach { taskId =>
        listener.onTaskStart(SparkListenerTaskStart(stageId, 0, createTaskInfo(taskId, 0)))
      }
      listener.onExecutorMetricsUpdate(SparkListenerExecutorMetricsUpdate("",
        taskValues.zipWithIndex.map { case (value, taskId) =>
          (taskId.toLong, stageId, 0, createAccumulatorInfos(Map(accumId -> value)))
        }))
      listener.onOtherEvent(SparkListenerDriverAccumUpdates(
        executionId, driverValues.map(accumId -> _)))
      statusStore.executionMetrics(executionId)(accumId)
    }

    // The largest driver value comes after a smaller one and exceeds the task max.
    val driverMax = Seq(50L, 400L, 50L)
    val driverMaxRendered = render(0, driverMax)
    assert(driverMaxRendered === MetricUtils.stringValue(
      MetricUtils.SIZE_METRIC, (allTaskValues ++ driverMax).toArray, Array.empty[Long]))
    assert(driverMaxRendered.contains("(driver)"))

    // All driver values, including a repeated one, stay below the task max.
    val belowTaskMax = Seq(150L, 50L, 150L)
    val belowTaskMaxRendered = render(1, belowTaskMax)
    assert(belowTaskMaxRendered === MetricUtils.stringValue(
      MetricUtils.SIZE_METRIC, (allTaskValues ++ belowTaskMax).toArray, Array(200L, 1L, 0L, 1L)))
    assert(belowTaskMaxRendered.contains("(stage 1.0: task 1)"))
  }

  test("roundtripping SparkListenerDriverAccumUpdates through JsonProtocol (SPARK-18462)") {
    val event = SparkListenerDriverAccumUpdates(1L, Seq((2L, 3L)))
    val json = JsonProtocol.sparkEventToJsonString(event)
    assertValidDataInJson(parse(json),
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
    val longJson =
      """
        |{
        |  "Event": "org.apache.spark.sql.execution.ui.SparkListenerDriverAccumUpdates",
        |  "executionId": 4294967294,
        |  "accumUpdates": [[4294967294,3]]
        |}
      """.stripMargin
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
      Some(1),
      "test",
      "test",
      df.queryExecution.toString,
      SparkPlanInfo.fromSparkPlan(df.queryExecution.executedPlan),
      time,
      Map.empty))
    time += 1
    listener.onOtherEvent(SparkListenerSQLExecutionStart(
      2,
      Some(2),
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
      Some(3),
      "test",
      "test",
      df.queryExecution.toString,
      SparkPlanInfo.fromSparkPlan(df.queryExecution.executedPlan),
      time,
      Map.empty))
    assert(statusStore.executionsCount() === 2)
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
      Some(executionId),
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

  private def replayPlanInfo(
      planInfo: SparkPlanInfo,
      adaptive: Boolean): (SQLAppStatusStore, SparkPlanInfo) = {
    val store = createStatusStore()
    val listener = store.listener.get
    val executionId = 0L
    val bus = new ReplayListenerBus
    bus.addListener(listener)
    def roundTrip(event: SparkListenerEvent): SparkListenerEvent = {
      val json = JsonProtocol.sparkEventToJsonString(event)
      assert(bus.replay(Iterator(json), "DPP SQL event log", maybeTruncated = false, _ => true))
      JsonProtocol.sparkEventFromJson(json)
    }
    val initial = if (adaptive) {
      new SparkPlanInfo("Initial", "initial plan", Nil, Map.empty, Nil)
    } else {
      planInfo
    }
    val start = roundTrip(SparkListenerSQLExecutionStart(
      executionId, Some(executionId), "DPP replay", "", "", initial, 0L, Map.empty))
      .asInstanceOf[SparkListenerSQLExecutionStart]
    val restored = if (adaptive) {
      val update = roundTrip(SparkListenerSQLAdaptiveExecutionUpdate(executionId, "", planInfo))
        .asInstanceOf[SparkListenerSQLAdaptiveExecutionUpdate]
      update.sparkPlanInfo
    } else {
      start.sparkPlanInfo
    }
    (store, restored)
  }

  protected def assertReplayGraphEndpoints(graph: SparkPlanGraph): Unit = {
    val visibleIds = graph.allNodes.filterNot(_.isInstanceOf[SparkPlanGraphCluster]).map(_.id).toSet
    assert(graph.edges.forall(e => visibleIds(e.fromId) && visibleIds(e.toId)))
    assert(!graph.allNodes.exists(_.name == "DPPSharedPlanReference"))
    val dotEdges = "(\\d+)->(\\d+);".r
      .findAllMatchIn(graph.makeDotFile(Map.empty))
      .map(m => (m.group(1).toLong, m.group(2).toLong)).toSeq
    assert(dotEdges.size == graph.edges.size)
    assert(dotEdges.forall { case (from, to) => visibleIds(from) && visibleIds(to) })
  }

  private def assertReplayNodeMetrics(
      node: SparkPlanGraphNode,
      expectedIds: Seq[Long],
      metricValues: Map[Long, String]): Unit = {
    assert(node.metrics.map(_.accumulatorId).sorted == expectedIds.sorted,
      s"Unexpected metric ownership on ${node.name} (${node.id})")
    // Mainline displays metrics in the node details panel rather than the DOT node label.
    val details = parse(SparkPlanGraph(Seq(node), Seq.empty).makeNodeDetailsJson(metricValues))
    val key = node match {
      case _: SparkPlanGraphCluster => s"cluster${node.id}"
      case _ => s"node${node.id}"
    }
    val displayed = (details \ key \ "metrics").children
    assert(displayed.size == expectedIds.size)
    node.metrics.foreach { metric =>
      val value = metricValues(metric.accumulatorId)
      assert(displayed.exists { entry =>
        (entry \ "name").values == metric.name &&
          (entry \ "value").values == value &&
          (entry \ "type").values == metric.metricType
      }, s"Missing metric ${metric.accumulatorId} on ${node.name}")
    }
  }

  for (adaptive <- Seq(false, true)) {
    val eventName = if (adaptive) "SQLAdaptiveExecutionUpdate" else "SQLExecutionStart"

    test(s"SPARK-48486: $eventName replays a captured physical DPP graph after JSON roundtrip") {
      // Build physical expressions directly: this exercises fromSparkPlan without preparing or
      // executing the subquery, and does not depend on optimizer choices for a small SQL query.
      val source = WholeStageCodegenExec(spark.range(2).queryExecution.sparkPlan)(7)
      val producer = SubqueryExec("dpp-event-source", source)
      val consumers = Seq(1L, 2L).map { value =>
        FilterExec(DynamicPruningExpression(InSubqueryExec(
          Literal(value), producer, NamedExpression.newExprId)),
          spark.range(3).queryExecution.sparkPlan)
      }
      val info = SparkPlanInfo.fromSparkPlan(UnionExec(consumers))
      val (store, restored) = replayPlanInfo(info, adaptive)
      def flatten(plan: SparkPlanInfo): Seq[SparkPlanInfo] = {
        plan +: plan.children.flatMap(flatten)
      }
      val descriptions = flatten(restored)
      val details = descriptions.filter(_.metadata.contains("spark.sql.dpp.planInfo.id"))
      val refs = descriptions.filter(_.nodeName == "DPPSharedPlanReference")
      assert(details.nonEmpty)
      assert(refs.nonEmpty)
      val ids = details.map(_.metadata("spark.sql.dpp.planInfo.id"))
      assert(ids.distinct.size == ids.size)
      refs.foreach { ref =>
        assert(ref.children.isEmpty && ref.metrics.isEmpty)
        assert(ref.metadata("spark.sql.dpp.planInfo.version") == "1")
        assert(ids.contains(ref.metadata("spark.sql.dpp.planInfo.ref")))
      }
      val graph = store.planGraph(0L)
      val subqueries = graph.allNodes.filter(_.name == "Subquery")
      val filters = graph.allNodes.filter(_.name == "Filter")
      assert(subqueries.size == 1)
      assert(filters.size == 2)
      assert(graph.edges.filter(_.fromId == subqueries.head.id).map(_.toId).toSet ==
        filters.map(_.id).toSet)
      val producerMetricIds = (producer.metrics.values ++ source.metrics.values ++
        source.child.metrics.values).map(_.id).toSeq
      assert(producerMetricIds.nonEmpty)
      producerMetricIds.foreach { id =>
        assert(graph.allNodes.flatMap(_.metrics).count(_.accumulatorId == id) == 1)
        assert(store.execution(0L).get.metrics.count(_.accumulatorId == id) == 1)
      }
      val sourceClusters = graph.allNodes.collect {
        case cluster: SparkPlanGraphCluster if cluster.name == source.nodeName => cluster
      }
      assert(sourceClusters.size == 1)
      val sourceChildren = graph.edges.filter(_.toId == subqueries.head.id).map(_.fromId)
      assert(sourceChildren.size == 1)
      val sourceChild = graph.allNodes.find(_.id == sourceChildren.head).get
      assert(sourceChild.name == source.child.nodeName)
      val metricUpdates = producerMetricIds.map(_ -> 123L)
      store.listener.get.onOtherEvent(SparkListenerDriverAccumUpdates(0L, metricUpdates))
      val physicalMetrics = producer.metrics.values ++ source.metrics.values ++
        source.child.metrics.values
      val expectedMetricValues = physicalMetrics.map { metric =>
        metric.id -> MetricUtils.stringValue(metric.metricType, Array(123L), Array.empty[Long])
      }.toMap
      assert(store.executionMetrics(0L) == expectedMetricValues)
      assertReplayNodeMetrics(subqueries.head, producer.metrics.values.map(_.id).toSeq,
        expectedMetricValues)
      assertReplayNodeMetrics(sourceClusters.head, source.metrics.values.map(_.id).toSeq,
        expectedMetricValues)
      assertReplayNodeMetrics(sourceChild, source.child.metrics.values.map(_.id).toSeq,
        expectedMetricValues)
      assertReplayGraphEndpoints(graph)
    }

    test(s"SPARK-48486: $eventName replays DPP aliases and legacy reuse after JSON roundtrip") {
      val prefix = "spark.sql.dpp.planInfo."
      def node(
          name: String,
          children: Seq[SparkPlanInfo] = Nil,
          id: String = "",
          metric: Option[Long] = None): SparkPlanInfo = {
        val metadata = if (id.isEmpty) Map.empty[String, String] else {
          Map(s"${prefix}version" -> "1", s"${prefix}id" -> id)
        }
        new SparkPlanInfo(name, name, children, metadata, metric.toSeq.map { m =>
          val metricName = if (m == 901L) "duration" else s"metric-$m"
          new SQLMetricInfo(metricName, m, "sum")
        })
      }
      def ref(id: String): SparkPlanInfo = new SparkPlanInfo(
        "DPPSharedPlanReference", s"ref=$id", Nil,
        Map(s"${prefix}version" -> "1", s"${prefix}ref" -> id), Nil)
      val deep = node("DeepProducer", id = "deep", metric = Some(902L))
      val leafA = node("Producer", Seq(deep), "leaf-a", Some(900L))
      val leafB = node("Producer", Seq(ref("deep")), "leaf-b", Some(900L))
      val stageA = node("WholeStageCodegen (1)", Seq(leafA), "stage-a", Some(901L))
      val stageB = node("WholeStageCodegen (1)", Seq(leafB), "stage-b", Some(901L))
      val exchangeA = node("Exchange", Seq(stageA), "exchange-a", Some(903L))
      val exchangeB = node("Exchange", Seq(stageB), "exchange-b", Some(903L))
      val subqueryA = node("Subquery", Seq(ref("leaf-a")), "subquery-a", Some(904L))
      val subqueryB = node("Subquery", Seq(ref("leaf-b")), "subquery-b", Some(904L))
      val info = node("Root", Seq(
        node("Forward", Seq(ref("stage-b"))),
        node("ExchangeConsumerA", Seq(exchangeA)),
        node("ExchangeConsumerB", Seq(node("ReusedExchange", Seq(exchangeB)))),
        node("StageConsumer", Seq(ref("stage-b"))),
        node("LeafConsumer", Seq(ref("leaf-b"))),
        node("DeepConsumer", Seq(ref("deep"))),
        node("SubqueryConsumerA", Seq(subqueryA)),
        node("SubqueryConsumerB", Seq(node("ReusedSubquery", Seq(subqueryB))))))
      val (store, restored) = replayPlanInfo(info, adaptive)
      assert(restored.children.head.children.head.metadata(s"${prefix}ref") == "stage-b")
      val graph = store.planGraph(0L)
      def only(name: String): SparkPlanGraphNode = {
        val matches = graph.allNodes.filter(_.name == name)
        assert(matches.size == 1, s"Expected one $name, found ${matches.size}")
        matches.head
      }
      val consumerNames = Seq("Forward", "ExchangeConsumerA", "ExchangeConsumerB",
        "StageConsumer", "LeafConsumer", "DeepConsumer", "SubqueryConsumerA",
        "SubqueryConsumerB")
      val expectedNames = consumerNames.map(_ -> "Root") ++ Seq(
        "Exchange" -> "ExchangeConsumerA", "Exchange" -> "ExchangeConsumerB",
        "Producer" -> "Exchange", "DeepProducer" -> "Producer",
        "Producer" -> "Forward", "Producer" -> "StageConsumer",
        "Producer" -> "LeafConsumer", "DeepProducer" -> "DeepConsumer",
        "Subquery" -> "SubqueryConsumerA", "Subquery" -> "SubqueryConsumerB",
        "Producer" -> "Subquery")
      val expected = expectedNames.map { case (from, to) => (only(from).id, only(to).id) }
      val actual = graph.edges.map(e => (e.fromId, e.toId))
      assert(actual.size == expected.size)
      assert(actual.toSet == expected.toSet)
      assert(graph.allNodes.count(_.isInstanceOf[SparkPlanGraphCluster]) == 1)
      val metricIds = Seq(900L, 901L, 902L, 903L, 904L)
      assert(graph.allNodes.flatMap(_.metrics).map(_.accumulatorId).sorted == metricIds)
      assert(store.execution(0L).get.metrics.map(_.accumulatorId).sorted == metricIds)
      store.listener.get.onOtherEvent(SparkListenerDriverAccumUpdates(
        0L, metricIds.map(_ -> 3L)))
      assert(store.executionMetrics(0L) == metricIds.map(_ -> "3").toMap)
      Seq("Producer" -> 900L, "WholeStageCodegen (1)" -> 901L,
        "DeepProducer" -> 902L, "Exchange" -> 903L, "Subquery" -> 904L).foreach {
        case (name, id) =>
          assertReplayNodeMetrics(only(name), Seq(id), store.executionMetrics(0L))
      }
      assertReplayGraphEndpoints(graph)
    }
  }

  for {
    adaptive <- Seq(false, true)
    reuse <- Seq("Subquery", "ReusedExchange", "ShuffleQueryStage")
  } {
    val eventName = if (adaptive) "SQLAdaptiveExecutionUpdate" else "SQLExecutionStart"
    test(s"SPARK-48486: $eventName materializes details skipped by $reuse reuse") {
      val prefix = "spark.sql.dpp.planInfo."
      def node(
          name: String,
          children: Seq[SparkPlanInfo] = Nil,
          id: String = "",
          metric: Option[Long] = None): SparkPlanInfo = {
        val metadata = if (id.isEmpty) Map.empty[String, String] else {
          Map(s"${prefix}version" -> "1", s"${prefix}id" -> id)
        }
        new SparkPlanInfo(name, name, children, metadata, metric.toSeq.map { m =>
          val metricName = if (m == 1000L) "duration" else s"metric-$m"
          new SQLMetricInfo(metricName, m, "sum")
        })
      }
      def ref(id: String): SparkPlanInfo = new SparkPlanInfo(
        "DPPSharedPlanReference", s"ref=$id", Nil,
        Map(s"${prefix}version" -> "1", s"${prefix}ref" -> id), Nil)

      val deep = node("Deep", Seq(ref("pending")), metric = Some(1005L))
      val middle = node("Middle", Seq(deep), metric = Some(1004L))
      val anchor = node("Anchor", Seq(middle), "anchor", Some(1003L))
      val pending = node("PendingProducer", id = "pending", metric = Some(1006L))
      val producerName = if (reuse == "Subquery") "Subquery" else "Exchange"
      val canonical = node(producerName, Seq(ref("anchor"), ref("pending")),
        metric = Some(1002L))
      // Both unique details are in the subtree that legacy semantic reuse skips. Materializing
      // anchor also adds a pending edge from PendingProducer to the non-ID Deep descendant.
      val duplicate = node(producerName, Seq(anchor, pending), metric = Some(1002L))
      val reused = if (reuse == "Subquery") duplicate else node(reuse, Seq(duplicate))
      val info = node("Root", Seq(
        node("WholeStageCodegen (17)", Seq(
          node("Host", Seq(canonical), metric = Some(1001L))), metric = Some(1000L)),
        node("SecondConsumer", Seq(reused)),
        node("LateConsumer", Seq(ref("anchor"), ref("anchor"), ref("pending")))))
      val (store, restored) = replayPlanInfo(info, adaptive)
      val restoredCanonical = restored.children.head.children.head.children.head
      assert(restoredCanonical.children.forall(_.nodeName == "DPPSharedPlanReference"))
      val graph = store.planGraph(0L)
      def only(name: String): SparkPlanGraphNode = {
        val matches = graph.allNodes.filter(_.name == name)
        assert(matches.size == 1, s"Expected one $name, found ${matches.size}")
        matches.head
      }
      val expectedNames = Seq(
        "Host" -> "Root", producerName -> "Host",
        "Anchor" -> producerName, "PendingProducer" -> producerName,
        "Middle" -> "Anchor", "Deep" -> "Middle", "PendingProducer" -> "Deep",
        "SecondConsumer" -> "Root", producerName -> "SecondConsumer",
        "LateConsumer" -> "Root", "Anchor" -> "LateConsumer",
        "Anchor" -> "LateConsumer", "PendingProducer" -> "LateConsumer")
      val expected = expectedNames.map { case (from, to) => (only(from).id, only(to).id) }
      val actual = graph.edges.map(e => (e.fromId, e.toId))
      // Preserve repeated requests from the same consumer as well as the edges added while
      // materializing the missing anchor; a set comparison alone cannot prove this contract.
      assert(actual.groupBy(identity).map { case (edge, occurrences) =>
        edge -> occurrences.size
      } == expected.groupBy(identity).map { case (edge, occurrences) =>
        edge -> occurrences.size
      })
      val clusters = graph.allNodes.collect { case c: SparkPlanGraphCluster => c }
      assert(clusters.size == 1)
      val expectedClusterNames = if (reuse == "Subquery") {
        Set("Host")
      } else {
        Set("Host", "Exchange", "Anchor", "Middle", "Deep", "PendingProducer")
      }
      assert(clusters.head.nodes.map(_.name).toSet == expectedClusterNames)
      val metricIds = 1000L to 1006L
      assert(graph.allNodes.flatMap(_.metrics).map(_.accumulatorId).sorted == metricIds)
      assert(store.execution(0L).get.metrics.map(_.accumulatorId).sorted == metricIds)
      val metricUpdates = metricIds.map(id => id -> (id - 990L))
      val driverUpdate = JsonProtocol.sparkEventFromJson(JsonProtocol.sparkEventToJsonString(
        SparkListenerDriverAccumUpdates(0L, metricUpdates)))
      store.listener.get.onOtherEvent(driverUpdate)
      val expectedMetrics = metricUpdates.map { case (id, value) => id -> value.toString }.toMap
      assert(store.executionMetrics(0L) == expectedMetrics)
      assertReplayNodeMetrics(clusters.head, Seq(1000L), expectedMetrics)
      Seq("Host" -> 1001L, producerName -> 1002L, "Anchor" -> 1003L,
        "Middle" -> 1004L, "Deep" -> 1005L, "PendingProducer" -> 1006L).foreach {
        case (name, id) => assertReplayNodeMetrics(only(name), Seq(id), expectedMetrics)
      }
      val details = parse(graph.makeNodeDetailsJson(store.executionMetrics(0L)))
      val displayedMetrics = details.children.flatMap(node => (node \ "metrics").children)
      metricUpdates.foreach { case (id, value) =>
        val metricName = if (id == 1000L) "duration" else s"metric-$id"
        assert(displayedMetrics.exists { metric =>
          (metric \ "name").values == metricName &&
            (metric \ "value").values == value.toString
        })
      }
      assertReplayGraphEndpoints(graph)
    }
  }

  test("SPARK-48486: SQL events without DPP metadata retain legacy reuse after JSON roundtrip") {
    def node(name: String, children: SparkPlanInfo*): SparkPlanInfo = {
      new SparkPlanInfo(name, name, children, Map.empty, Nil)
    }
    val producer = node("Exchange", node("Scan"))
    val subquery = node("Subquery", node("SubqueryScan"))
    val info = node("Root", node("ConsumerA", producer, subquery),
      node("ConsumerB", node("ReusedExchange", producer), node("ReusedSubquery", subquery)))
    val (store, restored) = replayPlanInfo(info, adaptive = false)
    assert(restored.metadata.isEmpty)
    val initialGraph = store.planGraph(0L)
    val update = JsonProtocol.sparkEventFromJson(JsonProtocol.sparkEventToJsonString(
      SparkListenerSQLAdaptiveExecutionUpdate(0L, "", info)))
    store.listener.get.onOtherEvent(update)
    val graph = store.planGraph(0L)
    assert(graph.allNodes.map(n => (n.id, n.name)) ==
      initialGraph.allNodes.map(n => (n.id, n.name)))
    assert(graph.edges == initialGraph.edges)
    Seq("Exchange", "Subquery").foreach { name =>
      val producers = graph.allNodes.filter(_.name == name)
      assert(producers.size == 1)
      val targets = graph.edges.filter(_.fromId == producers.head.id).map(_.toId).toSet
      assert(targets == graph.allNodes.filter(_.name.startsWith("Consumer")).map(_.id).toSet)
    }
    assertReplayGraphEndpoints(graph)
  }


  test("SPARK-34338: Report metrics from Datasource v2 scan") {
    val statusStore = spark.sharedState.statusStore

    val schema = new StructType().add("i", "int").add("j", "int")
    val physicalPlan = BatchScanExec(toAttributes(schema), new CustomMetricScanBuilder(), Seq.empty,
      table = new TestLocalScanTable("fake"))
    val dummyQueryExecution = new QueryExecution(spark, LocalRelation()) {
      override lazy val sparkPlan = physicalPlan
      override lazy val executedPlan = physicalPlan
    }

    val execId = runAndWaitForExecution {
      SQLExecution.withNewExecutionId(dummyQueryExecution) {
        physicalPlan.execute().collect()
      }
    }

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

  test("SPARK-39635: Report driver metrics from Datasource v2 scan") {
    val statusStore = spark.sharedState.statusStore

    val schema = new StructType().add("i", "int").add("j", "int")
    val physicalPlan = BatchScanExec(toAttributes(schema), new CustomDriverMetricScanBuilder(),
      Seq.empty, table = new TestLocalScanTable("fake"))
    val dummyQueryExecution = new QueryExecution(spark, LocalRelation()) {
      override lazy val sparkPlan = physicalPlan
      override lazy val executedPlan = physicalPlan
    }

    val execId = runAndWaitForExecution {
      SQLExecution.withNewExecutionId(dummyQueryExecution) {
        physicalPlan.execute().collect()
      }
    }

    val metrics = statusStore.executionMetrics(execId)
    val expectedMetric = physicalPlan.metrics("custom_driver_metric_partition_count")
    val expectedValue = "2"
    assert(metrics.contains(expectedMetric.id))
    assert(metrics(expectedMetric.id) === expectedValue)
  }

  test("SPARK-36030: Report metrics from Datasource v2 write") {
    withTempDir { dir =>
      val statusStore = spark.sharedState.statusStore

      val cls = classOf[CustomMetricsDataSource].getName
      val execId = runAndWaitForExecution {
        spark.range(10).select($"id" as Symbol("i"), -$"id" as Symbol("j"))
          .write.format(cls)
          .option("path", dir.getCanonicalPath).mode("append").save()
      }

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
        spark.range(0, 10, 1, 2).select($"id" as Symbol("i"), -$"id" as Symbol("j"))
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

  test("SPARK-40834: Use SparkListenerSQLExecutionEnd to track final SQL status in UI") {
    var received = false
    val e = new Exception("test")
    spark.sparkContext.addSparkListener(new SparkListener {
      override def onOtherEvent(event: SparkListenerEvent): Unit = {
        event match {
          case SparkListenerSQLExecutionEnd(_, _, Some(errorMessage), _) =>
            received = errorMessage == Utils.exceptionString(e)
          case _ =>
        }
      }
    })

    intercept[Exception] {
      SQLExecution.withNewExecutionId(spark.range(1).queryExecution) {
        throw e
      }
    }
    spark.sparkContext.listenerBus.waitUntilEmpty(10000)
    assert(received)
  }

  test("SPARK-42100: onJobStart handle event with unregistered executionId shouldn't throw NPE") {
    val statusStore = createStatusStore()
    val listener = statusStore.listener.get

    val executionId = 5
    // Using protobuf serialization will throw npe before SPARK-42100
    listener.onJobStart(SparkListenerJobStart(
      jobId = 0,
      time = System.currentTimeMillis(),
      stageInfos = Nil,
      createProperties(executionId)))

    assertJobs(statusStore.execution(executionId), running = Seq(0))
  }
}

class SQLAppStatusListenerWithInMemoryStoreSuite extends SQLAppStatusListenerSuite {
  override protected def createStatusStore(): SQLAppStatusStore = {
    val conf = sparkContext.conf
    kvstore = new ElementTrackingStore(new InMemoryStore, conf)
    val listener = new SQLAppStatusListener(conf, kvstore, live = true)
    new SQLAppStatusStore(kvstore, Some(listener))
  }

  test("SPARK-48486: History Server replays legacy and shared SQL plans in one log") {
    val prefix = "spark.sql.dpp.planInfo."
    val metric = new SQLMetricInfo("rows", 7L, "sum")
    val source = new SparkPlanInfo("Source", "source output=[k]", Nil, Map.empty, Seq(metric))
    val legacyProducer = new SparkPlanInfo("Subquery", "producer", Seq(source), Map.empty, Nil)
    val producer = new SparkPlanInfo("Subquery", "producer", Seq(source),
      Map(prefix + "version" -> "1", prefix + "id" -> "p"), Nil)
    val ref = new SparkPlanInfo("DPPSharedPlanReference", "reference p", Nil,
      Map(prefix + "version" -> "1", prefix + "ref" -> "p"), Nil)
    def root(first: SparkPlanInfo, second: SparkPlanInfo): SparkPlanInfo = {
      val consumers = Seq(first, second).zipWithIndex.map { case (child, index) =>
        new SparkPlanInfo(s"Consumer$index", s"consumer $index", Seq(child), Map.empty, Nil)
      }
      new SparkPlanInfo("Root", "root", consumers, Map.empty, Nil)
    }
    val legacy = root(legacyProducer, legacyProducer)
    val shared = root(producer, ref)
    val events = (0L to 2L).flatMap { id =>
      val start = SparkListenerSQLExecutionStart(id, Some(id), "history", "", "initial",
        if (id == 1L) shared else legacy, 0L)
      val update = if (id == 2L) {
        Seq(SparkListenerSQLAdaptiveExecutionUpdate(id, "final", shared))
      } else {
        Nil
      }
      Seq(start) ++ update ++ Seq(
        SparkListenerDriverAccumUpdates(id, Seq(7L -> 42L)),
        SparkListenerSQLExecutionEnd(id, 1L))
    }
    val conf = sparkContext.conf
    kvstore = new ElementTrackingStore(new InMemoryStore, conf)
    val bus = new ReplayListenerBus
    new SQLHistoryServerPlugin().createListeners(conf, kvstore).foreach(bus.addListener)
    val jsonLines = events.iterator.map(JsonProtocol.sparkEventToJsonString)
    assert(bus.replay(jsonLines, "mixed SQL history", maybeTruncated = false, _ => true))
    val store = new SQLAppStatusStore(kvstore)
    for (id <- 0L to 2L) {
      val graph = store.planGraph(id)
      assert(graph.allNodes.size == 5)
      assert(graph.edges.size == 5)
      assert(graph.allNodes.flatMap(_.metrics).map(_.accumulatorId) == Seq(7L))
      assert(store.executionMetrics(id) == Map(7L -> "42"))
      assert(store.execution(id).get.completionTime.isDefined)
      assert(store.execution(id).get.physicalPlanDescription ==
        (if (id == 2L) "final" else "initial"))
      assertReplayGraphEndpoints(graph)
    }
  }
}

class SQLAppStatusListenerWithRocksDBBackendSuite extends SQLAppStatusListenerSuite {
  private val storePath = Utils.createTempDir()

  override protected def createStatusStore(): SQLAppStatusStore = {
    val conf = sparkContext.conf
    conf.set(LIVE_UI_LOCAL_STORE_DIR, storePath.getCanonicalPath)
    val appStatusStore = AppStatusStore.createLiveStore(conf)
    kvstore = appStatusStore.store.asInstanceOf[ElementTrackingStore]
    val listener = new SQLAppStatusListener(conf, kvstore, live = true)
    new SQLAppStatusStore(kvstore, Some(listener))
  }

  protected override def afterAll(): Unit = {
    if (storePath.exists()) {
      Utils.deleteRecursively(storePath)
    }
    super.afterAll()
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
        // No live data should be left behind after all executions end. A SQL execution's live
        // entries are removed only once its end-event count reaches jobs.size + 1 (the
        // SparkListenerJobEnd(s) plus SparkListenerSQLExecutionEnd). For a failed job the
        // DAGScheduler notifies the job waiter -- unblocking the failing action on this thread --
        // *before* it posts SparkListenerJobEnd to the listener bus (see
        // DAGScheduler.failJobAndIndependentStages). That trailing JobEnd can therefore still be in
        // flight when this thread calls waitUntilEmpty() above; if it is enqueued just after the
        // bus is drained, the failed execution never reaches the cleanup threshold and lingers in
        // liveExecutions. Poll with eventually() so the trailing end event is delivered and the
        // live entries drain, rather than asserting once immediately.
        eventually(timeout(5.seconds), interval(10.milliseconds)) {
          assert(statusStore.listener.get.noLiveData())
        }
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

class SimpleCustomDriverMetric extends CustomMetric {
  override def name(): String = "custom_driver_metric_partition_count"
  override def description(): String = "Simple custom driver metrics - partition count"
  override def aggregateTaskMetrics(taskMetrics: Array[Long]): String = {
    taskMetrics.sum.toString
  }
}

class SimpleCustomDriverTaskMetric(value : Long) extends CustomTaskMetric {
  override def name(): String = "custom_driver_metric_partition_count"
  override def value(): Long = value
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

class CustomDriverMetricScanBuilder extends SimpleScanBuilder {

  var partitionCount: Long = 0L

  override def planInputPartitions(): Array[InputPartition] = {
    val partitions: Array[InputPartition] = Array(RangeInputPartition(0, 5),
      RangeInputPartition(5, 10))
    partitionCount = partitions.length
    partitions
  }

  override def createReaderFactory(): PartitionReaderFactory = CustomMetricReaderFactory

  override def supportedCustomMetrics(): Array[CustomMetric] = {
    Array(new SimpleCustomDriverMetric)
  }

  override def reportDriverMetrics(): Array[CustomTaskMetric] = {
    Array(new SimpleCustomDriverTaskMetric(partitionCount))
  }
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
