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

import scala.collection.mutable.ListBuffer

import org.json4s.jackson.JsonMethods._

import org.apache.spark._
import org.apache.spark.LocalSparkContext._
import org.apache.spark.internal.config
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.catalyst.util.quietly
import org.apache.spark.sql.execution.{LeafExecNode, QueryExecution, SparkPlanInfo, SQLExecution}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.status.config._
import org.apache.spark.util.{AccumulatorMetadata, JsonProtocol, LongAccumulator}
import org.apache.spark.util.kvstore.InMemoryStore

class SQLListenerSuite extends SparkFunSuite with SharedSQLContext with JsonTestUtils {
  import testImplicits._

  override protected def sparkConf = super.sparkConf.set(LIVE_ENTITY_UPDATE_PERIOD, 0L)

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

  private def createStageInfo(stageId: Int, attemptId: Int): StageInfo = new StageInfo(
    stageId = stageId,
    attemptId = attemptId,
    // The following fields are not used in tests
    name = "",
    numTasks = 0,
    rddInfos = Nil,
    parentIds = Nil,
    details = ""
  )

  private def createTaskInfo(
      taskId: Int,
      attemptNumber: Int,
      accums: Map[Long, Long] = Map()): TaskInfo = {
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

  /** Return the shared SQL store from the active SparkSession. */
  private def statusStore: SQLAppStatusStore =
    new SQLAppStatusStore(spark.sparkContext.statusStore.store)

  /**
   * Runs a test with a temporary SQLAppStatusStore tied to a listener bus. Events can be sent to
   * the listener bus to update the store, and all data will be cleaned up at the end of the test.
   */
  private def sqlStoreTest(name: String)
      (fn: (SQLAppStatusStore, SparkListenerBus) => Unit): Unit = {
    test(name) {
      val store = new InMemoryStore()
      val bus = new ReplayListenerBus()
      val listener = new SQLAppStatusListener(sparkConf, store, true)
      bus.addListener(listener)
      val sqlStore = new SQLAppStatusStore(store, Some(listener))
      fn(sqlStore, bus)
    }
  }

  sqlStoreTest("basic") { (store, bus) =>
    def checkAnswer(actual: Map[Long, String], expected: Map[Long, Long]): Unit = {
      assert(actual.size == expected.size)
      expected.foreach { case (id, value) =>
        // The values in actual can be SQL metrics meaning that they contain additional formatting
        // when converted to string. Verify that they start with the expected value.
        // TODO: this is brittle. There is no requirement that the actual string needs to start
        // with the accumulator value.
        assert(actual.contains(id))
        val v = actual.get(id).get.trim
        assert(v.startsWith(value.toString), s"Wrong value for accumulator $id")
      }
    }

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

    bus.postToAll(SparkListenerSQLExecutionStart(
      executionId,
      "test",
      "test",
      df.queryExecution.toString,
      SparkPlanInfo.fromSparkPlan(df.queryExecution.executedPlan),
      System.currentTimeMillis()))

    bus.postToAll(SparkListenerJobStart(
      jobId = 0,
      time = System.currentTimeMillis(),
      stageInfos = Seq(
        createStageInfo(0, 0),
        createStageInfo(1, 0)
      ),
      createProperties(executionId)))
    bus.postToAll(SparkListenerStageSubmitted(createStageInfo(0, 0)))

    assert(store.executionMetrics(0).isEmpty)

    bus.postToAll(SparkListenerExecutorMetricsUpdate("", Seq(
      // (task id, stage id, stage attempt, accum updates)
      (0L, 0, 0, createAccumulatorInfos(accumulatorUpdates)),
      (1L, 0, 0, createAccumulatorInfos(accumulatorUpdates))
    )))

    checkAnswer(store.executionMetrics(0), accumulatorUpdates.mapValues(_ * 2))

    // Driver accumulator updates don't belong to this execution should be filtered and no
    // exception will be thrown.
    bus.postToAll(SparkListenerDriverAccumUpdates(0, Seq((999L, 2L))))

    checkAnswer(store.executionMetrics(0), accumulatorUpdates.mapValues(_ * 2))

    bus.postToAll(SparkListenerExecutorMetricsUpdate("", Seq(
      // (task id, stage id, stage attempt, accum updates)
      (0L, 0, 0, createAccumulatorInfos(accumulatorUpdates)),
      (1L, 0, 0, createAccumulatorInfos(accumulatorUpdates.mapValues(_ * 2)))
    )))

    checkAnswer(store.executionMetrics(0), accumulatorUpdates.mapValues(_ * 3))

    // Retrying a stage should reset the metrics
    bus.postToAll(SparkListenerStageSubmitted(createStageInfo(0, 1)))

    bus.postToAll(SparkListenerExecutorMetricsUpdate("", Seq(
      // (task id, stage id, stage attempt, accum updates)
      (0L, 0, 1, createAccumulatorInfos(accumulatorUpdates)),
      (1L, 0, 1, createAccumulatorInfos(accumulatorUpdates))
    )))

    checkAnswer(store.executionMetrics(0), accumulatorUpdates.mapValues(_ * 2))

    // Ignore the task end for the first attempt
    bus.postToAll(SparkListenerTaskEnd(
      stageId = 0,
      stageAttemptId = 0,
      taskType = "",
      reason = null,
      createTaskInfo(0, 0, accums = accumulatorUpdates.mapValues(_ * 100)),
      null))

    checkAnswer(store.executionMetrics(0), accumulatorUpdates.mapValues(_ * 2))

    // Finish two tasks
    bus.postToAll(SparkListenerTaskEnd(
      stageId = 0,
      stageAttemptId = 1,
      taskType = "",
      reason = null,
      createTaskInfo(0, 0, accums = accumulatorUpdates.mapValues(_ * 2)),
      null))
    bus.postToAll(SparkListenerTaskEnd(
      stageId = 0,
      stageAttemptId = 1,
      taskType = "",
      reason = null,
      createTaskInfo(1, 0, accums = accumulatorUpdates.mapValues(_ * 3)),
      null))

    checkAnswer(store.executionMetrics(0), accumulatorUpdates.mapValues(_ * 5))

    // Summit a new stage
    bus.postToAll(SparkListenerStageSubmitted(createStageInfo(1, 0)))

    bus.postToAll(SparkListenerExecutorMetricsUpdate("", Seq(
      // (task id, stage id, stage attempt, accum updates)
      (0L, 1, 0, createAccumulatorInfos(accumulatorUpdates)),
      (1L, 1, 0, createAccumulatorInfos(accumulatorUpdates))
    )))

    checkAnswer(store.executionMetrics(0), accumulatorUpdates.mapValues(_ * 7))

    // Finish two tasks
    bus.postToAll(SparkListenerTaskEnd(
      stageId = 1,
      stageAttemptId = 0,
      taskType = "",
      reason = null,
      createTaskInfo(0, 0, accums = accumulatorUpdates.mapValues(_ * 3)),
      null))
    bus.postToAll(SparkListenerTaskEnd(
      stageId = 1,
      stageAttemptId = 0,
      taskType = "",
      reason = null,
      createTaskInfo(1, 0, accums = accumulatorUpdates.mapValues(_ * 3)),
      null))

    checkAnswer(store.executionMetrics(0), accumulatorUpdates.mapValues(_ * 11))

    assertJobs(store.execution(0), running = Seq(0))

    bus.postToAll(SparkListenerJobEnd(
      jobId = 0,
      time = System.currentTimeMillis(),
      JobSucceeded
    ))
    bus.postToAll(SparkListenerSQLExecutionEnd(
      executionId, System.currentTimeMillis()))

    assertJobs(store.execution(0), completed = Seq(0))
    checkAnswer(store.executionMetrics(0), accumulatorUpdates.mapValues(_ * 11))
  }

  sqlStoreTest("onExecutionEnd happens before onJobEnd(JobSucceeded)") { (store, bus) =>
    val executionId = 0
    val df = createTestDataFrame
    bus.postToAll(SparkListenerSQLExecutionStart(
      executionId,
      "test",
      "test",
      df.queryExecution.toString,
      SparkPlanInfo.fromSparkPlan(df.queryExecution.executedPlan),
      System.currentTimeMillis()))
    bus.postToAll(SparkListenerJobStart(
      jobId = 0,
      time = System.currentTimeMillis(),
      stageInfos = Nil,
      createProperties(executionId)))
    bus.postToAll(SparkListenerSQLExecutionEnd(
      executionId, System.currentTimeMillis()))
    bus.postToAll(SparkListenerJobEnd(
      jobId = 0,
      time = System.currentTimeMillis(),
      JobSucceeded
    ))

    assertJobs(store.execution(0), completed = Seq(0))
  }

  sqlStoreTest("onExecutionEnd happens before multiple onJobEnd(JobSucceeded)s") { (store, bus) =>
    val executionId = 0
    val df = createTestDataFrame
    bus.postToAll(SparkListenerSQLExecutionStart(
      executionId,
      "test",
      "test",
      df.queryExecution.toString,
      SparkPlanInfo.fromSparkPlan(df.queryExecution.executedPlan),
      System.currentTimeMillis()))
    bus.postToAll(SparkListenerJobStart(
      jobId = 0,
      time = System.currentTimeMillis(),
      stageInfos = Nil,
      createProperties(executionId)))
    bus.postToAll(SparkListenerJobEnd(
        jobId = 0,
        time = System.currentTimeMillis(),
        JobSucceeded
    ))

    bus.postToAll(SparkListenerJobStart(
      jobId = 1,
      time = System.currentTimeMillis(),
      stageInfos = Nil,
      createProperties(executionId)))
    bus.postToAll(SparkListenerSQLExecutionEnd(
      executionId, System.currentTimeMillis()))
    bus.postToAll(SparkListenerJobEnd(
      jobId = 1,
      time = System.currentTimeMillis(),
      JobSucceeded
    ))

    assertJobs(store.execution(0), completed = Seq(0, 1))
  }

  sqlStoreTest("onExecutionEnd happens before onJobEnd(JobFailed)") { (store, bus) =>
    val executionId = 0
    val df = createTestDataFrame
    bus.postToAll(SparkListenerSQLExecutionStart(
      executionId,
      "test",
      "test",
      df.queryExecution.toString,
      SparkPlanInfo.fromSparkPlan(df.queryExecution.executedPlan),
      System.currentTimeMillis()))
    bus.postToAll(SparkListenerJobStart(
      jobId = 0,
      time = System.currentTimeMillis(),
      stageInfos = Seq.empty,
      createProperties(executionId)))
    bus.postToAll(SparkListenerSQLExecutionEnd(
      executionId, System.currentTimeMillis()))
    bus.postToAll(SparkListenerJobEnd(
      jobId = 0,
      time = System.currentTimeMillis(),
      JobFailed(new RuntimeException("Oops"))
    ))

    assertJobs(store.execution(0), failed = Seq(0))
  }

  test("SPARK-11126: no memory leak when running non SQL jobs") {
    val previousStageNumber = statusStore.executionsList().size
    spark.sparkContext.parallelize(1 to 10).foreach(i => ())
    spark.sparkContext.listenerBus.waitUntilEmpty(10000)
    // listener should ignore the non SQL stage
    assert(statusStore.executionsList().size == previousStageNumber)

    spark.sparkContext.parallelize(1 to 10).toDF().foreach(i => ())
    spark.sparkContext.listenerBus.waitUntilEmpty(10000)
    // listener should save the SQL stage
    assert(statusStore.executionsList().size == previousStageNumber + 1)
  }

  test("driver side SQL metrics") {
    val oldCount = statusStore.executionsList().size
    val expectedAccumValue = 12345L
    val physicalPlan = MyPlan(sqlContext.sparkContext, expectedAccumValue)
    val dummyQueryExecution = new QueryExecution(spark, LocalRelation()) {
      override lazy val sparkPlan = physicalPlan
      override lazy val executedPlan = physicalPlan
    }

    SQLExecution.withNewExecutionId(spark, dummyQueryExecution) {
      physicalPlan.execute().collect()
    }

    while (statusStore.executionsList().size < oldCount) {
      Thread.sleep(100)
    }

    // Wait for listener to finish computing the metrics for the execution.
    while (statusStore.executionsList().last.metricValues == null) {
      Thread.sleep(100)
    }

    val execId = statusStore.executionsList().last.executionId
    val metrics = statusStore.executionMetrics(execId)
    val driverMetric = physicalPlan.metrics("dummy")
    val expectedValue = SQLMetrics.stringValue(driverMetric.metricType, Seq(expectedAccumValue))

    assert(metrics.contains(driverMetric.id))
    assert(metrics(driverMetric.id) === expectedValue)
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

    assert(actualRunning.toSeq.sorted === running)
    assert(actualCompleted.toSeq.sorted === completed)
    assert(actualFailed.toSeq.sorted === failed)
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

}


/**
 * A dummy [[org.apache.spark.sql.execution.SparkPlan]] that updates a [[SQLMetrics]]
 * on the driver.
 */
private case class MyPlan(sc: SparkContext, expectedValue: Long) extends LeafExecNode {
  override def sparkContext: SparkContext = sc
  override def output: Seq[Attribute] = Seq()

  override val metrics: Map[String, SQLMetric] = Map(
    "dummy" -> SQLMetrics.createMetric(sc, "dummy"))

  override def doExecute(): RDD[InternalRow] = {
    longMetric("dummy") += expectedValue

    SQLMetrics.postDriverMetricUpdates(
      sc,
      sc.getLocalProperty(SQLExecution.EXECUTION_ID_KEY),
      metrics.values.toSeq)
    sc.emptyRDD
  }
}


class SQLListenerMemoryLeakSuite extends SparkFunSuite {

  // TODO: this feature is not yet available in SQLAppStatusStore.
  ignore("no memory leak") {
    quietly {
      val conf = new SparkConf()
        .setMaster("local")
        .setAppName("test")
        .set(config.MAX_TASK_FAILURES, 1) // Don't retry the tasks to run this test quickly
        .set("spark.sql.ui.retainedExecutions", "50") // Set it to 50 to run this test quickly
      withSpark(new SparkContext(conf)) { sc =>
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
        sc.listenerBus.waitUntilEmpty(10000)

        val statusStore = new SQLAppStatusStore(sc.statusStore.store)
        assert(statusStore.executionsList().size <= 50)
      }
    }
  }
}
