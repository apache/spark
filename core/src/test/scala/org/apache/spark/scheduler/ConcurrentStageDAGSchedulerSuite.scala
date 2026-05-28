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

package org.apache.spark.scheduler

import java.util.Properties

import org.apache.spark.HashPartitioner
import org.apache.spark.ShuffleDependency
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.internal.config.SPECULATION_ENABLED
import org.apache.spark.internal.config.STREAMING_REALTIME_MODE_SLOTS_CHECK_DISABLED

class ConcurrentStageDAGSchedulerSuite extends DAGSchedulerSuiteBase {

  // The unit-test SparkContext runs in local[2] mode, but the concurrent pipelines exercised
  // here often need more slots than that. Disable the slot check so the tests aren't gated by
  // executor capacity.
  override def conf: SparkConf =
    super.conf.set(STREAMING_REALTIME_MODE_SLOTS_CHECK_DISABLED, true)

  class TestConcurrentStageDAGScheduler(sc: SparkContext)
    extends ConcurrentStageDAGScheduler(
      sc,
      taskScheduler,
      sc.listenerBus,
      mapOutputTracker,
      blockManagerMaster,
      sc.env)
    with TestDAGScheduler

  override def createInitialScheduler(sc: SparkContext): DAGScheduler = {
    new TestConcurrentStageDAGScheduler(sc)
  }

  /**
   * Asserts that the concurrent scheduler's internal state — `concurrentStages` and
   * `dependentStageMap` — is empty. Called from `assertDataStructuresEmpty` and at the end of
   * every test via `afterEach`, so every inherited test (and every locally-defined test) gets
   * free regression coverage against entries leaking into these maps.
   */
  override protected def extraEmptyChecks(): Unit = {
    // Inherited tests sometimes replace `scheduler` with a plain MyDAGScheduler (bypassing
    // createInitialScheduler), so pattern-match rather than cast.
    scheduler match {
      case s: TestConcurrentStageDAGScheduler =>
        assert(s.concurrentStages.isEmpty,
          s"concurrentStages should be empty but contains: ${s.concurrentStages}")
        assert(s.dependentStageMap.isEmpty,
          s"dependentStageMap should be empty but contains: ${s.dependentStageMap}")
      case _ => // Not a concurrent scheduler — nothing extra to assert.
    }
  }

  override def afterEach(): Unit = {
    try {
      extraEmptyChecks()
    } finally {
      super.afterEach()
    }
  }

  // Catch the job failure exception with a listener.
  private class TestJobListener extends JobListener {
    private var failureException: Option[Exception] = None

    override def jobFailed(exception: Exception): Unit = {
      failureException = Some(exception)
    }

    override def taskSucceeded(index: Int, result: Any): Unit = { }

    def expectFailure(): Exception = {
      assert(failureException.nonEmpty, "Job was expected to fail with an exception, but didn't")
      failureException.get
    }
  }


  /** Default job properties with query settings and concurrent stages enabled. */
  private val testProperties: Properties = {
    val properties = new Properties()
    properties.setProperty("sql.streaming.queryId", "test_query_id")
    properties.setProperty("streaming.sql.batchId", "5")
    properties.setProperty(ConcurrentStageDAGScheduler.CONCURRENT_STAGES_ENABLED_PROPERTY, "true")
    new Properties(properties) {
      // Make it read-only.
      override def setProperty(key: String, value: String): AnyRef = {
        throw new UnsupportedOperationException("Default properties are read-only.")
      }
    }
  }

  test("Simple job with two concurrent stages") {
    // Run a simple job with two stages. Both stages should be running concurrently.

    val mapStage = new MyRDD(sc, 1, Nil) // stage_0
    val shuffleDep = new ShuffleDependency(mapStage, new HashPartitioner(1))
    val resultStage = new MyRDD(sc, 3, List(shuffleDep)) // stage_1

    // Shape: [stage_0, map stage, parent] <--- [stage_1, result stage]

    submit(resultStage, Array(0), properties = testProperties)

    assert(scheduler.waitingStages.isEmpty) // Both are submitted.
    assert(scheduler.runningStages.map(_.id) === Set(0, 1)) // Both stages are running.

    // Verify concurrent scheduler specific state.
    val concurrentScheduler = scheduler.asInstanceOf[TestConcurrentStageDAGScheduler]

    assert(concurrentScheduler.concurrentStages.isEmpty) // All are already scheduled

    val depStageMap = concurrentScheduler.dependentStageMap
    assert(depStageMap.keys.map(_.id) == Set(1)) // Result stage is the key.
    assert(depStageMap.values.flatMap(_.parents.map(_.id)) == Seq(0)) // Map stage is the parent.
    assert(depStageMap.values.flatMap(_.delayedTaskCompletionEvents).isEmpty) // No completed tasks.

    // First complete the result stage. Its tasks will complete, but the actual stage would still
    // be running since its parent (map stage) hasn't completed yet.

    completeNextResultStageWithSuccess(1, 0)
    assert(scheduler.runningStages.map(_.id) === Set(0, 1)) // Both stages are still running.
    // dependentStageMap should have the completed task from result stage enqueued.
    assert(depStageMap.values.flatMap(_.delayedTaskCompletionEvents).size === 1)

    // Now complete the map stage. This should complete the result stage as well.
    completeShuffleMapStageSuccessfully(0, 0, 1)

    assert(scheduler.runningStages.map(_.id) === Set()) // Both stages are complete.
    assert(depStageMap.isEmpty) // No more dependent stages.

    assertDataStructuresEmpty()
  }

  test("Default scheduler using a simple job with concurrent stages disabled") {
    // This is opposite of the previous test. Concurrent stages are disabled, so the stages should
    // be submitted one after the other.

    val mapStage = new MyRDD(sc, 1, Nil) // stage_0
    val shuffleDep = new ShuffleDependency(mapStage, new HashPartitioner(1))
    val resultStage = new MyRDD(sc, 3, List(shuffleDep)) // stage_1

    // Shape: [stage_0, map stage, parent] <--- [stage_1, result stage]

    submit(resultStage, Array(0), properties = new Properties())

    assert(scheduler.runningStages.map(_.id) == Set(0)) // Only the map stage is running.
    assert(scheduler.waitingStages.map(_.id) == Set(1)) // Result stage is waiting.

    val concurrentScheduler = scheduler.asInstanceOf[TestConcurrentStageDAGScheduler]
    assert(concurrentScheduler.concurrentStages.isEmpty) // No concurrent stages.
    assert(concurrentScheduler.dependentStageMap.isEmpty) // No dependent stages.

    // Complete the map stage. This should submit the result stage.
    completeShuffleMapStageSuccessfully(0, 0, 1)

    assert(scheduler.runningStages.map(_.id) == Set(1)) // Only the result stage is running.
    assert(scheduler.waitingStages.map(_.id) == Set()) // No waiting stages

    completeNextResultStageWithSuccess(1, 0)
    assertDataStructuresEmpty()
  }

  test("Complex pipeline with many stages") {
    // Run a complex pipeline with multiple stages with multiple branches. Such a pipeline not
    // common, but useful to ensure scheduler works as expected.

    // Shape:
    //                  /<-------------------- stage_D
    // stage_A <--- stage_B <--- stage_C <---\   ^
    //        \         \<---------/          \  |
    //         \ <----------------/            \ |
    // stage_E <---------------------------  stage_F

    // All of these should be running concurrently.

    val rddA = new MyRDD(sc, 2, Nil).setName("rddA")
    val shuffleDepA = new ShuffleDependency(rddA, new HashPartitioner(1))

    val rddB = new MyRDD(sc, 1, List(shuffleDepA)).setName("rddB")
    val shuffleDepB = new ShuffleDependency(rddB, new HashPartitioner(1))

    val rddC = new MyRDD(sc, 1, List(shuffleDepA, shuffleDepB)).setName("rddC")
    val shuffleDepC = new ShuffleDependency(rddC, new HashPartitioner(5))

    val rddD = new MyRDD(sc, 4, List(shuffleDepB)).setName("rddD")
    val shuffleDepD = new ShuffleDependency(rddD, new HashPartitioner(5))

    val rddE = new MyRDD(sc, 3, Nil).setName("rddE")
    val shuffleDepE = new ShuffleDependency(rddE, new HashPartitioner(5))

    val rddF = new MyRDD(sc, 5, List(shuffleDepC, shuffleDepD, shuffleDepE)).setName("rddF")

    submit(rddF, Array(0, 1, 2, 3, 4), properties = testProperties)

    assert(scheduler.waitingStages.isEmpty) // All the stages are submitted.
    assert(scheduler.runningStages.map(_.id) == Set(0, 1, 2, 3, 4, 5)) // All 6 are running.

    // Assign stage ids corresponding to the RDDs A, B, etc
    def stageFor(rddName: String): Stage = scheduler.runningStages.find(_.rdd.name == rddName).get

    val sA = stageFor("rddA")
    val sB = stageFor("rddB")
    val sC = stageFor("rddC")
    val sD = stageFor("rddD")
    val sE = stageFor("rddE")
    val sF = stageFor("rddF")

    // log stage id mapping for debugging:
    for (name <- List("A", "B", "C", "D", "E", "F")) {
      logInfo(s"Stage id for stage $name is ${stageFor("rdd" + name).id}")
    }

    // Verify concurrent scheduler specific state.
    val concurrentScheduler = scheduler.asInstanceOf[TestConcurrentStageDAGScheduler]

    assert(concurrentScheduler.concurrentStages.isEmpty) // All are already scheduled

    val depStageMap = concurrentScheduler.dependentStageMap
    assert(depStageMap.keys === Set(sB, sC, sD, sF)) // All non-root stages are keys.
    assert(depStageMap.values.flatMap(_.parents).toSet === Set(
      sA, sB, sC, sD, sE)) // All except the results stage.
    assert(depStageMap.values.flatMap(_.delayedTaskCompletionEvents).isEmpty) // No completed tasks.

    // Complete stages in order-of-order and verify the state.

    // First complete C. Entry for C would be updated with the completed task.
    assert(depStageMap(sC).delayedTaskCompletionEvents.size === 0)
    completeShuffleMapStageSuccessfully(sC.id, 0, 5) // Complete stage C.
    assert(depStageMap(sC).delayedTaskCompletionEvents.size === 1)

    // All the 6 stages are still 'running' since C's completion events are delayed.
    assert(scheduler.runningStages.map(_.id) == Set(0, 1, 2, 3, 4, 5))

    // Now complete stage D. This is similar to completing C. The tasks are enqueued.
    assert(depStageMap(sD).delayedTaskCompletionEvents.size === 0)
    completeShuffleMapStageSuccessfully(sD.id, 0, 5) // Complete stage D
    assert(depStageMap(sD).delayedTaskCompletionEvents.size === 4) // 4 tasks in stage D.

    // Complete stage E. This is a root node and should complete normally.
    assert(depStageMap(sF).parents.contains(sE)) // E is one of the stages that F waits for.
    completeShuffleMapStageSuccessfully(sE.id, 0, 3)
    assert(depStageMap(sF).parents.contains(sE) === false) // E is removed from F's parents.

    // One less running stage.
    assert(scheduler.runningStages === Set(sA, sB, sC, sD, sF)) // E is complete.

    // Complete stage A. This is a root node and should complete normally.
    assert(depStageMap(sC).parents.contains(sA)) // A is one of the stages that C waits for.
    assert(depStageMap(sB).parents.contains(sA)) // Same for B
    completeShuffleMapStageSuccessfully(sA.id, 0, 2)
    assert(scheduler.runningStages === Set(sB, sC, sD, sF)) // A is complete.
    assert(depStageMap(sC).parents.contains(sA) === false) // A is removed from C's parents.
    // In the case B, it is bit more. Its only parent A is complete. So there is no need to
    // track it in depStageMap. So it is removed from `dependentStageMap` entirely.
    assert(depStageMap.contains(sB) === false) // B is removed from the depStageMap.

    // Complete result stage F. This will be enqueued, will complete later.
    completeNextResultStageWithSuccess(sF.id, 0)
    assert(scheduler.runningStages === Set(sB, sC, sD, sF)) // F is still running.
    assert(depStageMap(sF).delayedTaskCompletionEvents.size === 5)

    // Complete stage B, it will complete normally.
    completeShuffleMapStageSuccessfully(sB.id, 0, 1)
    // This will trigger completion of C as well since both its parents A & B are done.
    // This will also trigger completion of D as its parent B is done.
    // Which finally completes F as well.
    assert(scheduler.runningStages === Set()) // All stages are complete.
    assert(depStageMap.isEmpty) // No more dependent stages.

    assertDataStructuresEmpty()
  }

  test("dependentStageMap entry is cleaned up when a dependent stage aborts and its " +
       "parent stage is shared with another job") {
    // This exercises the cleanup path in markStageAsFinished. When the dependent stage
    // (here, B) aborts before its parent (A) finishes, the cascade through
    // failJobAndIndependentStages only marks stages that are *independent* to the failing job
    // as finished — shared stages are left alone. Without the explicit
    // `dependentStageMap.remove(stage)` at the end of markStageAsFinished, B's entry would
    // leak in dependentStageMap until A eventually finished for the other job.

    // Job 1 (regular batch): rddC depends on rddA via shuffleDepA.
    val rddA = new MyRDD(sc, 1, Nil).setName("rddA")
    val shuffleDepA = new ShuffleDependency(rddA, new HashPartitioner(1))
    val rddC = new MyRDD(sc, 1, List(shuffleDepA)).setName("rddC")
    submit(rddC, Array(0))  // properties = null implies non-concurrent.

    // After job 1: rddA's stage is running, rddC's stage is waiting.
    assert(scheduler.runningStages.exists(_.rdd.name == "rddA"),
      "rddA's stage should be running after job 1 submission")

    // Job 2 (concurrent): rddB also depends on the same shuffleDepA → rddA's stage is shared.
    val rddB = new MyRDD(sc, 3, List(shuffleDepA)).setName("rddB")
    submit(rddB, Array(0), properties = testProperties)

    val concurrentScheduler = scheduler.asInstanceOf[TestConcurrentStageDAGScheduler]
    val depStageMap = concurrentScheduler.dependentStageMap
    // B is in dependentStageMap with rddA's stage as a parent.
    assert(depStageMap.keys.map(_.rdd.name) === Set("rddB"))
    assert(depStageMap.values.flatMap(_.parents.map(_.rdd.name)).toSet === Set("rddA"))

    // Fail rddB's taskset. abortStage's failJobAndIndependentStages only marks rddB
    // finished — rddA is shared with job 1, so it is NOT cancelled.
    val taskSetB = taskSets.find(_.tasks.head.stageId ==
      scheduler.runningStages.find(_.rdd.name == "rddB").get.id).get
    failed(taskSetB, "test failure: rddB aborted before parent rddA finished")

    // rddA is still running for job 1.
    assert(scheduler.runningStages.exists(_.rdd.name == "rddA"),
      "Shared parent rddA's stage should still be running for job 1 after job 2 aborted")

    // rddB's entry must have been removed by markStageAsFinished's cleanup, even though
    // rddA is still running (and would normally be the stage whose markStageAsFinished
    // cleans up B's entry).
    assert(depStageMap.isEmpty,
      s"dependentStageMap should be empty after rddB aborted, but contains: $depStageMap")
  }

  test("concurrentStages is empty after slot-check failure") {
    // The DAG walk in onFinalStageCreated accumulates stages into a local set and only commits
    // them to `concurrentStages` after the slot check passes. This test verifies that a slot-
    // check failure leaves `concurrentStages` empty (rather than leaking the visited stages).
    sc.conf.set(STREAMING_REALTIME_MODE_SLOTS_CHECK_DISABLED, false)
    try {
      // local[2] gives us 2 slots; a 4-partition job exceeds that.
      val rdd = new MyRDD(sc, 4, Nil)

      val jobListener = new TestJobListener()
      submit(rdd, Array(0, 1, 2, 3), properties = testProperties, listener = jobListener)

      assert(jobListener.expectFailure().getMessage.contains(
        "CONCURRENT_SCHEDULER_INSUFFICIENT_SLOT"))

      val concurrentScheduler = scheduler.asInstanceOf[TestConcurrentStageDAGScheduler]
      assert(concurrentScheduler.concurrentStages.isEmpty,
        s"concurrentStages should be empty after slot-check failure, but contains: " +
          s"${concurrentScheduler.concurrentStages}")
    } finally {
      sc.conf.set(STREAMING_REALTIME_MODE_SLOTS_CHECK_DISABLED, true)
    }
  }

  test("concurrentStages and dependentStageMap are cleaned up after job cancellation") {
    val mapStage = new MyRDD(sc, 1, Nil) // stage_0
    val shuffleDep = new ShuffleDependency(mapStage, new HashPartitioner(1))
    val resultStage = new MyRDD(sc, 3, List(shuffleDep)) // stage_1

    val jobId = submit(resultStage, Array(0), properties = testProperties)

    val concurrentScheduler = scheduler.asInstanceOf[TestConcurrentStageDAGScheduler]
    // Both stages running concurrently and dependentStageMap is populated.
    assert(scheduler.runningStages.map(_.id) === Set(0, 1))
    assert(concurrentScheduler.dependentStageMap.nonEmpty)

    // Cancel the job mid-execution. handleJobCancellation marks all stages of the cancelled
    // job finished via markStageAsFinished, which runs our cleanup.
    cancel(jobId)

    assert(concurrentScheduler.concurrentStages.isEmpty,
      s"concurrentStages should be empty after cancellation, " +
        s"but contains: ${concurrentScheduler.concurrentStages}")
    assert(concurrentScheduler.dependentStageMap.isEmpty,
      s"dependentStageMap should be empty after cancellation, " +
        s"but contains: ${concurrentScheduler.dependentStageMap}")
  }

  test("concurrentStages and dependentStageMap are cleaned up after executor-loss " +
       "induced abort") {
    val mapStage = new MyRDD(sc, 1, Nil) // stage_0
    val shuffleDep = new ShuffleDependency(mapStage, new HashPartitioner(1))
    val resultStage = new MyRDD(sc, 3, List(shuffleDep)) // stage_1

    submit(resultStage, Array(0), properties = testProperties)

    val concurrentScheduler = scheduler.asInstanceOf[TestConcurrentStageDAGScheduler]
    assert(concurrentScheduler.dependentStageMap.nonEmpty)

    // In real-time mode TaskSchedulerImpl caps maxFailures at 1 and TaskSetManager counts
    // ExecutorLostFailure toward task failures, so a single executor loss aborts the
    // TaskSet immediately. Simulate the resulting TaskSetFailed event.
    failed(taskSets(1), "executor lost: simulated for test")

    assert(concurrentScheduler.concurrentStages.isEmpty,
      s"concurrentStages should be empty after executor-loss abort, " +
        s"but contains: ${concurrentScheduler.concurrentStages}")
    assert(concurrentScheduler.dependentStageMap.isEmpty,
      s"dependentStageMap should be empty after executor-loss abort, " +
        s"but contains: ${concurrentScheduler.dependentStageMap}")
  }

  test("Should fail if speculative execution is enabled (per-job property)") {
    // Try to a run job with two stages with speculative execution as a per-job local
    // property. It should fail the job with exception.

    val mapStage = new MyRDD(sc, 1, Nil) // stage_0
    val shuffleDep = new ShuffleDependency(mapStage, new HashPartitioner(1))
    val resultStage = new MyRDD(sc, 3, List(shuffleDep)) // stage_1

    val properties = new Properties(testProperties)
    properties.setProperty(SPECULATION_ENABLED.key, "true")

    val jobListener = new TestJobListener()
    submit(resultStage, Array(0), properties = properties, listener = jobListener)

    assert(jobListener.expectFailure().getMessage.contains(
      "Speculative execution is not supported with concurrent stages"))
  }

  test("Should fail if speculative execution is enabled (cluster-wide SparkConf)") {
    // Same as the previous test, but speculation is set on SparkConf (the documented way to
    // enable speculation) instead of the per-job local property. Every other consumer of
    // SPECULATION_ENABLED reads it via sc.conf, so this is the common case.

    sc.conf.set(SPECULATION_ENABLED, true)
    try {
      val mapStage = new MyRDD(sc, 1, Nil) // stage_0
      val shuffleDep = new ShuffleDependency(mapStage, new HashPartitioner(1))
      val resultStage = new MyRDD(sc, 3, List(shuffleDep)) // stage_1

      val jobListener = new TestJobListener()
      submit(resultStage, Array(0), properties = testProperties, listener = jobListener)

      assert(jobListener.expectFailure().getMessage.contains(
        "Speculative execution is not supported with concurrent stages"))
    } finally {
      sc.conf.set(SPECULATION_ENABLED, false)
    }
  }
}
