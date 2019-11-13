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

package org.apache.spark.status

import java.io.File
import java.lang.{Integer => JInteger, Long => JLong}
import java.util.{Arrays, Date, Properties}

import scala.collection.JavaConverters._
import scala.reflect.{classTag, ClassTag}

import org.scalatest.BeforeAndAfter

import org.apache.spark._
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.cluster._
import org.apache.spark.status.api.v1
import org.apache.spark.storage._
import org.apache.spark.util.Utils
import org.apache.spark.util.kvstore._

class AppStatusListenerSuite extends SparkFunSuite with BeforeAndAfter {

  import config._

  private val conf = new SparkConf()
    .set(LIVE_ENTITY_UPDATE_PERIOD, 0L)
    .set(ASYNC_TRACKING_ENABLED, false)

  private var time: Long = _
  private var testDir: File = _
  private var store: ElementTrackingStore = _
  private var taskIdTracker = -1L

  before {
    time = 0L
    testDir = Utils.createTempDir()
    store = new ElementTrackingStore(KVUtils.open(testDir, getClass().getName()), conf)
    taskIdTracker = -1L
  }

  after {
    store.close()
    Utils.deleteRecursively(testDir)
  }

  test("environment info") {
    val listener = new AppStatusListener(store, conf, true)

    val details = Map(
      "JVM Information" -> Seq(
        "Java Version" -> sys.props("java.version"),
        "Java Home" -> sys.props("java.home"),
        "Scala Version" -> scala.util.Properties.versionString
      ),
      "Spark Properties" -> Seq(
        "spark.conf.1" -> "1",
        "spark.conf.2" -> "2"
      ),
      "System Properties" -> Seq(
        "sys.prop.1" -> "1",
        "sys.prop.2" -> "2"
      ),
      "Classpath Entries" -> Seq(
        "/jar1" -> "System",
        "/jar2" -> "User"
      )
    )

    listener.onEnvironmentUpdate(SparkListenerEnvironmentUpdate(details))

    val appEnvKey = classOf[ApplicationEnvironmentInfoWrapper].getName()
    check[ApplicationEnvironmentInfoWrapper](appEnvKey) { env =>
      val info = env.info

      val runtimeInfo = Map(details("JVM Information"): _*)
      assert(info.runtime.javaVersion == runtimeInfo("Java Version"))
      assert(info.runtime.javaHome == runtimeInfo("Java Home"))
      assert(info.runtime.scalaVersion == runtimeInfo("Scala Version"))

      assert(info.sparkProperties === details("Spark Properties"))
      assert(info.systemProperties === details("System Properties"))
      assert(info.classpathEntries === details("Classpath Entries"))
    }
  }

  test("scheduler events") {
    val listener = new AppStatusListener(store, conf, true)

    listener.onOtherEvent(SparkListenerLogStart("TestSparkVersion"))

    // Start the application.
    time += 1
    listener.onApplicationStart(SparkListenerApplicationStart(
      "name",
      Some("id"),
      time,
      "user",
      Some("attempt"),
      None))

    check[ApplicationInfoWrapper]("id") { app =>
      assert(app.info.name === "name")
      assert(app.info.id === "id")
      assert(app.info.attempts.size === 1)

      val attempt = app.info.attempts.head
      assert(attempt.attemptId === Some("attempt"))
      assert(attempt.startTime === new Date(time))
      assert(attempt.lastUpdated === new Date(time))
      assert(attempt.endTime.getTime() === -1L)
      assert(attempt.sparkUser === "user")
      assert(!attempt.completed)
      assert(attempt.appSparkVersion === "TestSparkVersion")
    }

    // Start a couple of executors.
    time += 1
    val execIds = Array("1", "2")

    execIds.foreach { id =>
      listener.onExecutorAdded(SparkListenerExecutorAdded(time, id,
        new ExecutorInfo(s"$id.example.com", 1, Map())))
    }

    execIds.foreach { id =>
      check[ExecutorSummaryWrapper](id) { exec =>
        assert(exec.info.id === id)
        assert(exec.info.hostPort === s"$id.example.com")
        assert(exec.info.isActive)
      }
    }

    // Start a job with 2 stages / 4 tasks each
    time += 1
    val stages = Seq(
      new StageInfo(1, 0, "stage1", 4, Nil, Nil, "details1"),
      new StageInfo(2, 0, "stage2", 4, Nil, Seq(1), "details2"))

    val jobProps = new Properties()
    jobProps.setProperty(SparkContext.SPARK_JOB_DESCRIPTION, "jobDescription")
    jobProps.setProperty(SparkContext.SPARK_JOB_GROUP_ID, "jobGroup")
    jobProps.setProperty("spark.scheduler.pool", "schedPool")

    listener.onJobStart(SparkListenerJobStart(1, time, stages, jobProps))

    check[JobDataWrapper](1) { job =>
      assert(job.info.jobId === 1)
      assert(job.info.name === stages.last.name)
      assert(job.info.description === Some("jobDescription"))
      assert(job.info.status === JobExecutionStatus.RUNNING)
      assert(job.info.submissionTime === Some(new Date(time)))
      assert(job.info.jobGroup === Some("jobGroup"))
    }

    stages.foreach { info =>
      check[StageDataWrapper](key(info)) { stage =>
        assert(stage.info.status === v1.StageStatus.PENDING)
        assert(stage.jobIds === Set(1))
      }
    }

    // Submit stage 1
    time += 1
    stages.head.submissionTime = Some(time)
    listener.onStageSubmitted(SparkListenerStageSubmitted(stages.head, jobProps))

    check[JobDataWrapper](1) { job =>
      assert(job.info.numActiveStages === 1)
    }

    check[StageDataWrapper](key(stages.head)) { stage =>
      assert(stage.info.status === v1.StageStatus.ACTIVE)
      assert(stage.info.submissionTime === Some(new Date(stages.head.submissionTime.get)))
      assert(stage.info.numTasks === stages.head.numTasks)
    }

    // Start tasks from stage 1
    time += 1

    val s1Tasks = createTasks(4, execIds)
    s1Tasks.foreach { task =>
      listener.onTaskStart(SparkListenerTaskStart(stages.head.stageId,
        stages.head.attemptNumber,
        task))
    }

    assert(store.count(classOf[TaskDataWrapper]) === s1Tasks.size)

    check[JobDataWrapper](1) { job =>
      assert(job.info.numActiveTasks === s1Tasks.size)
    }

    check[StageDataWrapper](key(stages.head)) { stage =>
      assert(stage.info.numActiveTasks === s1Tasks.size)
      assert(stage.info.firstTaskLaunchedTime === Some(new Date(s1Tasks.head.launchTime)))
    }

    s1Tasks.foreach { task =>
      check[TaskDataWrapper](task.taskId) { wrapper =>
        assert(wrapper.taskId === task.taskId)
        assert(wrapper.stageId === stages.head.stageId)
        assert(wrapper.stageAttemptId === stages.head.attemptNumber)
        assert(wrapper.index === task.index)
        assert(wrapper.attempt === task.attemptNumber)
        assert(wrapper.launchTime === task.launchTime)
        assert(wrapper.executorId === task.executorId)
        assert(wrapper.host === task.host)
        assert(wrapper.status === task.status)
        assert(wrapper.taskLocality === task.taskLocality.toString())
        assert(wrapper.speculative === task.speculative)
      }
    }

    // Send two executor metrics update. Only update one metric to avoid a lot of boilerplate code.
    // The tasks are distributed among the two executors, so the executor-level metrics should
    // hold half of the cummulative value of the metric being updated.
    Seq(1L, 2L).foreach { value =>
      s1Tasks.foreach { task =>
        val accum = new AccumulableInfo(1L, Some(InternalAccumulator.MEMORY_BYTES_SPILLED),
          Some(value), None, true, false, None)
        listener.onExecutorMetricsUpdate(SparkListenerExecutorMetricsUpdate(
          task.executorId,
          Seq((task.taskId, stages.head.stageId, stages.head.attemptNumber, Seq(accum)))))
      }

      check[StageDataWrapper](key(stages.head)) { stage =>
        assert(stage.info.memoryBytesSpilled === s1Tasks.size * value)
      }

      val execs = store.view(classOf[ExecutorStageSummaryWrapper]).index("stage")
        .first(key(stages.head)).last(key(stages.head)).asScala.toSeq
      assert(execs.size > 0)
      execs.foreach { exec =>
        assert(exec.info.memoryBytesSpilled === s1Tasks.size * value / 2)
      }
    }

    // Blacklisting executor for stage
    time += 1
    listener.onExecutorBlacklistedForStage(SparkListenerExecutorBlacklistedForStage(
      time = time,
      executorId = execIds.head,
      taskFailures = 2,
      stageId = stages.head.stageId,
      stageAttemptId = stages.head.attemptNumber))

    val executorStageSummaryWrappers =
      store.view(classOf[ExecutorStageSummaryWrapper]).index("stage")
        .first(key(stages.head))
        .last(key(stages.head))
        .asScala.toSeq

    assert(executorStageSummaryWrappers.nonEmpty)
    executorStageSummaryWrappers.foreach { exec =>
      // only the first executor is expected to be blacklisted
      val expectedBlacklistedFlag = exec.executorId == execIds.head
      assert(exec.info.isBlacklistedForStage === expectedBlacklistedFlag)
    }

    check[ExecutorSummaryWrapper](execIds.head) { exec =>
      assert(exec.info.blacklistedInStages === Set(stages.head.stageId))
    }

    // Blacklisting node for stage
    time += 1
    listener.onNodeBlacklistedForStage(SparkListenerNodeBlacklistedForStage(
      time = time,
      hostId = "2.example.com", // this is where the second executor is hosted
      executorFailures = 1,
      stageId = stages.head.stageId,
      stageAttemptId = stages.head.attemptNumber))

    val executorStageSummaryWrappersForNode =
      store.view(classOf[ExecutorStageSummaryWrapper]).index("stage")
        .first(key(stages.head))
        .last(key(stages.head))
        .asScala.toSeq

    assert(executorStageSummaryWrappersForNode.nonEmpty)
    executorStageSummaryWrappersForNode.foreach { exec =>
      // both executor is expected to be blacklisted
      assert(exec.info.isBlacklistedForStage === true)
    }

    // Fail one of the tasks, re-start it.
    time += 1
    s1Tasks.head.markFinished(TaskState.FAILED, time)
    listener.onTaskEnd(SparkListenerTaskEnd(stages.head.stageId, stages.head.attemptNumber,
      "taskType", TaskResultLost, s1Tasks.head, null))

    time += 1
    val reattempt = newAttempt(s1Tasks.head, nextTaskId())
    listener.onTaskStart(SparkListenerTaskStart(stages.head.stageId, stages.head.attemptNumber,
      reattempt))

    assert(store.count(classOf[TaskDataWrapper]) === s1Tasks.size + 1)

    check[JobDataWrapper](1) { job =>
      assert(job.info.numFailedTasks === 1)
      assert(job.info.numActiveTasks === s1Tasks.size)
    }

    check[StageDataWrapper](key(stages.head)) { stage =>
      assert(stage.info.numFailedTasks === 1)
      assert(stage.info.numActiveTasks === s1Tasks.size)
    }

    check[TaskDataWrapper](s1Tasks.head.taskId) { task =>
      assert(task.status === s1Tasks.head.status)
      assert(task.errorMessage == Some(TaskResultLost.toErrorString))
    }

    check[TaskDataWrapper](reattempt.taskId) { task =>
      assert(task.index === s1Tasks.head.index)
      assert(task.attempt === reattempt.attemptNumber)
    }

    // Kill one task, restart it.
    time += 1
    val killed = s1Tasks.drop(1).head
    killed.finishTime = time
    killed.failed = true
    listener.onTaskEnd(SparkListenerTaskEnd(stages.head.stageId, stages.head.attemptNumber,
      "taskType", TaskKilled("killed"), killed, null))

    check[JobDataWrapper](1) { job =>
      assert(job.info.numKilledTasks === 1)
      assert(job.info.killedTasksSummary === Map("killed" -> 1))
    }

    check[StageDataWrapper](key(stages.head)) { stage =>
      assert(stage.info.numKilledTasks === 1)
      assert(stage.info.killedTasksSummary === Map("killed" -> 1))
    }

    check[TaskDataWrapper](killed.taskId) { task =>
      assert(task.index === killed.index)
      assert(task.errorMessage === Some("killed"))
    }

    // Start a new attempt and finish it with TaskCommitDenied, make sure it's handled like a kill.
    time += 1
    val denied = newAttempt(killed, nextTaskId())
    val denyReason = TaskCommitDenied(1, 1, 1)
    listener.onTaskStart(SparkListenerTaskStart(stages.head.stageId, stages.head.attemptNumber,
      denied))

    time += 1
    denied.finishTime = time
    denied.failed = true
    listener.onTaskEnd(SparkListenerTaskEnd(stages.head.stageId, stages.head.attemptNumber,
      "taskType", denyReason, denied, null))

    check[JobDataWrapper](1) { job =>
      assert(job.info.numKilledTasks === 2)
      assert(job.info.killedTasksSummary === Map("killed" -> 1, denyReason.toErrorString -> 1))
    }

    check[StageDataWrapper](key(stages.head)) { stage =>
      assert(stage.info.numKilledTasks === 2)
      assert(stage.info.killedTasksSummary === Map("killed" -> 1, denyReason.toErrorString -> 1))
    }

    check[TaskDataWrapper](denied.taskId) { task =>
      assert(task.index === killed.index)
      assert(task.errorMessage === Some(denyReason.toErrorString))
    }

    // Start a new attempt.
    val reattempt2 = newAttempt(denied, nextTaskId())
    listener.onTaskStart(SparkListenerTaskStart(stages.head.stageId, stages.head.attemptNumber,
      reattempt2))

    // Succeed all tasks in stage 1.
    val pending = s1Tasks.drop(2) ++ Seq(reattempt, reattempt2)

    val s1Metrics = TaskMetrics.empty
    s1Metrics.setExecutorCpuTime(2L)
    s1Metrics.setExecutorRunTime(4L)

    time += 1
    pending.foreach { task =>
      task.markFinished(TaskState.FINISHED, time)
      listener.onTaskEnd(SparkListenerTaskEnd(stages.head.stageId, stages.head.attemptNumber,
        "taskType", Success, task, s1Metrics))
    }

    check[JobDataWrapper](1) { job =>
      assert(job.info.numFailedTasks === 1)
      assert(job.info.numKilledTasks === 2)
      assert(job.info.numActiveTasks === 0)
      assert(job.info.numCompletedTasks === pending.size)
    }

    check[StageDataWrapper](key(stages.head)) { stage =>
      assert(stage.info.numFailedTasks === 1)
      assert(stage.info.numKilledTasks === 2)
      assert(stage.info.numActiveTasks === 0)
      assert(stage.info.numCompleteTasks === pending.size)
    }

    pending.foreach { task =>
      check[TaskDataWrapper](task.taskId) { wrapper =>
        assert(wrapper.errorMessage === None)
        assert(wrapper.executorCpuTime === 2L)
        assert(wrapper.executorRunTime === 4L)
        assert(wrapper.duration === task.duration)
      }
    }

    assert(store.count(classOf[TaskDataWrapper]) === pending.size + 3)

    // End stage 1.
    time += 1
    stages.head.completionTime = Some(time)
    listener.onStageCompleted(SparkListenerStageCompleted(stages.head))

    check[JobDataWrapper](1) { job =>
      assert(job.info.numActiveStages === 0)
      assert(job.info.numCompletedStages === 1)
    }

    check[StageDataWrapper](key(stages.head)) { stage =>
      assert(stage.info.status === v1.StageStatus.COMPLETE)
      assert(stage.info.numFailedTasks === 1)
      assert(stage.info.numActiveTasks === 0)
      assert(stage.info.numCompleteTasks === pending.size)
    }

    check[ExecutorSummaryWrapper](execIds.head) { exec =>
      assert(exec.info.blacklistedInStages === Set())
    }

    // Submit stage 2.
    time += 1
    stages.last.submissionTime = Some(time)
    listener.onStageSubmitted(SparkListenerStageSubmitted(stages.last, jobProps))

    check[JobDataWrapper](1) { job =>
      assert(job.info.numActiveStages === 1)
    }

    check[StageDataWrapper](key(stages.last)) { stage =>
      assert(stage.info.status === v1.StageStatus.ACTIVE)
      assert(stage.info.submissionTime === Some(new Date(stages.last.submissionTime.get)))
    }

    // Blacklisting node for stage
    time += 1
    listener.onNodeBlacklistedForStage(SparkListenerNodeBlacklistedForStage(
      time = time,
      hostId = "1.example.com",
      executorFailures = 1,
      stageId = stages.last.stageId,
      stageAttemptId = stages.last.attemptNumber))

    check[ExecutorSummaryWrapper](execIds.head) { exec =>
      assert(exec.info.blacklistedInStages === Set(stages.last.stageId))
    }

    // Start and fail all tasks of stage 2.
    time += 1
    val s2Tasks = createTasks(4, execIds)
    s2Tasks.foreach { task =>
      listener.onTaskStart(SparkListenerTaskStart(stages.last.stageId,
        stages.last.attemptNumber,
        task))
    }

    time += 1
    s2Tasks.foreach { task =>
      task.markFinished(TaskState.FAILED, time)
      listener.onTaskEnd(SparkListenerTaskEnd(stages.last.stageId, stages.last.attemptNumber,
        "taskType", TaskResultLost, task, null))
    }

    check[JobDataWrapper](1) { job =>
      assert(job.info.numFailedTasks === 1 + s2Tasks.size)
      assert(job.info.numActiveTasks === 0)
    }

    check[StageDataWrapper](key(stages.last)) { stage =>
      assert(stage.info.numFailedTasks === s2Tasks.size)
      assert(stage.info.numActiveTasks === 0)
    }

    // Fail stage 2.
    time += 1
    stages.last.completionTime = Some(time)
    stages.last.failureReason = Some("uh oh")
    listener.onStageCompleted(SparkListenerStageCompleted(stages.last))

    check[JobDataWrapper](1) { job =>
      assert(job.info.numCompletedStages === 1)
      assert(job.info.numFailedStages === 1)
    }

    check[StageDataWrapper](key(stages.last)) { stage =>
      assert(stage.info.status === v1.StageStatus.FAILED)
      assert(stage.info.numFailedTasks === s2Tasks.size)
      assert(stage.info.numActiveTasks === 0)
      assert(stage.info.numCompleteTasks === 0)
      assert(stage.info.failureReason === stages.last.failureReason)
    }

    // - Re-submit stage 2, all tasks, and succeed them and the stage.
    val oldS2 = stages.last
    val newS2 = new StageInfo(oldS2.stageId, oldS2.attemptNumber + 1, oldS2.name, oldS2.numTasks,
      oldS2.rddInfos, oldS2.parentIds, oldS2.details, oldS2.taskMetrics)

    time += 1
    newS2.submissionTime = Some(time)
    listener.onStageSubmitted(SparkListenerStageSubmitted(newS2, jobProps))
    assert(store.count(classOf[StageDataWrapper]) === 3)

    val newS2Tasks = createTasks(4, execIds)

    newS2Tasks.foreach { task =>
      listener.onTaskStart(SparkListenerTaskStart(newS2.stageId, newS2.attemptNumber, task))
    }

    time += 1
    newS2Tasks.foreach { task =>
      task.markFinished(TaskState.FINISHED, time)
      listener.onTaskEnd(SparkListenerTaskEnd(newS2.stageId, newS2.attemptNumber, "taskType",
        Success, task, null))
    }

    time += 1
    newS2.completionTime = Some(time)
    listener.onStageCompleted(SparkListenerStageCompleted(newS2))

    check[JobDataWrapper](1) { job =>
      assert(job.info.numActiveStages === 0)
      assert(job.info.numFailedStages === 1)
      assert(job.info.numCompletedStages === 2)
    }

    check[StageDataWrapper](key(newS2)) { stage =>
      assert(stage.info.status === v1.StageStatus.COMPLETE)
      assert(stage.info.numActiveTasks === 0)
      assert(stage.info.numCompleteTasks === newS2Tasks.size)
    }

    // End job.
    time += 1
    listener.onJobEnd(SparkListenerJobEnd(1, time, JobSucceeded))

    check[JobDataWrapper](1) { job =>
      assert(job.info.status === JobExecutionStatus.SUCCEEDED)
    }

    // Submit a second job that re-uses stage 1 and stage 2. Stage 1 won't be re-run, but
    // stage 2 will. In any case, the DAGScheduler creates new info structures that are copies
    // of the old stages, so mimic that behavior here. The "new" stage 1 is submitted without
    // a submission time, which means it is "skipped", and the stage 2 re-execution should not
    // change the stats of the already finished job.
    time += 1
    val j2Stages = Seq(
      new StageInfo(3, 0, "stage1", 4, Nil, Nil, "details1"),
      new StageInfo(4, 0, "stage2", 4, Nil, Seq(3), "details2"))
    j2Stages.last.submissionTime = Some(time)
    listener.onJobStart(SparkListenerJobStart(2, time, j2Stages, null))
    assert(store.count(classOf[JobDataWrapper]) === 2)

    listener.onStageSubmitted(SparkListenerStageSubmitted(j2Stages.head, jobProps))
    listener.onStageCompleted(SparkListenerStageCompleted(j2Stages.head))
    listener.onStageSubmitted(SparkListenerStageSubmitted(j2Stages.last, jobProps))
    assert(store.count(classOf[StageDataWrapper]) === 5)

    time += 1
    val j2s2Tasks = createTasks(4, execIds)

    j2s2Tasks.foreach { task =>
      listener.onTaskStart(SparkListenerTaskStart(j2Stages.last.stageId,
        j2Stages.last.attemptNumber,
        task))
    }

    time += 1
    j2s2Tasks.foreach { task =>
      task.markFinished(TaskState.FINISHED, time)
      listener.onTaskEnd(SparkListenerTaskEnd(j2Stages.last.stageId, j2Stages.last.attemptNumber,
        "taskType", Success, task, null))
    }

    time += 1
    j2Stages.last.completionTime = Some(time)
    listener.onStageCompleted(SparkListenerStageCompleted(j2Stages.last))

    time += 1
    listener.onJobEnd(SparkListenerJobEnd(2, time, JobSucceeded))

    check[JobDataWrapper](1) { job =>
      assert(job.info.numCompletedStages === 2)
      assert(job.info.numCompletedTasks === s1Tasks.size + s2Tasks.size)
    }

    check[JobDataWrapper](2) { job =>
      assert(job.info.status === JobExecutionStatus.SUCCEEDED)
      assert(job.info.numCompletedStages === 1)
      assert(job.info.numCompletedTasks === j2s2Tasks.size)
      assert(job.info.numSkippedStages === 1)
      assert(job.info.numSkippedTasks === s1Tasks.size)
    }

    // Blacklist an executor.
    time += 1
    listener.onExecutorBlacklisted(SparkListenerExecutorBlacklisted(time, "1", 42))
    check[ExecutorSummaryWrapper]("1") { exec =>
      assert(exec.info.isBlacklisted)
    }

    time += 1
    listener.onExecutorUnblacklisted(SparkListenerExecutorUnblacklisted(time, "1"))
    check[ExecutorSummaryWrapper]("1") { exec =>
      assert(!exec.info.isBlacklisted)
    }

    // Blacklist a node.
    time += 1
    listener.onNodeBlacklisted(SparkListenerNodeBlacklisted(time, "1.example.com", 2))
    check[ExecutorSummaryWrapper]("1") { exec =>
      assert(exec.info.isBlacklisted)
    }

    time += 1
    listener.onNodeUnblacklisted(SparkListenerNodeUnblacklisted(time, "1.example.com"))
    check[ExecutorSummaryWrapper]("1") { exec =>
      assert(!exec.info.isBlacklisted)
    }

    // Stop executors.
    time += 1
    listener.onExecutorRemoved(SparkListenerExecutorRemoved(time, "1", "Test"))
    listener.onExecutorRemoved(SparkListenerExecutorRemoved(time, "2", "Test"))

    Seq("1", "2").foreach { id =>
      check[ExecutorSummaryWrapper](id) { exec =>
        assert(exec.info.id === id)
        assert(!exec.info.isActive)
      }
    }

    // End the application.
    listener.onApplicationEnd(SparkListenerApplicationEnd(42L))

    check[ApplicationInfoWrapper]("id") { app =>
      assert(app.info.name === "name")
      assert(app.info.id === "id")
      assert(app.info.attempts.size === 1)

      val attempt = app.info.attempts.head
      assert(attempt.attemptId === Some("attempt"))
      assert(attempt.startTime === new Date(1L))
      assert(attempt.lastUpdated === new Date(42L))
      assert(attempt.endTime === new Date(42L))
      assert(attempt.duration === 41L)
      assert(attempt.sparkUser === "user")
      assert(attempt.completed)
    }
  }

  test("storage events") {
    val listener = new AppStatusListener(store, conf, true)
    val maxMemory = 42L

    // Register a couple of block managers.
    val bm1 = BlockManagerId("1", "1.example.com", 42)
    val bm2 = BlockManagerId("2", "2.example.com", 84)
    Seq(bm1, bm2).foreach { bm =>
      listener.onExecutorAdded(SparkListenerExecutorAdded(1L, bm.executorId,
        new ExecutorInfo(bm.host, 1, Map())))
      listener.onBlockManagerAdded(SparkListenerBlockManagerAdded(1L, bm, maxMemory))
      check[ExecutorSummaryWrapper](bm.executorId) { exec =>
        assert(exec.info.maxMemory === maxMemory)
      }
    }

    val rdd1b1 = RddBlock(1, 1, 1L, 2L)
    val rdd1b2 = RddBlock(1, 2, 3L, 4L)
    val rdd2b1 = RddBlock(2, 1, 5L, 6L)
    val level = StorageLevel.MEMORY_AND_DISK

    // Submit a stage and make sure the RDDs are recorded.
    val rdd1Info = new RDDInfo(rdd1b1.rddId, "rdd1", 2, level, Nil)
    val rdd2Info = new RDDInfo(rdd2b1.rddId, "rdd2", 1, level, Nil)
    val stage = new StageInfo(1, 0, "stage1", 4, Seq(rdd1Info, rdd2Info), Nil, "details1")
    listener.onStageSubmitted(SparkListenerStageSubmitted(stage, new Properties()))

    check[RDDStorageInfoWrapper](rdd1b1.rddId) { wrapper =>
      assert(wrapper.info.name === rdd1Info.name)
      assert(wrapper.info.numPartitions === rdd1Info.numPartitions)
      assert(wrapper.info.storageLevel === rdd1Info.storageLevel.description)
    }

    // Add partition 1 replicated on two block managers.
    listener.onBlockUpdated(SparkListenerBlockUpdated(
      BlockUpdatedInfo(bm1, rdd1b1.blockId, level, rdd1b1.memSize, rdd1b1.diskSize)))

    check[RDDStorageInfoWrapper](rdd1b1.rddId) { wrapper =>
      assert(wrapper.info.numCachedPartitions === 1L)
      assert(wrapper.info.memoryUsed === rdd1b1.memSize)
      assert(wrapper.info.diskUsed === rdd1b1.diskSize)

      assert(wrapper.info.dataDistribution.isDefined)
      assert(wrapper.info.dataDistribution.get.size === 1)

      val dist = wrapper.info.dataDistribution.get.head
      assert(dist.address === bm1.hostPort)
      assert(dist.memoryUsed === rdd1b1.memSize)
      assert(dist.diskUsed === rdd1b1.diskSize)
      assert(dist.memoryRemaining === maxMemory - dist.memoryUsed)

      assert(wrapper.info.partitions.isDefined)
      assert(wrapper.info.partitions.get.size === 1)

      val part = wrapper.info.partitions.get.head
      assert(part.blockName === rdd1b1.blockId.name)
      assert(part.storageLevel === level.description)
      assert(part.memoryUsed === rdd1b1.memSize)
      assert(part.diskUsed === rdd1b1.diskSize)
      assert(part.executors === Seq(bm1.executorId))
    }

    check[ExecutorSummaryWrapper](bm1.executorId) { exec =>
      assert(exec.info.rddBlocks === 1L)
      assert(exec.info.memoryUsed === rdd1b1.memSize)
      assert(exec.info.diskUsed === rdd1b1.diskSize)
    }

    listener.onBlockUpdated(SparkListenerBlockUpdated(
      BlockUpdatedInfo(bm2, rdd1b1.blockId, level, rdd1b1.memSize, rdd1b1.diskSize)))

    check[RDDStorageInfoWrapper](rdd1b1.rddId) { wrapper =>
      assert(wrapper.info.numCachedPartitions === 1L)
      assert(wrapper.info.memoryUsed === rdd1b1.memSize * 2)
      assert(wrapper.info.diskUsed === rdd1b1.diskSize * 2)
      assert(wrapper.info.dataDistribution.get.size === 2L)
      assert(wrapper.info.partitions.get.size === 1L)

      val dist = wrapper.info.dataDistribution.get.find(_.address == bm2.hostPort).get
      assert(dist.memoryUsed === rdd1b1.memSize)
      assert(dist.diskUsed === rdd1b1.diskSize)
      assert(dist.memoryRemaining === maxMemory - dist.memoryUsed)

      val part = wrapper.info.partitions.get(0)
      assert(part.memoryUsed === rdd1b1.memSize * 2)
      assert(part.diskUsed === rdd1b1.diskSize * 2)
      assert(part.executors === Seq(bm1.executorId, bm2.executorId))
    }

    check[ExecutorSummaryWrapper](bm2.executorId) { exec =>
      assert(exec.info.rddBlocks === 1L)
      assert(exec.info.memoryUsed === rdd1b1.memSize)
      assert(exec.info.diskUsed === rdd1b1.diskSize)
    }

    // Add a second partition only to bm 1.
    listener.onBlockUpdated(SparkListenerBlockUpdated(
      BlockUpdatedInfo(bm1, rdd1b2.blockId, level, rdd1b2.memSize, rdd1b2.diskSize)))

    check[RDDStorageInfoWrapper](rdd1b1.rddId) { wrapper =>
      assert(wrapper.info.numCachedPartitions === 2L)
      assert(wrapper.info.memoryUsed === 2 * rdd1b1.memSize + rdd1b2.memSize)
      assert(wrapper.info.diskUsed === 2 * rdd1b1.diskSize + rdd1b2.diskSize)
      assert(wrapper.info.dataDistribution.get.size === 2L)
      assert(wrapper.info.partitions.get.size === 2L)

      val dist = wrapper.info.dataDistribution.get.find(_.address == bm1.hostPort).get
      assert(dist.memoryUsed === rdd1b1.memSize + rdd1b2.memSize)
      assert(dist.diskUsed === rdd1b1.diskSize + rdd1b2.diskSize)
      assert(dist.memoryRemaining === maxMemory - dist.memoryUsed)

      val part = wrapper.info.partitions.get.find(_.blockName === rdd1b2.blockId.name).get
      assert(part.storageLevel === level.description)
      assert(part.memoryUsed === rdd1b2.memSize)
      assert(part.diskUsed === rdd1b2.diskSize)
      assert(part.executors === Seq(bm1.executorId))
    }

    check[ExecutorSummaryWrapper](bm1.executorId) { exec =>
      assert(exec.info.rddBlocks === 2L)
      assert(exec.info.memoryUsed === rdd1b1.memSize + rdd1b2.memSize)
      assert(exec.info.diskUsed === rdd1b1.diskSize + rdd1b2.diskSize)
    }

    // Remove block 1 from bm 1.
    listener.onBlockUpdated(SparkListenerBlockUpdated(
      BlockUpdatedInfo(bm1, rdd1b1.blockId, StorageLevel.NONE, rdd1b1.memSize, rdd1b1.diskSize)))

    check[RDDStorageInfoWrapper](rdd1b1.rddId) { wrapper =>
      assert(wrapper.info.numCachedPartitions === 2L)
      assert(wrapper.info.memoryUsed === rdd1b1.memSize + rdd1b2.memSize)
      assert(wrapper.info.diskUsed === rdd1b1.diskSize + rdd1b2.diskSize)
      assert(wrapper.info.dataDistribution.get.size === 2L)
      assert(wrapper.info.partitions.get.size === 2L)

      val dist = wrapper.info.dataDistribution.get.find(_.address == bm1.hostPort).get
      assert(dist.memoryUsed === rdd1b2.memSize)
      assert(dist.diskUsed === rdd1b2.diskSize)
      assert(dist.memoryRemaining === maxMemory - dist.memoryUsed)

      val part = wrapper.info.partitions.get.find(_.blockName === rdd1b1.blockId.name).get
      assert(part.storageLevel === level.description)
      assert(part.memoryUsed === rdd1b1.memSize)
      assert(part.diskUsed === rdd1b1.diskSize)
      assert(part.executors === Seq(bm2.executorId))
    }

    check[ExecutorSummaryWrapper](bm1.executorId) { exec =>
      assert(exec.info.rddBlocks === 1L)
      assert(exec.info.memoryUsed === rdd1b2.memSize)
      assert(exec.info.diskUsed === rdd1b2.diskSize)
    }

    // Remove block 1 from bm 2. This should leave only block 2's info in the store.
    listener.onBlockUpdated(SparkListenerBlockUpdated(
      BlockUpdatedInfo(bm2, rdd1b1.blockId, StorageLevel.NONE, rdd1b1.memSize, rdd1b1.diskSize)))

    check[RDDStorageInfoWrapper](rdd1b1.rddId) { wrapper =>
      assert(wrapper.info.numCachedPartitions === 1L)
      assert(wrapper.info.memoryUsed === rdd1b2.memSize)
      assert(wrapper.info.diskUsed === rdd1b2.diskSize)
      assert(wrapper.info.dataDistribution.get.size === 1L)
      assert(wrapper.info.partitions.get.size === 1L)
      assert(wrapper.info.partitions.get(0).blockName === rdd1b2.blockId.name)
    }

    check[ExecutorSummaryWrapper](bm1.executorId) { exec =>
      assert(exec.info.rddBlocks === 1L)
      assert(exec.info.memoryUsed === rdd1b2.memSize)
      assert(exec.info.diskUsed === rdd1b2.diskSize)
    }

    check[ExecutorSummaryWrapper](bm2.executorId) { exec =>
      assert(exec.info.rddBlocks === 0L)
      assert(exec.info.memoryUsed === 0L)
      assert(exec.info.diskUsed === 0L)
    }

    // Add a block from a different RDD. Verify the executor is updated correctly and also that
    // the distribution data for both rdds is updated to match the remaining memory.
    listener.onBlockUpdated(SparkListenerBlockUpdated(
      BlockUpdatedInfo(bm1, rdd2b1.blockId, level, rdd2b1.memSize, rdd2b1.diskSize)))

    check[ExecutorSummaryWrapper](bm1.executorId) { exec =>
      assert(exec.info.rddBlocks === 2L)
      assert(exec.info.memoryUsed === rdd1b2.memSize + rdd2b1.memSize)
      assert(exec.info.diskUsed === rdd1b2.diskSize + rdd2b1.diskSize)
    }

    check[RDDStorageInfoWrapper](rdd1b2.rddId) { wrapper =>
      assert(wrapper.info.dataDistribution.get.size === 1L)
      val dist = wrapper.info.dataDistribution.get(0)
      assert(dist.memoryRemaining === maxMemory - rdd2b1.memSize - rdd1b2.memSize )
    }

    check[RDDStorageInfoWrapper](rdd2b1.rddId) { wrapper =>
      assert(wrapper.info.dataDistribution.get.size === 1L)

      val dist = wrapper.info.dataDistribution.get(0)
      assert(dist.memoryUsed === rdd2b1.memSize)
      assert(dist.diskUsed === rdd2b1.diskSize)
      assert(dist.memoryRemaining === maxMemory - rdd2b1.memSize - rdd1b2.memSize )
    }

    // Add block1 of rdd1 back to bm 1.
    listener.onBlockUpdated(SparkListenerBlockUpdated(
      BlockUpdatedInfo(bm1, rdd1b1.blockId, level, rdd1b1.memSize, rdd1b1.diskSize)))

    check[ExecutorSummaryWrapper](bm1.executorId) { exec =>
      assert(exec.info.rddBlocks === 3L)
      assert(exec.info.memoryUsed === rdd1b1.memSize + rdd1b2.memSize + rdd2b1.memSize)
      assert(exec.info.diskUsed === rdd1b1.diskSize + rdd1b2.diskSize + rdd2b1.diskSize)
    }

    // Unpersist RDD1.
    listener.onUnpersistRDD(SparkListenerUnpersistRDD(rdd1b1.rddId))
    intercept[NoSuchElementException] {
      check[RDDStorageInfoWrapper](rdd1b1.rddId) { _ => () }
    }

    // executor1 now only contains block1 from rdd2.
    check[ExecutorSummaryWrapper](bm1.executorId) { exec =>
      assert(exec.info.rddBlocks === 1L)
      assert(exec.info.memoryUsed === rdd2b1.memSize)
      assert(exec.info.diskUsed === rdd2b1.diskSize)
    }

    // Unpersist RDD2.
    listener.onUnpersistRDD(SparkListenerUnpersistRDD(rdd2b1.rddId))
    intercept[NoSuchElementException] {
      check[RDDStorageInfoWrapper](rdd2b1.rddId) { _ => () }
    }

    check[ExecutorSummaryWrapper](bm1.executorId) { exec =>
      assert(exec.info.rddBlocks === 0L)
      assert(exec.info.memoryUsed === 0)
      assert(exec.info.diskUsed === 0)
    }

    // Update a StreamBlock.
    val stream1 = StreamBlockId(1, 1L)
    listener.onBlockUpdated(SparkListenerBlockUpdated(
      BlockUpdatedInfo(bm1, stream1, level, 1L, 1L)))

    check[StreamBlockData](Array(stream1.name, bm1.executorId)) { stream =>
      assert(stream.name === stream1.name)
      assert(stream.executorId === bm1.executorId)
      assert(stream.hostPort === bm1.hostPort)
      assert(stream.storageLevel === level.description)
      assert(stream.useMemory === level.useMemory)
      assert(stream.useDisk === level.useDisk)
      assert(stream.deserialized === level.deserialized)
      assert(stream.memSize === 1L)
      assert(stream.diskSize === 1L)
    }

    // Drop a StreamBlock.
    listener.onBlockUpdated(SparkListenerBlockUpdated(
      BlockUpdatedInfo(bm1, stream1, StorageLevel.NONE, 0L, 0L)))
    intercept[NoSuchElementException] {
      check[StreamBlockData](stream1.name) { _ => () }
    }

    // Update a BroadcastBlock.
    val broadcast1 = BroadcastBlockId(1L)
    listener.onBlockUpdated(SparkListenerBlockUpdated(
      BlockUpdatedInfo(bm1, broadcast1, level, 1L, 1L)))

    check[ExecutorSummaryWrapper](bm1.executorId) { exec =>
      assert(exec.info.memoryUsed === 1L)
      assert(exec.info.diskUsed === 1L)
    }

    // Drop a BroadcastBlock.
    listener.onBlockUpdated(SparkListenerBlockUpdated(
      BlockUpdatedInfo(bm1, broadcast1, StorageLevel.NONE, 1L, 1L)))
    check[ExecutorSummaryWrapper](bm1.executorId) { exec =>
      assert(exec.info.memoryUsed === 0)
      assert(exec.info.diskUsed === 0)
    }
  }

  test("eviction of old data") {
    val testConf = conf.clone()
      .set(MAX_RETAINED_JOBS, 2)
      .set(MAX_RETAINED_STAGES, 2)
      .set(MAX_RETAINED_TASKS_PER_STAGE, 2)
      .set(MAX_RETAINED_DEAD_EXECUTORS, 1)
    val listener = new AppStatusListener(store, testConf, true)

    // Start 3 jobs, all should be kept. Stop one, it should be evicted.
    time += 1
    listener.onJobStart(SparkListenerJobStart(1, time, Nil, null))
    listener.onJobStart(SparkListenerJobStart(2, time, Nil, null))
    listener.onJobStart(SparkListenerJobStart(3, time, Nil, null))
    assert(store.count(classOf[JobDataWrapper]) === 3)

    time += 1
    listener.onJobEnd(SparkListenerJobEnd(2, time, JobSucceeded))
    assert(store.count(classOf[JobDataWrapper]) === 2)
    intercept[NoSuchElementException] {
      store.read(classOf[JobDataWrapper], 2)
    }

    // Start 3 stages, all should be kept. Stop 2 of them, the stopped one with the lowest id should
    // be deleted. Start a new attempt of the second stopped one, and verify that the stage graph
    // data is not deleted.
    time += 1
    val stages = Seq(
      new StageInfo(1, 0, "stage1", 4, Nil, Nil, "details1"),
      new StageInfo(2, 0, "stage2", 4, Nil, Nil, "details2"),
      new StageInfo(3, 0, "stage3", 4, Nil, Nil, "details3"))

    // Graph data is generated by the job start event, so fire it.
    listener.onJobStart(SparkListenerJobStart(4, time, stages, null))

    stages.foreach { s =>
      time += 1
      s.submissionTime = Some(time)
      listener.onStageSubmitted(SparkListenerStageSubmitted(s, new Properties()))
    }

    assert(store.count(classOf[StageDataWrapper]) === 3)
    assert(store.count(classOf[RDDOperationGraphWrapper]) === 3)

    val dropped = stages.drop(1).head

    // Cache some quantiles by calling AppStatusStore.taskSummary(). For quantiles to be
    // calculated, we need at least one finished task. The code in AppStatusStore uses
    // `executorRunTime` to detect valid tasks, so that metric needs to be updated in the
    // task end event.
    time += 1
    val task = createTasks(1, Array("1")).head
    listener.onTaskStart(SparkListenerTaskStart(dropped.stageId, dropped.attemptNumber, task))

    time += 1
    task.markFinished(TaskState.FINISHED, time)
    val metrics = TaskMetrics.empty
    metrics.setExecutorRunTime(42L)
    listener.onTaskEnd(SparkListenerTaskEnd(dropped.stageId, dropped.attemptNumber,
      "taskType", Success, task, metrics))

    new AppStatusStore(store)
      .taskSummary(dropped.stageId, dropped.attemptNumber, Array(0.25d, 0.50d, 0.75d))
    assert(store.count(classOf[CachedQuantile], "stage", key(dropped)) === 3)

    stages.drop(1).foreach { s =>
      time += 1
      s.completionTime = Some(time)
      listener.onStageCompleted(SparkListenerStageCompleted(s))
    }

    assert(store.count(classOf[StageDataWrapper]) === 2)
    assert(store.count(classOf[RDDOperationGraphWrapper]) === 2)
    intercept[NoSuchElementException] {
      store.read(classOf[StageDataWrapper], Array(2, 0))
    }
    assert(store.count(classOf[CachedQuantile], "stage", key(dropped)) === 0)

    val attempt2 = new StageInfo(3, 1, "stage3", 4, Nil, Nil, "details3")
    time += 1
    attempt2.submissionTime = Some(time)
    listener.onStageSubmitted(SparkListenerStageSubmitted(attempt2, new Properties()))

    assert(store.count(classOf[StageDataWrapper]) === 2)
    assert(store.count(classOf[RDDOperationGraphWrapper]) === 2)
    intercept[NoSuchElementException] {
      store.read(classOf[StageDataWrapper], Array(2, 0))
    }
    intercept[NoSuchElementException] {
      store.read(classOf[StageDataWrapper], Array(3, 0))
    }
    store.read(classOf[StageDataWrapper], Array(3, 1))

    // Start 2 tasks. Finish the second one.
    time += 1
    val tasks = createTasks(2, Array("1"))
    tasks.foreach { task =>
      listener.onTaskStart(SparkListenerTaskStart(attempt2.stageId, attempt2.attemptNumber, task))
    }
    assert(store.count(classOf[TaskDataWrapper]) === 2)

    // Start a 3rd task. The finished tasks should be deleted.
    createTasks(1, Array("1")).foreach { task =>
      listener.onTaskStart(SparkListenerTaskStart(attempt2.stageId, attempt2.attemptNumber, task))
    }
    assert(store.count(classOf[TaskDataWrapper]) === 2)
    intercept[NoSuchElementException] {
      store.read(classOf[TaskDataWrapper], tasks.last.id)
    }

    // Start a 4th task. The first task should be deleted, even if it's still running.
    createTasks(1, Array("1")).foreach { task =>
      listener.onTaskStart(SparkListenerTaskStart(attempt2.stageId, attempt2.attemptNumber, task))
    }
    assert(store.count(classOf[TaskDataWrapper]) === 2)
    intercept[NoSuchElementException] {
      store.read(classOf[TaskDataWrapper], tasks.head.id)
    }
  }

  test("eviction should respect job completion time") {
    val testConf = conf.clone().set(MAX_RETAINED_JOBS, 2)
    val listener = new AppStatusListener(store, testConf, true)

    // Start job 1 and job 2
    time += 1
    listener.onJobStart(SparkListenerJobStart(1, time, Nil, null))
    time += 1
    listener.onJobStart(SparkListenerJobStart(2, time, Nil, null))

    // Stop job 2 before job 1
    time += 1
    listener.onJobEnd(SparkListenerJobEnd(2, time, JobSucceeded))
    time += 1
    listener.onJobEnd(SparkListenerJobEnd(1, time, JobSucceeded))

    // Start job 3 and job 2 should be evicted.
    time += 1
    listener.onJobStart(SparkListenerJobStart(3, time, Nil, null))
    assert(store.count(classOf[JobDataWrapper]) === 2)
    intercept[NoSuchElementException] {
      store.read(classOf[JobDataWrapper], 2)
    }
  }

  test("eviction should respect stage completion time") {
    val testConf = conf.clone().set(MAX_RETAINED_STAGES, 2)
    val listener = new AppStatusListener(store, testConf, true)

    val stage1 = new StageInfo(1, 0, "stage1", 4, Nil, Nil, "details1")
    val stage2 = new StageInfo(2, 0, "stage2", 4, Nil, Nil, "details2")
    val stage3 = new StageInfo(3, 0, "stage3", 4, Nil, Nil, "details3")

    // Start stage 1 and stage 2
    time += 1
    stage1.submissionTime = Some(time)
    listener.onStageSubmitted(SparkListenerStageSubmitted(stage1, new Properties()))
    time += 1
    stage2.submissionTime = Some(time)
    listener.onStageSubmitted(SparkListenerStageSubmitted(stage2, new Properties()))

    // Stop stage 2 before stage 1
    time += 1
    stage2.completionTime = Some(time)
    listener.onStageCompleted(SparkListenerStageCompleted(stage2))
    time += 1
    stage1.completionTime = Some(time)
    listener.onStageCompleted(SparkListenerStageCompleted(stage1))

    // Start stage 3 and stage 2 should be evicted.
    stage3.submissionTime = Some(time)
    listener.onStageSubmitted(SparkListenerStageSubmitted(stage3, new Properties()))
    assert(store.count(classOf[StageDataWrapper]) === 2)
    intercept[NoSuchElementException] {
      store.read(classOf[StageDataWrapper], Array(2, 0))
    }
  }

  test("skipped stages should be evicted before completed stages") {
    val testConf = conf.clone().set(MAX_RETAINED_STAGES, 2)
    val listener = new AppStatusListener(store, testConf, true)

    val stage1 = new StageInfo(1, 0, "stage1", 4, Nil, Nil, "details1")
    val stage2 = new StageInfo(2, 0, "stage2", 4, Nil, Nil, "details2")

    // Sart job 1
    time += 1
    listener.onJobStart(SparkListenerJobStart(1, time, Seq(stage1, stage2), null))

    // Start and stop stage 1
    time += 1
    stage1.submissionTime = Some(time)
    listener.onStageSubmitted(SparkListenerStageSubmitted(stage1, new Properties()))

    time += 1
    stage1.completionTime = Some(time)
    listener.onStageCompleted(SparkListenerStageCompleted(stage1))

    // Stop job 1 and stage 2 will become SKIPPED
    time += 1
    listener.onJobEnd(SparkListenerJobEnd(1, time, JobSucceeded))

    // Submit stage 3 and verify stage 2 is evicted
    val stage3 = new StageInfo(3, 0, "stage3", 4, Nil, Nil, "details3")
    time += 1
    stage3.submissionTime = Some(time)
    listener.onStageSubmitted(SparkListenerStageSubmitted(stage3, new Properties()))

    assert(store.count(classOf[StageDataWrapper]) === 2)
    intercept[NoSuchElementException] {
      store.read(classOf[StageDataWrapper], Array(2, 0))
    }
  }

  test("eviction should respect task completion time") {
    val testConf = conf.clone().set(MAX_RETAINED_TASKS_PER_STAGE, 2)
    val listener = new AppStatusListener(store, testConf, true)

    val stage1 = new StageInfo(1, 0, "stage1", 4, Nil, Nil, "details1")
    stage1.submissionTime = Some(time)
    listener.onStageSubmitted(SparkListenerStageSubmitted(stage1, new Properties()))

    // Start task 1 and task 2
    val tasks = createTasks(3, Array("1"))
    tasks.take(2).foreach { task =>
      listener.onTaskStart(SparkListenerTaskStart(stage1.stageId, stage1.attemptNumber, task))
    }

    // Stop task 2 before task 1
    time += 1
    tasks(1).markFinished(TaskState.FINISHED, time)
    listener.onTaskEnd(
      SparkListenerTaskEnd(stage1.stageId, stage1.attemptId, "taskType", Success, tasks(1), null))
    time += 1
    tasks(0).markFinished(TaskState.FINISHED, time)
    listener.onTaskEnd(
      SparkListenerTaskEnd(stage1.stageId, stage1.attemptId, "taskType", Success, tasks(0), null))

    // Start task 3 and task 2 should be evicted.
    listener.onTaskStart(SparkListenerTaskStart(stage1.stageId, stage1.attemptNumber, tasks(2)))
    assert(store.count(classOf[TaskDataWrapper]) === 2)
    intercept[NoSuchElementException] {
      store.read(classOf[TaskDataWrapper], tasks(1).id)
    }
  }

  test("lastStageAttempt should fail when the stage doesn't exist") {
    val testConf = conf.clone().set(MAX_RETAINED_STAGES, 1)
    val listener = new AppStatusListener(store, testConf, true)
    val appStore = new AppStatusStore(store)

    val stage1 = new StageInfo(1, 0, "stage1", 4, Nil, Nil, "details1")
    val stage2 = new StageInfo(2, 0, "stage2", 4, Nil, Nil, "details2")
    val stage3 = new StageInfo(3, 0, "stage3", 4, Nil, Nil, "details3")

    time += 1
    stage1.submissionTime = Some(time)
    listener.onStageSubmitted(SparkListenerStageSubmitted(stage1, new Properties()))
    stage1.completionTime = Some(time)
    listener.onStageCompleted(SparkListenerStageCompleted(stage1))

    // Make stage 3 complete before stage 2 so that stage 3 will be evicted
    time += 1
    stage3.submissionTime = Some(time)
    listener.onStageSubmitted(SparkListenerStageSubmitted(stage3, new Properties()))
    stage3.completionTime = Some(time)
    listener.onStageCompleted(SparkListenerStageCompleted(stage3))

    time += 1
    stage2.submissionTime = Some(time)
    listener.onStageSubmitted(SparkListenerStageSubmitted(stage2, new Properties()))
    stage2.completionTime = Some(time)
    listener.onStageCompleted(SparkListenerStageCompleted(stage2))

    assert(appStore.asOption(appStore.lastStageAttempt(1)) === None)
    assert(appStore.asOption(appStore.lastStageAttempt(2)).map(_.stageId) === Some(2))
    assert(appStore.asOption(appStore.lastStageAttempt(3)) === None)
  }

  test("SPARK-24415: update metrics for tasks that finish late") {
    val listener = new AppStatusListener(store, conf, true)

    val stage1 = new StageInfo(1, 0, "stage1", 4, Nil, Nil, "details1")
    val stage2 = new StageInfo(2, 0, "stage2", 4, Nil, Nil, "details2")

    // Start job
    listener.onJobStart(SparkListenerJobStart(1, time, Seq(stage1, stage2), null))

    // Start 2 stages
    listener.onStageSubmitted(SparkListenerStageSubmitted(stage1, new Properties()))
    listener.onStageSubmitted(SparkListenerStageSubmitted(stage2, new Properties()))

    // Start 2 Tasks
    val tasks = createTasks(2, Array("1"))
    tasks.foreach { task =>
      listener.onTaskStart(SparkListenerTaskStart(stage1.stageId, stage1.attemptNumber, task))
    }

    // Task 1 Finished
    time += 1
    tasks(0).markFinished(TaskState.FINISHED, time)
    listener.onTaskEnd(
      SparkListenerTaskEnd(stage1.stageId, stage1.attemptId, "taskType", Success, tasks(0), null))

    // Stage 1 Completed
    stage1.failureReason = Some("Failed")
    listener.onStageCompleted(SparkListenerStageCompleted(stage1))

    // Stop job 1
    time += 1
    listener.onJobEnd(SparkListenerJobEnd(1, time, JobSucceeded))

    // Task 2 Killed
    time += 1
    tasks(1).markFinished(TaskState.FINISHED, time)
    listener.onTaskEnd(
      SparkListenerTaskEnd(stage1.stageId, stage1.attemptId, "taskType",
        TaskKilled(reason = "Killed"), tasks(1), null))

    // Ensure killed task metrics are updated
    val allStages = store.view(classOf[StageDataWrapper]).reverse().asScala.map(_.info)
    val failedStages = allStages.filter(_.status == v1.StageStatus.FAILED)
    assert(failedStages.size == 1)
    assert(failedStages.head.numKilledTasks == 1)
    assert(failedStages.head.numCompleteTasks == 1)

    val allJobs = store.view(classOf[JobDataWrapper]).reverse().asScala.map(_.info)
    assert(allJobs.size == 1)
    assert(allJobs.head.numKilledTasks == 1)
    assert(allJobs.head.numCompletedTasks == 1)
    assert(allJobs.head.numActiveStages == 1)
    assert(allJobs.head.numFailedStages == 1)
  }

  Seq(true, false).foreach { live =>
    test(s"Total tasks in the executor summary should match total stage tasks (live = $live)") {

      val testConf = if (live) {
        conf.clone().set(LIVE_ENTITY_UPDATE_PERIOD, Long.MaxValue)
      } else {
        conf.clone().set(LIVE_ENTITY_UPDATE_PERIOD, -1L)
      }

      val listener = new AppStatusListener(store, testConf, live)

      Seq("1", "2").foreach { execId =>
        listener.onExecutorAdded(SparkListenerExecutorAdded(0L, execId,
          new ExecutorInfo("host1", 1, Map.empty)))
      }
      val stage = new StageInfo(1, 0, "stage", 4, Nil, Nil, "details")
      listener.onJobStart(SparkListenerJobStart(1, time, Seq(stage), null))
      listener.onStageSubmitted(SparkListenerStageSubmitted(stage, new Properties()))

      val tasks = createTasks(4, Array("1", "2"))
      tasks.foreach { task =>
        listener.onTaskStart(SparkListenerTaskStart(stage.stageId, stage.attemptNumber, task))
      }

      time += 1
      tasks(0).markFinished(TaskState.FINISHED, time)
      listener.onTaskEnd(SparkListenerTaskEnd(stage.stageId, stage.attemptNumber, "taskType",
        Success, tasks(0), null))
      time += 1
      tasks(1).markFinished(TaskState.FINISHED, time)
      listener.onTaskEnd(SparkListenerTaskEnd(stage.stageId, stage.attemptNumber, "taskType",
        Success, tasks(1), null))

      stage.failureReason = Some("Failed")
      listener.onStageCompleted(SparkListenerStageCompleted(stage))
      time += 1
      listener.onJobEnd(SparkListenerJobEnd(1, time, JobFailed(
        new RuntimeException("Bad Executor"))))

      time += 1
      tasks(2).markFinished(TaskState.FAILED, time)
      listener.onTaskEnd(SparkListenerTaskEnd(stage.stageId, stage.attemptNumber, "taskType",
        ExecutorLostFailure("1", true, Some("Lost executor")), tasks(2), null))
      time += 1
      tasks(3).markFinished(TaskState.FAILED, time)
      listener.onTaskEnd(SparkListenerTaskEnd(stage.stageId, stage.attemptNumber, "taskType",
        ExecutorLostFailure("2", true, Some("Lost executor")), tasks(3), null))

      val esummary = store.view(classOf[ExecutorStageSummaryWrapper]).asScala.map(_.info)
      esummary.foreach { execSummary =>
        assert(execSummary.failedTasks === 1)
        assert(execSummary.succeededTasks === 1)
        assert(execSummary.killedTasks === 0)
      }

      val allExecutorSummary = store.view(classOf[ExecutorSummaryWrapper]).asScala.map(_.info)
      assert(allExecutorSummary.size === 2)
      allExecutorSummary.foreach { allExecSummary =>
        assert(allExecSummary.failedTasks === 1)
        assert(allExecSummary.activeTasks === 0)
        assert(allExecSummary.completedTasks === 1)
      }
      store.delete(classOf[ExecutorSummaryWrapper], "1")
      store.delete(classOf[ExecutorSummaryWrapper], "2")
    }
  }

  test("driver logs") {
    val listener = new AppStatusListener(store, conf, true)

    val driver = BlockManagerId(SparkContext.DRIVER_IDENTIFIER, "localhost", 42)
    listener.onBlockManagerAdded(SparkListenerBlockManagerAdded(time, driver, 42L))
    listener.onApplicationStart(SparkListenerApplicationStart(
      "name",
      Some("id"),
      time,
      "user",
      Some("attempt"),
      Some(Map("stdout" -> "file.txt"))))

    check[ExecutorSummaryWrapper](SparkContext.DRIVER_IDENTIFIER) { d =>
      assert(d.info.executorLogs("stdout") === "file.txt")
    }
  }

  private def key(stage: StageInfo): Array[Int] = Array(stage.stageId, stage.attemptNumber)

  private def check[T: ClassTag](key: Any)(fn: T => Unit): Unit = {
    val value = store.read(classTag[T].runtimeClass, key).asInstanceOf[T]
    fn(value)
  }

  private def newAttempt(orig: TaskInfo, nextId: Long): TaskInfo = {
    // Task reattempts have a different ID, but the same index as the original.
    new TaskInfo(nextId, orig.index, orig.attemptNumber + 1, time, orig.executorId,
      s"${orig.executorId}.example.com", TaskLocality.PROCESS_LOCAL, orig.speculative)
  }

  private def createTasks(count: Int, execs: Array[String]): Seq[TaskInfo] = {
    (1 to count).map { id =>
      val exec = execs(id.toInt % execs.length)
      val taskId = nextTaskId()
      new TaskInfo(taskId, taskId.toInt, 1, time, exec, s"$exec.example.com",
        TaskLocality.PROCESS_LOCAL, id % 2 == 0)
    }
  }

  private def nextTaskId(): Long = {
    taskIdTracker += 1
    taskIdTracker
  }

  private case class RddBlock(
      rddId: Int,
      partId: Int,
      memSize: Long,
      diskSize: Long) {

    def blockId: BlockId = RDDBlockId(rddId, partId)

  }

}
