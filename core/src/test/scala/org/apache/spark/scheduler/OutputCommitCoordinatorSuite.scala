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

import java.io.File
import java.util.Date
import java.util.concurrent.TimeoutException

import scala.concurrent.duration._

import org.apache.hadoop.mapred._
import org.apache.hadoop.mapreduce.TaskType
import org.mockito.ArgumentMatchers.{any, eq => meq}
import org.mockito.Mockito.{doAnswer, spy, times, verify}
import org.mockito.invocation.InvocationOnMock
import org.scalatest.BeforeAndAfter

import org.apache.spark._
import org.apache.spark.internal.io.{FileCommitProtocol, HadoopMapRedCommitProtocol, SparkHadoopWriterUtils}
import org.apache.spark.rdd.{FakeOutputCommitter, RDD}
import org.apache.spark.shuffle.FetchFailedException
import org.apache.spark.util.{ThreadUtils, Utils}

/**
 * Unit tests for the output commit coordination functionality.
 *
 * The unit test makes both the original task and the speculated task
 * attempt to commit, where committing is emulated by creating a
 * directory. If both tasks create directories then the end result is
 * a failure.
 *
 * Note that there are some aspects of this test that are less than ideal.
 * In particular, the test mocks the speculation-dequeuing logic to always
 * dequeue a task and consider it as speculated. Immediately after initially
 * submitting the tasks and calling reviveOffers(), reviveOffers() is invoked
 * again to pick up the speculated task. This may be hacking the original
 * behavior in too much of an unrealistic fashion.
 *
 * Also, the validation is done by checking the number of files in a directory.
 * Ideally, an accumulator would be used for this, where we could increment
 * the accumulator in the output committer's commitTask() call. If the call to
 * commitTask() was called twice erroneously then the test would ideally fail because
 * the accumulator would be incremented twice.
 *
 * The problem with this test implementation is that when both a speculated task and
 * its original counterpart complete, only one of the accumulator's increments is
 * captured. This results in a paradox where if the OutputCommitCoordinator logic
 * was not in SparkHadoopWriter, the tests would still pass because only one of the
 * increments would be captured even though the commit in both tasks was executed
 * erroneously.
 *
 * See also: [[OutputCommitCoordinatorIntegrationSuite]] for integration tests that do
 * not use mocks.
 */
class OutputCommitCoordinatorSuite extends SparkFunSuite with BeforeAndAfter {

  private def doReturn(value: Any) = org.mockito.Mockito.doReturn(value, Seq.empty: _*)

  var outputCommitCoordinator: OutputCommitCoordinator = null
  var tempDir: File = null
  var sc: SparkContext = null

  before {
    tempDir = Utils.createTempDir()
    val conf = new SparkConf()
      .setMaster("local[4]")
      .setAppName(classOf[OutputCommitCoordinatorSuite].getSimpleName)
      .set("spark.hadoop.outputCommitCoordination.enabled", "true")
    sc = new SparkContext(conf) {
      override private[spark] def createSparkEnv(
          conf: SparkConf,
          isLocal: Boolean,
          listenerBus: LiveListenerBus): SparkEnv = {
        outputCommitCoordinator =
          spy(new OutputCommitCoordinator(conf, isDriver = true, Option(this)))
        // Use Mockito.spy() to maintain the default infrastructure everywhere else.
        // This mocking allows us to control the coordinator responses in test cases.
        SparkEnv.createDriverEnv(conf, isLocal, listenerBus,
          SparkContext.numDriverCores(master), this, Some(outputCommitCoordinator))
      }
    }
    // Use Mockito.spy() to maintain the default infrastructure everywhere else
    val mockTaskScheduler = spy(sc.taskScheduler.asInstanceOf[TaskSchedulerImpl])

    doAnswer { (invoke: InvocationOnMock) =>
      // Submit the tasks, then force the task scheduler to dequeue the
      // speculated task
      invoke.callRealMethod()
      mockTaskScheduler.backend.reviveOffers()
    }.when(mockTaskScheduler).submitTasks(any())

    doAnswer { (invoke: InvocationOnMock) =>
      val taskSet = invoke.getArguments()(0).asInstanceOf[TaskSet]
      new TaskSetManager(mockTaskScheduler, taskSet, 4) {
        private var hasDequeuedSpeculatedTask = false
        override def dequeueTaskHelper(
            execId: String,
            host: String,
            locality: TaskLocality.Value,
            speculative: Boolean): Option[(Int, TaskLocality.Value, Boolean)] = {
          if (!speculative) {
            super.dequeueTaskHelper(execId, host, locality, speculative)
          } else if (hasDequeuedSpeculatedTask) {
            None
          } else {
            hasDequeuedSpeculatedTask = true
            Some((0, TaskLocality.PROCESS_LOCAL, true))
          }
        }
      }
    }.when(mockTaskScheduler).createTaskSetManager(any(), any())

    sc.taskScheduler = mockTaskScheduler
    val dagSchedulerWithMockTaskScheduler = new DAGScheduler(sc, mockTaskScheduler)
    sc.taskScheduler.setDAGScheduler(dagSchedulerWithMockTaskScheduler)
    sc.dagScheduler = dagSchedulerWithMockTaskScheduler
  }

  after {
    sc.stop()
    tempDir.delete()
    outputCommitCoordinator = null
  }

  test("Only one of two duplicate commit tasks should commit") {
    val rdd = sc.parallelize(Seq(1), 1)
    sc.runJob(rdd, OutputCommitFunctions(tempDir.getAbsolutePath).commitSuccessfully _,
      rdd.partitions.indices)
    assert(tempDir.list().size === 1)
  }

  ignore("If commit fails, if task is retried it should not be locked, and will succeed.") {
    val rdd = sc.parallelize(Seq(1), 1)
    sc.runJob(rdd, OutputCommitFunctions(tempDir.getAbsolutePath).failFirstCommitAttempt _,
      rdd.partitions.indices)
    assert(tempDir.list().size === 1)
  }

  test("Job should not complete if all commits are denied") {
    // Create a mock OutputCommitCoordinator that denies all attempts to commit
    doReturn(false).when(outputCommitCoordinator).handleAskPermissionToCommit(
      any(), any(), any(), any())
    val rdd: RDD[Int] = sc.parallelize(Seq(1), 1)
    def resultHandler(x: Int, y: Unit): Unit = {}
    val futureAction: SimpleFutureAction[Unit] = sc.submitJob[Int, Unit, Unit](rdd,
      OutputCommitFunctions(tempDir.getAbsolutePath).commitSuccessfully,
      0 until rdd.partitions.size, resultHandler, () => ())
    // It's an error if the job completes successfully even though no committer was authorized,
    // so throw an exception if the job was allowed to complete.
    intercept[TimeoutException] {
      ThreadUtils.awaitResult(futureAction, 5.seconds)
    }
    assert(tempDir.list().size === 0)
  }

  test("Only authorized committer failures can clear the authorized committer lock (SPARK-6614)") {
    val stage: Int = 1
    val stageAttempt: Int = 1
    val partition: Int = 2
    val authorizedCommitter: Int = 3
    val nonAuthorizedCommitter: Int = 100
    outputCommitCoordinator.stageStart(stage, maxPartitionId = 2)

    assert(outputCommitCoordinator.canCommit(stage, stageAttempt, partition, authorizedCommitter))
    assert(!outputCommitCoordinator.canCommit(stage, stageAttempt, partition,
      nonAuthorizedCommitter))
    // The non-authorized committer fails
    outputCommitCoordinator.taskCompleted(stage, stageAttempt, partition,
      attemptNumber = nonAuthorizedCommitter, reason = TaskKilled("test"))
    // New tasks should still not be able to commit because the authorized committer has not failed
    assert(!outputCommitCoordinator.canCommit(stage, stageAttempt, partition,
      nonAuthorizedCommitter + 1))
    // The authorized committer now fails, clearing the lock
    outputCommitCoordinator.taskCompleted(stage, stageAttempt, partition,
      attemptNumber = authorizedCommitter, reason = TaskKilled("test"))
    // A new task should not be allowed to become stage failed because of potential data duplication
    assert(!outputCommitCoordinator.canCommit(stage, stageAttempt, partition,
      nonAuthorizedCommitter + 2))
  }

  test("SPARK-19631: Do not allow failed attempts to be authorized for committing") {
    val stage: Int = 1
    val stageAttempt: Int = 1
    val partition: Int = 1
    val failedAttempt: Int = 0
    outputCommitCoordinator.stageStart(stage, maxPartitionId = 1)
    outputCommitCoordinator.taskCompleted(stage, stageAttempt, partition,
      attemptNumber = failedAttempt,
      reason = ExecutorLostFailure("0", exitCausedByApp = true, None))
    assert(!outputCommitCoordinator.canCommit(stage, stageAttempt, partition, failedAttempt))
    assert(outputCommitCoordinator.canCommit(stage, stageAttempt, partition, failedAttempt + 1))
  }

  test("SPARK-24589: Differentiate tasks from different stage attempts") {
    var stage = 1
    val taskAttempt = 1
    val partition = 1

    outputCommitCoordinator.stageStart(stage, maxPartitionId = 1)
    assert(outputCommitCoordinator.canCommit(stage, 1, partition, taskAttempt))
    assert(!outputCommitCoordinator.canCommit(stage, 2, partition, taskAttempt))

    // Fail the task in the first attempt, the task in the second attempt should succeed.
    stage += 1
    outputCommitCoordinator.stageStart(stage, maxPartitionId = 1)
    outputCommitCoordinator.taskCompleted(stage, 1, partition, taskAttempt,
      ExecutorLostFailure("0", exitCausedByApp = true, None))
    assert(!outputCommitCoordinator.canCommit(stage, 1, partition, taskAttempt))
    assert(outputCommitCoordinator.canCommit(stage, 2, partition, taskAttempt))

    // Commit the 1st attempt, fail the 2nd attempt, make sure 3rd attempt cannot commit,
    // then fail the 1st attempt and since stage failed because of potential data duplication,
    // make sure fail the 4th attempt.
    stage += 1
    outputCommitCoordinator.stageStart(stage, maxPartitionId = 1)
    assert(outputCommitCoordinator.canCommit(stage, 1, partition, taskAttempt))
    outputCommitCoordinator.taskCompleted(stage, 2, partition, taskAttempt,
      ExecutorLostFailure("0", exitCausedByApp = true, None))
    assert(!outputCommitCoordinator.canCommit(stage, 3, partition, taskAttempt))
    outputCommitCoordinator.taskCompleted(stage, 1, partition, taskAttempt,
      ExecutorLostFailure("0", exitCausedByApp = true, None))
    // A new task should not be allowed to become the authorized committer since stage failed
    // because of potential data duplication
    assert(!outputCommitCoordinator.canCommit(stage, 4, partition, taskAttempt))
  }

  test("SPARK-24589: Make sure stage state is cleaned up") {
    // Normal application without stage failures.
    sc.parallelize(1 to 100, 100)
      .map { i => (i % 10, i) }
      .reduceByKey(_ + _)
      .collect()

    assert(sc.dagScheduler.outputCommitCoordinator.isEmpty)

    // Force failures in a few tasks so that a stage is retried. Collect the ID of the failing
    // stage so that we can check the state of the output committer.
    val retriedStage = sc.parallelize(1 to 100, 10)
      .map { i => (i % 10, i) }
      .reduceByKey { (_, _) =>
        val ctx = TaskContext.get()
        if (ctx.stageAttemptNumber() == 0) {
          throw new FetchFailedException(SparkEnv.get.blockManager.blockManagerId, 1, 1L, 1, 1,
            new Exception("Failure for test."))
        } else {
          ctx.stageId()
        }
      }
      .collect()
      .map { case (k, v) => v }
      .toSet

    assert(retriedStage.size === 1)
    assert(sc.dagScheduler.outputCommitCoordinator.isEmpty)
    verify(sc.env.outputCommitCoordinator, times(2))
      .stageStart(meq(retriedStage.head), any())
    verify(sc.env.outputCommitCoordinator).stageEnd(meq(retriedStage.head))
  }
}

/**
 * Class with methods that can be passed to runJob to test commits with a mock committer.
 */
private case class OutputCommitFunctions(tempDirPath: String) {

  private val jobId = new SerializableWritable(SparkHadoopWriterUtils.createJobID(new Date, 0))

  // Mock output committer that simulates a successful commit (after commit is authorized)
  private def successfulOutputCommitter = new FakeOutputCommitter {
    override def commitTask(context: TaskAttemptContext): Unit = {
      Utils.createDirectory(tempDirPath)
    }
  }

  // Mock output committer that simulates a failed commit (after commit is authorized)
  private def failingOutputCommitter = new FakeOutputCommitter {
    override def commitTask(taskAttemptContext: TaskAttemptContext): Unit = {
      throw new RuntimeException
    }
  }

  def commitSuccessfully(iter: Iterator[Int]): Unit = {
    val ctx = TaskContext.get()
    runCommitWithProvidedCommitter(ctx, iter, successfulOutputCommitter)
  }

  def failFirstCommitAttempt(iter: Iterator[Int]): Unit = {
    val ctx = TaskContext.get()
    runCommitWithProvidedCommitter(ctx, iter,
      if (ctx.attemptNumber == 0) failingOutputCommitter else successfulOutputCommitter)
  }

  private def runCommitWithProvidedCommitter(
      ctx: TaskContext,
      iter: Iterator[Int],
      outputCommitter: OutputCommitter): Unit = {
    def jobConf = new JobConf {
      override def getOutputCommitter(): OutputCommitter = outputCommitter
    }

    // Instantiate committer.
    val committer = FileCommitProtocol.instantiate(
      className = classOf[HadoopMapRedCommitProtocol].getName,
      jobId = jobId.value.getId.toString,
      outputPath = jobConf.get("mapred.output.dir"))

    // Create TaskAttemptContext.
    // Hadoop wants a 32-bit task attempt ID, so if ours is bigger than Int.MaxValue, roll it
    // around by taking a mod. We expect that no task will be attempted 2 billion times.
    val taskAttemptId = (ctx.taskAttemptId % Int.MaxValue).toInt
    val attemptId = new TaskAttemptID(
      new TaskID(jobId.value, TaskType.MAP, ctx.partitionId), taskAttemptId)
    val taskContext = new TaskAttemptContextImpl(jobConf, attemptId)

    committer.setupTask(taskContext)
    committer.commitTask(taskContext)
  }
}
