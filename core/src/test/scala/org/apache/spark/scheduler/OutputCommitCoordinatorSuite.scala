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
import java.util.concurrent.TimeoutException

import org.mockito.Matchers
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.BeforeAndAfter

import org.apache.hadoop.mapred.{TaskAttemptID, JobConf, TaskAttemptContext, OutputCommitter}

import org.apache.spark._
import org.apache.spark.rdd.{RDD, FakeOutputCommitter}
import org.apache.spark.util.Utils

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps

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

  var outputCommitCoordinator: OutputCommitCoordinator = null
  var tempDir: File = null
  var sc: SparkContext = null

  before {
    tempDir = Utils.createTempDir()
    val conf = new SparkConf()
      .setMaster("local[4]")
      .setAppName(classOf[OutputCommitCoordinatorSuite].getSimpleName)
      .set("spark.speculation", "true")
    sc = new SparkContext(conf) {
      override private[spark] def createSparkEnv(
          conf: SparkConf,
          isLocal: Boolean,
          listenerBus: LiveListenerBus): SparkEnv = {
        outputCommitCoordinator = spy(new OutputCommitCoordinator(conf, isDriver = true))
        // Use Mockito.spy() to maintain the default infrastructure everywhere else.
        // This mocking allows us to control the coordinator responses in test cases.
        SparkEnv.createDriverEnv(conf, isLocal, listenerBus, Some(outputCommitCoordinator))
      }
    }
    // Use Mockito.spy() to maintain the default infrastructure everywhere else
    val mockTaskScheduler = spy(sc.taskScheduler.asInstanceOf[TaskSchedulerImpl])

    doAnswer(new Answer[Unit]() {
      override def answer(invoke: InvocationOnMock): Unit = {
        // Submit the tasks, then force the task scheduler to dequeue the
        // speculated task
        invoke.callRealMethod()
        mockTaskScheduler.backend.reviveOffers()
      }
    }).when(mockTaskScheduler).submitTasks(Matchers.any())

    doAnswer(new Answer[TaskSetManager]() {
      override def answer(invoke: InvocationOnMock): TaskSetManager = {
        val taskSet = invoke.getArguments()(0).asInstanceOf[TaskSet]
        new TaskSetManager(mockTaskScheduler, taskSet, 4) {
          var hasDequeuedSpeculatedTask = false
          override def dequeueSpeculativeTask(
              execId: String,
              host: String,
              locality: TaskLocality.Value): Option[(Int, TaskLocality.Value)] = {
            if (!hasDequeuedSpeculatedTask) {
              hasDequeuedSpeculatedTask = true
              Some(0, TaskLocality.PROCESS_LOCAL)
            } else {
              None
            }
          }
        }
      }
    }).when(mockTaskScheduler).createTaskSetManager(Matchers.any(), Matchers.any())

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
      0 until rdd.partitions.size)
    assert(tempDir.list().size === 1)
  }

  test("If commit fails, if task is retried it should not be locked, and will succeed.") {
    val rdd = sc.parallelize(Seq(1), 1)
    sc.runJob(rdd, OutputCommitFunctions(tempDir.getAbsolutePath).failFirstCommitAttempt _,
      0 until rdd.partitions.size)
    assert(tempDir.list().size === 1)
  }

  test("Job should not complete if all commits are denied") {
    // Create a mock OutputCommitCoordinator that denies all attempts to commit
    doReturn(false).when(outputCommitCoordinator).handleAskPermissionToCommit(
      Matchers.any(), Matchers.any(), Matchers.any())
    val rdd: RDD[Int] = sc.parallelize(Seq(1), 1)
    def resultHandler(x: Int, y: Unit): Unit = {}
    val futureAction: SimpleFutureAction[Unit] = sc.submitJob[Int, Unit, Unit](rdd,
      OutputCommitFunctions(tempDir.getAbsolutePath).commitSuccessfully,
      0 until rdd.partitions.size, resultHandler, () => Unit)
    // It's an error if the job completes successfully even though no committer was authorized,
    // so throw an exception if the job was allowed to complete.
    intercept[TimeoutException] {
      Await.result(futureAction, 5 seconds)
    }
    assert(tempDir.list().size === 0)
  }

  test("Only authorized committer failures can clear the authorized committer lock (SPARK-6614)") {
    val stage: Int = 1
    val partition: Int = 2
    val authorizedCommitter: Int = 3
    val nonAuthorizedCommitter: Int = 100
    outputCommitCoordinator.stageStart(stage)

    assert(outputCommitCoordinator.canCommit(stage, partition, authorizedCommitter))
    assert(!outputCommitCoordinator.canCommit(stage, partition, nonAuthorizedCommitter))
    // The non-authorized committer fails
    outputCommitCoordinator.taskCompleted(
      stage, partition, attemptNumber = nonAuthorizedCommitter, reason = TaskKilled)
    // New tasks should still not be able to commit because the authorized committer has not failed
    assert(
      !outputCommitCoordinator.canCommit(stage, partition, nonAuthorizedCommitter + 1))
    // The authorized committer now fails, clearing the lock
    outputCommitCoordinator.taskCompleted(
      stage, partition, attemptNumber = authorizedCommitter, reason = TaskKilled)
    // A new task should now be allowed to become the authorized committer
    assert(
      outputCommitCoordinator.canCommit(stage, partition, nonAuthorizedCommitter + 2))
    // There can only be one authorized committer
    assert(
      !outputCommitCoordinator.canCommit(stage, partition, nonAuthorizedCommitter + 3))
  }
}

/**
 * Class with methods that can be passed to runJob to test commits with a mock committer.
 */
private case class OutputCommitFunctions(tempDirPath: String) {

  // Mock output committer that simulates a successful commit (after commit is authorized)
  private def successfulOutputCommitter = new FakeOutputCommitter {
    override def commitTask(context: TaskAttemptContext): Unit = {
      Utils.createDirectory(tempDirPath)
    }
  }

  // Mock output committer that simulates a failed commit (after commit is authorized)
  private def failingOutputCommitter = new FakeOutputCommitter {
    override def commitTask(taskAttemptContext: TaskAttemptContext) {
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
    val sparkHadoopWriter = new SparkHadoopWriter(jobConf) {
      override def newTaskAttemptContext(
        conf: JobConf,
        attemptId: TaskAttemptID): TaskAttemptContext = {
        mock(classOf[TaskAttemptContext])
      }
    }
    sparkHadoopWriter.setup(ctx.stageId, ctx.partitionId, ctx.attemptNumber)
    sparkHadoopWriter.commit()
  }
}
