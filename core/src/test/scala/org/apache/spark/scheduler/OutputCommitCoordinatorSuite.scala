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

import java.io.{File, ObjectInputStream, ObjectOutputStream, IOException}

import org.mockito.Matchers
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.concurrent.Timeouts
import org.scalatest.{BeforeAndAfter, FunSuite}

import org.apache.hadoop.mapred.{TaskAttemptID, JobConf, TaskAttemptContext, OutputCommitter}

import org.apache.spark._
import org.apache.spark.rdd.FakeOutputCommitter
import org.apache.spark.util.Utils

import scala.collection.mutable.ArrayBuffer

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
 */
class OutputCommitCoordinatorSuite
    extends FunSuite
    with BeforeAndAfter
    with Timeouts {

  val conf = new SparkConf()
    .set("spark.localExecution.enabled", "true")

  var dagScheduler: DAGScheduler = null
  var tempDir: File = null
  var tempDirPath: String = null
  var sc: SparkContext = null

  before {
    sc = new SparkContext("local[4]", "Output Commit Coordinator Suite")
    tempDir = Utils.createTempDir()
    tempDirPath = tempDir.getAbsolutePath()
    // Use Mockito.spy() to maintain the default infrastructure everywhere else
    val mockTaskScheduler = spy(sc.taskScheduler.asInstanceOf[TaskSchedulerImpl])

    doAnswer(new Answer[Unit]() {
      override def answer(invoke: InvocationOnMock): Unit = {
        // Submit the tasks, then, force the task scheduler to dequeue the
        // speculated task
        invoke.callRealMethod()
        mockTaskScheduler.backend.reviveOffers()
      }
    }).when(mockTaskScheduler).submitTasks(Matchers.any())

    doAnswer(new Answer[TaskSetManager]() {
      override def answer(invoke: InvocationOnMock): TaskSetManager = {
        val taskSet = invoke.getArguments()(0).asInstanceOf[TaskSet]
        return new TaskSetManager(mockTaskScheduler, taskSet, 4) {
          var hasDequeuedSpeculatedTask = false
          override def dequeueSpeculativeTask(
              execId: String,
              host: String,
              locality: TaskLocality.Value): Option[(Int, TaskLocality.Value)] = {
            if (!hasDequeuedSpeculatedTask) {
              hasDequeuedSpeculatedTask = true
              return Some(0, TaskLocality.PROCESS_LOCAL)
            } else {
              return None
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
  }

  /**
   * Function that constructs a SparkHadoopWriter with a mock committer and runs its commit
   */
  private class OutputCommittingFunction(private var tempDirPath: String)
      extends ((TaskContext, Iterator[Int]) => Int) with Serializable {

    def apply(ctxt: TaskContext, it: Iterator[Int]): Int = {
      val outputCommitter = new FakeOutputCommitter {
        override def commitTask(context: TaskAttemptContext) : Unit = {
          Utils.createDirectory(tempDirPath)
        }
      }
      runCommitWithProvidedCommitter(ctxt, it, outputCommitter)
    }

    protected def runCommitWithProvidedCommitter(
        ctxt: TaskContext,
        it: Iterator[Int],
        outputCommitter: OutputCommitter): Int = {
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
      sparkHadoopWriter.setup(ctxt.stageId, ctxt.partitionId, ctxt.attemptNumber)
      sparkHadoopWriter.commit
      0
    }

    // Need this otherwise the entire test suite attempts to be serialized
    @throws(classOf[IOException])
    private def writeObject(out: ObjectOutputStream): Unit = {
      out.writeUTF(tempDirPath)
    }

    @throws(classOf[IOException])
    private def readObject(in: ObjectInputStream): Unit = {
      tempDirPath = in.readUTF()
    }
  }

  /**
   * Function that will explicitly fail to commit on the first attempt
   */
  private class FailFirstTimeCommittingFunction(private var tempDirPath: String)
      extends OutputCommittingFunction(tempDirPath) {
    override def apply(ctxt: TaskContext, it: Iterator[Int]): Int = {
      if (ctxt.attemptNumber == 0) {
        val outputCommitter = new FakeOutputCommitter {
          override def commitTask(taskAttemptContext: TaskAttemptContext) {
            throw new RuntimeException
          }
        }
        runCommitWithProvidedCommitter(ctxt, it, outputCommitter)
      } else {
        super.apply(ctxt, it)
      }
    }
  }

  test("Only one of two duplicate commit tasks should commit") {
    val rdd = sc.parallelize(Seq(1), 1)
    sc.runJob(rdd, new OutputCommittingFunction(tempDirPath),
      0 until rdd.partitions.size, allowLocal = true)
    assert(tempDir.list().size === 1)
  }

  test("If commit fails, if task is retried it should not be locked, and will succeed.") {
    val rdd = sc.parallelize(Seq(1), 1)
    sc.runJob(rdd, new FailFirstTimeCommittingFunction(tempDirPath),
      0 until rdd.partitions.size, allowLocal = true)
    assert(tempDir.list().size === 1)
  }
}
