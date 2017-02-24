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

package org.apache.spark.executor

import java.io.{Externalizable, ObjectInput, ObjectOutput}
import java.nio.ByteBuffer
import java.util.Properties
import java.util.concurrent.{CountDownLatch, TimeUnit}

import scala.collection.mutable.Map
import scala.concurrent.duration._

import org.mockito.ArgumentCaptor
import org.mockito.Matchers.{any, eq => meq}
import org.mockito.Mockito.{inOrder, when}
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.concurrent.Eventually
import org.scalatest.mock.MockitoSugar

import org.apache.spark._
import org.apache.spark.TaskState.TaskState
import org.apache.spark.memory.MemoryManager
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.rpc.RpcEnv
import org.apache.spark.scheduler.{FakeTask, TaskDescription}
import org.apache.spark.serializer.JavaSerializer

class ExecutorSuite extends SparkFunSuite with LocalSparkContext with MockitoSugar with Eventually {

  test("SPARK-15963: Catch `TaskKilledException` correctly in Executor.TaskRunner") {
    // mock some objects to make Executor.launchTask() happy
    val conf = new SparkConf
    val serializer = new JavaSerializer(conf)
    val env = createMockEnv(conf, serializer)
    val serializedTask = serializer.newInstance().serialize(new FakeTask(0, 0))
    val taskDescription = createFakeTaskDescription(serializedTask)

    // we use latches to force the program to run in this order:
    // +-----------------------------+---------------------------------------+
    // |      main test thread       |      worker thread                    |
    // +-----------------------------+---------------------------------------+
    // |    executor.launchTask()    |                                       |
    // |                             | TaskRunner.run() begins               |
    // |                             |          ...                          |
    // |                             | execBackend.statusUpdate  // 1st time |
    // | executor.killAllTasks(true) |                                       |
    // |                             |          ...                          |
    // |                             |  task = ser.deserialize               |
    // |                             |          ...                          |
    // |                             | execBackend.statusUpdate  // 2nd time |
    // |                             |          ...                          |
    // |                             |   TaskRunner.run() ends               |
    // |       check results         |                                       |
    // +-----------------------------+---------------------------------------+

    val executorSuiteHelper = new ExecutorSuiteHelper

    val mockExecutorBackend = mock[ExecutorBackend]
    when(mockExecutorBackend.statusUpdate(any(), any(), any()))
      .thenAnswer(new Answer[Unit] {
        var firstTime = true
        override def answer(invocationOnMock: InvocationOnMock): Unit = {
          if (firstTime) {
            executorSuiteHelper.latch1.countDown()
            // here between latch1 and latch2, executor.killAllTasks() is called
            executorSuiteHelper.latch2.await()
            firstTime = false
          }
          else {
            // save the returned `taskState` and `testFailedReason` into `executorSuiteHelper`
            val taskState = invocationOnMock.getArguments()(1).asInstanceOf[TaskState]
            executorSuiteHelper.taskState = taskState
            val taskEndReason = invocationOnMock.getArguments()(2).asInstanceOf[ByteBuffer]
            executorSuiteHelper.testFailedReason =
              serializer.newInstance().deserialize(taskEndReason)
            // let the main test thread check `taskState` and `testFailedReason`
            executorSuiteHelper.latch3.countDown()
          }
        }
      })

    var executor: Executor = null
    try {
      executor = new Executor("id", "localhost", env, userClassPath = Nil, isLocal = true)
      // the task will be launched in a dedicated worker thread
      executor.launchTask(mockExecutorBackend, taskDescription)

      if (!executorSuiteHelper.latch1.await(5, TimeUnit.SECONDS)) {
        fail("executor did not send first status update in time")
      }
      // we know the task will be started, but not yet deserialized, because of the latches we
      // use in mockExecutorBackend.
      executor.killAllTasks(true)
      executorSuiteHelper.latch2.countDown()
      if (!executorSuiteHelper.latch3.await(5, TimeUnit.SECONDS)) {
        fail("executor did not send second status update in time")
      }

      // `testFailedReason` should be `TaskKilled`; `taskState` should be `KILLED`
      assert(executorSuiteHelper.testFailedReason === TaskKilled)
      assert(executorSuiteHelper.taskState === TaskState.KILLED)
    }
    finally {
      if (executor != null) {
        executor.stop()
      }
    }
  }

  test("Gracefully handle error in task deserialization") {
    val conf = new SparkConf
    val serializer = new JavaSerializer(conf)
    val env = createMockEnv(conf, serializer)
    val serializedTask = serializer.newInstance().serialize(new NonDeserializableTask)
    val taskDescription = createFakeTaskDescription(serializedTask)

    val failReason = runTaskAndGetFailReason(taskDescription)
    failReason match {
      case ef: ExceptionFailure =>
        assert(ef.exception.isDefined)
        assert(ef.exception.get.getMessage() === NonDeserializableTask.errorMsg)
      case _ =>
        fail(s"unexpected failure type: $failReason")
    }
  }

  private def createMockEnv(conf: SparkConf, serializer: JavaSerializer): SparkEnv = {
    val mockEnv = mock[SparkEnv]
    val mockRpcEnv = mock[RpcEnv]
    val mockMetricsSystem = mock[MetricsSystem]
    val mockMemoryManager = mock[MemoryManager]
    when(mockEnv.conf).thenReturn(conf)
    when(mockEnv.serializer).thenReturn(serializer)
    when(mockEnv.rpcEnv).thenReturn(mockRpcEnv)
    when(mockEnv.metricsSystem).thenReturn(mockMetricsSystem)
    when(mockEnv.memoryManager).thenReturn(mockMemoryManager)
    when(mockEnv.closureSerializer).thenReturn(serializer)
    SparkEnv.set(mockEnv)
    mockEnv
  }

  private def createFakeTaskDescription(serializedTask: ByteBuffer): TaskDescription = {
    new TaskDescription(
      taskId = 0,
      attemptNumber = 0,
      executorId = "",
      name = "",
      index = 0,
      addedFiles = Map[String, Long](),
      addedJars = Map[String, Long](),
      properties = new Properties,
      serializedTask)
  }

  private def runTaskAndGetFailReason(taskDescription: TaskDescription): TaskFailedReason = {
    val mockBackend = mock[ExecutorBackend]
    var executor: Executor = null
    try {
      executor = new Executor("id", "localhost", SparkEnv.get, userClassPath = Nil, isLocal = true)
      // the task will be launched in a dedicated worker thread
      executor.launchTask(mockBackend, taskDescription)
      eventually(timeout(5 seconds), interval(10 milliseconds)) {
        assert(executor.numRunningTasks === 0)
      }
    } finally {
      if (executor != null) {
        executor.stop()
      }
    }
    val orderedMock = inOrder(mockBackend)
    val statusCaptor = ArgumentCaptor.forClass(classOf[ByteBuffer])
    orderedMock.verify(mockBackend)
      .statusUpdate(meq(0L), meq(TaskState.RUNNING), statusCaptor.capture())
    orderedMock.verify(mockBackend)
      .statusUpdate(meq(0L), meq(TaskState.FAILED), statusCaptor.capture())
    // first statusUpdate for RUNNING has empty data
    assert(statusCaptor.getAllValues().get(0).remaining() === 0)
    // second update is more interesting
    val failureData = statusCaptor.getAllValues.get(1)
    SparkEnv.get.closureSerializer.newInstance().deserialize[TaskFailedReason](failureData)
  }
}

// Helps to test("SPARK-15963")
private class ExecutorSuiteHelper {

  val latch1 = new CountDownLatch(1)
  val latch2 = new CountDownLatch(1)
  val latch3 = new CountDownLatch(1)

  @volatile var taskState: TaskState = _
  @volatile var testFailedReason: TaskFailedReason = _
}

private class NonDeserializableTask extends FakeTask(0, 0) with Externalizable {
  def writeExternal(out: ObjectOutput): Unit = {}
  def readExternal(in: ObjectInput): Unit = {
    throw new RuntimeException(NonDeserializableTask.errorMsg)
  }
}

private object NonDeserializableTask {
  val errorMsg = "failure in deserialization"
}
