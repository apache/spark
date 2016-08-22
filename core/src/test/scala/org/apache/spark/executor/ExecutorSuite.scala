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

import java.nio.ByteBuffer
import java.util.concurrent.CountDownLatch

import scala.collection.mutable.HashMap

import org.mockito.Matchers._
import org.mockito.Mockito.{mock, when}
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer

import org.apache.spark._
import org.apache.spark.TaskState.TaskState
import org.apache.spark.memory.MemoryManager
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.network.buffer.ChunkedByteBuffer
import org.apache.spark.rpc.RpcEnv
import org.apache.spark.scheduler.{FakeTask, Task}
import org.apache.spark.serializer.JavaSerializer

class ExecutorSuite extends SparkFunSuite {

  test("SPARK-15963: Catch `TaskKilledException` correctly in Executor.TaskRunner") {
    // mock some objects to make Executor.launchTask() happy
    val conf = new SparkConf
    val serializer = new JavaSerializer(conf)
    val mockEnv = mock(classOf[SparkEnv])
    val mockRpcEnv = mock(classOf[RpcEnv])
    val mockMetricsSystem = mock(classOf[MetricsSystem])
    val mockMemoryManager = mock(classOf[MemoryManager])
    when(mockEnv.conf).thenReturn(conf)
    when(mockEnv.serializer).thenReturn(serializer)
    when(mockEnv.rpcEnv).thenReturn(mockRpcEnv)
    when(mockEnv.metricsSystem).thenReturn(mockMetricsSystem)
    when(mockEnv.memoryManager).thenReturn(mockMemoryManager)
    when(mockEnv.closureSerializer).thenReturn(serializer)
    val serializedTask =
      Task.serializeWithDependencies(
        new FakeTask(0, 0),
        HashMap[String, Long](),
        HashMap[String, Long](),
        serializer.newInstance())

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

    val mockExecutorBackend = mock(classOf[ExecutorBackend])
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
            val taskEndReason = invocationOnMock.getArguments()(2).asInstanceOf[ChunkedByteBuffer]
            executorSuiteHelper.testFailedReason
              = serializer.newInstance().deserialize(taskEndReason)
            // let the main test thread check `taskState` and `testFailedReason`
            executorSuiteHelper.latch3.countDown()
          }
        }
      })

    var executor: Executor = null
    try {
      executor = new Executor("id", "localhost", mockEnv, userClassPath = Nil, isLocal = true)
      // the task will be launched in a dedicated worker thread
      executor.launchTask(mockExecutorBackend, 0, 0, "", serializedTask)

      executorSuiteHelper.latch1.await()
      // we know the task will be started, but not yet deserialized, because of the latches we
      // use in mockExecutorBackend.
      executor.killAllTasks(true)
      executorSuiteHelper.latch2.countDown()
      executorSuiteHelper.latch3.await()

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
}

// Helps to test("SPARK-15963")
private class ExecutorSuiteHelper {

  val latch1 = new CountDownLatch(1)
  val latch2 = new CountDownLatch(1)
  val latch3 = new CountDownLatch(1)

  @volatile var taskState: TaskState = _
  @volatile var testFailedReason: TaskFailedReason = _
}
