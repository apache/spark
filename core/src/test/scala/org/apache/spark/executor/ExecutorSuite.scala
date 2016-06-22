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
    val mockMemoryManager = mock(classOf[MemoryManager])
    when(mockEnv.conf).thenReturn(conf)
    when(mockEnv.serializer).thenReturn(serializer)
    when(mockEnv.rpcEnv).thenReturn(mockRpcEnv)
    when(mockEnv.memoryManager).thenReturn(mockMemoryManager)
    when(mockEnv.closureSerializer).thenReturn(serializer)
    val serializedTask =
      Task.serializeWithDependencies(
        new FakeTask(0),
        HashMap[String, Long](),
        HashMap[String, Long](),
        serializer.newInstance())

    // the program should run in this order:
    // +-----------------------------+----------------------------------------------+
    // |      main test thread       |      worker thread                           |
    // +-----------------------------+----------------------------------------------+
    // |    executor.launchTask()    |                                              |
    // |                             | TaskRunner.run() begins                      |
    // |                             |          ...                                 |
    // |                             | execBackend.statusUpdate  // 1st time, #L240 |
    // | executor.killAllTasks(true) |                                              |
    // |                             |          ...                                 |
    // |                             |  task = ser.deserialize   // #L253           |
    // |                             |          ...                                 |
    // |                             | execBackend.statusUpdate  // 2nd time, #L365 |
    // |                             |          ...                                 |
    // |                             |   TaskRunner.run() ends                      |
    // |       check results         |                                              |
    // +-----------------------------+----------------------------------------------+

    val mockExecutorBackend = mock(classOf[ExecutorBackend])
    when(mockExecutorBackend.statusUpdate(any(), any(), any()))
      .thenAnswer(new Answer[Unit] {
        var firstTime = true
        override def answer(invocationOnMock: InvocationOnMock): Unit = {
          if (firstTime) {
            TestHelper.latch1.countDown()
            // here between latch1 and latch2, executor.killAllTasks() is called
            TestHelper.latch2.await()
            firstTime = false
          }
          else {
            val taskState = invocationOnMock.getArguments()(1).asInstanceOf[TaskState]
            // save the returned `taskState` and `testFailedReason` into TestHelper
            TestHelper.taskState = taskState
            val taskEndReason = invocationOnMock.getArguments()(2).asInstanceOf[ByteBuffer]
            TestHelper.testFailedReason = serializer.newInstance().deserialize(taskEndReason)
            // let the main test thread to check `taskState` and `testFailedReason`
            TestHelper.latch3.countDown()
          }
        }
      })

    val executor = new Executor("id", "localhost", mockEnv, userClassPath = Nil, isLocal = true)
    // the task will be launched in a dedicated worker thread
    executor.launchTask(mockExecutorBackend, 0, 0, "", serializedTask)

    TestHelper.latch1.await()
    executor.killAllTasks(true)
    TestHelper.latch2.countDown()
    TestHelper.latch3.await()

    // `exception` should be `TaskKilled`; `taskState` should be `KILLED`
    assert(TestHelper.testFailedReason === TaskKilled)
    assert(TestHelper.taskState === TaskState.KILLED)
  }
}

// Helps to test("SPARK-15963")
private object TestHelper {

  val latch1 = new CountDownLatch(1)
  val latch2 = new CountDownLatch(1)
  val latch3 = new CountDownLatch(1)

  var taskState: TaskState = _
  var testFailedReason: TaskFailedReason = _
}
