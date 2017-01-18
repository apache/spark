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
import java.util.Properties
import java.util.concurrent.CountDownLatch

import scala.collection.mutable.Map

import org.mockito.ArgumentCaptor
import org.mockito.Matchers.{any, eq => meq}
import org.mockito.Mockito.{inOrder, when}
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.mock.MockitoSugar

import org.apache.spark._
import org.apache.spark.TaskState.TaskState
import org.apache.spark.memory.MemoryManager
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.rdd.RDD
import org.apache.spark.rpc.RpcEnv
import org.apache.spark.scheduler.{FakeTask, ResultTask, TaskDescription}
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.shuffle.FetchFailedException
import org.apache.spark.storage.BlockManagerId

class ExecutorSuite extends SparkFunSuite with LocalSparkContext with MockitoSugar {

  test("SPARK-15963: Catch `TaskKilledException` correctly in Executor.TaskRunner") {
    // mock some objects to make Executor.launchTask() happy
    val conf = new SparkConf
    val serializer = new JavaSerializer(conf)
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
    val fakeTaskMetrics = serializer.newInstance().serialize(TaskMetrics.registered).array()
    val serializedTask = serializer.newInstance().serialize(
      new FakeTask(0, 0, Nil, fakeTaskMetrics))
    val taskDescription = fakeTaskDescription(serializedTask)

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
      executor.launchTask(mockExecutorBackend, taskDescription)

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

  test("SPARK-19276: Handle Fetch Failed for all intervening user code") {
    val conf = new SparkConf().setMaster("local").setAppName("executor suite test")
    val sc = new SparkContext(conf)

    val serializer = SparkEnv.get.closureSerializer.newInstance()
    val resultFunc = (context: TaskContext, itr: Iterator[Int]) => itr.size
    val inputRDD = new FakeShuffleRDD(sc)
    val secondRDD = new FetchFailureHidingRDD(sc, inputRDD)
    val taskBinary = sc.broadcast(serializer.serialize((secondRDD, resultFunc)).array())
    val serializedTaskMetrics = serializer.serialize(TaskMetrics.registered).array()
    val task = new ResultTask(
      stageId = 1,
      stageAttemptId = 0,
      taskBinary = taskBinary,
      partition = secondRDD.partitions(0),
      locs = Seq(),
      outputId = 0,
      localProperties = new Properties(),
      serializedTaskMetrics = serializedTaskMetrics
    )

    val serTask = serializer.serialize(task)
    val taskDescription = fakeTaskDescription(serTask)


    val mockBackend = mock[ExecutorBackend]
    var executor: Executor = null
    try {
      executor = new Executor("id", "localhost", SparkEnv.get, userClassPath = Nil, isLocal = true)
      executor.launchTask(mockBackend, taskDescription)
      val startTime = System.currentTimeMillis()
      val maxTime = startTime + 5000
      while (executor.numRunningTasks > 0 && System.currentTimeMillis() < maxTime) {
        Thread.sleep(10)
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
      val failReason = serializer.deserialize[TaskFailedReason](failureData)
      assert(failReason.isInstanceOf[FetchFailed])
    } finally {
      if (executor != null) {
        executor.stop()
      }
    }
  }

  private def fakeTaskDescription(serializedTask: ByteBuffer): TaskDescription = {
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

}

class FakeShuffleRDD(sc: SparkContext) extends RDD[Int](sc, Nil) {
  override def compute(split: Partition, context: TaskContext): Iterator[Int] = {
    new Iterator[Int] {
      override def hasNext: Boolean = true
      override def next(): Int = {
        throw new FetchFailedException(
          bmAddress = BlockManagerId("1", "hostA", 1234),
          shuffleId = 0,
          mapId = 0,
          reduceId = 0,
          message = "fake fetch failure"
        )
      }
    }
  }
  override protected def getPartitions: Array[Partition] = {
    Array(new SimplePartition)
  }
}

class SimplePartition extends Partition {
  override def index: Int = 0
}

class FetchFailureHidingRDD(
    sc: SparkContext,
    val input: FakeShuffleRDD) extends RDD[Int](input) {
  override def compute(split: Partition, context: TaskContext): Iterator[Int] = {
    val inItr = input.compute(split, context)
    try {
      Iterator(inItr.size)
    } catch {
      case t: Throwable =>
        throw new RuntimeException("User Exception that hides the original exception", t)
    }
  }

  override protected def getPartitions: Array[Partition] = {
    Array(new SimplePartition)
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
