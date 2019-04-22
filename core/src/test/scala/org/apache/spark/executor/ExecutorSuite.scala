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
import java.lang.Thread.UncaughtExceptionHandler
import java.nio.ByteBuffer
import java.util.Properties
import java.util.concurrent.{ConcurrentHashMap, CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Map
import scala.concurrent.duration._

import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.{any, eq => meq}
import org.mockito.Mockito.{inOrder, verify, when}
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.PrivateMethodTester
import org.scalatest.concurrent.Eventually
import org.scalatest.mockito.MockitoSugar

import org.apache.spark._
import org.apache.spark.TaskState.TaskState
import org.apache.spark.internal.config._
import org.apache.spark.internal.config.UI._
import org.apache.spark.memory.TestMemoryManager
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.rdd.RDD
import org.apache.spark.rpc.{RpcEndpointRef, RpcEnv, RpcTimeout}
import org.apache.spark.scheduler.{FakeTask, ResultTask, Task, TaskDescription}
import org.apache.spark.serializer.{JavaSerializer, SerializerManager}
import org.apache.spark.shuffle.FetchFailedException
import org.apache.spark.storage.{BlockManager, BlockManagerId}
import org.apache.spark.util.{LongAccumulator, UninterruptibleThread}

class ExecutorSuite extends SparkFunSuite
    with LocalSparkContext with MockitoSugar with Eventually with PrivateMethodTester {

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
      executor.killAllTasks(true, "test")
      executorSuiteHelper.latch2.countDown()
      if (!executorSuiteHelper.latch3.await(5, TimeUnit.SECONDS)) {
        fail("executor did not send second status update in time")
      }

      // `testFailedReason` should be `TaskKilled`; `taskState` should be `KILLED`
      assert(executorSuiteHelper.testFailedReason === TaskKilled("test"))
      assert(executorSuiteHelper.taskState === TaskState.KILLED)
    }
    finally {
      if (executor != null) {
        executor.stop()
      }
    }
  }

  test("SPARK-19276: Handle FetchFailedExceptions that are hidden by user exceptions") {
    val conf = new SparkConf().setMaster("local").setAppName("executor suite test")
    sc = new SparkContext(conf)
    val serializer = SparkEnv.get.closureSerializer.newInstance()
    val resultFunc = (context: TaskContext, itr: Iterator[Int]) => itr.size

    // Submit a job where a fetch failure is thrown, but user code has a try/catch which hides
    // the fetch failure.  The executor should still tell the driver that the task failed due to a
    // fetch failure, not a generic exception from user code.
    val inputRDD = new FetchFailureThrowingRDD(sc)
    val secondRDD = new FetchFailureHidingRDD(sc, inputRDD, throwOOM = false, interrupt = false)
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
    val taskDescription = createFakeTaskDescription(serTask)

    val failReason = runTaskAndGetFailReason(taskDescription)
    assert(failReason.isInstanceOf[FetchFailed])
  }

  test("Executor's worker threads should be UninterruptibleThread") {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("executor thread test")
      .set(UI_ENABLED.key, "false")
    sc = new SparkContext(conf)
    val executorThread = sc.parallelize(Seq(1), 1).map { _ =>
      Thread.currentThread.getClass.getName
    }.collect().head
    assert(executorThread === classOf[UninterruptibleThread].getName)
  }

  test("SPARK-19276: OOMs correctly handled with a FetchFailure") {
    val (failReason, uncaughtExceptionHandler) = testFetchFailureHandling(true)
    assert(failReason.isInstanceOf[ExceptionFailure])
    val exceptionCaptor = ArgumentCaptor.forClass(classOf[Throwable])
    verify(uncaughtExceptionHandler).uncaughtException(any(), exceptionCaptor.capture())
    assert(exceptionCaptor.getAllValues.size === 1)
    assert(exceptionCaptor.getAllValues().get(0).isInstanceOf[OutOfMemoryError])
  }

  test("SPARK-23816: interrupts are not masked by a FetchFailure") {
    // If killing the task causes a fetch failure, we still treat it as a task that was killed,
    // as the fetch failure could easily be caused by interrupting the thread.
    val (failReason, _) = testFetchFailureHandling(false)
    assert(failReason.isInstanceOf[TaskKilled])
  }

  /**
   * Helper for testing some cases where a FetchFailure should *not* get sent back, because its
   * superceded by another error, either an OOM or intentionally killing a task.
   * @param oom if true, throw an OOM after the FetchFailure; else, interrupt the task after the
    *            FetchFailure
   */
  private def testFetchFailureHandling(
      oom: Boolean): (TaskFailedReason, UncaughtExceptionHandler) = {
    // when there is a fatal error like an OOM, we don't do normal fetch failure handling, since it
    // may be a false positive.  And we should call the uncaught exception handler.
    // SPARK-23816 also handle interrupts the same way, as killing an obsolete speculative task
    // does not represent a real fetch failure.
    val conf = new SparkConf().setMaster("local").setAppName("executor suite test")
    sc = new SparkContext(conf)
    val serializer = SparkEnv.get.closureSerializer.newInstance()
    val resultFunc = (context: TaskContext, itr: Iterator[Int]) => itr.size

    // Submit a job where a fetch failure is thrown, but then there is an OOM or interrupt.  We
    // should treat the fetch failure as a false positive, and do normal OOM or interrupt handling.
    val inputRDD = new FetchFailureThrowingRDD(sc)
    if (!oom) {
      // we are trying to setup a case where a task is killed after a fetch failure -- this
      // is just a helper to coordinate between the task thread and this thread that will
      // kill the task
      ExecutorSuiteHelper.latches = new ExecutorSuiteHelper()
    }
    val secondRDD = new FetchFailureHidingRDD(sc, inputRDD, throwOOM = oom, interrupt = !oom)
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
    val taskDescription = createFakeTaskDescription(serTask)

    runTaskGetFailReasonAndExceptionHandler(taskDescription, killTask = !oom)
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

  test("Heartbeat should drop zero accumulator updates") {
    heartbeatZeroAccumulatorUpdateTest(true)
  }

  test("Heartbeat should not drop zero accumulator updates when the conf is disabled") {
    heartbeatZeroAccumulatorUpdateTest(false)
  }

  private def withHeartbeatExecutor(confs: (String, String)*)
      (f: (Executor, ArrayBuffer[Heartbeat]) => Unit): Unit = {
    val conf = new SparkConf
    confs.foreach { case (k, v) => conf.set(k, v) }
    val serializer = new JavaSerializer(conf)
    val env = createMockEnv(conf, serializer)
    val executor =
      new Executor("id", "localhost", SparkEnv.get, userClassPath = Nil, isLocal = true)
    val executorClass = classOf[Executor]

    // Save all heartbeats sent into an ArrayBuffer for verification
    val heartbeats = ArrayBuffer[Heartbeat]()
    val mockReceiver = mock[RpcEndpointRef]
    when(mockReceiver.askSync(any[Heartbeat], any[RpcTimeout])(any))
      .thenAnswer((invocation: InvocationOnMock) => {
        val args = invocation.getArguments()
        heartbeats += args(0).asInstanceOf[Heartbeat]
        HeartbeatResponse(false)
      })
    val receiverRef = executorClass.getDeclaredField("heartbeatReceiverRef")
    receiverRef.setAccessible(true)
    receiverRef.set(executor, mockReceiver)

    f(executor, heartbeats)
  }

  private def heartbeatZeroAccumulatorUpdateTest(dropZeroMetrics: Boolean): Unit = {
    val c = EXECUTOR_HEARTBEAT_DROP_ZERO_ACCUMULATOR_UPDATES.key -> dropZeroMetrics.toString
    withHeartbeatExecutor(c) { (executor, heartbeats) =>
      val reportHeartbeat = PrivateMethod[Unit]('reportHeartBeat)

      // When no tasks are running, there should be no accumulators sent in heartbeat
      executor.invokePrivate(reportHeartbeat())
      // invokeReportHeartbeat(executor)
      assert(heartbeats.length == 1)
      assert(heartbeats(0).accumUpdates.length == 0,
        "No updates should be sent when no tasks are running")

      // When we start a task with a nonzero accumulator, that should end up in the heartbeat
      val metrics = new TaskMetrics()
      val nonZeroAccumulator = new LongAccumulator()
      nonZeroAccumulator.add(1)
      metrics.registerAccumulator(nonZeroAccumulator)

      val executorClass = classOf[Executor]
      val tasksMap = {
        val field =
          executorClass.getDeclaredField("org$apache$spark$executor$Executor$$runningTasks")
        field.setAccessible(true)
        field.get(executor).asInstanceOf[ConcurrentHashMap[Long, executor.TaskRunner]]
      }
      val mockTaskRunner = mock[executor.TaskRunner]
      val mockTask = mock[Task[Any]]
      when(mockTask.metrics).thenReturn(metrics)
      when(mockTaskRunner.taskId).thenReturn(6)
      when(mockTaskRunner.task).thenReturn(mockTask)
      when(mockTaskRunner.startGCTime).thenReturn(1)
      tasksMap.put(6, mockTaskRunner)

      executor.invokePrivate(reportHeartbeat())
      assert(heartbeats.length == 2)
      val updates = heartbeats(1).accumUpdates
      assert(updates.length == 1 && updates(0)._1 == 6,
        "Heartbeat should only send update for the one task running")
      val accumsSent = updates(0)._2.length
      assert(accumsSent > 0, "The nonzero accumulator we added should be sent")
      if (dropZeroMetrics) {
        assert(accumsSent == metrics.accumulators().count(!_.isZero),
          "The number of accumulators sent should match the number of nonzero accumulators")
      } else {
        assert(accumsSent == metrics.accumulators().length,
          "The number of accumulators sent should match the number of total accumulators")
      }
    }
  }

  private def createMockEnv(conf: SparkConf, serializer: JavaSerializer): SparkEnv = {
    val mockEnv = mock[SparkEnv]
    val mockRpcEnv = mock[RpcEnv]
    val mockMetricsSystem = mock[MetricsSystem]
    val mockBlockManager = mock[BlockManager]
    when(mockEnv.conf).thenReturn(conf)
    when(mockEnv.serializer).thenReturn(serializer)
    when(mockEnv.serializerManager).thenReturn(mock[SerializerManager])
    when(mockEnv.rpcEnv).thenReturn(mockRpcEnv)
    when(mockEnv.metricsSystem).thenReturn(mockMetricsSystem)
    when(mockEnv.memoryManager).thenReturn(new TestMemoryManager(conf))
    when(mockEnv.closureSerializer).thenReturn(serializer)
    when(mockBlockManager.blockManagerId).thenReturn(BlockManagerId("1", "hostA", 1234))
    when(mockEnv.blockManager).thenReturn(mockBlockManager)
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
      partitionId = 0,
      addedFiles = Map[String, Long](),
      addedJars = Map[String, Long](),
      properties = new Properties,
      serializedTask)
  }

  private def runTaskAndGetFailReason(taskDescription: TaskDescription): TaskFailedReason = {
    runTaskGetFailReasonAndExceptionHandler(taskDescription, false)._1
  }

  private def runTaskGetFailReasonAndExceptionHandler(
      taskDescription: TaskDescription,
      killTask: Boolean): (TaskFailedReason, UncaughtExceptionHandler) = {
    val mockBackend = mock[ExecutorBackend]
    val mockUncaughtExceptionHandler = mock[UncaughtExceptionHandler]
    var executor: Executor = null
    val timedOut = new AtomicBoolean(false)
    try {
      executor = new Executor("id", "localhost", SparkEnv.get, userClassPath = Nil, isLocal = true,
        uncaughtExceptionHandler = mockUncaughtExceptionHandler)
      // the task will be launched in a dedicated worker thread
      executor.launchTask(mockBackend, taskDescription)
      if (killTask) {
        val killingThread = new Thread("kill-task") {
          override def run(): Unit = {
            // wait to kill the task until it has thrown a fetch failure
            if (ExecutorSuiteHelper.latches.latch1.await(10, TimeUnit.SECONDS)) {
              // now we can kill the task
              executor.killAllTasks(true, "Killed task, eg. because of speculative execution")
            } else {
              timedOut.set(true)
            }
          }
        }
        killingThread.start()
      }
      eventually(timeout(5.seconds), interval(10.milliseconds)) {
        assert(executor.numRunningTasks === 0)
      }
      assert(!timedOut.get(), "timed out waiting to be ready to kill tasks")
    } finally {
      if (executor != null) {
        executor.stop()
      }
    }
    val orderedMock = inOrder(mockBackend)
    val statusCaptor = ArgumentCaptor.forClass(classOf[ByteBuffer])
    orderedMock.verify(mockBackend)
      .statusUpdate(meq(0L), meq(TaskState.RUNNING), statusCaptor.capture())
    val finalState = if (killTask) TaskState.KILLED else TaskState.FAILED
    orderedMock.verify(mockBackend)
      .statusUpdate(meq(0L), meq(finalState), statusCaptor.capture())
    // first statusUpdate for RUNNING has empty data
    assert(statusCaptor.getAllValues().get(0).remaining() === 0)
    // second update is more interesting
    val failureData = statusCaptor.getAllValues.get(1)
    val failReason =
      SparkEnv.get.closureSerializer.newInstance().deserialize[TaskFailedReason](failureData)
    (failReason, mockUncaughtExceptionHandler)
  }
}

class FetchFailureThrowingRDD(sc: SparkContext) extends RDD[Int](sc, Nil) {
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
    val input: FetchFailureThrowingRDD,
    throwOOM: Boolean,
    interrupt: Boolean) extends RDD[Int](input) {
  override def compute(split: Partition, context: TaskContext): Iterator[Int] = {
    val inItr = input.compute(split, context)
    try {
      Iterator(inItr.size)
    } catch {
      case t: Throwable =>
        if (throwOOM) {
          // scalastyle:off throwerror
          throw new OutOfMemoryError("OOM while handling another exception")
          // scalastyle:on throwerror
        } else if (interrupt) {
          // make sure our test is setup correctly
          assert(TaskContext.get().asInstanceOf[TaskContextImpl].fetchFailed.isDefined)
          // signal our test is ready for the task to get killed
          ExecutorSuiteHelper.latches.latch1.countDown()
          // then wait for another thread in the test to kill the task -- this latch
          // is never actually decremented, we just wait to get killed.
          ExecutorSuiteHelper.latches.latch2.await(10, TimeUnit.SECONDS)
          throw new IllegalStateException("timed out waiting to be interrupted")
        } else {
          throw new RuntimeException("User Exception that hides the original exception", t)
        }
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

// helper for coordinating killing tasks
private object ExecutorSuiteHelper {
  var latches: ExecutorSuiteHelper = null
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
