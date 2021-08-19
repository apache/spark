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
import java.net.URL
import java.nio.ByteBuffer
import java.util.Properties
import java.util.concurrent.{ConcurrentHashMap, CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.immutable
import scala.collection.mutable.{ArrayBuffer, Map}
import scala.concurrent.duration._

import com.google.common.cache.{CacheBuilder, CacheLoader}
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.{any, eq => meq}
import org.mockito.Mockito.{inOrder, verify, when}
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.Assertions._
import org.scalatest.PrivateMethodTester
import org.scalatest.concurrent.Eventually
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark._
import org.apache.spark.TaskState.TaskState
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.config._
import org.apache.spark.internal.config.UI._
import org.apache.spark.memory.{SparkOutOfMemoryError, TestMemoryManager}
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.rdd.RDD
import org.apache.spark.resource.ResourceInformation
import org.apache.spark.rpc.{RpcEndpointRef, RpcEnv, RpcTimeout}
import org.apache.spark.scheduler.{DirectTaskResult, FakeTask, ResultTask, Task, TaskDescription}
import org.apache.spark.serializer.{JavaSerializer, SerializerInstance, SerializerManager}
import org.apache.spark.shuffle.FetchFailedException
import org.apache.spark.storage.{BlockManager, BlockManagerId}
import org.apache.spark.util.{LongAccumulator, SparkUncaughtExceptionHandler, ThreadUtils, UninterruptibleThread}

class ExecutorSuite extends SparkFunSuite
    with LocalSparkContext with MockitoSugar with Eventually with PrivateMethodTester {

  override def afterEach(): Unit = {
    // Unset any latches after each test; each test that needs them initializes new ones.
    ExecutorSuiteHelper.latches = null
    super.afterEach()
  }

  /**
   * Creates an Executor with the provided arguments, is then passed to `f`
   * and will be stopped after `f` returns.
   */
  def withExecutor(
      executorId: String,
      executorHostname: String,
      env: SparkEnv,
      userClassPath: Seq[URL] = Nil,
      isLocal: Boolean = true,
      uncaughtExceptionHandler: UncaughtExceptionHandler
        = new SparkUncaughtExceptionHandler,
      resources: immutable.Map[String, ResourceInformation]
        = immutable.Map.empty[String, ResourceInformation])(f: Executor => Unit): Unit = {
    var executor: Executor = null
    try {
      executor = new Executor(executorId, executorHostname, env, userClassPath, isLocal,
        uncaughtExceptionHandler, resources)

      f(executor)
    } finally {
      if (executor != null) {
        executor.stop()
      }
    }
  }

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

    withExecutor("id", "localhost", env) { executor =>

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
      assert(executorSuiteHelper.testFailedReason.isInstanceOf[TaskKilled])
      assert(executorSuiteHelper.testFailedReason.toErrorString === "TaskKilled (test)")
      assert(executorSuiteHelper.taskState === TaskState.KILLED)
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
    val taskDescription = createResultTaskDescription(serializer, taskBinary, secondRDD, 1)

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
   * Helper for testing some cases where a FetchFailure should *not* get sent back, because it's
   * superseded by another error, either an OOM or intentionally killing a task.
   * @param oom if true, throw an OOM after the FetchFailure; else, interrupt the task after the
   *            FetchFailure
   * @param poll if true, poll executor metrics after launching task
   */
  private def testFetchFailureHandling(
      oom: Boolean,
      poll: Boolean = false): (TaskFailedReason, UncaughtExceptionHandler) = {
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
    // helper to coordinate between the task thread and this thread that will kill the task
    // (and to poll executor metrics if necessary)
    ExecutorSuiteHelper.latches = new ExecutorSuiteHelper
    val secondRDD = new FetchFailureHidingRDD(sc, inputRDD, throwOOM = oom, interrupt = !oom)
    val taskBinary = sc.broadcast(serializer.serialize((secondRDD, resultFunc)).array())
    val taskDescription = createResultTaskDescription(serializer, taskBinary, secondRDD, 1)

    runTaskGetFailReasonAndExceptionHandler(taskDescription, killTask = !oom, poll)
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

  private def withMockHeartbeatReceiverRef(executor: Executor)
      (func: RpcEndpointRef => Unit): Unit = {
    val executorClass = classOf[Executor]
    val mockReceiverRef = mock[RpcEndpointRef]
    val receiverRef = executorClass.getDeclaredField("heartbeatReceiverRef")
    receiverRef.setAccessible(true)
    receiverRef.set(executor, mockReceiverRef)

    func(mockReceiverRef)
  }

  private def withHeartbeatExecutor(confs: (String, String)*)
      (f: (Executor, ArrayBuffer[Heartbeat]) => Unit): Unit = {
    val conf = new SparkConf
    confs.foreach { case (k, v) => conf.set(k, v) }
    val serializer = new JavaSerializer(conf)
    val env = createMockEnv(conf, serializer)
    withExecutor("id", "localhost", SparkEnv.get) { executor =>
      withMockHeartbeatReceiverRef(executor) { mockReceiverRef =>
        // Save all heartbeats sent into an ArrayBuffer for verification
        val heartbeats = ArrayBuffer[Heartbeat]()
        when(mockReceiverRef.askSync(any[Heartbeat], any[RpcTimeout])(any))
          .thenAnswer((invocation: InvocationOnMock) => {
            val args = invocation.getArguments()
            heartbeats += args(0).asInstanceOf[Heartbeat]
            HeartbeatResponse(false)
          })

        f(executor, heartbeats)
      }
    }
  }

  private def heartbeatZeroAccumulatorUpdateTest(dropZeroMetrics: Boolean): Unit = {
    val c = EXECUTOR_HEARTBEAT_DROP_ZERO_ACCUMULATOR_UPDATES.key -> dropZeroMetrics.toString
    withHeartbeatExecutor(c) { (executor, heartbeats) =>
      val reportHeartbeat = PrivateMethod[Unit](Symbol("reportHeartBeat"))

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

  test("Send task executor metrics in DirectTaskResult") {
    // Run a successful, trivial result task
    // We need to ensure, however, that executor metrics are polled after the task is started
    // so this requires some coordination using ExecutorSuiteHelper.
    val conf = new SparkConf().setMaster("local").setAppName("executor suite test")
    sc = new SparkContext(conf)
    val serializer = SparkEnv.get.closureSerializer.newInstance()
    ExecutorSuiteHelper.latches = new ExecutorSuiteHelper
    val resultFunc =
      (context: TaskContext, itr: Iterator[Int]) => {
        // latch1 tells the test that the task is running, so it can ask the metricsPoller
        // to poll; latch2 waits for the polling to be done
        ExecutorSuiteHelper.latches.latch1.countDown()
        ExecutorSuiteHelper.latches.latch2.await(5, TimeUnit.SECONDS)
        itr.size
      }
    val rdd = new RDD[Int](sc, Nil) {
      override def compute(split: Partition, context: TaskContext): Iterator[Int] = {
        Iterator(1)
      }
      override protected def getPartitions: Array[Partition] = {
        Array(new SimplePartition)
      }
    }
    val taskBinary = sc.broadcast(serializer.serialize((rdd, resultFunc)).array())
    val taskDescription = createResultTaskDescription(serializer, taskBinary, rdd, 0)

    val mockBackend = mock[ExecutorBackend]
    withExecutor("id", "localhost", SparkEnv.get) { executor =>
      executor.launchTask(mockBackend, taskDescription)

      // Ensure that the executor's metricsPoller is polled so that values are recorded for
      // the task metrics
      ExecutorSuiteHelper.latches.latch1.await(5, TimeUnit.SECONDS)
      executor.metricsPoller.poll()
      ExecutorSuiteHelper.latches.latch2.countDown()
      eventually(timeout(5.seconds), interval(10.milliseconds)) {
        assert(executor.numRunningTasks === 0)
      }
    }

    // Verify that peak values for task metrics get sent in the TaskResult
    val orderedMock = inOrder(mockBackend)
    val statusCaptor = ArgumentCaptor.forClass(classOf[ByteBuffer])
    orderedMock.verify(mockBackend)
      .statusUpdate(meq(0L), meq(TaskState.RUNNING), statusCaptor.capture())
    orderedMock.verify(mockBackend)
      .statusUpdate(meq(0L), meq(TaskState.FINISHED), statusCaptor.capture())
    val resultData = statusCaptor.getAllValues.get(1)
    val result = serializer.deserialize[DirectTaskResult[Int]](resultData)
    val taskMetrics = new ExecutorMetrics(result.metricPeaks)
    assert(taskMetrics.getMetricValue("JVMHeapMemory") > 0)
  }

  test("Send task executor metrics in TaskKilled") {
    val (taskFailedReason, _) = testFetchFailureHandling(false, true)
    assert(taskFailedReason.isInstanceOf[TaskKilled])
    val metrics = taskFailedReason.asInstanceOf[TaskKilled].metricPeaks.toArray
    val taskMetrics = new ExecutorMetrics(metrics)
    assert(taskMetrics.getMetricValue("JVMHeapMemory") > 0)
  }

  test("Send task executor metrics in ExceptionFailure") {
    val (taskFailedReason, _) = testFetchFailureHandling(true, true)
    assert(taskFailedReason.isInstanceOf[ExceptionFailure])
    val metrics = taskFailedReason.asInstanceOf[ExceptionFailure].metricPeaks.toArray
    val taskMetrics = new ExecutorMetrics(metrics)
    assert(taskMetrics.getMetricValue("JVMHeapMemory") > 0)
  }

  test("SPARK-34949: do not re-register BlockManager when executor is shutting down") {
    val reregisterInvoked = new AtomicBoolean(false)
    val mockBlockManager = mock[BlockManager]
    when(mockBlockManager.reregister()).thenAnswer { (_: InvocationOnMock) =>
      reregisterInvoked.getAndSet(true)
    }
    val conf = new SparkConf(false).setAppName("test").setMaster("local[2]")
    val mockEnv = createMockEnv(conf, new JavaSerializer(conf))
    when(mockEnv.blockManager).thenReturn(mockBlockManager)

    withExecutor("id", "localhost", mockEnv) { executor =>
      withMockHeartbeatReceiverRef(executor) { mockReceiverRef =>
        when(mockReceiverRef.askSync(any[Heartbeat], any[RpcTimeout])(any)).thenAnswer {
          (_: InvocationOnMock) => HeartbeatResponse(reregisterBlockManager = true)
        }
        val reportHeartbeat = PrivateMethod[Unit](Symbol("reportHeartBeat"))
        executor.invokePrivate(reportHeartbeat())
        assert(reregisterInvoked.get(), "BlockManager.reregister should be invoked " +
          "on HeartbeatResponse(reregisterBlockManager = true) when executor is not shutting down")

        reregisterInvoked.getAndSet(false)
        executor.stop()
        executor.invokePrivate(reportHeartbeat())
        assert(!reregisterInvoked.get(),
          "BlockManager.reregister should not be invoked when executor is shutting down")
      }
    }
  }

  test("SPARK-33587: isFatalError") {
    def errorInThreadPool(e: => Throwable): Throwable = {
      intercept[Throwable] {
        val taskPool = ThreadUtils.newDaemonFixedThreadPool(1, "test")
        try {
          val f = taskPool.submit(new java.util.concurrent.Callable[String] {
            override def call(): String = throw e
          })
          f.get()
        } finally {
          taskPool.shutdown()
        }
      }
    }

    def errorInGuavaCache(e: => Throwable): Throwable = {
      val cache = CacheBuilder.newBuilder()
        .build(new CacheLoader[String, String] {
          override def load(key: String): String = throw e
        })
      intercept[Throwable] {
        cache.get("test")
      }
    }

    def testThrowable(
        e: => Throwable,
        depthToCheck: Int,
        isFatal: Boolean): Unit = {
      import Executor.isFatalError
      // `e`'s depth is 1 so `depthToCheck` needs to be at least 3 to detect fatal errors.
      assert(isFatalError(e, depthToCheck) == (depthToCheck >= 1 && isFatal))
      // `e`'s depth is 2 so `depthToCheck` needs to be at least 3 to detect fatal errors.
      assert(isFatalError(errorInThreadPool(e), depthToCheck) == (depthToCheck >= 2 && isFatal))
      assert(isFatalError(errorInGuavaCache(e), depthToCheck) == (depthToCheck >= 2 && isFatal))
      assert(isFatalError(
        new SparkException("foo", e),
        depthToCheck) == (depthToCheck >= 2 && isFatal))
      // `e`'s depth is 3 so `depthToCheck` needs to be at least 3 to detect fatal errors.
      assert(isFatalError(
        errorInThreadPool(errorInGuavaCache(e)),
        depthToCheck) == (depthToCheck >= 3 && isFatal))
      assert(isFatalError(
        errorInGuavaCache(errorInThreadPool(e)),
        depthToCheck) == (depthToCheck >= 3 && isFatal))
      assert(isFatalError(
        new SparkException("foo", new SparkException("foo", e)),
        depthToCheck) == (depthToCheck >= 3 && isFatal))
    }

    for (depthToCheck <- 0 to 5) {
      testThrowable(new OutOfMemoryError(), depthToCheck, isFatal = true)
      testThrowable(new InterruptedException(), depthToCheck, isFatal = false)
      testThrowable(new RuntimeException("test"), depthToCheck, isFatal = false)
      testThrowable(new SparkOutOfMemoryError("test"), depthToCheck, isFatal = false)
    }

    // Verify we can handle the cycle in the exception chain
    val e1 = new Exception("test1")
    val e2 = new Exception("test2")
    e1.initCause(e2)
    e2.initCause(e1)
    for (depthToCheck <- 0 to 5) {
      testThrowable(e1, depthToCheck, isFatal = false)
      testThrowable(e2, depthToCheck, isFatal = false)
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

  private def createResultTaskDescription(
      serializer: SerializerInstance,
      taskBinary: Broadcast[Array[Byte]],
      rdd: RDD[Int],
      stageId: Int): TaskDescription = {
    val serializedTaskMetrics = serializer.serialize(TaskMetrics.registered).array()
    val task = new ResultTask(
      stageId = stageId,
      stageAttemptId = 0,
      taskBinary = taskBinary,
      partition = rdd.partitions(0),
      locs = Seq(),
      outputId = 0,
      localProperties = new Properties(),
      serializedTaskMetrics = serializedTaskMetrics
    )
    val serTask = serializer.serialize(task)
    createFakeTaskDescription(serTask)
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
      addedArchives = Map[String, Long](),
      properties = new Properties,
      cpus = 1,
      resources = immutable.Map[String, ResourceInformation](),
      serializedTask)
  }

  private def runTaskAndGetFailReason(taskDescription: TaskDescription): TaskFailedReason = {
    runTaskGetFailReasonAndExceptionHandler(taskDescription, false)._1
  }

  private def runTaskGetFailReasonAndExceptionHandler(
      taskDescription: TaskDescription,
      killTask: Boolean,
      poll: Boolean = false): (TaskFailedReason, UncaughtExceptionHandler) = {
    val mockBackend = mock[ExecutorBackend]
    val mockUncaughtExceptionHandler = mock[UncaughtExceptionHandler]
    val timedOut = new AtomicBoolean(false)

    withExecutor("id", "localhost", SparkEnv.get,
        uncaughtExceptionHandler = mockUncaughtExceptionHandler) { executor =>

      // the task will be launched in a dedicated worker thread
      executor.launchTask(mockBackend, taskDescription)
      if (killTask) {
        val killingThread = new Thread("kill-task") {
          override def run(): Unit = {
            // wait to kill the task until it has thrown a fetch failure
            if (ExecutorSuiteHelper.latches.latch1.await(10, TimeUnit.SECONDS)) {
              // now we can kill the task
              // but before that, ensure that the executor's metricsPoller is polled
              if (poll) {
                executor.metricsPoller.poll()
              }
              executor.killAllTasks(true, "Killed task, e.g. because of speculative execution")
            } else {
              timedOut.set(true)
            }
          }
        }
        killingThread.start()
      } else {
        if (ExecutorSuiteHelper.latches != null) {
          ExecutorSuiteHelper.latches.latch1.await(5, TimeUnit.SECONDS)
          if (poll) {
            executor.metricsPoller.poll()
          }
          ExecutorSuiteHelper.latches.latch2.countDown()
        }
      }
      eventually(timeout(5.seconds), interval(10.milliseconds)) {
        assert(executor.numRunningTasks === 0)
      }
      assert(!timedOut.get(), "timed out waiting to be ready to kill tasks")
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
          mapId = 0L,
          mapIndex = 0,
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

// NOTE: When instantiating this class, except with throwOOM = false and interrupt = false,
// ExecutorSuiteHelper.latches need to be set (not null).
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
          // Allow executor metrics to be polled (if necessary) before throwing the OOMError
          ExecutorSuiteHelper.latches.latch1.countDown()
          ExecutorSuiteHelper.latches.latch2.await(5, TimeUnit.SECONDS)
          // scalastyle:off throwerror
          throw new OutOfMemoryError("OOM while handling another exception")
          // scalastyle:on throwerror
        } else if (interrupt) {
          // make sure our test is setup correctly
          assert(TaskContext.get().asInstanceOf[TaskContextImpl].fetchFailed.isDefined)
          // signal we are ready for executor metrics to be polled (if necessary) and for
          // the task to get killed
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

// Helper for coordinating killing tasks as well as polling executor metrics
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
