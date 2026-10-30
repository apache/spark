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

import java.util.Properties
import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch, Semaphore, TimeUnit}
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

import org.apache.logging.log4j.Level
import org.apache.logging.log4j.core.LogEvent
import org.apache.logging.log4j.core.appender.AbstractAppender
import org.apache.logging.log4j.core.config.Property
import org.mockito.ArgumentMatchers.{any, eq => mockitoEq}
import org.mockito.Mockito._
import org.scalatest.BeforeAndAfter
import org.scalatest.concurrent.Eventually.{eventually, timeout}

import org.apache.spark._
import org.apache.spark.api.plugin.{DriverPlugin, ExecutorPlugin, SparkPlugin}
import org.apache.spark.executor.{Executor, TaskMetrics, TaskMetricsSuite}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.METRICS_CONF
import org.apache.spark.memory.TaskMemoryManager
import org.apache.spark.metrics.source.JvmSource
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.shuffle.FetchFailedException
import org.apache.spark.util._
import org.apache.spark.util.ArrayImplicits._

class TaskContextSuite extends SparkFunSuite with BeforeAndAfter with LocalSparkContext {

  test("provide metrics sources") {
    val filePath = getClass.getClassLoader.getResource("test_metrics_config.properties").getFile
    val conf = new SparkConf(loadDefaults = false)
      .set(METRICS_CONF, filePath)
    sc = new SparkContext("local", "test", conf)
    val rdd = sc.makeRDD(1 to 1)
    val result = sc.runJob(rdd, (tc: TaskContext, it: Iterator[Int]) => {
      tc.getMetricsSources("jvm").count {
        case source: JvmSource => true
        case _ => false
      }
    }).sum
    assert(result > 0)
  }

  test("calls TaskCompletionListener after failure") {
    TaskContextSuite.completed = false
    sc = new SparkContext("local", "test")
    val rdd = new RDD[String](sc, List()) {
      override def getPartitions = Array[Partition](StubPartition(0))
      override def compute(split: Partition, context: TaskContext) = {
        context.addTaskCompletionListener(new TaskCompletionListener {
          override def onTaskCompletion(context: TaskContext): Unit =
            TaskContextSuite.completed = true
        })
        sys.error("failed")
      }
    }
    val closureSerializer = SparkEnv.get.closureSerializer.newInstance()
    val func = (c: TaskContext, i: Iterator[String]) => i.next()
    val taskBinary = sc.broadcast(JavaUtils.bufferToArray(closureSerializer.serialize((rdd, func))))
    val task = new ResultTask[String, String](
      0, 0, taskBinary, rdd.partitions(0), 1, Seq.empty, 0,
      JobArtifactSet.getActiveOrDefault(sc), new Properties,
      closureSerializer.serialize(TaskMetrics.registered).array())
    intercept[RuntimeException] {
      task.run(0, 0, null, 1, null, Option.empty)
    }
    assert(TaskContextSuite.completed)
  }

  test("calls TaskFailureListeners after failure") {
    TaskContextSuite.lastError = null
    sc = new SparkContext("local", "test")
    val rdd = new RDD[String](sc, List()) {
      override def getPartitions = Array[Partition](StubPartition(0))
      override def compute(split: Partition, context: TaskContext) = {
        context.addTaskFailureListener((context, error) => TaskContextSuite.lastError = error)
        sys.error("damn error")
      }
    }
    val closureSerializer = SparkEnv.get.closureSerializer.newInstance()
    val func = (c: TaskContext, i: Iterator[String]) => i.next()
    val taskBinary = sc.broadcast(JavaUtils.bufferToArray(closureSerializer.serialize((rdd, func))))
    val task = new ResultTask[String, String](
      0, 0, taskBinary, rdd.partitions(0), 1, Seq.empty, 0,
      JobArtifactSet.getActiveOrDefault(sc), new Properties,
      closureSerializer.serialize(TaskMetrics.registered).array())
    intercept[RuntimeException] {
      task.run(0, 0, null, 1, null, Option.empty)
    }
    assert(TaskContextSuite.lastError.getMessage == "damn error")
  }

  test("all TaskCompletionListeners should be called even if some fail") {
    val context = TaskContext.empty()
    val listener = mock(classOf[TaskCompletionListener])
    context.addTaskCompletionListener(new TaskCompletionListener {
      override def onTaskCompletion(context: TaskContext): Unit = throw new Exception("blah")
    })
    context.addTaskCompletionListener(listener)
    context.addTaskCompletionListener(new TaskCompletionListener {
      override def onTaskCompletion(context: TaskContext): Unit = throw new Exception("blah")
    })

    intercept[TaskCompletionListenerException] {
      context.markTaskCompleted(None)
    }

    verify(listener, times(1)).onTaskCompletion(any())
  }

  test("all TaskFailureListeners should be called even if some fail") {
    val context = TaskContext.empty()
    val listener = mock(classOf[TaskFailureListener])
    context.addTaskFailureListener(new TaskFailureListener {
      override def onTaskFailure(context: TaskContext, error: Throwable): Unit =
        throw new Exception("exception in listener1")
    })
    context.addTaskFailureListener(listener)
    context.addTaskFailureListener(new TaskFailureListener {
      override def onTaskFailure(context: TaskContext, error: Throwable): Unit =
        throw new Exception("exception in listener3")
    })

    val e = intercept[TaskCompletionListenerException] {
      context.markTaskFailed(new Exception("exception in task"))
    }

    // Make sure listener 2 was called.
    verify(listener, times(1)).onTaskFailure(any(), any())

    // also need to check failure in TaskFailureListener does not mask earlier exception
    assert(e.getMessage.contains("exception in listener1"))
    assert(e.getMessage.contains("exception in listener3"))
    assert(e.getMessage.contains("exception in task"))
  }

  test("all TaskInterruptListeners should be called even if some fail") {
    val context = TaskContext.empty()
    val listener = mock(classOf[TaskInterruptListener])
    context.addTaskInterruptListener(new TaskInterruptListener {
      override def onTaskInterrupted(context: TaskContext, reason: String): Unit =
        throw new Exception("exception in listener1")
    })
    context.addTaskInterruptListener(listener)
    context.addTaskInterruptListener(new TaskInterruptListener {
      override def onTaskInterrupted(context: TaskContext, reason: String): Unit =
        throw new Exception("exception in listener3")
    })

    // Interrupt listener failures mark the task failed but are not thrown from `markInterrupted`
    // (kill / RPC thread must not see TaskCompletionListenerException).
    context.markInterrupted("test interrupt")
    assert(context.isFailed())

    verify(listener, times(1)).onTaskInterrupted(any(), mockitoEq("test interrupt"))

    val taskFailure = context.getTaskFailure.get
    assert(taskFailure.isInstanceOf[TaskCompletionListenerException])
    val e = taskFailure.asInstanceOf[TaskCompletionListenerException]
    assert(e.getMessage.contains("exception in listener1"))
    assert(e.getMessage.contains("exception in listener3"))
    assert(e.getMessage.contains("test interrupt"))
  }

  test("SPARK-60554: a failing TaskFailureListener does not escape markInterrupted") {
    val context = TaskContext.empty()
    val failureListenerCalls = new AtomicInteger(0)
    context.addTaskInterruptListener(new TaskInterruptListener {
      override def onTaskInterrupted(context: TaskContext, reason: String): Unit =
        throw new Exception("exception in interrupt listener")
    })
    context.addTaskFailureListener(new TaskFailureListener {
      override def onTaskFailure(context: TaskContext, error: Throwable): Unit = {
        failureListenerCalls.incrementAndGet()
        throw new Exception("exception in failure listener")
      }
    })

    // Neither the interrupt listener failure nor the failure listener failure it triggers may be
    // thrown from `markInterrupted` (kill / RPC thread must not see them).
    context.markInterrupted("test interrupt")
    assert(context.isInterrupted())
    assert(context.isFailed())
    assert(failureListenerCalls.get() == 1)

    // The task failure is the interrupt listener failure and keeps the failure listener failure.
    val taskFailure = context.getTaskFailure.get
    assert(taskFailure.isInstanceOf[TaskCompletionListenerException])
    assert(taskFailure.getMessage.contains("exception in interrupt listener"))
    assert(taskFailure.getSuppressed.exists(
      _.getMessage.contains("exception in failure listener")))
  }

  test("SPARK-60554: Task.kill interrupts the task thread even if interrupt and " +
    "failure listeners fail") {
    sc = new SparkContext("local", "test")
    val taskStarted = new CountDownLatch(1)
    val threadInterrupted = new CountDownLatch(1)
    val serializedTaskMetrics =
      SparkEnv.get.closureSerializer.newInstance().serialize(TaskMetrics.registered).array()
    val task = new Task[Int](0, 0, 0, 1, JobArtifactSet.emptyJobArtifactSet,
      serializedTaskMetrics = serializedTaskMetrics) {
      override def runTask(context: TaskContext): Int = {
        context.addTaskInterruptListener(new TaskInterruptListener {
          override def onTaskInterrupted(context: TaskContext, reason: String): Unit =
            throw new Exception("exception in interrupt listener")
        })
        context.addTaskFailureListener(new TaskFailureListener {
          override def onTaskFailure(context: TaskContext, error: Throwable): Unit =
            throw new Exception("exception in failure listener")
        })
        taskStarted.countDown()
        try {
          // Block until `Task.kill` interrupts this thread.
          new CountDownLatch(1).await(10, TimeUnit.SECONDS)
        } catch {
          case _: InterruptedException => threadInterrupted.countDown()
        }
        0
      }
    }
    val taskThread = new Thread(
      () => Utils.tryLogNonFatalError { task.run(0, 0, null, 1, null, Option.empty) },
      "task-kill-test")
    taskThread.start()
    try {
      assert(taskStarted.await(10, TimeUnit.SECONDS))
      task.kill(interruptThread = true, "test kill")
      assert(threadInterrupted.await(10, TimeUnit.SECONDS))
      assert(task.context.isInterrupted())
      assert(task.context.isFailed())
    } finally {
      taskThread.interrupt()
      taskThread.join(10000)
    }
  }

  test("SPARK-60567: all TaskInterruptListeners should be called even if logging a failure " +
    "fails") {
    val context = TaskContext.empty()
    val listener = mock(classOf[TaskInterruptListener])
    val first = new Exception("exception in listener1")
    context.addTaskInterruptListener(listener)
    // Added last, so it runs first.
    context.addTaskInterruptListener(new TaskInterruptListener {
      override def onTaskInterrupted(context: TaskContext, reason: String): Unit = throw first
    })

    val logFailure = new IllegalStateException("log failure")
    withLogAppender(new FailingAppender("Error in TaskInterruptListener", logFailure)) {
      context.markInterrupted("test interrupt")
    }
    verify(listener, times(1)).onTaskInterrupted(any(), mockitoEq("test interrupt"))
    assert(context.isFailed())
    val taskFailure = context.getTaskFailure.get
    assert(taskFailure.isInstanceOf[TaskCompletionListenerException])
    assert(taskFailure.getSuppressed.toSeq === Seq(first))
    // The failure to log is reported with the failure it was logging.
    assert(first.getSuppressed.length === 1)
    val causes = Iterator.iterate[Throwable](first.getSuppressed.head)(_.getCause)
    assert(causes.takeWhile(_ != null).contains(logFailure))
  }

  test("SPARK-60567: markInterrupted marks the task failed even if logging the listener " +
    "failures fails") {
    val context = TaskContext.empty()
    val failureListenerCalls = new AtomicInteger(0)
    val first = new Exception("exception in interrupt listener")
    context.addTaskInterruptListener(new TaskInterruptListener {
      override def onTaskInterrupted(context: TaskContext, reason: String): Unit = throw first
    })
    context.addTaskFailureListener(new TaskFailureListener {
      override def onTaskFailure(context: TaskContext, error: Throwable): Unit =
        failureListenerCalls.incrementAndGet()
    })

    val logFailure = new IllegalStateException("log failure")
    withLogAppender(new FailingAppender("Exception(s) in TaskInterruptListener(s)", logFailure)) {
      // The failure to log must not be thrown from `markInterrupted` (kill / RPC thread must not
      // see it).
      context.markInterrupted("test interrupt")
    }
    assert(context.isInterrupted())
    assert(context.isFailed())
    assert(failureListenerCalls.get() == 1)

    // The task failure is the aggregate failure of the interrupt listeners and reports the failure
    // to log it.
    val taskFailure = context.getTaskFailure.get
    assert(taskFailure.isInstanceOf[TaskCompletionListenerException])
    assert(taskFailure.getMessage.contains("exception in interrupt listener"))
    assert(taskFailure.getSuppressed.contains(first))
    assert(taskFailure.getSuppressed.exists { t =>
      Iterator.iterate[Throwable](t)(_.getCause).takeWhile(_ != null).contains(logFailure)
    })
  }

  test("SPARK-60567: Task.kill interrupts the task thread even if logging the listener " +
    "failures fails") {
    sc = new SparkContext("local", "test")
    val taskStarted = new CountDownLatch(1)
    val threadInterrupted = new CountDownLatch(1)
    val serializedTaskMetrics =
      SparkEnv.get.closureSerializer.newInstance().serialize(TaskMetrics.registered).array()
    val task = new Task[Int](0, 0, 0, 1, JobArtifactSet.emptyJobArtifactSet,
      serializedTaskMetrics = serializedTaskMetrics) {
      override def runTask(context: TaskContext): Int = {
        // Both listeners fail, so both the per-listener and the aggregate failure are logged.
        (1 to 2).foreach { i =>
          context.addTaskInterruptListener(new TaskInterruptListener {
            override def onTaskInterrupted(context: TaskContext, reason: String): Unit =
              throw new Exception(s"exception in interrupt listener $i")
          })
        }
        taskStarted.countDown()
        try {
          // Block until `Task.kill` interrupts this thread.
          new CountDownLatch(1).await(10, TimeUnit.SECONDS)
        } catch {
          case _: InterruptedException => threadInterrupted.countDown()
        }
        0
      }
    }
    val taskThread = new Thread(
      () => Utils.tryLogNonFatalError { task.run(0, 0, null, 1, null, Option.empty) },
      "task-kill-test")
    taskThread.start()
    try {
      assert(taskStarted.await(10, TimeUnit.SECONDS))
      withLogAppender(
        new FailingAppender("TaskInterruptListener", new IllegalStateException("log failure"))) {
        task.kill(interruptThread = true, "test kill")
      }
      assert(threadInterrupted.await(10, TimeUnit.SECONDS))
      assert(task.context.isInterrupted())
      assert(task.context.isFailed())
    } finally {
      taskThread.interrupt()
      taskThread.join(10000)
    }
  }

  test("immediately call an interrupt listener if the context is already interrupted") {
    var invocations = 0
    var lastReason: String = null
    val context = TaskContext.empty()
    context.markInterrupted("already interrupted")
    context.addTaskInterruptListener(new TaskInterruptListener {
      override def onTaskInterrupted(context: TaskContext, reason: String): Unit = {
        invocations += 1
        lastReason = reason
      }
    })
    assert(invocations == 1)
    assert(lastReason == "already interrupted")
    context.addTaskInterruptListener(new TaskInterruptListener {
      override def onTaskInterrupted(context: TaskContext, reason: String): Unit = {
        invocations += 1
      }
    })
    assert(invocations == 2)
  }

  test("SPARK-56330: task completion during interrupt listener execution") {
    val context = TaskContext.empty()
    val completionListener = mock(classOf[TaskCompletionListener])
    context.addTaskCompletionListener(completionListener)

    // Add a task interrupt listener that blocks until released.
    val interruptListenerStarted = new Semaphore(0)
    val interruptListenerRelease = new Semaphore(0)
    context.addTaskInterruptListener(new TaskInterruptListener {
      override def onTaskInterrupted(context: TaskContext, reason: String): Unit = {
        interruptListenerStarted.release()
        interruptListenerRelease.acquire()
      }
    })

    // Interrupt the task from a separate thread and wait until the interrupt listener starts.
    val interruptThread = startDaemonThread("interrupt")(context.markInterrupted("test interrupt"))
    try {
      interruptListenerStarted.acquire()

      // While the interrupt listener is running on the interrupt thread, mark the task completed
      // on another thread. With the dedicated interrupt listener loop, this must NOT be blocked.
      // If `invokeListeners()` were shared between interrupt and completion listeners, the
      // interrupt thread would hold `listenerInvocationThread`, and the completion would wait
      // for it, so the join below would time out.
      joinThread(startDaemonThread("completion")(context.markTaskCompleted(None)))
      verify(completionListener, times(1)).onTaskCompletion(any())
    } finally {
      // Release the interrupt listener and join the interrupt thread.
      interruptListenerRelease.release()
    }
    joinThread(interruptThread)
  }

  test("FailureListener throws after task body fails") {
    val context = TaskContext.empty()
    val listenerCalls = ArrayBuffer.empty[String]
    context.addTaskFailureListener(new TaskFailureListener {
      override def onTaskFailure(context: TaskContext, error: Throwable): Unit = {
        listenerCalls += "bad failure"
        throw new Exception("bad failure listener")
      }
    })
    context.addTaskCompletionListener(new TaskCompletionListener {
      override def onTaskCompletion(context: TaskContext): Unit = {
        listenerCalls += "completion listener"
      }
    })

    val e = intercept[TaskContextSuite.FakeTaskFailureException] {
      context.runTaskWithListeners(new Task[Int](0, 0, 0, 1, JobArtifactSet.emptyJobArtifactSet,
        serializedTaskMetrics = Array.empty) {
        override def runTask(context: TaskContext): Int = {
          throw new TaskContextSuite.FakeTaskFailureException
        }
      })
    }
    assert(listenerCalls.toSeq === Seq("bad failure", "completion listener"))
    assert(Utils.exceptionString(e).contains("bad failure listener"))
  }

  test("CompletionListener throws after task body fails") {
    val context = TaskContext.empty()
    val listenerCalls = ArrayBuffer.empty[String]
    context.addTaskFailureListener(new TaskFailureListener {
      override def onTaskFailure(context: TaskContext, error: Throwable): Unit = {
        listenerCalls += "failure listener"
      }
    })
    context.addTaskCompletionListener(new TaskCompletionListener {
      override def onTaskCompletion(context: TaskContext): Unit = {
        listenerCalls += "other completion"
      }
    })
    context.addTaskCompletionListener(new TaskCompletionListener {
      override def onTaskCompletion(context: TaskContext): Unit = {
        listenerCalls += "bad completion"
        throw new Exception("bad completion listener")
      }
    })

    val e = intercept[TaskContextSuite.FakeTaskFailureException] {
      context.runTaskWithListeners(new Task[Int](0, 0, 0, 1, JobArtifactSet.emptyJobArtifactSet,
        serializedTaskMetrics = Array.empty) {
        override def runTask(context: TaskContext): Int = {
          throw new TaskContextSuite.FakeTaskFailureException
        }
      })
    }
    assert(listenerCalls.toSeq === Seq("failure listener", "bad completion", "other completion"))
    assert(Utils.exceptionString(e).contains("bad completion listener"))
  }

  test("CompletionListener throws after task body succeeds") {
    val context = TaskContext.empty()
    val listenerCalls = ArrayBuffer.empty[String]
    context.addTaskFailureListener(new TaskFailureListener {
      override def onTaskFailure(context: TaskContext, error: Throwable): Unit = {
        listenerCalls += "failure listener"
      }
    })
    context.addTaskCompletionListener(new TaskCompletionListener {
      override def onTaskCompletion(context: TaskContext): Unit = {
        listenerCalls += "other completion"
      }
    })
    context.addTaskCompletionListener(new TaskCompletionListener {
      override def onTaskCompletion(context: TaskContext): Unit = {
        listenerCalls += "bad completion"
        throw new Exception("bad completion listener")
      }
    })

    val e = intercept[TaskCompletionListenerException] {
      context.runTaskWithListeners(new Task[Int](0, 0, 0, 1, JobArtifactSet.emptyJobArtifactSet,
        serializedTaskMetrics = Array.empty) {
        override def runTask(context: TaskContext): Int = 0
      })
    }
    assert(listenerCalls.toSeq === Seq("bad completion", "failure listener", "other completion"))
    assert(Utils.exceptionString(e).contains("bad completion listener"))
  }

  test("FailureListener throws after task body succeeds and CompletionListener fails") {
    val context = TaskContext.empty()
    val listenerCalls = ArrayBuffer.empty[String]
    context.addTaskFailureListener(new TaskFailureListener {
      override def onTaskFailure(context: TaskContext, error: Throwable): Unit = {
        listenerCalls += "failure listener"
        throw new Exception("bad failure listener")
      }
    })
    context.addTaskCompletionListener(new TaskCompletionListener {
      override def onTaskCompletion(context: TaskContext): Unit = {
        listenerCalls += "other completion"
      }
    })
    context.addTaskCompletionListener(new TaskCompletionListener {
      override def onTaskCompletion(context: TaskContext): Unit = {
        listenerCalls += "bad completion"
        throw new Exception("bad completion listener")
      }
    })

    val e = intercept[TaskCompletionListenerException] {
      context.runTaskWithListeners(new Task[Int](0, 0, 0, 1, JobArtifactSet.emptyJobArtifactSet,
        serializedTaskMetrics = Array.empty) {
        override def runTask(context: TaskContext): Int = 0
      })
    }
    assert(listenerCalls.toSeq === Seq("bad completion", "failure listener", "other completion"))
    val msg = Utils.exceptionString(e)
    assert(msg.contains("bad failure listener"))
    assert(msg.contains("bad completion listener"))
  }

  test("CompletionListener throws after task body succeeds and CompletionListener fails") {
    val context = TaskContext.empty()
    val listenerCalls = ArrayBuffer.empty[String]
    context.addTaskFailureListener(new TaskFailureListener {
      override def onTaskFailure(context: TaskContext, error: Throwable): Unit = {
        listenerCalls += "failure listener"
      }
    })
    context.addTaskCompletionListener(new TaskCompletionListener {
      override def onTaskCompletion(context: TaskContext): Unit = {
        listenerCalls += "other completion"
        throw new Exception("second bad completion listener")
      }
    })
    context.addTaskCompletionListener(new TaskCompletionListener {
      override def onTaskCompletion(context: TaskContext): Unit = {
        listenerCalls += "bad completion"
        throw new Exception("first bad completion listener")
      }
    })

    val e = intercept[TaskCompletionListenerException] {
      context.runTaskWithListeners(new Task[Int](0, 0, 0, 1, JobArtifactSet.emptyJobArtifactSet,
        serializedTaskMetrics = Array.empty) {
        override def runTask(context: TaskContext): Int = 0
      })
    }
    assert(listenerCalls.toSeq === Seq("bad completion", "failure listener", "other completion"))
    val msg = Utils.exceptionString(e)
    assert(msg.contains("first bad completion listener"))
    assert(msg.contains("second bad completion listener"))
  }

  test("CompletionListener throws after task body fails and FailureListener fails") {
    val context = TaskContext.empty()
    val listenerCalls = ArrayBuffer.empty[String]
    context.addTaskFailureListener(new TaskFailureListener {
      override def onTaskFailure(context: TaskContext, error: Throwable): Unit = {
        listenerCalls += "bad failure"
        throw new Exception("bad failure listener")
      }
    })
    context.addTaskCompletionListener(new TaskCompletionListener {
      override def onTaskCompletion(context: TaskContext): Unit = {
        listenerCalls += "other completion"
      }
    })
    context.addTaskCompletionListener(new TaskCompletionListener {
      override def onTaskCompletion(context: TaskContext): Unit = {
        listenerCalls += "bad completion"
        throw new Exception("bad completion listener")
      }
    })

    val e = intercept[TaskContextSuite.FakeTaskFailureException] {
      context.runTaskWithListeners(new Task[Int](0, 0, 0, 1, JobArtifactSet.emptyJobArtifactSet,
        serializedTaskMetrics = Array.empty) {
        override def runTask(context: TaskContext): Int = {
          throw new TaskContextSuite.FakeTaskFailureException
        }
      })
    }
    assert(listenerCalls.toSeq === Seq("bad failure", "bad completion", "other completion"))
    val msg = Utils.exceptionString(e)
    assert(msg.contains("bad failure listener"))
    assert(msg.contains("bad completion listener"))
  }

  test("TaskContext.attemptNumber should return attempt number, not task id (SPARK-4014)") {
    sc = new SparkContext("local[1,2]", "test")  // use maxRetries = 2 because we test failed tasks
    // Check that attemptIds are 0 for all tasks' initial attempts
    val attemptIds = sc.parallelize(Seq(1, 2), 2).mapPartitions { iter =>
      Seq(TaskContext.get().attemptNumber()).iterator
    }.collect()
    assert(attemptIds.toSet === Set(0))

    // Test a job with failed tasks
    val attemptIdsWithFailedTask = sc.parallelize(Seq(1, 2), 2).mapPartitions { iter =>
      val attemptId = TaskContext.get().attemptNumber()
      if (iter.next() == 1 && attemptId == 0) {
        throw new Exception("First execution of task failed")
      }
      Seq(attemptId).iterator
    }.collect()
    assert(attemptIdsWithFailedTask.toSet === Set(0, 1))
  }

  test("TaskContext.stageAttemptNumber getter") {
    sc = new SparkContext("local[1,2]", "test")

    // Check stageAttemptNumbers are 0 for initial stage
    val stageAttemptNumbers = sc.parallelize(Seq(1, 2), 2).mapPartitions { _ =>
      Seq(TaskContext.get().stageAttemptNumber()).iterator
    }.collect()
    assert(stageAttemptNumbers.toSet === Set(0))

    // Check stageAttemptNumbers that are resubmitted when tasks have FetchFailedException
    val stageAttemptNumbersWithFailedStage =
      sc.parallelize(Seq(1, 2, 3, 4), 4).repartition(1).mapPartitions { _ =>
      val stageAttemptNumber = TaskContext.get().stageAttemptNumber()
      if (stageAttemptNumber < 2) {
        // Throw FetchFailedException to explicitly trigger stage resubmission. A normal exception
        // will only trigger task resubmission in the same stage.
        throw new FetchFailedException(null, 0, 0L, 0, 0, "Fake")
      }
      Seq(stageAttemptNumber).iterator
    }.collect()

    assert(stageAttemptNumbersWithFailedStage.toSet === Set(2))
  }

  test("TaskContext.get.numPartitions getter") {
    sc = new SparkContext("local[1,2]", "test")

    for (numPartitions <- 1 to 10) {
      val numPartitionsFromContext = sc.parallelize(1 to 1000, numPartitions)
        .mapPartitions { _ =>
          Seq(TaskContext.get().numPartitions()).iterator
        }.collect()
      assert(numPartitionsFromContext.toSet === Set(numPartitions),
        s"numPartitions = $numPartitions")
    }

    for (numPartitions <- 1 to 10) {
      val numPartitionsFromContext = sc.parallelize(1 to 1000, 2).repartition(numPartitions)
        .mapPartitions { _ =>
          Seq(TaskContext.get().numPartitions()).iterator
        }.collect()
      assert(numPartitionsFromContext.toSet === Set(numPartitions),
        s"numPartitions = $numPartitions")
    }
  }

  test("accumulators are updated on exception failures") {
    // This means use 1 core and 4 max task failures
    sc = new SparkContext("local[1,4]", "test")
    // Create 2 accumulators, one that counts failed values and another that doesn't
    val acc1 = AccumulatorSuite.createLongAccum("x", true)
    val acc2 = AccumulatorSuite.createLongAccum("y", false)
    // Fail first 3 attempts of every task. This means each task should be run 4 times.
    sc.parallelize(1 to 10, 10).map { i =>
      acc1.add(1)
      acc2.add(1)
      if (TaskContext.get().attemptNumber() <= 2) {
        throw new Exception("you did something wrong")
      } else {
        0
      }
    }.count()
    // The one that counts failed values should be 4x the one that didn't,
    // since we ran each task 4 times
    assert(AccumulatorContext.get(acc1.id).get.value === 40L)
    assert(AccumulatorContext.get(acc2.id).get.value === 10L)
  }

  test("failed tasks collect only accumulators whose values count during failures") {
    sc = new SparkContext("local", "test")
    val acc1 = AccumulatorSuite.createLongAccum("x", false)
    val acc2 = AccumulatorSuite.createLongAccum("y", true)
    acc1.add(1)
    acc2.add(1)
    // Create a dummy task. We won't end up running this; we just want to collect
    // accumulator updates from it.
    val taskMetrics = TaskMetrics.empty
    val task = new Task[Int](0, 0, 0, 1, JobArtifactSet.getActiveOrDefault(sc)) {
      context = new TaskContextImpl(0, 0, 0, 0L, 0, 1,
        new TaskMemoryManager(SparkEnv.get.memoryManager, 0L),
        new Properties,
        SparkEnv.get.metricsSystem,
        taskMetrics)
      taskMetrics.registerAccumulator(acc1)
      taskMetrics.registerAccumulator(acc2)
      override def runTask(tc: TaskContext): Int = 0
    }
    // First, simulate task success. This should give us all the accumulators.
    val accumUpdates1 = task.collectAccumulatorUpdates(taskFailed = false)
    TaskMetricsSuite.assertUpdatesEquals(accumUpdates1.takeRight(2), Seq(acc1, acc2))
    // Now, simulate task failures. This should give us only the accums that count failed values.
    val accumUpdates2 = task.collectAccumulatorUpdates(taskFailed = true)
    TaskMetricsSuite.assertUpdatesEquals(accumUpdates2.takeRight(1), Seq(acc2))
  }

  test("only updated internal accumulators will be sent back to driver") {
    sc = new SparkContext("local", "test")
    // Create a dummy task. We won't end up running this; we just want to collect
    // accumulator updates from it.
    val taskMetrics = TaskMetrics.registered
    val task = new Task[Int](0, 0, 0, 1, JobArtifactSet.getActiveOrDefault(sc)) {
      context = new TaskContextImpl(0, 0, 0, 0L, 0, 1,
        new TaskMemoryManager(SparkEnv.get.memoryManager, 0L),
        new Properties,
        SparkEnv.get.metricsSystem,
        taskMetrics)
      taskMetrics.incMemoryBytesSpilled(10)
      override def runTask(tc: TaskContext): Int = 0
    }
    val updatedAccums = task.collectAccumulatorUpdates()
    assert(updatedAccums.length == 2)
    // the RESULT_SIZE accumulator will be sent back anyway.
    assert(updatedAccums(0).name == Some(InternalAccumulator.RESULT_SIZE))
    assert(updatedAccums(0).value == 0)
    assert(updatedAccums(1).name == Some(InternalAccumulator.MEMORY_BYTES_SPILLED))
    assert(updatedAccums(1).value == 10)
  }

  test("localProperties are propagated to executors correctly") {
    sc = new SparkContext("local", "test")
    sc.setLocalProperty("testPropKey", "testPropValue")
    val res = sc.parallelize(Array(1).toImmutableArraySeq, 1).map(i => i).map(i => {
      val inTask = TaskContext.get().getLocalProperty("testPropKey")
      val inDeser = Executor.taskDeserializationProps.get().getProperty("testPropKey")
      s"$inTask,$inDeser"
    }).collect()
    assert(res === Array("testPropValue,testPropValue"))
  }

  test("immediately call a completion listener if the context is completed") {
    var invocations = 0
    val context = TaskContext.empty()
    context.markTaskCompleted(None)
    context.addTaskCompletionListener(new TaskCompletionListener {
      override def onTaskCompletion(context: TaskContext): Unit =
        invocations += 1
    })
    assert(invocations == 1)
    context.markTaskCompleted(None)
    assert(invocations == 1)
  }

  test("immediately call a failure listener if the context has failed") {
    var invocations = 0
    var lastError: Throwable = null
    val error = new RuntimeException
    val context = TaskContext.empty()
    context.markTaskFailed(error)
    context.addTaskFailureListener(new TaskFailureListener {
      override def onTaskFailure(context: TaskContext, e: Throwable): Unit = {
        lastError = e
        invocations += 1
      }
    })
    assert(lastError == error)
    assert(invocations == 1)
    context.markTaskFailed(error)
    assert(lastError == error)
    assert(invocations == 1)
  }

  test("TaskCompletionListenerException.getMessage should include previousError") {
    val listenerErrorMessage = "exception in listener"
    val taskErrorMessage = "exception in task"
    val e = new TaskCompletionListenerException(
      Seq(listenerErrorMessage),
      Some(new RuntimeException(taskErrorMessage)))
    assert(e.getMessage.contains(listenerErrorMessage) && e.getMessage.contains(taskErrorMessage))
  }

  test("all TaskCompletionListeners should be called even if some fail or a task") {
    val context = TaskContext.empty()
    val listener = mock(classOf[TaskCompletionListener])
    context.addTaskCompletionListener(new TaskCompletionListener {
      override def onTaskCompletion(context: TaskContext): Unit =
        throw new Exception("exception in listener1")
    })
    context.addTaskCompletionListener(listener)
    context.addTaskCompletionListener(new TaskCompletionListener {
      override def onTaskCompletion(context: TaskContext): Unit =
        throw new Exception("exception in listener3")
    })

    val e = intercept[TaskCompletionListenerException] {
      context.markTaskCompleted(Some(new Exception("exception in task")))
    }

    // Make sure listener 2 was called.
    verify(listener, times(1)).onTaskCompletion(any())

    // also need to check failure in TaskCompletionListener does not mask earlier exception
    assert(e.getMessage.contains("exception in listener1"))
    assert(e.getMessage.contains("exception in listener3"))
    assert(e.getMessage.contains("exception in task"))
  }

  test("listener registers another listener (reentrancy)") {
    val context = TaskContext.empty()
    var invocations = 0
    val simpleListener = new TaskCompletionListener {
      override def onTaskCompletion(context: TaskContext): Unit = {
        invocations += 1
      }
    }

    // Create a listener that registers another listener.
    val reentrantListener = new TaskCompletionListener {
      override def onTaskCompletion(context: TaskContext): Unit = {
        context.addTaskCompletionListener(simpleListener)
        invocations += 1
      }
    }
    context.addTaskCompletionListener(reentrantListener)

    // Ensure the listener can execute without encountering deadlock.
    assert(invocations == 0)
    context.markTaskCompleted(None)
    assert(invocations == 2)
  }

  test("listener registers another listener using a second thread") {
    val context = TaskContext.empty()
    val invocations = new AtomicInteger(0)
    val simpleListener = new TaskCompletionListener {
      override def onTaskCompletion(context: TaskContext): Unit = {
        invocations.getAndIncrement()
      }
    }

    // Create a listener that registers another listener using a second thread.
    val multithreadedListener = new TaskCompletionListener {
      override def onTaskCompletion(context: TaskContext): Unit = {
        val thread = startDaemonThread("adder")(context.addTaskCompletionListener(simpleListener))
        invocations.getAndIncrement()
        joinThread(thread)
      }
    }
    context.addTaskCompletionListener(multithreadedListener)

    // Ensure the listener can execute without encountering deadlock.
    assert(invocations.get() == 0)
    context.markTaskCompleted(None)
    assert(invocations.get() == 2)
  }

  test("listeners registered from different threads are called sequentially") {
    val context = TaskContext.empty()
    val invocations = new AtomicInteger(0)
    val numRunningListeners = new AtomicInteger(0)

    // Create a listener that will throw if more than one instance is running at the same time.
    val registerExclusiveListener = new Runnable {
      override def run(): Unit = {
        context.addTaskCompletionListener(new TaskCompletionListener {
          override def onTaskCompletion(context: TaskContext): Unit = {
            if (numRunningListeners.getAndIncrement() != 0) throw new Exception()
            Thread.sleep(100)
            if (numRunningListeners.decrementAndGet() != 0) throw new Exception()
            invocations.getAndIncrement()
          }
        })
      }
    }

    // Register it multiple times from different threads before and after the task completes.
    assert(invocations.get() == 0)
    assert(numRunningListeners.get() == 0)
    val thread1 = new Thread(registerExclusiveListener)
    val thread2 = new Thread(registerExclusiveListener)
    thread1.start()
    thread2.start()
    thread1.join()
    thread2.join()
    assert(invocations.get() == 0)
    context.markTaskCompleted(None)
    assert(invocations.get() == 2)
    val thread3 = new Thread(registerExclusiveListener)
    val thread4 = new Thread(registerExclusiveListener)
    thread3.start()
    thread4.start()
    thread3.join()
    thread4.join()
    assert(invocations.get() == 4)
    assert(numRunningListeners.get() == 0)
  }

  test("listeners registered from same thread are called in reverse order") {
    val context = TaskContext.empty()
    val invocationOrder = ArrayBuffer.empty[String]

    // Create listeners that log an id to `invocationOrder` when they are invoked.
    def makeLoggingListener(id: String): TaskCompletionListener = new TaskCompletionListener {
      override def onTaskCompletion(context: TaskContext): Unit = {
        invocationOrder += id
      }
    }
    context.addTaskCompletionListener(makeLoggingListener("A"))
    context.addTaskCompletionListener(makeLoggingListener("B"))
    context.addTaskCompletionListener(makeLoggingListener("C"))

    // Ensure the listeners are called in reverse order of registration, except when they are called
    // after the task is complete.
    assert(invocationOrder === Seq.empty)
    context.markTaskCompleted(None)
    assert(invocationOrder === Seq("C", "B", "A"))
    context.addTaskCompletionListener(makeLoggingListener("D"))
    assert(invocationOrder === Seq("C", "B", "A", "D"))
  }

  test("SPARK-60113: pre-completion listeners run before all completion listeners") {
    val context = TaskContext.empty()
    val invocationOrder = ArrayBuffer.empty[String]

    def makeLoggingListener(id: String): TaskCompletionListener = new TaskCompletionListener {
      override def onTaskCompletion(context: TaskContext): Unit = {
        invocationOrder += id
      }
    }
    context.addTaskCompletionListener(makeLoggingListener("A"))
    context.addTaskPreCompletionListener(makeLoggingListener("P1"))
    context.addTaskPreCompletionListener(makeLoggingListener("P2"))
    // Added after the pre-completion listeners, e.g. lazily by the thread they stop. Had P1 and
    // P2 been added with addTaskCompletionListener, B would run before them.
    context.addTaskCompletionListener(makeLoggingListener("B"))

    context.markTaskCompleted(None)
    assert(invocationOrder === Seq("P2", "P1", "B", "A"))
    // Called immediately once the task has completed, like completion listeners.
    context.addTaskPreCompletionListener(makeLoggingListener("P3"))
    assert(invocationOrder === Seq("P2", "P1", "B", "A", "P3"))
  }

  test("SPARK-60113: completion listeners run even if a pre-completion listener fails") {
    val context = TaskContext.empty()
    val listener = mock(classOf[TaskCompletionListener])
    context.addTaskCompletionListener(listener)
    context.addTaskPreCompletionListener(new TaskCompletionListener {
      override def onTaskCompletion(context: TaskContext): Unit = throw new Exception("pre")
    })

    val e = intercept[TaskCompletionListenerException] {
      context.markTaskCompleted(None)
    }
    assert(e.getMessage.contains("pre"))
    verify(listener, times(1)).onTaskCompletion(any())
    assert(context.isFailed())
  }

  test("SPARK-60113: failures of pre-completion and completion listeners are both reported") {
    val context = TaskContext.empty()
    val post = new Exception("post")
    val pre = new Exception("pre")
    context.addTaskCompletionListener(new TaskCompletionListener {
      override def onTaskCompletion(context: TaskContext): Unit = throw post
    })
    context.addTaskPreCompletionListener(new TaskCompletionListener {
      override def onTaskCompletion(context: TaskContext): Unit = throw pre
    })

    val e = intercept[TaskCompletionListenerException] {
      context.markTaskCompleted(None)
    }
    assert(e.getMessage.contains("pre"))
    assert(e.getMessage.contains("post"))
    // The completion listener's own failure, not the exception that wraps it.
    assert(e.getSuppressed.toSeq === Seq(pre, post))
  }

  test("SPARK-60113: a pre-completion listener added after completion runs as a completion one") {
    val context = TaskContext.empty()
    val invocations = new ConcurrentLinkedQueue[(String, Thread)]()
    context.addTaskCompletionListener(loggingListener(invocations, "A"))
    context.addTaskCompletionListener(new TaskCompletionListener {
      override def onTaskCompletion(context: TaskContext): Unit = {
        invocations.add(("B", Thread.currentThread()))
        // Added from a second thread while this thread runs the completion listeners: it is
        // queued like a completion listener, and this thread runs it next, after this one.
        val thread = new Thread(() => {
          context.addTaskPreCompletionListener(loggingListener(invocations, "P"))
        })
        thread.setDaemon(true)
        thread.start()
        thread.join(TimeUnit.MINUTES.toMillis(1))
        assert(!thread.isAlive)
        assert(invocations.asScala.toSeq === Seq(("B", Thread.currentThread())))
      }
    })

    context.markTaskCompleted(None)
    val completing = Thread.currentThread()
    assert(invocations.asScala.toSeq ===
      Seq(("B", completing), ("P", completing), ("A", completing)))
    // Added once all listeners have run: it runs right away on the adding thread.
    context.addTaskPreCompletionListener(loggingListener(invocations, "Q"))
    assert(invocations.asScala.toSeq.last === (("Q", completing)))
  }

  test("SPARK-60113: a failing pre-completion listener added after completion fails the task") {
    val context = TaskContext.empty()
    context.markTaskCompleted(None)
    val error = new Exception("late pre-completion listener")
    val e = intercept[TaskCompletionListenerException] {
      context.addTaskPreCompletionListener(new TaskCompletionListener {
        override def onTaskCompletion(context: TaskContext): Unit = throw error
      })
    }
    assert(e.getMessage.contains("late pre-completion listener"))
    assert(e.getSuppressed.toSeq === Seq(error))
    assert(context.isFailed())
  }

  test("SPARK-60113: listeners added while pre-completion listeners run are queued") {
    val context = TaskContext.empty()
    val invocations = new ConcurrentLinkedQueue[(String, Thread)]()
    context.addTaskCompletionListener(loggingListener(invocations, "A"))
    val completion = completeWhilePreCompletionListenerBlocks(context) {
      // Like the writer adding listeners lazily while the pre-completion listener waits for it:
      // the task has not completed yet, so these calls only queue them, and the adding thread
      // cannot start running the listeners.
      context.addTaskCompletionListener(loggingListener(invocations, "B"))
      context.addTaskPreCompletionListener(loggingListener(invocations, "P"))
      assert(!context.isCompleted())
      assert(invocations.isEmpty)
    }
    assert(context.isCompleted())
    assert(invocations.asScala.toSeq ===
      Seq(("P", completion), ("B", completion), ("A", completion)))
  }

  test("SPARK-60113: failure listeners run while pre-completion listeners run") {
    val context = TaskContext.empty()
    val failureListenerThread = new AtomicReference[Thread]()
    context.addTaskFailureListener(new TaskFailureListener {
      override def onTaskFailure(context: TaskContext, error: Throwable): Unit =
        failureListenerThread.set(Thread.currentThread())
    })
    completeWhilePreCompletionListenerBlocks(context) {
      // E.g. a kill whose interrupt listener fails while the pre-completion listener waits for a
      // writer. This thread is not held up by the waiting listener and runs the failure listeners.
      context.markTaskFailed(new Exception("failed elsewhere"))
      assert(failureListenerThread.get eq Thread.currentThread())
    }
  }

  test("SPARK-60556: listeners still run if logging a pre-completion listener's failure fails") {
    val context = TaskContext.empty()
    val invocationOrder = ArrayBuffer.empty[String]

    def makeLoggingListener(id: String): TaskCompletionListener = new TaskCompletionListener {
      override def onTaskCompletion(context: TaskContext): Unit = {
        invocationOrder += id
      }
    }
    val pre = new Exception("pre")
    context.addTaskCompletionListener(makeLoggingListener("A"))
    context.addTaskPreCompletionListener(makeLoggingListener("P2"))
    // Added last, so it runs first.
    context.addTaskPreCompletionListener(new TaskCompletionListener {
      override def onTaskCompletion(context: TaskContext): Unit = throw pre
    })

    val logFailure = new IllegalStateException("log failure")
    withLogAppender(new FailingAppender("Error in TaskPreCompletionListener", logFailure)) {
      val e = intercept[TaskCompletionListenerException] {
        context.markTaskCompleted(None)
      }
      assert(e.getSuppressed.toSeq === Seq(pre))
      // The failure to log is reported with the failure it was logging.
      assert(pre.getSuppressed.length === 1)
      val causes = Iterator.iterate[Throwable](pre.getSuppressed.head)(_.getCause)
      assert(causes.takeWhile(_ != null).contains(logFailure))
    }
    // The pre-completion listener after the failing one still runs, and before the completion one.
    assert(invocationOrder === Seq("P2", "A"))
    assert(context.isCompleted())
  }

  test("SPARK-60556: completion listeners still run if logging a listener's failure fails") {
    val context = TaskContext.empty()
    val invocations = new ConcurrentLinkedQueue[(String, Thread)]()
    val first = new Exception("first")
    context.addTaskCompletionListener(loggingListener(invocations, "B"))
    // Added last, so it runs first.
    context.addTaskCompletionListener(new TaskCompletionListener {
      override def onTaskCompletion(context: TaskContext): Unit = throw first
    })

    val logFailure = new IllegalStateException("log failure")
    withLogAppender(new FailingAppender("Error in TaskCompletionListener", logFailure)) {
      val e = intercept[TaskCompletionListenerException] {
        context.markTaskCompleted(None)
      }
      assert(e.getSuppressed.toSeq === Seq(first))
      // The failure to log is reported with the failure it was logging.
      assert(first.getSuppressed.length === 1)
    }
    assert(invocations.asScala.map(_._1).toSeq === Seq("B"))
    // This thread no longer counts as the one invoking listeners: a listener added now runs
    // right away.
    context.addTaskCompletionListener(loggingListener(invocations, "C"))
    assert(invocations.asScala.map(_._1).toSeq === Seq("B", "C"))
  }

  test("SPARK-60556: completion listeners run if logging a failure listener's failure fails") {
    val context = TaskContext.empty()
    val invocations = new ConcurrentLinkedQueue[(String, Thread)]()
    context.addTaskCompletionListener(loggingListener(invocations, "A"))
    context.addTaskFailureListener(new TaskFailureListener {
      override def onTaskFailure(context: TaskContext, error: Throwable): Unit =
        throw new Exception("failure listener")
    })
    // Its failure runs the failure listener, which fails too.
    context.addTaskPreCompletionListener(new TaskCompletionListener {
      override def onTaskCompletion(context: TaskContext): Unit = throw new Exception("pre")
    })

    val logFailure = new IllegalStateException("log failure")
    withLogAppender(new FailingAppender("Error in TaskFailureListener", logFailure)) {
      intercept[TaskCompletionListenerException] {
        context.markTaskCompleted(None)
      }
    }
    assert(invocations.asScala.map(_._1).toSeq === Seq("A"))
    assert(context.isCompleted())
  }

  /** A completion listener that records its id and the thread it runs on in `invocations`. */
  private def loggingListener(
      invocations: ConcurrentLinkedQueue[(String, Thread)],
      id: String): TaskCompletionListener = new TaskCompletionListener {
    override def onTaskCompletion(context: TaskContext): Unit = {
      invocations.add((id, Thread.currentThread()))
    }
  }

  /**
   * Marks `context` completed on a new thread, runs `body` while a pre-completion listener of it
   * blocks, like the one that waits for a pipelined Python UDF writer to exit, and returns that
   * thread once the completion has finished.
   */
  private def completeWhilePreCompletionListenerBlocks(context: TaskContextImpl)(
      body: => Unit): Thread = {
    val inPreCompletion = new CountDownLatch(1)
    val release = new CountDownLatch(1)
    context.addTaskPreCompletionListener(new TaskCompletionListener {
      override def onTaskCompletion(context: TaskContext): Unit = {
        inPreCompletion.countDown()
        release.await(1, TimeUnit.MINUTES)
      }
    })
    val completion = new Thread(() => context.markTaskCompleted(None))
    completion.setDaemon(true)
    completion.start()
    try {
      assert(inPreCompletion.await(1, TimeUnit.MINUTES))
      body
    } finally {
      release.countDown()
    }
    completion.join(TimeUnit.MINUTES.toMillis(1))
    assert(!completion.isAlive)
    completion
  }

  /** A daemon thread that records what `body` throws, for `joinThread` to check. */
  private class DaemonThread(name: String, body: => Unit) extends Thread(name) {
    setDaemon(true)
    @volatile var error: Throwable = _
    override def run(): Unit = {
      try {
        body
      } catch {
        case t: Throwable => error = t
      }
    }
  }

  private def startDaemonThread(name: String)(body: => Unit): DaemonThread = {
    val thread = new DaemonThread(name, body)
    thread.start()
    thread
  }

  private def joinThread(thread: DaemonThread): Unit = {
    thread.join(TimeUnit.MINUTES.toMillis(1))
    assert(!thread.isAlive, s"${thread.getName} is still running")
    Option(thread.error).foreach(e => fail(s"${thread.getName} failed", e))
  }

  /**
   * Runs `takeInvocation` on a new thread named `name`, by default failing the task, and waits
   * until a failure listener on it holds the listener invocation, which it does until the
   * returned latch is counted down. Returns that latch and the thread.
   */
  private def holdListenerInvocation(context: TaskContext, name: String = "listener-holder")(
      takeInvocation: => Unit = context.markTaskFailed(new Exception("failed on another thread")))
      : (CountDownLatch, DaemonThread) = {
    val holding = new CountDownLatch(1)
    val release = new CountDownLatch(1)
    context.addTaskFailureListener(new TaskFailureListener {
      override def onTaskFailure(context: TaskContext, error: Throwable): Unit = {
        holding.countDown()
        release.await(1, TimeUnit.MINUTES)
      }
    })
    val holder = startDaemonThread(name)(takeInvocation)
    assert(holding.await(1, TimeUnit.MINUTES))
    (release, holder)
  }

  /**
   * Fails every logging call whose message contains `trigger` with `failure`. Unlike
   * `LogAppender`, it does not ignore its exceptions, so log4j passes them to the caller.
   */
  private class FailingAppender(trigger: String, failure: RuntimeException)
    extends AbstractAppender("failingAppender", null, null, false, Property.EMPTY_ARRAY) {
    val failures = new AtomicInteger()
    // Of these, the failures of events that carry a throwable.
    val failuresWithThrown = new AtomicInteger()
    override def append(event: LogEvent): Unit = {
      if (event.getMessage.getFormattedMessage.contains(trigger)) {
        failures.incrementAndGet()
        if (event.getThrown != null) failuresWithThrown.incrementAndGet()
        throw failure
      }
    }
  }

  private def isCausedBy(error: Throwable, cause: Throwable): Boolean =
    Iterator.iterate(error)(_.getCause).takeWhile(_ != null).exists(_ eq cause)

  /** An exception whose `getMessage`, which is listener code too, throws `failure`. */
  private class MessageFailingException(failure: Throwable) extends Exception {
    override def getMessage: String = throw failure
  }

  private def addFailureListenerThrowing(context: TaskContext)(failure: => Throwable): Unit =
    context.addTaskFailureListener(new TaskFailureListener {
      override def onTaskFailure(context: TaskContext, error: Throwable): Unit = throw failure
    })

  /** Waits until `thread` waits in `markTaskCompleted()` for the listener invocation. */
  private def awaitHandoverWait(thread: Thread): Unit = {
    eventually(timeout(1.minute)) {
      assert(!thread.isAlive || thread.getState == Thread.State.TIMED_WAITING)
    }
    assert(thread.isAlive, s"${thread.getName} returned instead of waiting")
  }

  test("SPARK-60222: a listener added as completion starts does not take over the listeners") {
    // A thread that sees the task completed and adds a listener at that moment must not become
    // the thread that runs the pending listeners: the completing thread claims the listener
    // invocation together with setting `completed`.
    (1 to 500).foreach { _ =>
      val context = TaskContext.empty()
      val pendingListenerThread = new AtomicReference[Thread]()
      context.addTaskCompletionListener[Unit] { _ =>
        pendingListenerThread.set(Thread.currentThread())
      }
      val ready = new CountDownLatch(1)
      val adder = startDaemonThread("adder") {
        ready.countDown()
        while (!context.isCompleted()) {}
        context.addTaskCompletionListener[Unit] { _ => }
      }
      ready.await()
      context.markTaskCompleted(None)
      joinThread(adder)
      assert(pendingListenerThread.get eq Thread.currentThread())
    }
  }

  test("SPARK-60222: a failing listener does not let another thread take over the listeners") {
    // While a failing listener's failure listeners run, the thread invoking the listeners must
    // stay registered, so that another thread adding listeners at that moment does not start
    // running the pending ones, possibly concurrently with this thread.
    (1 to 200).foreach { _ =>
      val context = TaskContext.empty()
      val pendingListenerThread = new AtomicReference[Thread]()
      context.addTaskCompletionListener[Unit] { _ =>
        pendingListenerThread.set(Thread.currentThread())
      }
      // Two failing listeners, which run first: the second failure finds the task already failed.
      (1 to 2).foreach { i =>
        context.addTaskCompletionListener(new TaskCompletionListener {
          override def onTaskCompletion(context: TaskContext): Unit = throw new Exception(s"$i")
        })
      }
      context.addTaskFailureListener(new TaskFailureListener {
        override def onTaskFailure(context: TaskContext, error: Throwable): Unit = {}
      })
      val ready = new CountDownLatch(1)
      val adder = startDaemonThread("adder") {
        ready.countDown()
        while (!context.isCompleted()) {}
        (1 to 200).foreach { _ => context.addTaskCompletionListener[Unit] { _ => } }
      }
      ready.await()
      intercept[TaskCompletionListenerException] {
        context.markTaskCompleted(None)
      }
      joinThread(adder)
      assert(pendingListenerThread.get eq Thread.currentThread())
    }
  }

  test("SPARK-60222: completion waits for another thread invoking listeners and runs them") {
    val context = TaskContext.empty()
    val listenerThreads = new ConcurrentLinkedQueue[(String, Thread)]()
    context.addTaskCompletionListener(loggingListener(listenerThreads, "pending"))
    context.addTaskInterruptListener(new TaskInterruptListener {
      override def onTaskInterrupted(context: TaskContext, reason: String): Unit =
        throw new Exception("interrupt listener")
    })
    // Like a kill: an interrupt listener fails, so the kill thread runs the failure listeners.
    val (release, killer) = holdListenerInvocation(context, "killer") {
      context.markInterrupted("killed")
    }

    val completion = startDaemonThread("completion")(context.markTaskCompleted(None))
    try {
      awaitHandoverWait(completion)
      // Neither returns early nor runs the completion listeners while the kill thread holds the
      // listener invocation, and a third thread adding a listener does not take them over.
      joinThread(startDaemonThread("adder") {
        context.addTaskCompletionListener(loggingListener(listenerThreads, "added"))
      })
      assert(completion.isAlive)
      assert(listenerThreads.isEmpty)
    } finally {
      release.countDown()
    }
    joinThread(completion)
    joinThread(killer)
    assert(listenerThreads.asScala.toSeq === Seq(("added", completion), ("pending", completion)))
  }

  test("SPARK-60222: a listener on the invoking thread completing the task runs them nested") {
    val context = TaskContext.empty()
    val completionListenerThread = new AtomicReference[Thread]()
    context.addTaskCompletionListener[Unit] { _ =>
      completionListenerThread.set(Thread.currentThread())
    }
    val ranBeforeReturn = new AtomicReference[Option[Boolean]](None)
    val laterListenerThread = new AtomicReference[Thread]()
    context.addTaskFailureListener(new TaskFailureListener {
      override def onTaskFailure(context: TaskContext, error: Throwable): Unit = {
        // This thread holds the listener invocation, so it must not wait for itself.
        context.markTaskCompleted(Some(error))
        ranBeforeReturn.set(Some(completionListenerThread.get eq Thread.currentThread()))
        // And it still holds the invocation, so another thread adding a listener does not run it.
        joinThread(startDaemonThread("adder") {
          context.addTaskCompletionListener[Unit] { _ =>
            laterListenerThread.set(Thread.currentThread())
          }
        })
      }
    })
    joinThread(startDaemonThread("failer")(context.markTaskFailed(new Exception("failed"))))
    assert(ranBeforeReturn.get === Some(true))
    assert(laterListenerThread.get == null)
  }

  test("SPARK-60222: a failure listener left to the completing thread runs before the next " +
      "completion listener") {
    val context = TaskContext.empty()
    val invocations = new ConcurrentLinkedQueue[(String, Thread)]()
    context.addTaskFailureListener(new TaskFailureListener {
      override def onTaskFailure(context: TaskContext, error: Throwable): Unit = {
        invocations.add(("failure", Thread.currentThread()))
        throw new Exception("failure listener")
      }
    })
    // E.g. a completion listener that releases a state store, which a failure listener aborts.
    context.addTaskCompletionListener(loggingListener(invocations, "second"))
    context.addTaskCompletionListener[Unit] { _ =>
      invocations.add(("first", Thread.currentThread()))
      // Another thread fails the task while this thread runs the completion listeners, e.g. a
      // kill whose interrupt listener failed. It finds the listener invocation taken and leaves
      // the failure listeners to this thread.
      joinThread(startDaemonThread("failer") {
        context.markTaskFailed(new Exception("failed elsewhere"))
      })
      assert(invocations.size == 1)
    }

    val error = intercept[TaskCompletionListenerException] {
      context.markTaskCompleted(None)
    }
    val current = Thread.currentThread()
    assert(invocations.asScala.toSeq ===
      Seq(("first", current), ("failure", current), ("second", current)))
    // The failure listener's failure is reported as its own, not wrapped in the exception of the
    // failure listener invocation.
    assert(error.getSuppressed.map(_.getMessage).toSeq === Seq("failure listener"))
  }

  test("SPARK-60222: a failure listener left to the thread the invocation was handed over to " +
      "runs before its next completion listener") {
    val context = TaskContext.empty()
    val invocations = new ConcurrentLinkedQueue[(String, Thread)]()
    context.addTaskCompletionListener(loggingListener(invocations, "second"))
    context.addTaskCompletionListener[Unit] { _ =>
      invocations.add(("first", Thread.currentThread()))
      // The task has failed, so the added failure listener is to run right away. This thread
      // holds the listener invocation, so the adding thread leaves it to this one.
      joinThread(startDaemonThread("adder") {
        context.addTaskFailureListener(new TaskFailureListener {
          override def onTaskFailure(context: TaskContext, error: Throwable): Unit =
            invocations.add(("failure", Thread.currentThread()))
        })
      })
      assert(invocations.size == 1)
    }
    val (release, holder) = holdListenerInvocation(context)()
    val completion = startDaemonThread("completion")(context.markTaskCompleted(None))
    try {
      awaitHandoverWait(completion)
    } finally {
      release.countDown()
    }
    joinThread(completion)
    joinThread(holder)
    assert(invocations.asScala.toSeq ===
      Seq(("first", completion), ("failure", completion), ("second", completion)))
  }

  test("SPARK-60222: an exception whose getMessage throws does not end the listener loops") {
    val messageFailure = new IllegalStateException("no message")
    val current = Thread.currentThread()
    // The failure listener invocation that a failing pre-completion or completion listener
    // starts throws the failure of `getMessage`, while building its exception.
    Seq(true, false).foreach { preCompletion =>
      val context = TaskContext.empty()
      val invocations = new ConcurrentLinkedQueue[(String, Thread)]()
      addFailureListenerThrowing(context)(new MessageFailingException(messageFailure))
      context.addTaskCompletionListener(loggingListener(invocations, "next"))
      val failing = new TaskCompletionListener {
        override def onTaskCompletion(context: TaskContext): Unit = throw new Exception("listener")
      }
      if (preCompletion) {
        context.addTaskPreCompletionListener(failing)
      } else {
        context.addTaskCompletionListener(failing)
      }
      val error = intercept[TaskCompletionListenerException] {
        context.markTaskCompleted(None)
      }
      assert(invocations.asScala.toSeq === Seq(("next", current)), s"preCompletion=$preCompletion")
      assert(context.isCompleted())
      val listenerFailure = error.getSuppressed.toSeq.head
      assert(listenerFailure.getMessage === "listener")
      assert(listenerFailure.getSuppressed.exists(_ eq messageFailure))
    }

    // Likewise for the failure listeners left to the completing thread.
    val context = TaskContext.empty()
    val invocations = new ConcurrentLinkedQueue[(String, Thread)]()
    addFailureListenerThrowing(context)(new MessageFailingException(messageFailure))
    context.addTaskCompletionListener(loggingListener(invocations, "second"))
    context.addTaskCompletionListener[Unit] { _ =>
      joinThread(startDaemonThread("failer") {
        context.markTaskFailed(new Exception("failed elsewhere"))
      })
    }
    val error = intercept[TaskCompletionListenerException] {
      context.markTaskCompleted(None)
    }
    assert(invocations.asScala.toSeq === Seq(("second", current)))
    assert(error.getSuppressed.toSeq === Seq(messageFailure))
  }

  test("SPARK-60222: an error that escapes the listener loop still releases the invocation") {
    val context = TaskContext.empty()
    context.markTaskCompleted(None)
    val fatal = new LinkageError("no message")
    addFailureListenerThrowing(context)(new MessageFailingException(fatal))
    val error = intercept[LinkageError] {
      // Runs right away on this thread and fails, and the failure listener invocation that this
      // starts throws `fatal` while building its exception.
      context.addTaskCompletionListener[Unit] { _ => throw new Exception("listener") }
    }
    assert(error eq fatal)
    // Released: a listener added now runs right away on this thread.
    val laterListenerThread = new AtomicReference[Thread]()
    context.addTaskCompletionListener[Unit] { _ =>
      laterListenerThread.set(Thread.currentThread())
    }
    assert(laterListenerThread.get eq Thread.currentThread())
  }

  test("SPARK-60222: a fatal error of a failure listener invocation is not hidden by the failure " +
      "of the listener that started it") {
    val fatal = new LinkageError("no message")
    // Both for a failing completion listener and for a failing pre-completion listener.
    Seq(true, false).foreach { preCompletion =>
      val context = TaskContext.empty()
      addFailureListenerThrowing(context)(new MessageFailingException(fatal))
      val failing = new TaskCompletionListener {
        override def onTaskCompletion(context: TaskContext): Unit = throw new Exception("listener")
      }
      if (preCompletion) {
        context.addTaskPreCompletionListener(failing)
      } else {
        context.addTaskCompletionListener(failing)
      }
      // It propagates, so that the executor sees it, instead of being a suppressed exception of
      // the listener's failure.
      val error = intercept[LinkageError] {
        context.markTaskCompleted(None)
      }
      assert(error eq fatal, s"preCompletion=$preCompletion")
    }
  }

  test("SPARK-60222: an interrupt does not stop the handover wait and is kept") {
    val context = TaskContext.empty()
    val listenerThreads = new ConcurrentLinkedQueue[(String, Thread)]()
    (1 to 2).foreach { i =>
      context.addTaskCompletionListener(loggingListener(listenerThreads, s"completion $i"))
    }
    val (release, holder) = holdListenerInvocation(context)()
    val interruptedAfterCompletion = new AtomicReference[Option[Boolean]](None)
    val completion = startDaemonThread("completion") {
      context.markTaskCompleted(None)
      interruptedAfterCompletion.set(Some(Thread.currentThread().isInterrupted))
    }
    try {
      awaitHandoverWait(completion)
      completion.interrupt()
      // Still waiting for the holder, without having run any completion listener.
      eventually(timeout(1.minute)) {
        assert(!completion.isAlive ||
          !completion.isInterrupted && completion.getState == Thread.State.TIMED_WAITING)
      }
      assert(completion.isAlive, "completion returned instead of waiting")
      assert(listenerThreads.isEmpty)
    } finally {
      release.countDown()
    }
    joinThread(completion)
    joinThread(holder)
    assert(interruptedAfterCompletion.get === Some(true))
    assert(listenerThreads.asScala.toSeq ===
      Seq(("completion 2", completion), ("completion 1", completion)))
  }

  test("SPARK-60222: the handover wait warns with the holder's name and stack trace") {
    val context = TaskContext.empty()
    context.listenerHandoverWarnIntervalMs = 100
    val (release, holder) = holdListenerInvocation(context)()
    val logAppender = new LogAppender("listener handover wait")
    var completion: DaemonThread = null
    try {
      withLogAppender(logAppender, Seq(classOf[TaskContextImpl].getName), Some(Level.WARN)) {
        completion = startDaemonThread("completion")(context.markTaskCompleted(None))
        eventually(timeout(1.minute)) {
          assert(!completion.isAlive ||
            logAppender.loggingEvents.count(_.getLevel == Level.WARN) >= 2)
        }
        assert(completion.isAlive, "completion returned instead of waiting")
      }
    } finally {
      release.countDown()
    }
    joinThread(completion)
    joinThread(holder)
    val warnings = logAppender.loggingEvents.filter(_.getLevel == Level.WARN).toSeq
    warnings.foreach { event =>
      assert(event.getMessage.getFormattedMessage.contains("until thread listener-holder"))
    }
    // The first warning carries the holder's stack trace, blocked in its failure listener.
    val holderStack = warnings.head.getThrown
    assert(holderStack.getMessage.contains("listener-holder"))
    assert(holderStack.getStackTrace.exists(_.getMethodName == "onTaskFailure"))
    // The later ones do not walk the stack again.
    warnings.tail.foreach { event => assert(event.getThrown == null) }
  }

  test("SPARK-60222: a failing handover warning does not end the wait") {
    val context = TaskContext.empty()
    context.listenerHandoverWarnIntervalMs = 50
    val listenerThreads = new ConcurrentLinkedQueue[(String, Thread)]()
    context.addTaskCompletionListener(loggingListener(listenerThreads, "pending"))
    val (release, holder) = holdListenerInvocation(context)()
    val logFailure = new IllegalStateException("cannot log")
    val appender = new FailingAppender("has been waiting", logFailure)
    val completionError = new AtomicReference[Throwable]()
    var completion: DaemonThread = null
    try {
      withLogAppender(appender, Seq(classOf[TaskContextImpl].getName), Some(Level.WARN)) {
        completion = startDaemonThread("completion") {
          try {
            context.markTaskCompleted(None)
          } catch {
            case t: Throwable => completionError.set(t)
          }
        }
        eventually(timeout(1.minute)) {
          assert(!completion.isAlive || appender.failures.get >= 2)
        }
        assert(completion.isAlive, "completion returned instead of waiting")
        assert(listenerThreads.isEmpty)
      }
    } finally {
      release.countDown()
    }
    joinThread(completion)
    joinThread(holder)
    // The completing thread runs the listeners once the holder has handed over, and then throws
    // the first failure.
    assert(listenerThreads.asScala.toSeq === Seq(("pending", completion)))
    assert(isCausedBy(completionError.get, logFailure))
    // Only the first attempt to warn took the holder's stack trace, although it failed.
    assert(appender.failuresWithThrown.get === 1)
    // And it has released the invocation: a listener added now runs right away on this thread.
    val laterListenerThread = new AtomicReference[Thread]()
    context.addTaskCompletionListener[Unit] { _ =>
      laterListenerThread.set(Thread.currentThread())
    }
    assert(laterListenerThread.get eq Thread.currentThread())
  }

  test("SPARK-60222: a failing handover warning is reported with the failures of the " +
      "pre-completion listeners") {
    val context = TaskContext.empty()
    context.listenerHandoverWarnIntervalMs = 50
    val preCompletionFailure = new Exception("pre-completion listener")
    context.addTaskPreCompletionListener(new TaskCompletionListener {
      override def onTaskCompletion(context: TaskContext): Unit = throw preCompletionFailure
    })
    val (release, holder) = holdListenerInvocation(context)()
    val logFailure = new IllegalStateException("cannot log")
    val appender = new FailingAppender("has been waiting", logFailure)
    val completionError = new AtomicReference[Throwable]()
    var completion: DaemonThread = null
    try {
      withLogAppender(appender, Seq(classOf[TaskContextImpl].getName), Some(Level.WARN)) {
        completion = startDaemonThread("completion") {
          try {
            context.markTaskCompleted(None)
          } catch {
            case t: Throwable => completionError.set(t)
          }
        }
        eventually(timeout(1.minute)) {
          assert(!completion.isAlive || appender.failures.get >= 1)
        }
      }
    } finally {
      release.countDown()
    }
    joinThread(completion)
    joinThread(holder)
    val error = completionError.get
    assert(error.isInstanceOf[TaskCompletionListenerException], error)
    val reported = error.getSuppressed.toSeq
    // The failure of the pre-completion listener, which came first, then the failure to warn.
    assert(reported.head eq preCompletionFailure)
    assert(reported.tail.exists(isCausedBy(_, logFailure)))
  }

  test("SPARK-60222: the handover wait does not warn about a holder that has handed over") {
    val context = TaskContext.empty()
    context.listenerHandoverWarnIntervalMs = 50
    val holding = new CountDownLatch(1)
    val release = new CountDownLatch(1)
    context.addTaskFailureListener(new TaskFailureListener {
      override def onTaskFailure(context: TaskContext, error: Throwable): Unit = {
        holding.countDown()
        release.await(1, TimeUnit.MINUTES)
      }
    })
    // While the first warning takes its stack trace, the holder hands over and goes on: here it
    // even exits, so its stack trace no longer shows the listeners.
    val holder = new DaemonThread("holder", context.markTaskFailed(new Exception("failed"))) {
      override def getStackTrace: Array[StackTraceElement] = {
        release.countDown()
        join(TimeUnit.MINUTES.toMillis(1))
        super.getStackTrace
      }
    }
    holder.start()
    assert(holding.await(1, TimeUnit.MINUTES))
    val logAppender = new LogAppender("listener handover wait")
    withLogAppender(logAppender, Seq(classOf[TaskContextImpl].getName), Some(Level.WARN)) {
      joinThread(startDaemonThread("completion")(context.markTaskCompleted(None)))
    }
    joinThread(holder)
    assert(!logAppender.loggingEvents.exists(_.getLevel == Level.WARN))
  }

  test("SPARK-60222: listeners added after completion are not run by a thread invoking other " +
      "listeners") {
    val context = TaskContext.empty()
    val error = new Exception("failed")
    context.markTaskFailed(error)
    context.markTaskCompleted(Some(error))
    val holding = new CountDownLatch(1)
    val release = new CountDownLatch(1)
    context.addPostStatusUpdateListener(new PostStatusUpdateListener {
      override def onStatusUpdateSent(context: TaskContext): Unit = {
        holding.countDown()
        release.await(1, TimeUnit.MINUTES)
      }
    })
    // Like the task thread once the executor has released the task's block locks and memory.
    val invoker = startDaemonThread("status-update")(context.invokePostStatusUpdateListeners())
    assert(holding.await(1, TimeUnit.MINUTES))
    val listenerThreads = new ConcurrentLinkedQueue[(String, Thread)]()
    try {
      // Find the invocation taken, so they do not run the new listeners.
      context.addTaskCompletionListener(loggingListener(listenerThreads, "added"))
      context.addTaskFailureListener(new TaskFailureListener {
        override def onTaskFailure(context: TaskContext, error: Throwable): Unit =
          listenerThreads.add(("failure", Thread.currentThread()))
      })
    } finally {
      release.countDown()
    }
    joinThread(invoker)
    assert(listenerThreads.isEmpty)
    // A listener added once no thread is invoking listeners runs it, on the adding thread.
    context.addTaskCompletionListener(loggingListener(listenerThreads, "later"))
    val current = Thread.currentThread()
    assert(listenerThreads.asScala.toSeq === Seq(("later", current), ("added", current)))
  }

  test("SPARK-46480: Add isFailed in TaskContext") {
    val context = TaskContext.empty()
    var isFailed = false
    context.addTaskCompletionListener[Unit] { context =>
      isFailed = context.isFailed()
    }
    context.markTaskFailed(new RuntimeException())
    context.markTaskCompleted(None)
    assert(isFailed)
  }

  test("SPARK-57491: invokePostStatusUpdateListeners triggers PostStatusUpdateListener") {
    // Basic invocation - listener is called when invokePostStatusUpdateListeners is invoked
    val context1 = TaskContext.empty()
    var invoked = false
    context1.addPostStatusUpdateListener(new PostStatusUpdateListener {
      override def onStatusUpdateSent(context: TaskContext): Unit = invoked = true
    })
    assert(!invoked)
    context1.invokePostStatusUpdateListeners()
    assert(invoked)

    // Multiple listeners are called in reverse registration order
    val context2 = TaskContext.empty()
    val callOrder = ArrayBuffer.empty[String]
    context2.addPostStatusUpdateListener(new PostStatusUpdateListener {
      override def onStatusUpdateSent(context: TaskContext): Unit = callOrder += "first"
    })
    context2.addPostStatusUpdateListener(new PostStatusUpdateListener {
      override def onStatusUpdateSent(context: TaskContext): Unit = callOrder += "second"
    })
    context2.invokePostStatusUpdateListeners()
    assert(callOrder === Seq("second", "first"))

    // Listener exception is wrapped in TaskCompletionListenerException
    val context3 = TaskContext.empty()
    context3.addPostStatusUpdateListener(new PostStatusUpdateListener {
      override def onStatusUpdateSent(context: TaskContext): Unit =
        throw new RuntimeException("post-status-update failure")
    })
    val e = intercept[TaskCompletionListenerException] {
      context3.invokePostStatusUpdateListeners()
    }
    assert(e.getMessage.contains("post-status-update failure"))

    // All listeners are called even if one throws (same behavior as completion listeners)
    val context4 = TaskContext.empty()
    val listener = mock(classOf[PostStatusUpdateListener])
    context4.addPostStatusUpdateListener(new PostStatusUpdateListener {
      override def onStatusUpdateSent(context: TaskContext): Unit =
        throw new RuntimeException("bad listener")
    })
    context4.addPostStatusUpdateListener(listener)
    context4.addPostStatusUpdateListener(new PostStatusUpdateListener {
      override def onStatusUpdateSent(context: TaskContext): Unit =
        throw new RuntimeException("another bad listener")
    })

    intercept[TaskCompletionListenerException] {
      context4.invokePostStatusUpdateListeners()
    }

    verify(listener, times(1)).onStatusUpdateSent(any())

    // invokePostStatusUpdateListeners is independent from TaskCompletionListener
    // (completion listener should NOT be triggered by invokePostStatusUpdateListeners)
    val context5 = TaskContext.empty()
    var postStatusInvoked = false
    var completionInvoked = false
    context5.addPostStatusUpdateListener(new PostStatusUpdateListener {
      override def onStatusUpdateSent(context: TaskContext): Unit = postStatusInvoked = true
    })
    context5.addTaskCompletionListener(new TaskCompletionListener {
      override def onTaskCompletion(context: TaskContext): Unit = completionInvoked = true
    })

    context5.invokePostStatusUpdateListeners()
    assert(postStatusInvoked)
    assert(!completionInvoked) // completion listener should NOT be triggered
  }

  test("SPARK-46947: ensure the correct block manager is used to unroll memory for task") {
    import BlockManagerValidationPlugin._
    BlockManagerValidationPlugin.resetState()

    // run a task which ignores thread interruption when spark context is shutdown
    sc = new SparkContext("local", "test")

    val rdd = new RDD[String](sc, List()) {
      override def getPartitions = Array[Partition](StubPartition(0))

      override def compute(split: Partition, context: TaskContext): Iterator[String] = {
        context.addTaskCompletionListener(new TaskCompletionListener {
          override def onTaskCompletion(context: TaskContext): Unit = {
            try {
              releaseTaskSem.acquire()
            } catch {
              case _: InterruptedException =>
                // ignore thread interruption
            }
          }
        })
        taskStartedSem.release()
        Iterator.empty
      }
    }
    // submit the job, but don't block this thread
    rdd.collectAsync()
    // wait for task to start
    taskStartedSem.acquire()

    sc.stop()
    assert(sc.isStopped)

    // create a new SparkContext which will be blocked for certain amount of time
    // during initializing the driver plugin below
    val conf = new SparkConf()
    conf.set("spark.plugins", classOf[BlockManagerValidationPlugin].getName)
    sc = new SparkContext("local", "test", conf)
  }
}

private object TaskContextSuite extends Logging {
  @volatile var completed = false

  @volatile var lastError: Throwable = _

  class FakeTaskFailureException extends Exception("Fake task failure")
}

class BlockManagerValidationPlugin extends SparkPlugin {
  override def driverPlugin(): DriverPlugin = {
    new DriverPlugin() {
      // does nothing but block the current thread for certain time for the task thread
      // to progress and reproduce the issue.
      BlockManagerValidationPlugin.releaseTaskSem.release()
      Thread.sleep(2500)
    }
  }
  override def executorPlugin(): ExecutorPlugin = {
    new ExecutorPlugin() {
      // do nothing
    }
  }
}

object BlockManagerValidationPlugin {
  val releaseTaskSem = new Semaphore(0)
  val taskStartedSem = new Semaphore(0)

  def resetState(): Unit = {
    releaseTaskSem.drainPermits()
    taskStartedSem.drainPermits()
  }
}

private case class StubPartition(index: Int) extends Partition
