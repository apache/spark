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
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable.ArrayBuffer

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.scalatest.BeforeAndAfter

import org.apache.spark._
import org.apache.spark.executor.{Executor, TaskMetrics, TaskMetricsSuite}
import org.apache.spark.internal.config.METRICS_CONF
import org.apache.spark.memory.TaskMemoryManager
import org.apache.spark.metrics.source.JvmSource
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.shuffle.FetchFailedException
import org.apache.spark.util._

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
      0, 0, taskBinary, rdd.partitions(0), 1, Seq.empty, 0, new Properties,
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
      0, 0, taskBinary, rdd.partitions(0), 1, Seq.empty, 0, new Properties,
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
      context.runTaskWithListeners(new Task[Int](0, 0, 0, 1, serializedTaskMetrics = Array.empty) {
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
      context.runTaskWithListeners(new Task[Int](0, 0, 0, 1, serializedTaskMetrics = Array.empty) {
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
      context.runTaskWithListeners(new Task[Int](0, 0, 0, 1, serializedTaskMetrics = Array.empty) {
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
      context.runTaskWithListeners(new Task[Int](0, 0, 0, 1, serializedTaskMetrics = Array.empty) {
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
      context.runTaskWithListeners(new Task[Int](0, 0, 0, 1, serializedTaskMetrics = Array.empty) {
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
      context.runTaskWithListeners(new Task[Int](0, 0, 0, 1, serializedTaskMetrics = Array.empty) {
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
      Seq(TaskContext.get().attemptNumber).iterator
    }.collect()
    assert(attemptIds.toSet === Set(0))

    // Test a job with failed tasks
    val attemptIdsWithFailedTask = sc.parallelize(Seq(1, 2), 2).mapPartitions { iter =>
      val attemptId = TaskContext.get().attemptNumber
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
          Seq(TaskContext.get.numPartitions()).iterator
        }.collect()
      assert(numPartitionsFromContext.toSet === Set(numPartitions),
        s"numPartitions = $numPartitions")
    }

    for (numPartitions <- 1 to 10) {
      val numPartitionsFromContext = sc.parallelize(1 to 1000, 2).repartition(numPartitions)
        .mapPartitions { _ =>
          Seq(TaskContext.get.numPartitions()).iterator
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
      if (TaskContext.get.attemptNumber() <= 2) {
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
    val task = new Task[Int](0, 0, 0, 1) {
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
    val task = new Task[Int](0, 0, 0, 1) {
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
    val res = sc.parallelize(Array(1), 1).map(i => i).map(i => {
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
        val thread = new Thread(new Runnable {
          override def run(): Unit = {
            context.addTaskCompletionListener(simpleListener)
          }
        })
        thread.start()
        invocations.getAndIncrement()
        thread.join()
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

}

private object TaskContextSuite {
  @volatile var completed = false

  @volatile var lastError: Throwable = _

  class FakeTaskFailureException extends Exception("Fake task failure")
}

private case class StubPartition(index: Int) extends Partition
