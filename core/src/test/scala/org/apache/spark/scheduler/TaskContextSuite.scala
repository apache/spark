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

import org.mockito.Matchers.any
import org.mockito.Mockito._
import org.scalatest.BeforeAndAfter

import org.apache.spark._
import org.apache.spark.metrics.source.JvmSource
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.util._

class TaskContextSuite extends SparkFunSuite with BeforeAndAfter with LocalSparkContext {

  test("provide metrics sources") {
    val filePath = getClass.getClassLoader.getResource("test_metrics_config.properties").getFile
    val conf = new SparkConf(loadDefaults = false)
      .set("spark.metrics.conf", filePath)
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
        context.addTaskCompletionListener(context => TaskContextSuite.completed = true)
        sys.error("failed")
      }
    }
    val closureSerializer = SparkEnv.get.closureSerializer.newInstance()
    val func = (c: TaskContext, i: Iterator[String]) => i.next()
    val taskBinary = sc.broadcast(closureSerializer.serialize((rdd, func)).array)
    val task = new ResultTask[String, String](
      0, 0, taskBinary, rdd.partitions(0), Seq.empty, 0, Seq.empty)
    intercept[RuntimeException] {
      task.run(0, 0, null)
    }
    assert(TaskContextSuite.completed === true)
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
      0, 0, taskBinary, rdd.partitions(0), Seq.empty, 0, Seq.empty)
    intercept[RuntimeException] {
      task.run(0, 0, null)
    }
    assert(TaskContextSuite.lastError.getMessage == "damn error")
  }

  test("all TaskCompletionListeners should be called even if some fail") {
    val context = TaskContext.empty()
    val listener = mock(classOf[TaskCompletionListener])
    context.addTaskCompletionListener(_ => throw new Exception("blah"))
    context.addTaskCompletionListener(listener)
    context.addTaskCompletionListener(_ => throw new Exception("blah"))

    intercept[TaskCompletionListenerException] {
      context.markTaskCompleted()
    }

    verify(listener, times(1)).onTaskCompletion(any())
  }

  test("all TaskFailureListeners should be called even if some fail") {
    val context = TaskContext.empty()
    val listener = mock(classOf[TaskFailureListener])
    context.addTaskFailureListener((_, _) => throw new Exception("exception in listener1"))
    context.addTaskFailureListener(listener)
    context.addTaskFailureListener((_, _) => throw new Exception("exception in listener3"))

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

  test("TaskContext.attemptId returns taskAttemptId for backwards-compatibility (SPARK-4014)") {
    sc = new SparkContext("local", "test")
    val attemptIds = sc.parallelize(Seq(1, 2, 3, 4), 4).mapPartitions { iter =>
      Seq(TaskContext.get().attemptId).iterator
    }.collect()
    assert(attemptIds.toSet === Set(0, 1, 2, 3))
  }
}

private object TaskContextSuite {
  @volatile var completed = false

  @volatile var lastError: Throwable = _
}

private case class StubPartition(index: Int) extends Partition
