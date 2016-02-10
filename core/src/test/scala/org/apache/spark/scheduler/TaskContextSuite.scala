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
import org.apache.spark.executor.TaskMetricsSuite
import org.apache.spark.memory.TaskMemoryManager
import org.apache.spark.metrics.source.JvmSource
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.util.{TaskCompletionListener, TaskCompletionListenerException}

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
    val taskBinary = sc.broadcast(JavaUtils.bufferToArray(closureSerializer.serialize((rdd, func))))
    val task = new ResultTask[String, String](0, 0, taskBinary, rdd.partitions(0), Seq.empty, 0)
    intercept[RuntimeException] {
      task.run(0, 0, null)
    }
    assert(TaskContextSuite.completed === true)
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

  test("accumulators are updated on exception failures") {
    // This means use 1 core and 4 max task failures
    sc = new SparkContext("local[1,4]", "test")
    val param = AccumulatorParam.LongAccumulatorParam
    // Create 2 accumulators, one that counts failed values and another that doesn't
    val acc1 = new Accumulator(0L, param, Some("x"), internal = false, countFailedValues = true)
    val acc2 = new Accumulator(0L, param, Some("y"), internal = false, countFailedValues = false)
    // Fail first 3 attempts of every task. This means each task should be run 4 times.
    sc.parallelize(1 to 10, 10).map { i =>
      acc1 += 1
      acc2 += 1
      if (TaskContext.get.attemptNumber() <= 2) {
        throw new Exception("you did something wrong")
      } else {
        0
      }
    }.count()
    // The one that counts failed values should be 4x the one that didn't,
    // since we ran each task 4 times
    assert(Accumulators.get(acc1.id).get.value === 40L)
    assert(Accumulators.get(acc2.id).get.value === 10L)
  }

  test("failed tasks collect only accumulators whose values count during failures") {
    sc = new SparkContext("local", "test")
    val param = AccumulatorParam.LongAccumulatorParam
    val acc1 = new Accumulator(0L, param, Some("x"), internal = false, countFailedValues = true)
    val acc2 = new Accumulator(0L, param, Some("y"), internal = false, countFailedValues = false)
    val initialAccums = InternalAccumulator.createAll()
    // Create a dummy task. We won't end up running this; we just want to collect
    // accumulator updates from it.
    val task = new Task[Int](0, 0, 0, Seq.empty[Accumulator[_]]) {
      context = new TaskContextImpl(0, 0, 0L, 0,
        new TaskMemoryManager(SparkEnv.get.memoryManager, 0L),
        SparkEnv.get.metricsSystem,
        initialAccums)
      context.taskMetrics.registerAccumulator(acc1)
      context.taskMetrics.registerAccumulator(acc2)
      override def runTask(tc: TaskContext): Int = 0
    }
    // First, simulate task success. This should give us all the accumulators.
    val accumUpdates1 = task.collectAccumulatorUpdates(taskFailed = false)
    val accumUpdates2 = (initialAccums ++ Seq(acc1, acc2)).map(TaskMetricsSuite.makeInfo)
    TaskMetricsSuite.assertUpdatesEquals(accumUpdates1, accumUpdates2)
    // Now, simulate task failures. This should give us only the accums that count failed values.
    val accumUpdates3 = task.collectAccumulatorUpdates(taskFailed = true)
    val accumUpdates4 = (initialAccums ++ Seq(acc1)).map(TaskMetricsSuite.makeInfo)
    TaskMetricsSuite.assertUpdatesEquals(accumUpdates3, accumUpdates4)
  }

}

private object TaskContextSuite {
  @volatile var completed = false
}

private case class StubPartition(index: Int) extends Partition
