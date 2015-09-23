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

package org.apache.spark.scheduler.cluster

import java.util.concurrent.ConcurrentHashMap

import scala.collection.mutable
import scala.collection.JavaConverters._

import org.apache.spark.{LocalSparkContext, SparkConf, SparkContext, SparkException, SparkFunSuite}
import org.apache.spark.scheduler.{SparkListenerTaskStart, SparkListener}
import org.apache.spark.util.{AkkaUtils, SerializableBuffer}

class CoarseGrainedSchedulerBackendSuite extends SparkFunSuite with LocalSparkContext {
  import CoarseGrainedSchedulerBackendSuite._

  test("serialized task larger than akka frame size") {
    val conf = new SparkConf
    conf.set("spark.akka.frameSize", "1")
    conf.set("spark.default.parallelism", "1")
    sc = new SparkContext("local-cluster[2, 1, 1024]", "test", conf)
    val frameSize = AkkaUtils.maxFrameSizeBytes(sc.conf)
    val buffer = new SerializableBuffer(java.nio.ByteBuffer.allocate(2 * frameSize))
    val larger = sc.parallelize(Seq(buffer))
    val thrown = intercept[SparkException] {
      larger.collect()
    }
    assert(thrown.getMessage.contains("using broadcast variables for large values"))
    val smaller = sc.parallelize(1 to 4).collect()
    assert(smaller.size === 4)
  }

  test("avoid scheduling tasks on preempted executors") {
    // Initialize the SparkContext add register the listener
    val conf = new SparkConf()
    sc = new SparkContext("local-cluster[2, 1, 1024]", "test", conf)
    val taskTracker = new TaskStateTracker
    sc.addSparkListener(taskTracker)

    sc.parallelize(1 to 100).collect()
    taskTracker.taskSet.clear()

    val backend = sc.schedulerBackend
    val executors = {
      val method = backend.getClass.getSuperclass.getDeclaredMethod(
        "org$apache$spark$scheduler$cluster$CoarseGrainedSchedulerBackend$$executorDataMap"
      )
      method.setAccessible(true)
      method.invoke(backend).asInstanceOf[mutable.HashMap[String, ExecutorData]]
    }.keySet

    // There should be two executors in running
    assert(executors.size === 2)

    val preemptedExecutors = {
      val method = backend.getClass.getSuperclass.getDeclaredMethod("preemptedExecutorIDs")
      method.setAccessible(true)
      method.invoke(backend).asInstanceOf[mutable.HashSet[String]]
    }

    // There should be no preempted executors when initialized
    assert(preemptedExecutors.size === 0)
    sc.parallelize(1 to 100, 10).collect()
    assert(taskTracker.taskSet.size() === 10)
    assert(taskTracker.taskSet.values().asScala.toSet === Set("0", "1"))
    taskTracker.taskSet.clear()


    // Add executor "1" as a preempted executor
    preemptedExecutors.add("1")
    assert(preemptedExecutors === Set("1"))
    sc.parallelize(1 to 100, 10).collect()
    assert(taskTracker.taskSet.size() === 10)
    // All the tasks should be only scheduled on executor "0"
    assert(taskTracker.taskSet.values().asScala.toSet === Set("0"))
    taskTracker.taskSet.clear()

    // Clear the preempted executors
    preemptedExecutors.clear()
    assert(preemptedExecutors === Set.empty)
    sc.parallelize(1 to 100, 10).collect()
    assert(taskTracker.taskSet.size() === 10)
    // All the tasks will be scheduled on executor "0" and "1"
    assert(taskTracker.taskSet.values().asScala.toSet === Set("0", "1"))
    taskTracker.taskSet.clear()
  }
}

private object CoarseGrainedSchedulerBackendSuite {
  class TaskStateTracker extends SparkListener {
    val taskSet = new ConcurrentHashMap[Long, String]()

    override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
      val taskId = taskStart.taskInfo.taskId
      val executorId = taskStart.taskInfo.executorId
      taskSet.put(taskId, executorId)
    }
  }
}
