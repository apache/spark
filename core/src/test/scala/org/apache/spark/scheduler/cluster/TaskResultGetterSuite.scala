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

import java.nio.ByteBuffer

import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSuite}

import org.apache.spark.{LocalSparkContext, SparkContext, SparkEnv}
import org.apache.spark.scheduler.{DirectTaskResult, IndirectTaskResult, TaskResult}
import org.apache.spark.storage.TaskResultBlockId

/**
 * Removes the TaskResult from the BlockManager before delegating to a normal TaskResultGetter.
 *
 * Used to test the case where a BlockManager evicts the task result (or dies) before the
 * TaskResult is retrieved.
 */
class ResultDeletingTaskResultGetter(sparkEnv: SparkEnv, scheduler: ClusterScheduler)
  extends TaskResultGetter(sparkEnv, scheduler) {
  var removedResult = false

  override def enqueueSuccessfulTask(
    taskSetManager: ClusterTaskSetManager, tid: Long, serializedData: ByteBuffer) {
    if (!removedResult) {
      // Only remove the result once, since we'd like to test the case where the task eventually
      // succeeds.
      serializer.get().deserialize[TaskResult[_]](serializedData) match {
        case IndirectTaskResult(blockId) =>
          sparkEnv.blockManager.master.removeBlock(blockId)
        case directResult: DirectTaskResult[_] =>
          taskSetManager.abort("Internal error: expect only indirect results") 
      }
      serializedData.rewind()
      removedResult = true
    }
    super.enqueueSuccessfulTask(taskSetManager, tid, serializedData)
  } 
}

/**
 * Tests related to handling task results (both direct and indirect).
 */
class TaskResultGetterSuite extends FunSuite with BeforeAndAfter with BeforeAndAfterAll
  with LocalSparkContext {

  override def beforeAll {
    // Set the Akka frame size to be as small as possible (it must be an integer, so 1 is as small
    // as we can make it) so the tests don't take too long.
    System.setProperty("spark.akka.frameSize", "1")
  }

  before {
    // Use local-cluster mode because results are returned differently when running with the
    // LocalScheduler.
    sc = new SparkContext("local-cluster[1,1,512]", "test")
  }

  override def afterAll {
    System.clearProperty("spark.akka.frameSize")
  }

  test("handling results smaller than Akka frame size") {
    val result = sc.parallelize(Seq(1), 1).map(x => 2 * x).reduce((x, y) => x)
    assert(result === 2)
  }

  test("handling results larger than Akka frame size") { 
    val akkaFrameSize =
      sc.env.actorSystem.settings.config.getBytes("akka.remote.netty.tcp.maximum-frame-size").toInt
    val result = sc.parallelize(Seq(1), 1).map(x => 1.to(akkaFrameSize).toArray).reduce((x, y) => x)
    assert(result === 1.to(akkaFrameSize).toArray)

    val RESULT_BLOCK_ID = TaskResultBlockId(0)
    assert(sc.env.blockManager.master.getLocations(RESULT_BLOCK_ID).size === 0,
      "Expect result to be removed from the block manager.")
  }

  test("task retried if result missing from block manager") {
    // If this test hangs, it's probably because no resource offers were made after the task
    // failed.
    val scheduler: ClusterScheduler = sc.taskScheduler match {
      case clusterScheduler: ClusterScheduler =>
        clusterScheduler
      case _ =>
        assert(false, "Expect local cluster to use ClusterScheduler")
        throw new ClassCastException
    }
    scheduler.taskResultGetter = new ResultDeletingTaskResultGetter(sc.env, scheduler)
    val akkaFrameSize =
      sc.env.actorSystem.settings.config.getBytes("akka.remote.netty.tcp.maximum-frame-size").toInt
    val result = sc.parallelize(Seq(1), 1).map(x => 1.to(akkaFrameSize).toArray).reduce((x, y) => x)
    assert(result === 1.to(akkaFrameSize).toArray)

    // Make sure two tasks were run (one failed one, and a second retried one).
    assert(scheduler.nextTaskId.get() === 2)
  }
}

