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

import java.util.concurrent.TimeoutException

import scala.concurrent.duration._
import scala.language.postfixOps

import org.apache.spark._
import org.apache.spark.rpc.RpcTimeout

class BarrierCoordinatorSuite extends SparkFunSuite with LocalSparkContext {

  /**
   * Get the current barrierEpoch from barrierCoordinator.states by ContextBarrierId
   */
  def getCurrentBarrierEpoch(
      stageId: Int, stageAttemptId: Int, barrierCoordinator: BarrierCoordinator): Int = {
    val barrierId = ContextBarrierId(stageId, stageAttemptId)
    barrierCoordinator.states.get(barrierId).barrierEpoch
  }

  test("normal test for single task") {
    sc = new SparkContext("local", "test")
    val barrierCoordinator = new BarrierCoordinator(5, sc.listenerBus, sc.env.rpcEnv)
    val rpcEndpointRef = sc.env.rpcEnv.setupEndpoint("barrierCoordinator", barrierCoordinator)
    val stageId = 0
    val stageAttemptNumber = 0
    rpcEndpointRef.askSync[Unit](
      message = RequestToSync(numTasks = 1, stageId, stageAttemptNumber, taskAttemptId = 0,
        barrierEpoch = 0),
      timeout = new RpcTimeout(5 seconds, "rpcTimeOut"))
    // sleep for waiting barrierEpoch value change
    Thread.sleep(500)
    assert(getCurrentBarrierEpoch(stageId, stageAttemptNumber, barrierCoordinator) == 1)
  }

  test("normal test for multi tasks") {
    sc = new SparkContext("local", "test")
    val barrierCoordinator = new BarrierCoordinator(5, sc.listenerBus, sc.env.rpcEnv)
    val rpcEndpointRef = sc.env.rpcEnv.setupEndpoint("barrierCoordinator", barrierCoordinator)
    val numTasks = 3
    val stageId = 0
    val stageAttemptNumber = 0
    val rpcTimeOut = new RpcTimeout(5 seconds, "rpcTimeOut")
    // sync request from 3 tasks
    (0 until numTasks).foreach { taskId =>
      new Thread(s"task-$taskId-thread") {
        setDaemon(true)
        override def run(): Unit = {
          rpcEndpointRef.askSync[Unit](
            message = RequestToSync(numTasks, stageId, stageAttemptNumber, taskAttemptId = taskId,
              barrierEpoch = 0),
            timeout = rpcTimeOut)
        }
      }.start()
    }
    // sleep for waiting barrierEpoch value change
    Thread.sleep(500)
    assert(getCurrentBarrierEpoch(stageId, stageAttemptNumber, barrierCoordinator) == 1)
  }

  test("abnormal test for syncing with illegal barrierId") {
    sc = new SparkContext("local", "test")
    val barrierCoordinator = new BarrierCoordinator(5, sc.listenerBus, sc.env.rpcEnv)
    val rpcEndpointRef = sc.env.rpcEnv.setupEndpoint("barrierCoordinator", barrierCoordinator)
    val numTasks = 3
    val stageId = 0
    val stageAttemptNumber = 0
    val rpcTimeOut = new RpcTimeout(5 seconds, "rpcTimeOut")
    intercept[SparkException](
      rpcEndpointRef.askSync[Unit](
        message = RequestToSync(numTasks, stageId, stageAttemptNumber, taskAttemptId = 0,
          barrierEpoch = -1), // illegal barrierId = -1
        timeout = rpcTimeOut))
  }

  test("abnormal test for syncing with old barrierId") {
    sc = new SparkContext("local", "test")
    val barrierCoordinator = new BarrierCoordinator(5, sc.listenerBus, sc.env.rpcEnv)
    val rpcEndpointRef = sc.env.rpcEnv.setupEndpoint("barrierCoordinator", barrierCoordinator)
    val numTasks = 3
    val stageId = 0
    val stageAttemptNumber = 0
    val rpcTimeOut = new RpcTimeout(5 seconds, "rpcTimeOut")
    // sync request from 3 tasks
    (0 until numTasks).foreach { taskId =>
      new Thread(s"task-$taskId-thread") {
        setDaemon(true)
        override def run(): Unit = {
          rpcEndpointRef.askSync[Unit](
            message = RequestToSync(numTasks, stageId, stageAttemptNumber, taskAttemptId = taskId,
              barrierEpoch = 0),
            timeout = rpcTimeOut)
        }
      }.start()
    }
    // sleep for waiting barrierEpoch value change
    Thread.sleep(500)
    assert(getCurrentBarrierEpoch(stageId, stageAttemptNumber, barrierCoordinator) == 1)
    intercept[SparkException](
      rpcEndpointRef.askSync[Unit](
        message = RequestToSync(numTasks, stageId, stageAttemptNumber, taskAttemptId = 0,
        barrierEpoch = 0),
        timeout = rpcTimeOut))
  }

  test("abnormal test for timeout when rpcTimeOut < barrierTimeOut") {
    sc = new SparkContext("local", "test")
    val barrierCoordinator = new BarrierCoordinator(2, sc.listenerBus, sc.env.rpcEnv)
    val rpcEndpointRef = sc.env.rpcEnv.setupEndpoint("barrierCoordinator", barrierCoordinator)
    val numTasks = 3
    val stageId = 0
    val stageAttemptNumber = 0
    val rpcTimeOut = new RpcTimeout(1 seconds, "rpcTimeOut")
    intercept[TimeoutException](
      rpcEndpointRef.askSync[Unit](
        message = RequestToSync(numTasks, stageId, stageAttemptNumber, taskAttemptId = 0,
          barrierEpoch = 0),
        timeout = rpcTimeOut))
  }

  test("abnormal test for timeout when rpcTimeOut > barrierTimeOut") {
    sc = new SparkContext("local", "test")
    val barrierCoordinator = new BarrierCoordinator(2, sc.listenerBus, sc.env.rpcEnv)
    val rpcEndpointRef = sc.env.rpcEnv.setupEndpoint("barrierCoordinator", barrierCoordinator)
    val numTasks = 3
    val stageId = 0
    val stageAttemptNumber = 0
    val rpcTimeOut = new RpcTimeout(4 seconds, "rpcTimeOut")
    intercept[SparkException](
      rpcEndpointRef.askSync[Unit](
        message = RequestToSync(numTasks, stageId, stageAttemptNumber, taskAttemptId = 0,
          barrierEpoch = 0),
        timeout = rpcTimeOut))
  }
}
