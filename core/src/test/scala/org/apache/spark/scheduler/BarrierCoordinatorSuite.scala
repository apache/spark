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

import scala.concurrent.duration._
import scala.language.postfixOps

import org.mockito.ArgumentMatcher
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.concurrent.Eventually

import org.apache.spark._
import org.apache.spark.rpc.{RpcAddress, RpcCallContext}

class BarrierCoordinatorSuite extends SparkFunSuite with LocalSparkContext with Eventually {

  /**
   * Get the current ContextBarrierState from barrierCoordinator.states by ContextBarrierId.
   */
  private def getBarrierState(
      stageId: Int,
      stageAttemptId: Int,
      barrierCoordinator: BarrierCoordinator) = {
    val barrierId = ContextBarrierId(stageId, stageAttemptId)
    barrierCoordinator.states.get(barrierId)
  }

  private def mockRpcCallContext() = {
    val rpcAddress = mock(classOf[RpcAddress])
    val rpcCallContext = mock(classOf[RpcCallContext])
    when(rpcCallContext.senderAddress).thenReturn(rpcAddress)
    rpcCallContext
  }

  test("normal test for single task") {
    sc = new SparkContext("local", "test")
    val barrierCoordinator = new BarrierCoordinator(5, sc.listenerBus, sc.env.rpcEnv)
    val stageId = 0
    val stageAttemptNumber = 0
    barrierCoordinator.receiveAndReply(mockRpcCallContext())(
      RequestToSync(
        numTasks = 1,
        stageId,
        stageAttemptNumber,
        taskAttemptId = 0,
        barrierEpoch = 0))
    // Ensure barrierEpoch value have been changed.
    val barrierState = getBarrierState(stageId, stageAttemptNumber, barrierCoordinator)
    assert(barrierState.getBarrierEpoch() == 1)
    assert(barrierState.cleanCheck())
  }

  test("normal test for multi tasks") {
    sc = new SparkContext("local", "test")
    val barrierCoordinator = new BarrierCoordinator(5, sc.listenerBus, sc.env.rpcEnv)
    val numTasks = 3
    val stageId = 0
    val stageAttemptNumber = 0
    // request from 3 tasks
    (0 until numTasks).foreach { taskId =>
      barrierCoordinator.receiveAndReply(mockRpcCallContext())(
        RequestToSync(
          numTasks,
          stageId,
          stageAttemptNumber,
          taskAttemptId = taskId,
          barrierEpoch = 0))
    }
    // Ensure barrierEpoch value have been changed.
    val barrierState = getBarrierState(stageId, stageAttemptNumber, barrierCoordinator)
    assert(barrierState.getBarrierEpoch() == 1)
    assert(barrierState.cleanCheck())
  }

  test("abnormal test for syncing with illegal barrierId") {
    sc = new SparkContext("local", "test")
    val barrierCoordinator = new BarrierCoordinator(5, sc.listenerBus, sc.env.rpcEnv)
    val numTasks = 3
    val stageId = 0
    val stageAttemptNumber = 0
    val rpcCallContext = mockRpcCallContext()
    barrierCoordinator.receiveAndReply(rpcCallContext)(
      // illegal barrierId = -1
      RequestToSync(
        numTasks,
        stageId,
        stageAttemptNumber,
        taskAttemptId = 0,
        barrierEpoch = -1))
    verify(rpcCallContext, times(1))
      .sendFailure(argThat(new ArgumentMatcher[SparkException] {
        override def matches(e: Any): Boolean = {
          e.asInstanceOf[SparkException].getMessage ==
            "The request to sync of Stage 0 (Attempt 0) with barrier epoch -1 has already" +
              " finished. Maybe task 0 is not properly killed."
        }
      }))
  }

  test("abnormal test for syncing with old barrierId") {
    sc = new SparkContext("local", "test")
    val barrierCoordinator = new BarrierCoordinator(5, sc.listenerBus, sc.env.rpcEnv)
    val numTasks = 3
    val stageId = 0
    val stageAttemptNumber = 0
    val rpcCallContext = mockRpcCallContext()
    // request from 3 tasks
    (0 until numTasks).foreach { taskId =>
      barrierCoordinator.receiveAndReply(mockRpcCallContext())(
        RequestToSync(
          numTasks,
          stageId,
          stageAttemptNumber,
          taskAttemptId = taskId,
          barrierEpoch = 0))
    }
    // Ensure barrierEpoch value have been changed.
    val barrierState = getBarrierState(stageId, stageAttemptNumber, barrierCoordinator)
    assert(barrierState.getBarrierEpoch() == 1)
    assert(barrierState.cleanCheck())
    barrierCoordinator.receiveAndReply(rpcCallContext)(
      RequestToSync(
        numTasks,
        stageId,
        stageAttemptNumber,
        taskAttemptId = 0,
        barrierEpoch = 0))
    verify(rpcCallContext, times(1))
      .sendFailure(argThat(new ArgumentMatcher[SparkException] {
        override def matches(e: Any): Boolean = {
          e.asInstanceOf[SparkException].getMessage ==
            "The request to sync of Stage 0 (Attempt 0) with barrier epoch 0 has already" +
              " finished. Maybe task 0 is not properly killed."
        }}))
  }

  test("abnormal test for timeout when rpcTimeOut > barrierTimeOut") {
    sc = new SparkContext("local", "test")
    val barrierCoordinator = new BarrierCoordinator(2, sc.listenerBus, sc.env.rpcEnv)
    val numTasks = 3
    val stageId = 0
    val stageAttemptNumber = 0
    val rpcCallContext = mockRpcCallContext()
    barrierCoordinator.receiveAndReply(rpcCallContext)(
      // illegal barrierId = -1
      RequestToSync(
        numTasks,
        stageId,
        stageAttemptNumber,
        taskAttemptId = 0,
        barrierEpoch = 0))
    eventually(timeout(5.seconds)) {
      verify(rpcCallContext, times(1))
        .sendFailure(argThat(new ArgumentMatcher[SparkException] {
          override def matches(e: Any): Boolean = {
            e.asInstanceOf[SparkException].getMessage ==
              "The coordinator didn't get all barrier sync requests for barrier epoch" +
                " 0 from Stage 0 (Attempt 0) within 2 second(s)."
          }
        }))
    }
  }
}
