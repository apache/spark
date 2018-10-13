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
import org.apache.spark.rpc.{RpcAddress, RpcCallContext, RpcEnv}

class ContextBarrierStateSuite extends SparkFunSuite with LocalSparkContext with Eventually {

  private def mockRpcCallContext() = {
    val rpcAddress = mock(classOf[RpcAddress])
    val rpcCallContext = mock(classOf[RpcCallContext])
    when(rpcCallContext.senderAddress).thenReturn(rpcAddress)
    rpcCallContext
  }

  test("normal test for single task") {
    val barrierCoordinator = new BarrierCoordinator(
      5, mock(classOf[LiveListenerBus]), mock(classOf[RpcEnv]))
    val stageId = 0
    val stageAttemptNumber = 0
    val state = new barrierCoordinator.ContextBarrierState(
      ContextBarrierId(stageId, stageAttemptNumber), numTasks = 1)
    state.handleRequest(
      mockRpcCallContext(),
      RequestToSync(
        numTasks = 1,
        stageId,
        stageAttemptNumber,
        taskAttemptId = 0,
        barrierEpoch = 0))
    // Ensure barrierEpoch value have been changed.
    assert(state.getBarrierEpoch() == 1)
    assert(state.isInternalStateClear())
  }

  test("normal test for multi tasks") {
    val barrierCoordinator = new BarrierCoordinator(
      5, mock(classOf[LiveListenerBus]), mock(classOf[RpcEnv]))
    val numTasks = 3
    val stageId = 0
    val stageAttemptNumber = 0
    val state = new barrierCoordinator.ContextBarrierState(
      ContextBarrierId(stageId, stageAttemptNumber), numTasks)
    // request from 3 tasks
    (0 until numTasks).foreach { taskId =>
      state.handleRequest(mockRpcCallContext(), RequestToSync(
        numTasks,
        stageId,
        stageAttemptNumber,
        taskAttemptId = taskId,
        barrierEpoch = 0))
    }
    // Ensure barrierEpoch value have been changed.
    assert(state.getBarrierEpoch() == 1)
    assert(state.isInternalStateClear())
  }

  test("abnormal test for syncing with illegal barrierId") {
    val barrierCoordinator = new BarrierCoordinator(
      5, mock(classOf[LiveListenerBus]), mock(classOf[RpcEnv]))
    val numTasks = 3
    val stageId = 0
    val stageAttemptNumber = 0
    val rpcCallContext = mockRpcCallContext()
    val state = new barrierCoordinator.ContextBarrierState(
      ContextBarrierId(stageId, stageAttemptNumber), numTasks)
    state.handleRequest(
      rpcCallContext,
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
    val barrierCoordinator = new BarrierCoordinator(
      5, mock(classOf[LiveListenerBus]), mock(classOf[RpcEnv]))
    val numTasks = 3
    val stageId = 0
    val stageAttemptNumber = 0
    val rpcCallContext = mockRpcCallContext()
    val state = new barrierCoordinator.ContextBarrierState(
      ContextBarrierId(stageId, stageAttemptNumber), numTasks)
    // request from 3 tasks
    (0 until numTasks).foreach { taskId =>
      state.handleRequest(
        rpcCallContext,
        RequestToSync(
          numTasks,
          stageId,
          stageAttemptNumber,
          taskAttemptId = taskId,
          barrierEpoch = 0))
    }
    // Ensure barrierEpoch value have been changed.
    assert(state.getBarrierEpoch() == 1)
    assert(state.isInternalStateClear())
    state.handleRequest(
      rpcCallContext,
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
    val barrierCoordinator = new BarrierCoordinator(
      2, mock(classOf[LiveListenerBus]), mock(classOf[RpcEnv]))
    val numTasks = 3
    val stageId = 0
    val stageAttemptNumber = 0
    val rpcCallContext = mockRpcCallContext()
    val state = new barrierCoordinator.ContextBarrierState(
      ContextBarrierId(stageId, stageAttemptNumber), numTasks)
    state.handleRequest(rpcCallContext, RequestToSync(
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
