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

package org.apache.spark

import scala.concurrent.duration._
import scala.language.postfixOps

import org.scalatest.{BeforeAndAfterEach, FunSuite, PrivateMethodTester}
import org.scalatest.concurrent.Eventually._
import org.mockito.Mockito.{mock, spy, verify, when}
import org.mockito.Matchers
import org.mockito.Matchers._

import org.apache.spark.executor.TaskMetrics
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.scheduler._
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.util.ManualClock

class HeartbeatReceiverSuite
  extends FunSuite
  with BeforeAndAfterEach
  with PrivateMethodTester
  with LocalSparkContext {

  private val executorId1 = "executor-1"
  private val executorId2 = "executor-2"

  // Shared state that must be reset before and after each test
  private var scheduler: TaskScheduler = null
  private var heartbeatReceiver: HeartbeatReceiver = null
  private var heartbeatReceiverRef: RpcEndpointRef = null
  private var heartbeatReceiverClock: ManualClock = null

  override def beforeEach(): Unit = {
    sc = spy(new SparkContext("local[2]", "test"))
    scheduler = mock(classOf[TaskScheduler])
    when(sc.taskScheduler).thenReturn(scheduler)
    heartbeatReceiverClock = new ManualClock
    heartbeatReceiver = new HeartbeatReceiver(sc, heartbeatReceiverClock)
    heartbeatReceiverRef = sc.env.rpcEnv.setupEndpoint("heartbeat", heartbeatReceiver)
    when(scheduler.executorHeartbeatReceived(any(), any(), any())).thenReturn(true)
  }

  override def afterEach(): Unit = {
    resetSparkContext()
    scheduler = null
    heartbeatReceiver = null
    heartbeatReceiverRef = null
    heartbeatReceiverClock = null
  }

  test("task scheduler is set correctly") {
    assert(heartbeatReceiver.scheduler === null)
    heartbeatReceiverRef.send(TaskSchedulerIsSet)
    eventually(timeout(5 seconds), interval(5 millis)) {
      assert(heartbeatReceiver.scheduler !== null)
    }
  }

  test("normal heartbeat") {
    heartbeatReceiverRef.send(TaskSchedulerIsSet)
    heartbeatReceiver.onExecutorAdded(SparkListenerExecutorAdded(0, executorId1, null))
    heartbeatReceiver.onExecutorAdded(SparkListenerExecutorAdded(0, executorId2, null))
    triggerHeartbeat(executorId1, executorShouldReregister = false)
    triggerHeartbeat(executorId2, executorShouldReregister = false)
    val trackedExecutors = executorLastSeen(heartbeatReceiver)
    assert(trackedExecutors.size === 2)
    assert(trackedExecutors.contains(executorId1))
    assert(trackedExecutors.contains(executorId2))
  }

  test("reregister if scheduler is not ready yet") {
    heartbeatReceiver.onExecutorAdded(SparkListenerExecutorAdded(0, executorId1, null))
    // Task scheduler not set in HeartbeatReceiver
    triggerHeartbeat(executorId1, executorShouldReregister = true)
  }

  test("reregister if heartbeat from unregistered executor") {
    heartbeatReceiverRef.send(TaskSchedulerIsSet)
    // Received heartbeat from unknown receiver, so we ask it to re-register
    triggerHeartbeat(executorId1, executorShouldReregister = true)
    assert(executorLastSeen(heartbeatReceiver).isEmpty)
  }

  test("reregister if heartbeat from removed executor") {
    heartbeatReceiverRef.send(TaskSchedulerIsSet)
    heartbeatReceiver.onExecutorAdded(SparkListenerExecutorAdded(0, executorId1, null))
    heartbeatReceiver.onExecutorAdded(SparkListenerExecutorAdded(0, executorId2, null))
    // Remove the second executor but not the first
    heartbeatReceiver.onExecutorRemoved(SparkListenerExecutorRemoved(0, executorId2, "bad boy"))
    // Now trigger the heartbeats
    // A heartbeat from the second executor should require reregistering
    triggerHeartbeat(executorId1, executorShouldReregister = false)
    triggerHeartbeat(executorId2, executorShouldReregister = true)
    val trackedExecutors = executorLastSeen(heartbeatReceiver)
    assert(trackedExecutors.size === 1)
    assert(trackedExecutors.contains(executorId1))
    assert(!trackedExecutors.contains(executorId2))
  }

  test("expire dead hosts") {
    val executorTimeout = executorTimeoutMs(heartbeatReceiver)
    heartbeatReceiverRef.send(TaskSchedulerIsSet)
    heartbeatReceiver.onExecutorAdded(SparkListenerExecutorAdded(0, executorId1, null))
    heartbeatReceiver.onExecutorAdded(SparkListenerExecutorAdded(0, executorId2, null))
    triggerHeartbeat(executorId1, executorShouldReregister = false)
    triggerHeartbeat(executorId2, executorShouldReregister = false)
    // Advance the clock and only trigger a heartbeat for the first executor
    heartbeatReceiverClock.advance(executorTimeout / 2)
    triggerHeartbeat(executorId1, executorShouldReregister = false)
    heartbeatReceiverClock.advance(executorTimeout)
    heartbeatReceiverRef.askWithRetry[Boolean](ExpireDeadHosts)
    // Only the second executor should be expired as a dead host
    verify(scheduler).executorLost(Matchers.eq(executorId2), any())
    val trackedExecutors = executorLastSeen(heartbeatReceiver)
    assert(trackedExecutors.size === 1)
    assert(trackedExecutors.contains(executorId1))
    assert(!trackedExecutors.contains(executorId2))
  }

  /** Manually send a heartbeat and return the response. */
  private def triggerHeartbeat(
      executorId: String,
      executorShouldReregister: Boolean): Unit = {
    val metrics = new TaskMetrics
    val blockManagerId = BlockManagerId(executorId, "localhost", 12345)
    val response = heartbeatReceiverRef.askWithRetry[HeartbeatResponse](
      Heartbeat(executorId, Array(1L -> metrics), blockManagerId))
    if (executorShouldReregister) {
      assert(response.reregisterBlockManager)
    } else {
      assert(!response.reregisterBlockManager)
      // Additionally verify that the scheduler callback is called with the correct parameters
      verify(scheduler).executorHeartbeatReceived(
        Matchers.eq(executorId), Matchers.eq(Array(1L -> metrics)), Matchers.eq(blockManagerId))
    }
  }

  // Helper methods to access private fields in HeartbeatReceiver
  private val _executorLastSeen = PrivateMethod[collection.Map[String, Long]]('executorLastSeen)
  private val _executorTimeoutMs = PrivateMethod[Long]('executorTimeoutMs)
  private def executorLastSeen(receiver: HeartbeatReceiver): collection.Map[String, Long] = {
    receiver invokePrivate _executorLastSeen()
  }
  private def executorTimeoutMs(receiver: HeartbeatReceiver): Long = {
    receiver invokePrivate _executorTimeoutMs()
  }

}
