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

import org.apache.spark.executor.TaskMetrics
import org.apache.spark.storage.BlockManagerId
import org.mockito.Mockito.{mock, spy, verify, when}
import org.mockito.Matchers
import org.mockito.Matchers._

import org.apache.spark.scheduler.TaskScheduler
import org.apache.spark.util.RpcUtils
import org.scalatest.concurrent.Eventually._

class HeartbeatReceiverSuite extends SparkFunSuite with LocalSparkContext {

  test("HeartbeatReceiver") {
    sc = spy(new SparkContext("local[2]", "test"))
    val scheduler = mock(classOf[TaskScheduler])
    when(scheduler.executorHeartbeatReceived(any(), any(), any())).thenReturn(true)
    when(sc.taskScheduler).thenReturn(scheduler)

    val heartbeatReceiver = new HeartbeatReceiver(sc)
    sc.env.rpcEnv.setupEndpoint("heartbeat", heartbeatReceiver).send(TaskSchedulerIsSet)
    eventually(timeout(5 seconds), interval(5 millis)) {
      assert(heartbeatReceiver.scheduler != null)
    }
    val receiverRef = RpcUtils.makeDriverRef("heartbeat", sc.conf, sc.env.rpcEnv)

    val metrics = new TaskMetrics
    val blockManagerId = BlockManagerId("executor-1", "localhost", 12345)
    val response = receiverRef.askWithRetry[HeartbeatResponse](
      Heartbeat("executor-1", Array(1L -> metrics), blockManagerId))

    verify(scheduler).executorHeartbeatReceived(
      Matchers.eq("executor-1"), Matchers.eq(Array(1L -> metrics)), Matchers.eq(blockManagerId))
    assert(false === response.reregisterBlockManager)
  }

  test("HeartbeatReceiver re-register") {
    sc = spy(new SparkContext("local[2]", "test"))
    val scheduler = mock(classOf[TaskScheduler])
    when(scheduler.executorHeartbeatReceived(any(), any(), any())).thenReturn(false)
    when(sc.taskScheduler).thenReturn(scheduler)

    val heartbeatReceiver = new HeartbeatReceiver(sc)
    sc.env.rpcEnv.setupEndpoint("heartbeat", heartbeatReceiver).send(TaskSchedulerIsSet)
    eventually(timeout(5 seconds), interval(5 millis)) {
      assert(heartbeatReceiver.scheduler != null)
    }
    val receiverRef = RpcUtils.makeDriverRef("heartbeat", sc.conf, sc.env.rpcEnv)

    val metrics = new TaskMetrics
    val blockManagerId = BlockManagerId("executor-1", "localhost", 12345)
    val response = receiverRef.askWithRetry[HeartbeatResponse](
      Heartbeat("executor-1", Array(1L -> metrics), blockManagerId))

    verify(scheduler).executorHeartbeatReceived(
      Matchers.eq("executor-1"), Matchers.eq(Array(1L -> metrics)), Matchers.eq(blockManagerId))
    assert(true === response.reregisterBlockManager)
  }
}
