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

import org.mockito.Mockito.{verify, when}
import org.scalatest.mock.MockitoSugar

import org.apache.spark.{HostState, LocalSparkContext, SparkConf, SparkContext, SparkException, SparkFunSuite}
import org.apache.spark.rpc.RpcEnv
import org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend
import org.apache.spark.util.{RpcUtils, SerializableBuffer}

class CoarseGrainedSchedulerBackendSuite extends SparkFunSuite with MockitoSugar
  with LocalSparkContext {

  private var conf: SparkConf = _
  private var scheduler: TaskSchedulerImpl = _
  private var schedulerBackend: CoarseGrainedSchedulerBackend = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    conf = new SparkConf
  }

  override def afterEach(): Unit = {
    super.afterEach()
    if (scheduler != null) {
      scheduler.stop()
      scheduler = null
    }
    if (schedulerBackend != null) {
      schedulerBackend.stop()
      schedulerBackend = null
    }
  }

  private def setupSchedulerBackend(): Unit = {
    sc = new SparkContext("local", "test", conf)
    scheduler = mock[TaskSchedulerImpl]
    when(scheduler.sc).thenReturn(sc)
    schedulerBackend = new CoarseGrainedSchedulerBackend(scheduler, mock[RpcEnv])
  }

  test("serialized task larger than max RPC message size") {
    conf.set("spark.rpc.message.maxSize", "1")
    conf.set("spark.default.parallelism", "1")
    sc = new SparkContext("local-cluster[2, 1, 1024]", "test", conf)
    val frameSize = RpcUtils.maxMessageSizeBytes(sc.conf)
    val buffer = new SerializableBuffer(java.nio.ByteBuffer.allocate(2 * frameSize))
    val larger = sc.parallelize(Seq(buffer))
    val thrown = intercept[SparkException] {
      larger.collect()
    }
    assert(thrown.getMessage.contains("using broadcast variables for large values"))
    val smaller = sc.parallelize(1 to 4).collect()
    assert(smaller.size === 4)
  }

  test("handle updated node status received") {
    setupSchedulerBackend()
    schedulerBackend.handleUpdatedHostState("host1", HostState.Decommissioning)
    verify(scheduler).blacklistExecutorsOnHost("host1", NodeDecommissioning)

    schedulerBackend.handleUpdatedHostState("host1", HostState.Running)
    verify(scheduler).unblacklistExecutorsOnHost("host1", NodeRunning)
  }

}
