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

package org.apache.spark.storage

import scala.concurrent.{Future, Promise}
import scala.reflect.ClassTag

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar._

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.internal.config.CLEANER_REFERENCE_TRACKING_BLOCKING_TIMEOUT
import org.apache.spark.rpc.{RpcEndpointRef, RpcTimeoutException}
import org.apache.spark.storage.BlockManagerMessages.RemoveRdd

class BlockManagerMasterSuite extends SparkFunSuite {

  test("SPARK-31422: getMemoryStatus should not fail after BlockManagerMaster stops") {
    val bmm = new BlockManagerMaster(null, null, new SparkConf, true)
    assert(bmm.getMemoryStatus.isEmpty)
  }

  test("SPARK-31422: getStorageStatus should not fail after BlockManagerMaster stops") {
    val bmm = new BlockManagerMaster(null, null, new SparkConf, true)
    assert(bmm.getStorageStatus.isEmpty)
  }

  test("SPARK-54219: wait block removal timeout - success case") {
    val conf = new SparkConf()
    conf.set(CLEANER_REFERENCE_TRACKING_BLOCKING_TIMEOUT, 5L)

    val mockDriverEndpoint = mock[RpcEndpointRef]
    val mockHeartbeatEndpoint = mock[RpcEndpointRef]

    // Create a Future that completes successfully
    val promise = Promise[Seq[Int]]()
    val future = promise.future

    // Mock the askSync to return the Future
    when(
      mockDriverEndpoint.askSync[Future[Seq[Int]]](any[RemoveRdd])(any[ClassTag[Future[Seq[Int]]]])
    ).thenReturn(future)

    val bmm = new BlockManagerMaster(mockDriverEndpoint, mockHeartbeatEndpoint, conf, true)

    // Complete the future successfully
    promise.success(Seq(1, 2, 3))

    // This should not throw an exception
    bmm.removeRdd(1, blocking = true)
  }

  test("SPARK-54219: wait block removal timeout - timeout case") {
    val conf = new SparkConf()
    // Set a very short timeout (1 second)
    conf.set(CLEANER_REFERENCE_TRACKING_BLOCKING_TIMEOUT, 1L)

    val mockDriverEndpoint = mock[RpcEndpointRef]
    val mockHeartbeatEndpoint = mock[RpcEndpointRef]

    // Create a Future that never completes (simulating timeout)
    val promise = Promise[Seq[Int]]()
    val future = promise.future

    // Mock the askSync to return the Future
    when(
      mockDriverEndpoint.askSync[Future[Seq[Int]]](any[RemoveRdd])(any[ClassTag[Future[Seq[Int]]]])
    ).thenReturn(future)

    val bmm = new BlockManagerMaster(mockDriverEndpoint, mockHeartbeatEndpoint, conf, true)

    // This should throw RpcTimeoutException
    val exception = intercept[RpcTimeoutException] {
      bmm.removeRdd(1, blocking = true)
    }

    assert(exception.getMessage.contains(CLEANER_REFERENCE_TRACKING_BLOCKING_TIMEOUT.key))
  }
}
