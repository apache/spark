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

package org.apache.spark.executor

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.memory.TestMemoryManager

class ExecutorMetricsPollerSuite extends SparkFunSuite {

  test("SPARK-34779: stage entry shouldn't be removed before a heartbeat occurs") {
    val testMemoryManager = new TestMemoryManager(new SparkConf())
    val poller = new ExecutorMetricsPoller(testMemoryManager, 1000, None)

    poller.onTaskStart(0L, 0, 0)
    // stage (0, 0) has an active task, so it remains on stageTCMP after heartbeat.
    assert(poller.getExecutorUpdates().size === 1)
    assert(poller.stageTCMP.size === 1)
    assert(poller.stageTCMP.get((0, 0)).count === 1)

    poller.onTaskCompletion(0L, 0, 0)
    // stage (0, 0) doesn't have active tasks, but its entry will be kept until next
    // heartbeat.
    assert(poller.stageTCMP.size === 1)
    assert(poller.stageTCMP.get((0, 0)).count === 0)

    // the next heartbeat will report the peak metrics of stage (0, 0) during the
    // previous heartbeat interval, then remove it from stageTCMP.
    assert(poller.getExecutorUpdates().size === 1)
    assert(poller.stageTCMP.size === 0)

    poller.stop()
  }
}
