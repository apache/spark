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
package org.apache.spark.scheduler.cluster.k8s

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference

import io.fabric8.kubernetes.api.model.{Pod, PodBuilder}
import org.jmock.lib.concurrent.{DeterministicExecutor, DeterministicScheduler}
import org.scalatest.BeforeAndAfter
import scala.collection.mutable

import org.apache.spark.SparkFunSuite

class ExecutorPodsEventQueueSuite extends SparkFunSuite with BeforeAndAfter {

  private var eventBufferScheduler: DeterministicScheduler = _
  private var executeSubscriptionsExecutor: DeterministicExecutor = _
  private var eventQueueUnderTest: ExecutorPodsEventQueueImpl = _

  before {
    eventBufferScheduler = new DeterministicScheduler()
    executeSubscriptionsExecutor = new DeterministicExecutor
    eventQueueUnderTest = new ExecutorPodsEventQueueImpl(
      eventBufferScheduler,
      executeSubscriptionsExecutor)
  }

  test("Subscribers get notified of events periodically.") {
    val receivedEvents1 = mutable.Buffer.empty[Pod]
    val receivedEvents2 = mutable.Buffer.empty[Pod]
    eventQueueUnderTest.addSubscriber(1000) { receivedEvents1.appendAll(_) }
    eventQueueUnderTest.addSubscriber(2000) { receivedEvents2.appendAll(_) }

    pushPodWithIndex(1)
    assert(receivedEvents1.isEmpty)
    assert(receivedEvents2.isEmpty)
    // Force time to move forward so that the buffer is emitted, scheduling the
    // processing task on the subscription executor...
    eventBufferScheduler.tick(1000, TimeUnit.MILLISECONDS)
    // ... then actually execute the subscribers.
    executeSubscriptionsExecutor.runUntilIdle()
    assertIndicesMatch(receivedEvents1, 1)
    assert(receivedEvents2.isEmpty)
    eventBufferScheduler.tick(1000, TimeUnit.MILLISECONDS)
    executeSubscriptionsExecutor.runUntilIdle()
    assertIndicesMatch(receivedEvents2, 1)
    pushPodWithIndex(2)
    pushPodWithIndex(3)
    eventBufferScheduler.tick(1000, TimeUnit.MILLISECONDS)
    executeSubscriptionsExecutor.runUntilIdle()
    assertIndicesMatch(receivedEvents1, 1, 2, 3)
    assertIndicesMatch(receivedEvents2, 1)
    eventBufferScheduler.tick(2000, TimeUnit.MILLISECONDS)
    executeSubscriptionsExecutor.runUntilIdle()
    assertIndicesMatch(receivedEvents1, 1, 2, 3)
    assertIndicesMatch(receivedEvents2, 1, 2, 3)
  }

  test("Even without sending events, initially receive an empty buffer.") {
    val receivedInitialBuffer = new AtomicReference[Seq[Pod]](null)
    eventQueueUnderTest.addSubscriber(1000) { receivedInitialBuffer.set }
    assert(receivedInitialBuffer.get == null)
    executeSubscriptionsExecutor.runPendingCommands()
    assert(receivedInitialBuffer.get != null)
  }

  private def assertIndicesMatch(buffer: mutable.Buffer[Pod], indices: Int*): Unit = {
    assert(buffer === indices.map(podWithIndex))
  }

  private def pushPodWithIndex(index: Int): Unit =
    eventQueueUnderTest.pushPodUpdate(podWithIndex(index))

  private def podWithIndex(index: Int): Pod =
    new PodBuilder()
      .editOrNewMetadata()
        .withName(s"pod-$index")
        .endMetadata()
      .build()
}
