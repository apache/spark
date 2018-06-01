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
import org.jmock.lib.concurrent.DeterministicScheduler
import org.mockito.Matchers.any
import org.mockito.Mockito.{mock, when}
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.BeforeAndAfter
import scala.collection.mutable

import org.apache.spark.SparkFunSuite

class ExecutorPodsEventQueueSuite extends SparkFunSuite with BeforeAndAfter {

  private var eventBufferScheduler: DeterministicScheduler = _
  private var executeSubscriptionsExecutor: DeterministicScheduler = _
  private var eventQueueUnderTest: ExecutorPodsEventQueueImpl = _

  before {
    eventBufferScheduler = new DeterministicScheduler()
    executeSubscriptionsExecutor = new DeterministicScheduler()
    eventQueueUnderTest = new ExecutorPodsEventQueueImpl(
      eventBufferScheduler,
      executeSubscriptionsExecutor)
  }

  test("Subscribers get notified of events periodically.") {
    val receivedEvents1 = mutable.Buffer.empty[Pod]
    val receivedEvents2 = mutable.Buffer.empty[Pod]
    eventQueueUnderTest.addSubscriber(1000, testBatchSubscriber(receivedEvents1))
    eventQueueUnderTest.addSubscriber(2000, testBatchSubscriber(receivedEvents2))

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
    val receivedInitialBuffer = new AtomicReference[Iterable[Pod]](null)
    eventQueueUnderTest.addSubscriber(1000, testSetBufferSubscriber(receivedInitialBuffer))
    assert(receivedInitialBuffer.get == null)
    executeSubscriptionsExecutor.runUntilIdle()
    assert(receivedInitialBuffer.get != null)
  }

  private def assertIndicesMatch(buffer: mutable.Buffer[Pod], indices: Int*): Unit = {
    assert(buffer === indices.map(podWithIndex))
  }

  private def pushPodWithIndex(index: Int): Unit =
    eventQueueUnderTest.enqueue(podWithIndex(index))

  private def podWithIndex(index: Int): Pod =
    new PodBuilder()
      .editOrNewMetadata()
        .withName(s"pod-$index")
        .endMetadata()
      .build()

  private def testBatchSubscriber(eventBuffer: mutable.Buffer[Pod]): ExecutorPodBatchSubscriber = {
    val subscriber = mock(classOf[ExecutorPodBatchSubscriber])
    when(subscriber.onNextBatch(any(classOf[Iterable[Pod]])))
      .thenAnswer(new Answer[Unit] {
        override def answer(invocationOnMock: InvocationOnMock): Unit = {
          val pods = invocationOnMock.getArgumentAt(0, classOf[Iterable[Pod]])
          eventBuffer ++= pods
        }
      })
    subscriber
  }

  private def testSetBufferSubscriber(
      eventBuffer: AtomicReference[Iterable[Pod]]): ExecutorPodBatchSubscriber = {
    val subscriber = mock(classOf[ExecutorPodBatchSubscriber])
    when(subscriber.onNextBatch(any(classOf[Iterable[Pod]])))
      .thenAnswer(new Answer[Unit] {
        override def answer(invocationOnMock: InvocationOnMock): Unit = {
          val pods = invocationOnMock.getArgumentAt(0, classOf[Iterable[Pod]])
          eventBuffer.set(pods)
        }
      })
    subscriber
  }
}
