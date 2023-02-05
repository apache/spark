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
import org.scalatest.BeforeAndAfter
import scala.collection.mutable

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.util.ManualClock

class ExecutorPodsSnapshotsStoreSuite extends SparkFunSuite with BeforeAndAfter {

  private var eventBufferScheduler: DeterministicScheduler = _
  private var eventQueueUnderTest: ExecutorPodsSnapshotsStoreImpl = _
  private var clock: ManualClock = _

  before {
    eventBufferScheduler = new DeterministicScheduler()
    clock = new ManualClock()
    val conf = new SparkConf()
    eventQueueUnderTest = new ExecutorPodsSnapshotsStoreImpl(eventBufferScheduler, clock, conf)
    ExecutorPodsSnapshot.setShouldCheckAllContainers(false)
  }

  test("Subscribers get notified of events periodically.") {
    val receivedSnapshots1 = mutable.Buffer.empty[ExecutorPodsSnapshot]
    val receivedSnapshots2 = mutable.Buffer.empty[ExecutorPodsSnapshot]
    eventQueueUnderTest.addSubscriber(1000) {
      receivedSnapshots1 ++= _
    }
    eventQueueUnderTest.addSubscriber(2000) {
      receivedSnapshots2 ++= _
    }

    eventBufferScheduler.runUntilIdle()
    assert(receivedSnapshots1 === Seq(ExecutorPodsSnapshot()))
    assert(receivedSnapshots2 === Seq(ExecutorPodsSnapshot()))

    clock.advance(100)
    pushPodWithIndex(1)
    // Force time to move forward so that the buffer is emitted, scheduling the
    // processing task on the subscription executor...
    eventBufferScheduler.tick(1000, TimeUnit.MILLISECONDS)
    // ... then actually execute the subscribers.

    assert(receivedSnapshots1 === Seq(
      ExecutorPodsSnapshot(),
      ExecutorPodsSnapshot(Seq(podWithIndex(1)), 0)))
    assert(receivedSnapshots2 === Seq(ExecutorPodsSnapshot()))

    eventBufferScheduler.tick(1000, TimeUnit.MILLISECONDS)

    // Don't repeat snapshots
    assert(receivedSnapshots1 === Seq(
      ExecutorPodsSnapshot(),
      ExecutorPodsSnapshot(Seq(podWithIndex(1)), 0)))
    assert(receivedSnapshots2 === Seq(
      ExecutorPodsSnapshot(),
      ExecutorPodsSnapshot(Seq(podWithIndex(1)), 0)))
    pushPodWithIndex(2)
    pushPodWithIndex(3)
    eventBufferScheduler.tick(1000, TimeUnit.MILLISECONDS)

    assert(receivedSnapshots1 === Seq(
      ExecutorPodsSnapshot(),
      ExecutorPodsSnapshot(Seq(podWithIndex(1)), 0),
      ExecutorPodsSnapshot(Seq(podWithIndex(1), podWithIndex(2)), 0),
      ExecutorPodsSnapshot(Seq(podWithIndex(1), podWithIndex(2), podWithIndex(3)), 0)))
    assert(receivedSnapshots2 === Seq(
      ExecutorPodsSnapshot(),
      ExecutorPodsSnapshot(Seq(podWithIndex(1)), 0)))

    eventBufferScheduler.tick(1000, TimeUnit.MILLISECONDS)
    assert(receivedSnapshots1 === Seq(
      ExecutorPodsSnapshot(),
      ExecutorPodsSnapshot(Seq(podWithIndex(1)), 0),
      ExecutorPodsSnapshot(Seq(podWithIndex(1), podWithIndex(2)), 0),
      ExecutorPodsSnapshot(Seq(podWithIndex(1), podWithIndex(2), podWithIndex(3)), 0)))
    assert(receivedSnapshots1 === receivedSnapshots2)
  }

  test("Even without sending events, initially receive an empty buffer.") {
    val receivedInitialSnapshot = new AtomicReference[Seq[ExecutorPodsSnapshot]](null)
    eventQueueUnderTest.addSubscriber(1000) {
      receivedInitialSnapshot.set
    }
    assert(receivedInitialSnapshot.get == null)
    eventBufferScheduler.runUntilIdle()
    assert(receivedInitialSnapshot.get === Seq(ExecutorPodsSnapshot()))
  }

  test("Replacing the snapshot passes the new snapshot to subscribers.") {
    val receivedSnapshots = mutable.Buffer.empty[ExecutorPodsSnapshot]
    eventQueueUnderTest.addSubscriber(1000) {
      receivedSnapshots ++= _
    }
    eventQueueUnderTest.updatePod(podWithIndex(1))
    eventBufferScheduler.tick(1000, TimeUnit.MILLISECONDS)
    assert(receivedSnapshots === Seq(
      ExecutorPodsSnapshot(),
      ExecutorPodsSnapshot(Seq(podWithIndex(1)), 0)))
    clock.advance(100)
    eventQueueUnderTest.replaceSnapshot(Seq(podWithIndex(2)))
    eventBufferScheduler.tick(1000, TimeUnit.MILLISECONDS)
    assert(receivedSnapshots === Seq(
      ExecutorPodsSnapshot(),
      ExecutorPodsSnapshot(Seq(podWithIndex(1)), 0),
      ExecutorPodsSnapshot(Seq(podWithIndex(2)), 100)))
  }

  private def pushPodWithIndex(index: Int): Unit =
    eventQueueUnderTest.updatePod(podWithIndex(index))

  private def podWithIndex(index: Int): Pod =
    new PodBuilder()
      .editOrNewMetadata()
        .withName(s"pod-$index")
        .addToLabels(SPARK_EXECUTOR_ID_LABEL, index.toString)
        .endMetadata()
      .editOrNewStatus()
        .withPhase("running")
        .endStatus()
      .build()
}
