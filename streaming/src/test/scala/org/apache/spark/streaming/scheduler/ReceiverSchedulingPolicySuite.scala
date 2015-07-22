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

package org.apache.spark.streaming.scheduler

import scala.collection.mutable

import org.apache.spark.SparkFunSuite
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

class ReceiverSchedulingPolicySuite extends SparkFunSuite {

  val receiverSchedulingPolicy = new ReceiverSchedulingPolicy

  test("empty executors") {
    val scheduledLocations =
      receiverSchedulingPolicy.rescheduleReceiver(0, None, Map.empty, executors = Seq.empty)
    assert(scheduledLocations === Seq.empty)
  }

  test("receiver preferredLocation") {
    val receiverTrackingInfoMap = Map(
      0 -> ReceiverTrackingInfo(0, ReceiverState.INACTIVE, None, None))
    val scheduledLocations = receiverSchedulingPolicy.rescheduleReceiver(
      0, Some("host1"), receiverTrackingInfoMap, executors = Seq("host2"))
    assert(scheduledLocations.toSet === Set("host1", "host2"))
  }

  test("return all idle executors if more than 3 idle executors") {
    val executors = Seq("host1", "host2", "host3", "host4", "host5")
    // host3 is idle
    val receiverTrackingInfoMap = Map(
      0 -> ReceiverTrackingInfo(0, ReceiverState.ACTIVE, None, Some("host1")))
    val scheduledLocations = receiverSchedulingPolicy.rescheduleReceiver(
      1, None, receiverTrackingInfoMap, executors)
    assert(scheduledLocations.toSet === Set("host2", "host3", "host4", "host5"))
  }

  test("return 3 best options if less than 3 idle executors") {
    val executors = Seq("host1", "host2", "host3", "host4", "host5")
    // Weights: host1 = 1.5, host2 = 0.5, host3 = 1.0
    // host4 and host5 are idle
    val receiverTrackingInfoMap = Map(
      0 -> ReceiverTrackingInfo(0, ReceiverState.ACTIVE, None, Some("host1")),
      1 -> ReceiverTrackingInfo(1, ReceiverState.SCHEDULED, Some(Seq("host2", "host3")), None),
      2 -> ReceiverTrackingInfo(1, ReceiverState.SCHEDULED, Some(Seq("host1", "host3")), None))
    val scheduledLocations = receiverSchedulingPolicy.rescheduleReceiver(
      3, None, receiverTrackingInfoMap, executors)
    assert(scheduledLocations.toSet === Set("host2", "host4", "host5"))
  }

  test("scheduleReceivers should schedule receivers evenly " +
    "when there are more receivers than executors") {
    val receivers = (0 until 6).map(new DummyReceiver(_))
    val executors = (10000 until 10003).map(port => s"localhost:${port}")
    val scheduledLocations = receiverSchedulingPolicy.scheduleReceivers(receivers, executors)
    val numReceiversOnExecutor = mutable.HashMap[String, Int]()
    // There should be 2 receivers running on each executor and each receiver has one location
    scheduledLocations.foreach { case (receiverId, locations) =>
      assert(locations.size == 1)
      numReceiversOnExecutor(locations(0)) = numReceiversOnExecutor.getOrElse(locations(0), 0) + 1
    }
    assert(numReceiversOnExecutor === executors.map(_ -> 2).toMap)
  }


  test("scheduleReceivers should schedule receivers evenly " +
    "when there are more executors than receivers") {
    val receivers = (0 until 3).map(new DummyReceiver(_))
    val executors = (10000 until 10006).map(port => s"localhost:${port}")
    val scheduledLocations = receiverSchedulingPolicy.scheduleReceivers(receivers, executors)
    val numReceiversOnExecutor = mutable.HashMap[String, Int]()
    // There should be 1 receiver running on each executor and each receiver has two locations
    scheduledLocations.foreach { case (receiverId, locations) =>
      assert(locations.size == 2)
      locations.foreach { l =>
        numReceiversOnExecutor(l) = numReceiversOnExecutor.getOrElse(l, 0) + 1
      }
    }
    assert(numReceiversOnExecutor === executors.map(_ -> 1).toMap)
  }

  test("scheduleReceivers should schedule receivers evenly " +
    "when the preferredLocations are even") {
    val receivers = (0 until 3).map(new DummyReceiver(_)) ++
      (3 until 6).map(new DummyReceiver(_, Some("localhost")))
    val executors = (10000 until 10003).map(port => s"localhost:${port}") ++
      (10003 until 10006).map(port => s"localhost2:${port}")
    val scheduledLocations = receiverSchedulingPolicy.scheduleReceivers(receivers, executors)
    val numReceiversOnExecutor = mutable.HashMap[String, Int]()
    // There should be 1 receiver running on each executor and each receiver has 1 location
    scheduledLocations.foreach { case (receiverId, locations) =>
      assert(locations.size == 1)
      locations.foreach { l =>
        numReceiversOnExecutor(l) = numReceiversOnExecutor.getOrElse(l, 0) + 1
      }
    }
    assert(numReceiversOnExecutor === executors.map(_ -> 1).toMap)
    // Make sure we schedule the receivers to their preferredLocations
    val locationsForReceiversWithPreferredLocation =
      scheduledLocations.filter { case (receiverId, locations) => receiverId >= 3 }.flatMap(_._2)
    // We can simply check the location set because we only know each receiver only has 1 location
    assert(locationsForReceiversWithPreferredLocation.toSet ===
      (10000 until 10003).map(port => s"localhost:${port}").toSet)
  }

  test("scheduleReceivers should return empty if no receiver") {
    assert(receiverSchedulingPolicy.scheduleReceivers(Seq.empty, Seq("localhost:10000")).isEmpty)
  }
}

/**
 * Dummy receiver implementation
 */
private class DummyReceiver(receiverId: Int, host: Option[String] = None)
  extends Receiver[Int](StorageLevel.MEMORY_ONLY) {

  setReceiverId(receiverId)

  override def onStart(): Unit = {}

  override def onStop(): Unit = {}

  override def preferredLocation: Option[String] = host
}
