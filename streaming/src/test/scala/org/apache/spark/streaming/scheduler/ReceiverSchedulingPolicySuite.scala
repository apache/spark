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
import org.apache.spark.scheduler.{ExecutorCacheTaskLocation, HostTaskLocation, TaskLocation}

class ReceiverSchedulingPolicySuite extends SparkFunSuite {

  val receiverSchedulingPolicy = new ReceiverSchedulingPolicy

  test("rescheduleReceiver: empty executors") {
    val scheduledLocations =
      receiverSchedulingPolicy.rescheduleReceiver(0, None, Map.empty, executors = Seq.empty)
    assert(scheduledLocations === Seq.empty)
  }

  test("rescheduleReceiver: receiver preferredLocation") {
    val executors = Seq(ExecutorCacheTaskLocation("host2", "2"))
    val receiverTrackingInfoMap = Map(
      0 -> ReceiverTrackingInfo(0, ReceiverState.INACTIVE, None, None))
    val scheduledLocations = receiverSchedulingPolicy.rescheduleReceiver(
      0, Some("host1"), receiverTrackingInfoMap, executors)
    assert(scheduledLocations.toSet === Set(HostTaskLocation("host1"), executors(0)))
  }

  test("rescheduleReceiver: return all idle executors if there are any idle executors") {
    val executors = (1 to 5).map(i => ExecutorCacheTaskLocation(s"host$i", s"$i"))
    // executor 1 is busy, others are idle.
    val receiverTrackingInfoMap = Map(
      0 -> ReceiverTrackingInfo(0, ReceiverState.ACTIVE, None, Some(executors(0))))
    val scheduledLocations = receiverSchedulingPolicy.rescheduleReceiver(
      1, None, receiverTrackingInfoMap, executors)
    assert(scheduledLocations.toSet === executors.tail.toSet)
  }

  test("rescheduleReceiver: return all executors that have minimum weight if no idle executors") {
    val executors = Seq(
      ExecutorCacheTaskLocation("host1", "1"),
      ExecutorCacheTaskLocation("host2", "2"),
      ExecutorCacheTaskLocation("host3", "3"),
      ExecutorCacheTaskLocation("host4", "4"),
      ExecutorCacheTaskLocation("host5", "5")
    )
    // Weights: host1 = 1.5, host2 = 0.5, host3 = 1.0, host4 = 0.5, host5 = 0.5
    val receiverTrackingInfoMap = Map(
      0 -> ReceiverTrackingInfo(0, ReceiverState.ACTIVE, None,
        Some(ExecutorCacheTaskLocation("host1", "1"))),
      1 -> ReceiverTrackingInfo(1, ReceiverState.SCHEDULED,
        Some(Seq(ExecutorCacheTaskLocation("host2", "2"), ExecutorCacheTaskLocation("host3", "3"))),
        None),
      2 -> ReceiverTrackingInfo(2, ReceiverState.SCHEDULED,
        Some(Seq(ExecutorCacheTaskLocation("host1", "1"), ExecutorCacheTaskLocation("host3", "3"))),
        None),
      3 -> ReceiverTrackingInfo(4, ReceiverState.SCHEDULED,
        Some(Seq(ExecutorCacheTaskLocation("host4", "4"),
          ExecutorCacheTaskLocation("host5", "5"))), None))
    val scheduledLocations = receiverSchedulingPolicy.rescheduleReceiver(
      4, None, receiverTrackingInfoMap, executors)
    val expectedScheduledLocations = Set(
      ExecutorCacheTaskLocation("host2", "2"),
      ExecutorCacheTaskLocation("host4", "4"),
      ExecutorCacheTaskLocation("host5", "5")
    )
    assert(scheduledLocations.toSet === expectedScheduledLocations)
  }

  test("scheduleReceivers: " +
    "schedule receivers evenly when there are more receivers than executors") {
    val receivers = (0 until 6).map(new RateTestReceiver(_))
    val executors = (0 until 3).map(executorId =>
      ExecutorCacheTaskLocation("localhost", executorId.toString))
    val scheduledLocations = receiverSchedulingPolicy.scheduleReceivers(receivers, executors)
    val numReceiversOnExecutor = mutable.HashMap[TaskLocation, Int]()
    // There should be 2 receivers running on each executor and each receiver has one executor
    scheduledLocations.foreach { case (receiverId, locations) =>
      assert(locations.size == 1)
      assert(locations(0).isInstanceOf[ExecutorCacheTaskLocation])
      numReceiversOnExecutor(locations(0)) = numReceiversOnExecutor.getOrElse(locations(0), 0) + 1
    }
    assert(numReceiversOnExecutor === executors.map(_ -> 2).toMap)
  }

  test("scheduleReceivers: " +
    "schedule receivers evenly when there are more executors than receivers") {
    val receivers = (0 until 3).map(new RateTestReceiver(_))
    val executors = (0 until 6).map(executorId =>
      ExecutorCacheTaskLocation("localhost", executorId.toString))
    val scheduledLocations = receiverSchedulingPolicy.scheduleReceivers(receivers, executors)
    val numReceiversOnExecutor = mutable.HashMap[TaskLocation, Int]()
    // There should be 1 receiver running on each executor and each receiver has two executors
    scheduledLocations.foreach { case (receiverId, locations) =>
      assert(locations.size == 2)
      locations.foreach { l =>
        assert(l.isInstanceOf[ExecutorCacheTaskLocation])
        numReceiversOnExecutor(l) = numReceiversOnExecutor.getOrElse(l, 0) + 1
      }
    }
    assert(numReceiversOnExecutor === executors.map(_ -> 1).toMap)
  }

  test("scheduleReceivers: schedule receivers evenly when the preferredLocations are even") {
    val receivers = (0 until 3).map(new RateTestReceiver(_)) ++
      (3 until 6).map(new RateTestReceiver(_, Some("localhost")))
    val executors = (0 until 3).map(executorId =>
      ExecutorCacheTaskLocation("localhost", executorId.toString)) ++
      (3 until 6).map(executorId =>
        ExecutorCacheTaskLocation("localhost2", executorId.toString))
    val scheduledLocations = receiverSchedulingPolicy.scheduleReceivers(receivers, executors)
    val numReceiversOnExecutor = mutable.HashMap[TaskLocation, Int]()
    // There should be 1 receiver running on each executor and each receiver has 1 executor
    scheduledLocations.foreach { case (receiverId, executors) =>
      assert(executors.size == 1)
      executors.foreach { l =>
        assert(l.isInstanceOf[ExecutorCacheTaskLocation])
        numReceiversOnExecutor(l) = numReceiversOnExecutor.getOrElse(l, 0) + 1
      }
    }
    assert(numReceiversOnExecutor === executors.map(_ -> 1).toMap)
    // Make sure we schedule the receivers to their preferredLocations
    val executorsForReceiversWithPreferredLocation =
      scheduledLocations.filter { case (receiverId, executors) => receiverId >= 3 }.flatMap(_._2)
    // We can simply check the executor set because we only know each receiver only has 1 executor
    assert(executorsForReceiversWithPreferredLocation.toSet ===
      (0 until 3).map(executorId =>
        ExecutorCacheTaskLocation("localhost", executorId.toString)
      ).toSet)
  }

  test("scheduleReceivers: return empty if no receiver") {
    val scheduledLocations = receiverSchedulingPolicy.
      scheduleReceivers(Seq.empty, Seq(ExecutorCacheTaskLocation("localhost", "1")))
    assert(scheduledLocations.isEmpty)
  }

  test("scheduleReceivers: return empty scheduled executors if no executors") {
    val receivers = (0 until 3).map(new RateTestReceiver(_))
    val scheduledLocations = receiverSchedulingPolicy.scheduleReceivers(receivers, Seq.empty)
    scheduledLocations.foreach { case (receiverId, executors) =>
      assert(executors.isEmpty)
    }
  }

}
