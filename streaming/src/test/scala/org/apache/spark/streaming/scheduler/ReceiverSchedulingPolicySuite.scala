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

class ReceiverSchedulingPolicySuite extends SparkFunSuite {

  val receiverSchedulingPolicy = new ReceiverSchedulingPolicy

  test("rescheduleReceiver: empty executors") {
    val scheduledExecutors =
      receiverSchedulingPolicy.rescheduleReceiver(0, None, Map.empty, executors = Seq.empty)
    assert(scheduledExecutors === Seq.empty)
  }

  test("rescheduleReceiver: receiver preferredLocation") {
    val receiverTrackingInfoMap = Map(
      0 -> ReceiverTrackingInfo(0, ReceiverState.INACTIVE, None, None))
    val scheduledExecutors = receiverSchedulingPolicy.rescheduleReceiver(
      0, Some("host1"), receiverTrackingInfoMap, executors = Seq("host2"))
    assert(scheduledExecutors.toSet === Set("host1", "host2"))
  }

  test("rescheduleReceiver: return all idle executors if there are any idle executors") {
    val executors = Seq("host1", "host2", "host3", "host4", "host5")
    // host3 is idle
    val receiverTrackingInfoMap = Map(
      0 -> ReceiverTrackingInfo(0, ReceiverState.ACTIVE, None, Some("host1")))
    val scheduledExecutors = receiverSchedulingPolicy.rescheduleReceiver(
      1, None, receiverTrackingInfoMap, executors)
    assert(scheduledExecutors.toSet === Set("host2", "host3", "host4", "host5"))
  }

  test("rescheduleReceiver: return all executors that have minimum weight if no idle executors") {
    val executors = Seq("host1", "host2", "host3", "host4", "host5")
    // Weights: host1 = 1.5, host2 = 0.5, host3 = 1.0, host4 = 0.5, host5 = 0.5
    val receiverTrackingInfoMap = Map(
      0 -> ReceiverTrackingInfo(0, ReceiverState.ACTIVE, None, Some("host1")),
      1 -> ReceiverTrackingInfo(1, ReceiverState.SCHEDULED, Some(Seq("host2", "host3")), None),
      2 -> ReceiverTrackingInfo(2, ReceiverState.SCHEDULED, Some(Seq("host1", "host3")), None),
      3 -> ReceiverTrackingInfo(4, ReceiverState.SCHEDULED, Some(Seq("host4", "host5")), None))
    val scheduledExecutors = receiverSchedulingPolicy.rescheduleReceiver(
      4, None, receiverTrackingInfoMap, executors)
    assert(scheduledExecutors.toSet === Set("host2", "host4", "host5"))
  }

  test("scheduleReceivers: " +
    "schedule receivers evenly when there are more receivers than executors") {
    val receivers = (0 until 6).map(new RateTestReceiver(_))
    val executors = (10000 until 10003).map(port => s"localhost:${port}")
    val scheduledExecutors = receiverSchedulingPolicy.scheduleReceivers(receivers, executors)
    val numReceiversOnExecutor = mutable.HashMap[String, Int]()
    // There should be 2 receivers running on each executor and each receiver has one executor
    scheduledExecutors.foreach { case (receiverId, executors) =>
      assert(executors.size == 1)
      numReceiversOnExecutor(executors(0)) = numReceiversOnExecutor.getOrElse(executors(0), 0) + 1
    }
    assert(numReceiversOnExecutor === executors.map(_ -> 2).toMap)
  }


  test("scheduleReceivers: " +
    "schedule receivers evenly when there are more executors than receivers") {
    val receivers = (0 until 3).map(new RateTestReceiver(_))
    val executors = (10000 until 10006).map(port => s"localhost:${port}")
    val scheduledExecutors = receiverSchedulingPolicy.scheduleReceivers(receivers, executors)
    val numReceiversOnExecutor = mutable.HashMap[String, Int]()
    // There should be 1 receiver running on each executor and each receiver has two executors
    scheduledExecutors.foreach { case (receiverId, executors) =>
      assert(executors.size == 2)
      executors.foreach { l =>
        numReceiversOnExecutor(l) = numReceiversOnExecutor.getOrElse(l, 0) + 1
      }
    }
    assert(numReceiversOnExecutor === executors.map(_ -> 1).toMap)
  }

  test("scheduleReceivers: schedule receivers evenly when the preferredLocations are even") {
    val receivers = (0 until 3).map(new RateTestReceiver(_)) ++
      (3 until 6).map(new RateTestReceiver(_, Some("localhost")))
    val executors = (10000 until 10003).map(port => s"localhost:${port}") ++
      (10003 until 10006).map(port => s"localhost2:${port}")
    val scheduledExecutors = receiverSchedulingPolicy.scheduleReceivers(receivers, executors)
    val numReceiversOnExecutor = mutable.HashMap[String, Int]()
    // There should be 1 receiver running on each executor and each receiver has 1 executor
    scheduledExecutors.foreach { case (receiverId, executors) =>
      assert(executors.size == 1)
      executors.foreach { l =>
        numReceiversOnExecutor(l) = numReceiversOnExecutor.getOrElse(l, 0) + 1
      }
    }
    assert(numReceiversOnExecutor === executors.map(_ -> 1).toMap)
    // Make sure we schedule the receivers to their preferredLocations
    val executorsForReceiversWithPreferredLocation =
      scheduledExecutors.filter { case (receiverId, executors) => receiverId >= 3 }.flatMap(_._2)
    // We can simply check the executor set because we only know each receiver only has 1 executor
    assert(executorsForReceiversWithPreferredLocation.toSet ===
      (10000 until 10003).map(port => s"localhost:${port}").toSet)
  }

  test("scheduleReceivers: return empty if no receiver") {
    assert(receiverSchedulingPolicy.scheduleReceivers(Seq.empty, Seq("localhost:10000")).isEmpty)
  }

  test("scheduleReceivers: return empty scheduled executors if no executors") {
    val receivers = (0 until 3).map(new RateTestReceiver(_))
    val scheduledExecutors = receiverSchedulingPolicy.scheduleReceivers(receivers, Seq.empty)
    scheduledExecutors.foreach { case (receiverId, executors) =>
      assert(executors.isEmpty)
    }
  }

}
