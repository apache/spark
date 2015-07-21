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

import org.apache.spark.SparkFunSuite

class ReceiverSchedulingPolicySuite extends SparkFunSuite {

  val receiverSchedulingPolicy = new ReceiverSchedulingPolicy

  test("empty executors") {
    val scheduledLocations =
      receiverSchedulingPolicy.scheduleReceiver(0, None, Map.empty, executors = Seq.empty)
    assert(scheduledLocations === Seq.empty)
  }

  test("receiver preferredLocation") {
    val receiverTrackingInfoMap = Map(
      0 -> ReceiverTrackingInfo(0, ReceiverState.INACTIVE, None, None))
    val scheduledLocations = receiverSchedulingPolicy.scheduleReceiver(
      0, Some("host1"), receiverTrackingInfoMap, executors = Seq("host2"))
    assert(scheduledLocations.toSet === Set("host1", "host2"))
  }

  test("return all idle executors if more than 3 idle executors") {
    val executors = Seq("host1", "host2", "host3", "host4", "host5")
    // host3 is idle
    val receiverTrackingInfoMap = Map(
      0 -> ReceiverTrackingInfo(0, ReceiverState.ACTIVE, None, Some("host1")))
    val scheduledLocations = receiverSchedulingPolicy.scheduleReceiver(
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
    val scheduledLocations = receiverSchedulingPolicy.scheduleReceiver(
      3, None, receiverTrackingInfoMap, executors)
    assert(scheduledLocations.toSet === Set("host2", "host4", "host5"))
  }
}
