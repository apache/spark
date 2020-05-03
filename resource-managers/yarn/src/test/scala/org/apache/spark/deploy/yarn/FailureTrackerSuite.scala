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
package org.apache.spark.deploy.yarn

import org.scalatest.Matchers

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.util.ManualClock

class FailureTrackerSuite extends SparkFunSuite with Matchers {

  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  test("failures expire if validity interval is set") {
    val sparkConf = new SparkConf()
    sparkConf.set(config.EXECUTOR_ATTEMPT_FAILURE_VALIDITY_INTERVAL_MS, 100L)

    val clock = new ManualClock()
    val failureTracker = new FailureTracker(sparkConf, clock)

    clock.setTime(0)
    failureTracker.registerFailureOnHost("host1")
    failureTracker.numFailuresOnHost("host1") should be (1)
    failureTracker.numFailedExecutors should be (1)

    clock.setTime(10)
    failureTracker.registerFailureOnHost("host2")
    failureTracker.numFailuresOnHost("host2") should be (1)
    failureTracker.numFailedExecutors should be (2)

    clock.setTime(20)
    failureTracker.registerFailureOnHost("host1")
    failureTracker.numFailuresOnHost("host1") should be (2)
    failureTracker.numFailedExecutors should be (3)

    clock.setTime(30)
    failureTracker.registerFailureOnHost("host2")
    failureTracker.numFailuresOnHost("host2") should be (2)
    failureTracker.numFailedExecutors should be (4)

    clock.setTime(101)
    failureTracker.numFailuresOnHost("host1") should be (1)
    failureTracker.numFailedExecutors should be (3)

    clock.setTime(231)
    failureTracker.numFailuresOnHost("host1") should be (0)
    failureTracker.numFailuresOnHost("host2") should be (0)
    failureTracker.numFailedExecutors should be (0)
  }


  test("failures never expire if validity interval is not set (-1)") {
    val sparkConf = new SparkConf()

    val clock = new ManualClock()
    val failureTracker = new FailureTracker(sparkConf, clock)

    clock.setTime(0)
    failureTracker.registerFailureOnHost("host1")
    failureTracker.numFailuresOnHost("host1") should be (1)
    failureTracker.numFailedExecutors should be (1)

    clock.setTime(10)
    failureTracker.registerFailureOnHost("host2")
    failureTracker.numFailuresOnHost("host2") should be (1)
    failureTracker.numFailedExecutors should be (2)

    clock.setTime(20)
    failureTracker.registerFailureOnHost("host1")
    failureTracker.numFailuresOnHost("host1") should be (2)
    failureTracker.numFailedExecutors should be (3)

    clock.setTime(30)
    failureTracker.registerFailureOnHost("host2")
    failureTracker.numFailuresOnHost("host2") should be (2)
    failureTracker.numFailedExecutors should be (4)

    clock.setTime(1000)
    failureTracker.numFailuresOnHost("host1") should be (2)
    failureTracker.numFailuresOnHost("host2") should be (2)
    failureTracker.numFailedExecutors should be (4)
  }

}
