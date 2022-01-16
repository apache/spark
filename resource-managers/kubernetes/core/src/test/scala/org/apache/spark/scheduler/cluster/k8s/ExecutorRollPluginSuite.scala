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

import java.util.Date

import org.junit.Assert.assertEquals
import org.scalatest.PrivateMethodTester

import org.apache.spark.SparkFunSuite
import org.apache.spark.deploy.k8s.Config.ExecutorRollPolicy
import org.apache.spark.status.api.v1.ExecutorSummary

class ExecutorRollPluginSuite extends SparkFunSuite with PrivateMethodTester {

  val plugin = new ExecutorRollPlugin().driverPlugin()

  private val _choose = PrivateMethod[Option[String]](Symbol("choose"))

  val driverSummary = new ExecutorSummary("driver", "host:port", true, 1,
    10, 10, 1, 1, 1,
    0, 0, 1, 100,
    1, 100, 100,
    10, false, 20, new Date(1639300000000L),
    Option.empty, Option.empty, Map(), Option.empty, Set(), Option.empty, Map(), Map(), 1,
    false, Set())

  val execWithSmallestID = new ExecutorSummary("1", "host:port", true, 1,
    10, 10, 1, 1, 1,
    0, 0, 1, 100,
    20, 100, 100,
    10, false, 20, new Date(1639300001000L),
    Option.empty, Option.empty, Map(), Option.empty, Set(), Option.empty, Map(), Map(), 1,
    false, Set())

  // The smallest addTime
  val execWithSmallestAddTime = new ExecutorSummary("2", "host:port", true, 1,
    10, 10, 1, 1, 1,
    0, 0, 1, 100,
    20, 100, 100,
    10, false, 20, new Date(1639300000000L),
    Option.empty, Option.empty, Map(), Option.empty, Set(), Option.empty, Map(), Map(), 1,
    false, Set())

  // The biggest totalGCTime
  val execWithBiggestTotalGCTime = new ExecutorSummary("3", "host:port", true, 1,
    10, 10, 1, 1, 1,
    0, 0, 1, 100,
    40, 100, 100,
    10, false, 20, new Date(1639300002000L),
    Option.empty, Option.empty, Map(), Option.empty, Set(), Option.empty, Map(), Map(), 1,
    false, Set())

  // The biggest totalDuration
  val execWithBiggestTotalDuration = new ExecutorSummary("4", "host:port", true, 1,
    10, 10, 1, 1, 1,
    0, 0, 4, 400,
    20, 100, 100,
    10, false, 20, new Date(1639300003000L),
    Option.empty, Option.empty, Map(), Option.empty, Set(), Option.empty, Map(), Map(), 1,
    false, Set())

  // The biggest failedTasks
  val execWithBiggestFailedTasks = new ExecutorSummary("5", "host:port", true, 1,
    10, 10, 1, 1, 1,
    5, 0, 1, 100,
    20, 100, 100,
    10, false, 20, new Date(1639300003000L),
    Option.empty, Option.empty, Map(), Option.empty, Set(), Option.empty, Map(), Map(), 1,
    false, Set())

  // The biggest average duration (= totalDuration / totalTask)
  val execWithBiggestAverageDuration = new ExecutorSummary("6", "host:port", true, 1,
    10, 10, 1, 1, 1,
    0, 0, 2, 300,
    20, 100, 100,
    10, false, 20, new Date(1639300003000L),
    Option.empty, Option.empty, Map(), Option.empty, Set(), Option.empty, Map(), Map(), 1,
    false, Set())

  // The executor with no tasks
  val execWithoutTasks = new ExecutorSummary("7", "host:port", true, 1,
    0, 0, 1, 0, 0,
    0, 0, 0, 0,
    0, 0, 0,
    0, false, 0, new Date(1639300001000L),
    Option.empty, Option.empty, Map(), Option.empty, Set(), Option.empty, Map(), Map(), 1,
    false, Set())

  // This is used to stabilize 'mean' and 'sd' in OUTLIER test cases.
  val execNormal = new ExecutorSummary("8", "host:port", true, 1,
    10, 10, 1, 1, 1,
    4, 0, 2, 280,
    30, 100, 100,
    10, false, 20, new Date(1639300001000L),
    Option.empty, Option.empty, Map(), Option.empty, Set(), Option.empty, Map(), Map(), 1,
    false, Set())

  val list = Seq(driverSummary, execWithSmallestID, execWithSmallestAddTime,
    execWithBiggestTotalGCTime, execWithBiggestTotalDuration, execWithBiggestFailedTasks,
    execWithBiggestAverageDuration, execWithoutTasks, execNormal)

  override def beforeEach(): Unit = {
    super.beforeEach()
    plugin.asInstanceOf[ExecutorRollDriverPlugin].minTasks = 0
  }

  test("Empty executor list") {
    ExecutorRollPolicy.values.foreach { value =>
      assertEquals(None, plugin.invokePrivate[Option[String]](_choose(Seq.empty, value)))
    }
  }

  test("Driver summary should be ignored") {
    ExecutorRollPolicy.values.foreach { value =>
      assertEquals(plugin.invokePrivate(_choose(Seq(driverSummary), value)), None)
    }
  }

  test("A one-item executor list") {
    ExecutorRollPolicy.values.foreach { value =>
      assertEquals(
        Some(execWithSmallestID.id),
        plugin.invokePrivate(_choose(Seq(execWithSmallestID), value)))
    }
  }

  test("SPARK-37806: All policy should ignore executor if totalTasks < minTasks") {
    plugin.asInstanceOf[ExecutorRollDriverPlugin].minTasks = 1000
    ExecutorRollPolicy.values.foreach { value =>
      assertEquals(None, plugin.invokePrivate(_choose(list, value)))
    }
  }

  test("Policy: ID") {
    assertEquals(Some("1"), plugin.invokePrivate(_choose(list, ExecutorRollPolicy.ID)))
  }

  test("Policy: ADD_TIME") {
    assertEquals(Some("2"), plugin.invokePrivate(_choose(list, ExecutorRollPolicy.ADD_TIME)))
  }

  test("Policy: TOTAL_GC_TIME") {
    assertEquals(Some("3"), plugin.invokePrivate(_choose(list, ExecutorRollPolicy.TOTAL_GC_TIME)))
  }

  test("Policy: TOTAL_DURATION") {
    assertEquals(Some("4"), plugin.invokePrivate(_choose(list, ExecutorRollPolicy.TOTAL_DURATION)))
  }

  test("Policy: FAILED_TASKS") {
    assertEquals(Some("5"), plugin.invokePrivate(_choose(list, ExecutorRollPolicy.FAILED_TASKS)))
  }

  test("Policy: AVERAGE_DURATION") {
    assertEquals(
      Some("6"),
      plugin.invokePrivate(_choose(list, ExecutorRollPolicy.AVERAGE_DURATION)))
  }

  test("Policy: OUTLIER - Work like TOTAL_DURATION if there is no outlier") {
    assertEquals(
      plugin.invokePrivate(_choose(list, ExecutorRollPolicy.TOTAL_DURATION)),
      plugin.invokePrivate(_choose(list, ExecutorRollPolicy.OUTLIER)))
  }

  test("Policy: OUTLIER - Detect an average task duration outlier") {
    val outlier = new ExecutorSummary("9999", "host:port", true, 1,
      0, 0, 1, 0, 0,
      3, 0, 1, 300,
      20, 0, 0,
      0, false, 0, new Date(1639300001000L),
      Option.empty, Option.empty, Map(), Option.empty, Set(), Option.empty, Map(), Map(), 1,
      false, Set())
    assertEquals(
      plugin.invokePrivate(_choose(list :+ outlier, ExecutorRollPolicy.AVERAGE_DURATION)),
      plugin.invokePrivate(_choose(list :+ outlier, ExecutorRollPolicy.OUTLIER)))
  }

  test("Policy: OUTLIER - Detect a total task duration outlier") {
    val outlier = new ExecutorSummary("9999", "host:port", true, 1,
      0, 0, 1, 0, 0,
      3, 0, 1000, 1000,
      0, 0, 0,
      0, false, 0, new Date(1639300001000L),
      Option.empty, Option.empty, Map(), Option.empty, Set(), Option.empty, Map(), Map(), 1,
      false, Set())
    assertEquals(
      plugin.invokePrivate(_choose(list :+ outlier, ExecutorRollPolicy.TOTAL_DURATION)),
      plugin.invokePrivate(_choose(list :+ outlier, ExecutorRollPolicy.OUTLIER)))
  }

  test("Policy: OUTLIER - Detect a total GC time outlier") {
    val outlier = new ExecutorSummary("9999", "host:port", true, 1,
      0, 0, 1, 0, 0,
      3, 0, 1, 100,
      1000, 0, 0,
      0, false, 0, new Date(1639300001000L),
      Option.empty, Option.empty, Map(), Option.empty, Set(), Option.empty, Map(), Map(), 1,
      false, Set())
    assertEquals(
      plugin.invokePrivate(_choose(list :+ outlier, ExecutorRollPolicy.TOTAL_GC_TIME)),
      plugin.invokePrivate(_choose(list :+ outlier, ExecutorRollPolicy.OUTLIER)))
  }
}
