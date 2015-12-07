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

package org.apache.spark.deploy.history.yarn.integration

import org.scalatest.Matchers

import org.apache.spark.deploy.history.yarn.YarnHistoryService._
import org.apache.spark.deploy.history.yarn.YarnTimelineUtils._
import org.apache.spark.deploy.history.yarn.server.{TimelineApplicationHistoryInfo, TimelineApplicationAttemptInfo}
import org.apache.spark.deploy.history.yarn.server.YarnProviderUtils._
import org.apache.spark.deploy.history.yarn.testtools.ExtraAssertions
import org.apache.spark.scheduler.cluster.{StubApplicationAttemptId, StubApplicationId}
import org.apache.spark.{Logging, SparkFunSuite}

/**
 * Test of utility methods in [[org.apache.spark.deploy.history.yarn.server.YarnProviderUtils]]
 */
class YarnProviderUtilsSuite extends SparkFunSuite with Logging
  with ExtraAssertions with Matchers {

  def historyInfo(id: String, started: Long, ended: Long, complete: Boolean):
      TimelineApplicationHistoryInfo = {
    historyInfo(id, Some(id), started, ended, ended, complete )
  }

  def historyInfo(appId: String, attemptId: Option[String], started: Long, ended: Long,
      updated: Long, complete: Boolean):
      TimelineApplicationHistoryInfo = {
    val updated = Math.max(started, ended)
    val attempt = new TimelineApplicationAttemptInfo(attemptId,
      started, ended, updated, "user", complete , attemptId.get, attemptId)
    new TimelineApplicationHistoryInfo(appId, appId, List(attempt))
  }

  def historyInfo(old: TimelineApplicationHistoryInfo,
    attempts: List[TimelineApplicationAttemptInfo]): TimelineApplicationHistoryInfo = {
    new TimelineApplicationHistoryInfo(old.id, old.name, attempts)
  }

  val yarnAppId = new StubApplicationId(5, 0)
  val yarnAttemptId = new StubApplicationAttemptId(yarnAppId, 1)

  val h12 = historyInfo("h12", 1, 2, true)
  val h22 = historyInfo("h22", 2, 2, true)
  val i20 = historyInfo("i20", 2, 0, false)
  val i30 = historyInfo("i30", 3, 0, false)
  val h33 = historyInfo("h30", 3, 3, true)
  val h44 = historyInfo("h44", 4, 4, true)
  val iA10_incomplete = historyInfo("iA", 1, 0, false)
  val iA11_completed = historyInfo("iA", 1, 1, true)
  val a1_attempt_1 = historyInfo("iA", Some("attempt_1"), 100, 102, 102, false)
  val a1_attempt_2 = historyInfo("iA", Some("attempt_2"), 200, 202, 202, true)
  val none_incomplete = new TimelineApplicationAttemptInfo(None, 100, 0, 102, "spark", false,
    "001", None)
  val none_completed = new TimelineApplicationAttemptInfo(None, 200, 202, 202, "spark", true,
    "001", None)
  val none_completed_orig_time = new TimelineApplicationAttemptInfo(None, 100, 0, 102, "spark",
    true, "001", None)
  // app attempt started @ 100, updated @102, version 1
  val attempt_1_1 = new TimelineApplicationAttemptInfo(Some("attempt_1_1"),
    100, 0, 102, "spark", false, "001", None, 1)
  // attempt 1.1 updated at time = 102; no version field
  val attempt_1_1_updated = new TimelineApplicationAttemptInfo(Some("attempt_1_1"),
    100, 0, 150, "spark", false, "001", None)
  // attempt 1.1 with the version field updated to 2; it should always be newer
  val attempt_1_1_updated_version = new TimelineApplicationAttemptInfo(Some("attempt_1_1"),
    100, 0, 102, "spark", false, "001", None, 2)
  val attempt_1_2 = new TimelineApplicationAttemptInfo(Some("attempt_1_2"), 200, 202, 202,
    "spark", true, "001", None, 3)

  test("timeShort") {
    assert("unset" === timeShort(0, "unset"))
    assert("unset" !== timeShort(System.currentTimeMillis(), "unset"))
  }

  test("findOldest") {
    assert(Some(h12) === findOldestApplication(List(h12, h22, i20)))
  }

  test("findOldest-2") {
    assert(Some(h22) === findOldestApplication(List(h44, h22, i20)))
  }

  test("findOldest-3") {
    assert(Some(i20) === findOldestApplication(List(h44, h33, i20)))
  }

  test("findOldest-4") {
    assert(None === findOldestApplication(Nil))
  }

  test("findIncomplete") {
    assert(List(i20, i30) === findIncompleteApplications(List(h44, i20, i30, h33)))
  }

  test("findIncomplete-2") {
    assert(Nil === findIncompleteApplications(Nil))
  }

  test("findIncomplete-3") {
    assert(Nil === findIncompleteApplications(List(h44, h33)))
  }

  test("countIncomplete") {
    assert(2 === countIncompleteApplications(List(h44, i20, i30, h33)))
  }

  test("countIncomplete-2") {
    assert(0 === countIncompleteApplications(Nil))
  }

  test("countIncomplete-3") {
    assert(0 === countIncompleteApplications(List(h44, h33)))
  }

  test("findStartOfWindow") {
    assert(Some(i20) === findStartOfWindow(List(h44, i20, i30, h33)))
  }

  test("findStartOfWindow-2") {
    assert(Some(h44) === findStartOfWindow(List(h44, h12, h33)))
  }

  test("combineResults-list-nil") {
    assert((h44 :: Nil) === combineResults(List(h44), Nil))
  }

  test("combineResults-2-Nil-list") {
    assert((h44 :: Nil) === combineResults(Nil, List(h44)))
  }

  test("combineResults-3-Nil-lists") {
    assert(Nil === combineResults(Nil, Nil))
  }

  test("combineResults-5") {
    assert((h44 :: i20 :: Nil) === combineResults(List(h44), List(i20)))
  }

  test("combineResults-6-merge-duplicate-to-one") {
    assert(List(h44) === combineResults(List(h44), List(h44)))
  }

  test("combineResults-7-completed") {
    assert(List(iA11_completed) === combineResults(List(iA10_incomplete), List(iA11_completed)))
  }

  test("merge-multiple_attempts") {
    assert(List(historyInfo(a1_attempt_1, a1_attempt_2.attempts ++ a1_attempt_1.attempts))
       === combineResults(List(a1_attempt_1), List(a1_attempt_2)))
  }

  test("SortApplications-1") {
    assert((h33 :: h44 :: Nil) === sortApplicationsByStartTime(List(h44, h33)))
  }

  test("SortApplications-2") {
    assert((h22 :: i20 :: h33 :: Nil) === sortApplicationsByStartTime(List(h22, i20, h33)))
  }

  test("SortApplications-3") {
    assert((i20 :: h22 :: Nil) === sortApplicationsByStartTime(List(i20, h22)))
  }

  test("findLatest") {
    assert(Some(h22) === findLatestApplication(List(h12, h22, i20)))
  }

  test("findLatest-2") {
    assert(Some(h22) === findLatestApplication(List(h22, i20)))
  }

  test("findLatest-3") {
    assert(Some(i20) === findLatestApplication(List(h12, i20)))
  }

  test("buildEntityIds") {
    val sparkAppId = "spark_app_id_2"
    val attempt = "attempt_id"
    val yarnAppStr = yarnAppId.toString
    val attemptId = Some(yarnAttemptId)
    val attemptIdStr = yarnAttemptId.toString
    assert(attemptIdStr === buildEntityId(yarnAppId, attemptId),
      "all fields")
    assert(attemptIdStr === buildEntityId(yarnAppId, attemptId),
      "no attempt ID")
    assert(yarnAppStr === buildEntityId(yarnAppId, None), "yarnAppId only")
  }

  test("buildApplicationAttemptIdField") {
    val sparkAppId = "spark_app_id_2"
    val attempt = "attempt_id"
    assert(attempt === buildApplicationAttemptIdField(Some(attempt)),
      "all fields")
    assert(SINGLE_ATTEMPT === buildApplicationAttemptIdField(None),
      "attempt = None")
  }

  test("EntityAndBack") {
    val sparkAppId = Some("spark-app-id-1")
    val yarnAppStr = yarnAppId.toString
    val sparkAttemptId = Some("spark-attempt-id")
    val yarnAttemptIdStr = yarnAttemptId.toString

    val entity = createTimelineEntity(yarnAppId,
      Some(yarnAttemptId),
      sparkAppId,
      sparkAttemptId,
      "app",
      "user",
      1000, 0, 1000)
    val entityDescription = describeEntity(entity)
    val ev1 = entity.getOtherInfo.get(FIELD_ENTITY_VERSION)
    val version = numberField(entity, FIELD_ENTITY_VERSION, -1).longValue()
    assert (0 < version, s"wrong version in $entityDescription")

    // build an TimelineApplicationHistoryInfo instance
    val info = toApplicationHistoryInfo(entity)
    assert(yarnAppStr === info.id, "info.id")
    val attempt = info.attempts.head
    assert(sparkAttemptId === attempt.attemptId, s"attempt.attemptId in $attempt")
    assert(yarnAttemptIdStr === attempt.entityId, s"attempt.entityId in $attempt")
    assert(version === attempt.version, s"version in $attempt")
  }

  test("EntityWithoutAttempt") {
    val sparkAppId = Some("spark-app-id-1")
    val yarnAppStr = yarnAppId.toString
    val yarnAttemptIdStr = yarnAttemptId.toString

    val entity = createTimelineEntity(yarnAppId,
      None,
      sparkAppId,
      None,
      "app",
      "user",
      1000, 0, 1000)
    val info = toApplicationHistoryInfo(entity)
    assert(yarnAppStr === info.id)

    val attempt = info.attempts.head
    assert("1" === attempt.attemptId.get, "attempt.attemptId")
    assert(yarnAppStr === attempt.entityId, "attempt.entityId")
  }

  test("MergeHistoryEvents") {
    val one_1 = new TimelineApplicationHistoryInfo("app1", "one", attempt_1_1 :: Nil)
    val one_2 = new TimelineApplicationHistoryInfo("app1", "one", attempt_1_2 :: Nil)
    val one_0 = new TimelineApplicationHistoryInfo("app1", "one", Nil)

    val merge_12 = mergeAttempts(one_1, one_2)
    assertListSize( merge_12.attempts, 2, "merged attempt list")
    assert(List(attempt_1_2, attempt_1_1) === merge_12.attempts)

    val merge_10 = mergeAttempts(one_1, one_0)
    assert(1 === merge_10.attempts.size)
    assert(one_1 === mergeAttempts(one_1, one_1))
  }

  test("MergeHistoryEventsIdNone") {
    val one_1 = new TimelineApplicationHistoryInfo("1", "one", none_incomplete :: Nil)
    val one_2 = new TimelineApplicationHistoryInfo("1", "one", none_completed :: Nil)
    val one_0 = new TimelineApplicationHistoryInfo("1", "one", Nil)
  }

  test("merge-results-None-attemptId-incomplete-first") {
    assert(List(none_completed) === mergeAttemptInfoLists(List(none_incomplete),
      List(none_completed)))
  }

  test("merge-results-None-attemptId-incomplete-second") {
    // and in the other order
    assert(List(none_completed) === mergeAttemptInfoLists(List(none_completed),
      List(none_incomplete)))
  }

  test("MergeAttemptOrdering-1") {
    assert(none_completed === mostRecentAttempt(none_completed, none_incomplete))
  }

  test("MergeAttemptOrdering-2") {
    assert(none_completed === mostRecentAttempt(none_incomplete, none_completed))
  }

  test("MergeAttemptOrdering-3") {
    assert(none_completed === mostRecentAttempt(none_incomplete, none_completed))
  }

  test("MergeAttemptOrdering-4") {
    assert(attempt_1_1_updated === mostRecentAttempt(attempt_1_1, attempt_1_1_updated))
  }

  test("MergeAttemptOrdering-5") {
    assert(attempt_1_1_updated === mostRecentAttempt(attempt_1_1_updated, attempt_1_1))
  }

  test("MergeAttemptOrdering-6") {
    assert(none_completed_orig_time ===
        mostRecentAttempt(none_incomplete, none_completed_orig_time))
  }

  test("MergeAttemptOrdering-7") {
    assert(none_completed_orig_time ===
        mostRecentAttempt(none_completed_orig_time, none_incomplete))
  }

  test("MergeAttemptOrdering-8") {
    assert(attempt_1_1_updated_version === mostRecentAttempt(attempt_1_1,
      attempt_1_1_updated_version))
  }

  test("MergeAttemptOrdering-9") {
    assert(attempt_1_1_updated_version === mostRecentAttempt(attempt_1_1_updated_version,
      attempt_1_1))
  }

}
