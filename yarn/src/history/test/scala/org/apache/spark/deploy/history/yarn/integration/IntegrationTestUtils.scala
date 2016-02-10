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

import java.io.IOException
import java.net.URL

import org.apache.hadoop.yarn.api.records.{ApplicationId, ApplicationReport, YarnApplicationState}
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationReportPBImpl
import org.scalatest.exceptions.TestFailedException

import org.apache.spark.deploy.history.yarn.rest.SpnegoUrlConnector
import org.apache.spark.deploy.history.yarn.server.{TimelineApplicationHistoryInfo, TimelineApplicationAttemptInfo, YarnHistoryProvider}
import org.apache.spark.deploy.history.yarn.server.YarnProviderUtils._
import org.apache.spark.deploy.history.yarn.testtools.YarnTestUtils._
import org.apache.spark.scheduler.cluster.{StubApplicationAttemptId, StubApplicationId}

/**
 * Utils for the integration test, including provider-related code
 */
private[yarn] trait IntegrationTestUtils {

  def appHistoryInfo(
      id: String,
      attempts: List[TimelineApplicationAttemptInfo]): TimelineApplicationHistoryInfo = {
    new TimelineApplicationHistoryInfo(id, id, attempts)
  }

  def appHistoryInfo(
      id: String,
      attempt: TimelineApplicationAttemptInfo): TimelineApplicationHistoryInfo = {
    new TimelineApplicationHistoryInfo(id, id, attempt :: Nil)
  }

  def attempt(
      id: String,
      startTime: Long,
      endTime: Long,
      lastUpdated: Long,
      completed: Boolean = false): TimelineApplicationAttemptInfo = {
    new TimelineApplicationAttemptInfo(Some(id), startTime, endTime, lastUpdated, "user",
      completed, id, Some(id))
  }

  /**
   * Create a stub application report
   * @param id integer app Id (essentially the RM counter)
   * @param clusterTimestamp timestamp for application Id
   * @param attempt attempt ID
   * @param state app state
   * @param startTime start time
   * @param finishTime finish time or 0
   * @return the report
   */
  def stubApplicationReport(
      id: Int,
      clusterTimestamp: Long,
      attempt: Int,
      state: YarnApplicationState,
      startTime: Long,
      finishTime: Long = 0): ApplicationReport = {
    val yarnId = new StubApplicationId(id, clusterTimestamp)
    // this is tagged as hadoop private. The alternate tactic: create your own implementation,
    // is brittle against Hadoop versions, as new fields are added. Using this
    // class does at least ensure that it is current.
    val report = new ApplicationReportPBImpl()
    report.setApplicationId(yarnId)
    report.setCurrentApplicationAttemptId(new StubApplicationAttemptId(yarnId, attempt))
    report.setYarnApplicationState(state)
    report.setStartTime(startTime)
    report.setFinishTime(finishTime)
    report
  }

  /**
   * Create an attempt from an application report;
   * @param report source report
   * @param endTime end time
   * @param completed completed flag
   * @param updateTime update time, will be taken from report start time otherwise
   * @return
   */
  def attemptFromAppReport(
      report: ApplicationReport,
      endTime: Long,
      completed: Boolean,
      updateTime: Long = 0): TimelineApplicationAttemptInfo = {

    val entityId = report.getCurrentApplicationAttemptId.toString
    val updated = if (updateTime > 0) updateTime else report.getStartTime
    new TimelineApplicationAttemptInfo(
      Some(entityId),
      report.getStartTime,
      endTime,
      updated,
      "user",
      completed,
      entityId,
      Some(entityId)
    )
  }

  def appHistoryInfoFromAppReport(report: ApplicationReport, endTime: Long, completed: Boolean)
    : TimelineApplicationHistoryInfo = {
    val attempt = attemptFromAppReport(report, endTime, completed)
    val id = report.getApplicationId.toString
    new TimelineApplicationHistoryInfo(id, id, List(attempt))
  }

  /**
   * Wait for the listing size to match that desired.
   * @param provider provider
   * @param size size to require
   * @param timeout timeout
   * @return the successful listing
   */
  def awaitApplicationListingSize(provider: YarnHistoryProvider, size: Long, timeout: Long)
    : Seq[TimelineApplicationHistoryInfo] = {
    def listingProbe(): Outcome = {
      val listing = provider.getListing()
      outcomeFromBool(listing.size == size)
    }
    def failure(outcome: Outcome, i: Int, b: Boolean): Unit = {
      fail(s"after $i attempts, provider listing size !=$size:  ${provider.getListing()}\n" +
          s"$provider")
    }
    spinForState(s"await listing size=$size", 100, timeout, listingProbe, failure)
    provider.getListing()
  }

  /**
   * Wait for the application to be listed with the given number of application attempts.
   *
   * The function retries if the application is not found in the listing or if the
   * number of attempts is not that required
   * @param provider history provider
   * @param appId application ID
   * @param attempts number of required attempts which the entry must have
   * @param timeout timeout
   * @return the application details.
   */
  def awaitListingEntry(
      provider: YarnHistoryProvider,
      appId: String,
      attempts: Int,
      timeout: Long): TimelineApplicationHistoryInfo = {
    def probe(): Outcome = {
      findAppById(provider.getListing(), appId) match {
        case Some(applicationInfo) => outcomeFromBool(applicationInfo.attempts.size == attempts)
        case None => Retry()
      }
    }
    // complex failure reporting
    def failure(outcome: Outcome, i: Int, b: Boolean): Unit = {
      val listing = provider.getListing()
      findAppById(listing, appId) match {
        case Some(info) =>
          fail(s"After $timeout mS, application $appId did not have $attempts attempts:" +
              s" ${describeApplicationHistoryInfo(info)}")
        case None =>
          fail(s"After $timeout mS, application $appId was not found in ${provider.getListing()}")
      }
    }
    spinForState(s"await listing entry=$appId", 100, timeout, probe, failure)
    lookupApplication(provider.getListing(), appId)
  }

  /**
   * Wait for the refresh count to increment by at least one iteration
   * @param provider provider
   * @param timeout timeout
   * @return the successful listing
   */
  def awaitRefreshExecuted(
      provider: YarnHistoryProvider,
      triggerRefresh: Boolean,
      timeout: Long): Unit = {
    val initialCount = provider.refreshCount
    def listingProbe(): Outcome = {
      outcomeFromBool(provider.refreshCount > initialCount)
    }
    def failure(outcome: Outcome, i: Int, b: Boolean): Unit = {
      fail(s"After $i attempts, refresh count is $initialCount: $provider")
    }
    require(provider.isRefreshThreadRunning,
      s"refresh thread is not running in $provider")
    if (triggerRefresh) {
      provider.triggerRefresh()
    }
    spinForState("await refresh count", 100, timeout, listingProbe, failure)
  }

  /**
   * Spin awaiting a URL to not contain some text
   * @param connector connector to use
   * @param url URL to probe
   * @param text text which must not be present
   * @param timeout timeout in mils
   */
  def awaitURLDoesNotContainText(
      connector: SpnegoUrlConnector,
      url: URL,
      text: String,
      timeout: Long,
      message: String = ""): String = {
    def get: String = {
      connector.execHttpOperation("GET", url, null, "").responseBody
    }
    def probe(): Outcome = {
      try {
        outcomeFromBool(!get.contains(text))
      } catch {
        case ioe: IOException =>
          Retry()
        case ex: Exception =>
          throw ex
      }
    }


    // failure action is simply to attempt the connection without
    // catching the exception raised
    def failure(outcome: Outcome, iterations: Int, timeout: Boolean): Unit = {
      assertDoesNotContain(get, text, message)
    }

    spinForState(s"Awaiting a response from URL $url",
      interval = 50, timeout = timeout, probe = probe, failure = failure)

    get
  }

  /**
   * Spin awaiting a URL to contain some text
   * @param connector connector to use
   * @param url URL to probe
   * @param text text which must be present
   * @param timeout timeout in mils
   */
  def awaitURLContainsText(
      connector: SpnegoUrlConnector,
      url: URL,
      text: String,
      timeout: Long): String = {
    def get: String = {
      connector.execHttpOperation("GET", url, null, "").responseBody
    }
    def probe(): Outcome = {
      try {
        outcomeFromBool(get.contains(text))
      } catch {
        case ioe: IOException =>
          Retry()
        case ex: Exception =>
          throw ex
      }
    }

    /*
     failure action is simply to attempt the connection without
     catching the exception raised
     */
    def failure(outcome: Outcome, iterations: Int, timeout: Boolean): Unit = {
      assertContains(get, text)
    }

    spinForState(s"Awaiting a response from URL $url",
      interval = 50, timeout = timeout, probe = probe, failure = failure)

    get
  }

  def lookupApplication(listing: Seq[TimelineApplicationHistoryInfo], id: ApplicationId)
      : TimelineApplicationHistoryInfo = {
    lookupApplication(listing, id.toString)
  }

  def lookupApplication(listing: Seq[TimelineApplicationHistoryInfo], id: String)
    : TimelineApplicationHistoryInfo = {
    findAppById(listing, id) match {
      case Some(applicationInfo2) =>
        applicationInfo2
      case None =>
        throw new TestFailedException(s"Did not find $id entry in $listing", 4)
    }
  }

  def assertAppCompleted(info: TimelineApplicationHistoryInfo, text: String = ""): Unit = {
    assert(isCompleted(info), s"$text $info not flagged as completed")
  }

  def assertCompletedAttempt(attempt: TimelineApplicationAttemptInfo, text: String = ""): Unit = {
    assert(attempt.completed, s"$text attempt not flagged as completed $attempt")
  }

}
