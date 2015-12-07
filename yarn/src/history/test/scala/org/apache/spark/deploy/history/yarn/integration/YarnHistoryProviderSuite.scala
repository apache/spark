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

import org.apache.hadoop.yarn.api.records.{ApplicationReport, YarnApplicationState}

import org.apache.spark.deploy.history.yarn.server.{TimelineApplicationHistoryInfo, YarnHistoryProvider}
import org.apache.spark.deploy.history.yarn.server.YarnProviderUtils._
import org.apache.spark.deploy.history.yarn.testtools.AbstractYarnHistoryTests
import org.apache.spark.deploy.history.yarn.testtools.YarnTestUtils._
import org.apache.spark.ui.SparkUI

/**
 * Basic lifecycle/method calls on the history provider.
 * It's not quite an integration test, but kept together as it is part
 * of the server-side code.
 */
class YarnHistoryProviderSuite extends AbstractYarnHistoryTests with IntegrationTestUtils {

  var provider: YarnHistoryProvider = _

  val app1FailedReport = stubApplicationReport(1, 1, 1, YarnApplicationState.FAILED, 1000, 1500)
  val app1Incomplete = appHistoryInfoFromAppReport(app1FailedReport, 0, false)
  val app2FinishedReport = stubApplicationReport(2, 2, 1, YarnApplicationState.FINISHED, 2000, 2500)
  val app2Complete = appHistoryInfoFromAppReport(app2FinishedReport, 2500, true)

  val app3RunningReport = stubApplicationReport(3, 3, 1, YarnApplicationState.RUNNING, 3000, 0)
  val app3Running = appHistoryInfoFromAppReport(app3RunningReport, 0, false)

  val app4AcceptedReport = stubApplicationReport(4, 4, 2, YarnApplicationState.ACCEPTED, 4000, 0)
  val app4FirstAttemptReport = stubApplicationReport(4, 4, 1, YarnApplicationState.RUNNING, 4000, 0)

  // a more complex history of two attempts; first one failed
  val app4Running = new TimelineApplicationHistoryInfo(app4AcceptedReport.getApplicationId.toString,
    app4AcceptedReport.getApplicationId.toString,
    List(attemptFromAppReport(app4AcceptedReport, 0, false, 4500),
      attemptFromAppReport(app4FirstAttemptReport, 0, false, 4000)))

  val allApps = reportsToMap(List(app1FailedReport, app2FinishedReport,
    app3RunningReport, app4AcceptedReport))
  val runningApps = reportsToMap(List(app3RunningReport, app4AcceptedReport))

  /*
  * Setup creates the spark context
  */
  override protected def setup(): Unit = {
    super.setup()
    provider = new YarnHistoryProvider(sc.conf)
  }

  /**
   * Stop the provider
   */
  override def afterEach(): Unit = {
    if (provider != null) {
      provider.stop()
    }
    super.afterEach()
  }

  test("Empty Provider List") {
    describe("Provider listing")
    assertResult(Nil) {
      provider.getListing()
    }
    provider.stop()
    provider.stop()
  }

  test("Provider Stop+Stop") {
    describe("Provider Stop+Stop")
    val historyProvider = new YarnHistoryProvider(sc.conf)
    // check there are no re-entrancy bugs here
    historyProvider.stop()
    historyProvider.stop()
  }

  test("getAppUi(unknown-app)") {
    describe("getAppUi(unknown-app) -> none")
    assertResult(None) {
      getAppUI("unknown-app")
    }
  }

  test("getAppUi(unknown-app, Some(attempt)") {
    describe("getAppUi(unknown-app, Some(attempt)) -> none")
    assertResult(None) {
      getAppUI("unknown-app", Some("attempt"))
    }
  }

  test("getAppUi(\"\")") {
    describe("getAppUi(\"\")")
    assertResult(None) {
      getAppUI("")
    }
  }

  test("getAppUi('',Some(attempt)") {
    describe("getAppUi('',Some(attempt)")
    assertResult(None) {
      getAppUI("", Some("attempt"))
    }
  }

  test("CompleteNoRunning") {
    assertUnmodified(app2Complete, Map())
  }

  test("CompleteOtherRunning") {
    assertUnmodified(app2Complete, runningApps)
  }

  test("CompleteInReport") {
    assertUnmodified(app2Complete, allApps)
  }

  test("IncompleteRunning") {
    // There's an incomplete App, as it is running all is well
    assertUnmodified(app3Running, allApps)
  }
  test("IncompleteAccepted") {
    // There's an incomplete App, as it is running all is well
    assertUnmodified(app4Running, allApps)
  }

  test("Incomplete with failure reported") {
    assertBecomesCompleted(app1Incomplete, allApps)
  }

  test("Incomplete with no entry outside window") {
    // as it is incomplete but with no entry: culled
    assertBecomesCompleted(app4Running, Map())
  }

  test("Incomplete with entry inside window") {
    // do a scan with the window expanded to 3 seconds, enough for the
    // last attempt updated field to be in range
    val (head :: tail) = completeAppsFromYARN(List(app4Running), Map(), 5000, 3000)
    assert(app4Running === head,
      s"history was modified to ${describeApplicationHistoryInfo(head) }")

  }

  test("state.FINISHED is completion event") {
    assertBecomesCompleted(app2Complete, allApps)
  }

  test("Completion Probes") {
    assert(isFinished(app2FinishedReport))
    assert(isFinished(app1FailedReport))
    assert(!isFinished(app3RunningReport))
    assert(!isFinished(app4FirstAttemptReport))
    assert(!isFinished(app4AcceptedReport))
  }

  /**
   * Assert that an application history entry becomes completed in
   * [[completeAppsFromYARN()]]
   * @param info history info
   * @param reports list of applications known by the RM
   */
  def assertBecomesCompleted(info: TimelineApplicationHistoryInfo,
      reports: Map[String, ApplicationReport]): Unit = {
    val (head:: tail) = completeAppsFromYARN(List(info), reports, 5000, 100)
    assert(isCompleted(head))
    assert(info.id === head.id )
  }

  /**
   * Assert that an application history entry is not modified by
   * [[completeAppsFromYARN()]]
   * @param info history info
   * @param reports list of applications known by the RM
   */
  def assertUnmodified(info: TimelineApplicationHistoryInfo,
      reports: Map[String, ApplicationReport]): Unit = {
    val (head :: tail) = completeAppsFromYARN(List(info), reports, 5000, 100)
    assert(info === head, s"history was modified to ${describeApplicationHistoryInfo(head)}")
  }

  /**
   * Create the provider, get the app UI.
   *
   * The provider member variable will be set as a side-effect
   * @param appid application ID
   * @return the result of <code>provider.getAppUI</code>
   */
  def getAppUI(appid: String, attemptId: Option[String] = None): Option[SparkUI] = {
    logDebug(s"GET appID =$appid attemptId=$attemptId" )
    provider.getAppUI(appid, attemptId)
  }

}
