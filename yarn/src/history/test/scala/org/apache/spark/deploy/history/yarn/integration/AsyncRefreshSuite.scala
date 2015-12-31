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

import org.apache.spark.SparkConf
import org.apache.spark.deploy.history.yarn.server.{YarnHistoryProviderMetrics, YarnHistoryProvider}
import org.apache.spark.deploy.history.yarn.server.YarnHistoryProvider._
import org.apache.spark.deploy.history.yarn.testtools.YarnTestUtils._

/**
 * Tests to verify async refreshes work as expected.
 * These tests disable background updates
 */
class AsyncRefreshSuite extends AbstractHistoryIntegrationTests {

  val SLEEP_INTERVAL = 100
  val EVENT_PROCESSED_TIMEOUT = 5000
  val WINDOW_SECONDS = YarnHistoryProvider.DEFAULT_MANUAL_REFRESH_INTERVAL_SECONDS
  val WINDOW_MILLIS = WINDOW_SECONDS * 1000

  override def setupConfiguration(sparkConf: SparkConf): SparkConf = {
    super.setupConfiguration(sparkConf)
    sparkConf.set(OPTION_MANUAL_REFRESH_INTERVAL, s"${WINDOW_SECONDS}s")
    sparkConf.set(OPTION_BACKGROUND_REFRESH_INTERVAL, "0")
  }

  /**
   * Start time. This has to be big enough that `now()-lastRefreshTime > window`,
   * even for the initial last refresh time value of 0
   */
  val CLOCK_START_TIME = 10 * 60 * 1000
  val TICK_TIME = 1000

  /**
   * Create time managed history provider whose clock can be set
   * by the tests themselves
   * @param conf configuration
   * @return the instance
   */
  override protected def createHistoryProvider(conf: SparkConf): YarnHistoryProvider = {
    new TimeManagedHistoryProvider(conf, CLOCK_START_TIME, TICK_TIME)
  }

  def metrics()(implicit history: YarnHistoryProvider): YarnHistoryProviderMetrics = {
    history.metrics
  }

  /**
   * This test verifies that `YarnHistoryProvider.getListing()` events
   * trigger asynchronous refreshes, except when the last refresh
   * was within the limits of the refresh window.
   *
   * To do this it makes use of the counter in the refresher recording the number
   * of events received in its action queue; so verifying when the (async) refresh
   * actions are received and processed.
   */
  test("RefreshInterval") {
    describe("verify reset interval logic")
    implicit val history = createHistoryProvider(sc.conf)
        .asInstanceOf[TimeManagedHistoryProvider]
    // checks that the time reading/parsing logic works
    assert(WINDOW_MILLIS === history.manualRefreshInterval, s"refresh interval in $history")
    val refresher = history.refresher
    assert(refresher.isRunning, s"refresher not running in $history")
    awaitRefreshMessageProcessed(history, 0, EVENT_PROCESSED_TIMEOUT, s"expecting startup refresh")

    val attempt0 = history.lastRefreshAttemptTime
    assert(CLOCK_START_TIME === attempt0, s"initial event in $refresher")
    // the refresh is expected to succeed, but a small delay is needed for the async nature of the
    // GET operation
    Thread.sleep(5000)
    assert(0 === history.refreshFailedCount, s"Failed refreshes in $history")
    assert(1 === metrics.refreshCount.getCount,
      s"Refreshes successfully executed in $refresher")

    def messagesProcessed: Long = { metrics.backgroundOperationsProcessed.getCount }
    val count0 = messagesProcessed
    // add thirty seconds
    history.incrementTime(15000)
    history.getListing()
    // yield a bit for the refresher thread
    awaitRefreshMessageProcessed(history, count0, EVENT_PROCESSED_TIMEOUT, "in-window")

    // expect no refresh
    assert(history.lastRefreshAttemptTime === CLOCK_START_TIME,
      s"last refresh was not clock time of $CLOCK_START_TIME in $refresher")

    // now move clock forwards a minute
    history.incrementTime(2 * WINDOW_SECONDS * 1000)
    // and expect the new listing to trigger another refresh
    val count2 = messagesProcessed
    val time2 = history.now()
    history.getListing()
    awaitRefreshMessageProcessed(history, count2, EVENT_PROCESSED_TIMEOUT, "out of window")
    val refreshAttemptTime2 = history.lastRefreshAttemptTime
    val time3 = history.now()
    assert(2 === history.metrics.refreshCount.getCount, s"refreshes executed in $refresher")
    assert(time2 <= refreshAttemptTime2,
      s"refresh time in $refresher was $refreshAttemptTime2; time2 was $time2; current =$time3")
  }

  test("Size Zero refresh window") {
    describe("Test with a zero size window to verify that the refresh always takes ")
    val conf = sc.conf
    conf.set(OPTION_MANUAL_REFRESH_INTERVAL, "0")
    implicit val history = createHistoryProvider(conf).asInstanceOf[TimeManagedHistoryProvider]
    val refresher = history.refresher
    assert(refresher.isRunning, s"refresher not running in $history")
    awaitRefreshMessageProcessed(history, 0, EVENT_PROCESSED_TIMEOUT, s"startup of  $refresher")

    val attempt0 = history.lastRefreshAttemptTime
    assert(attempt0 === CLOCK_START_TIME, s"initial event in $refresher")
    val count0 = refresher.messagesProcessed
    val t2 = history.tick()
    history.getListing()
    // yield a bit for the refresher thread
    awaitRefreshMessageProcessed(history, count0, EVENT_PROCESSED_TIMEOUT, "in-window")

    // expect no refresh
    assert(history.lastRefreshAttemptTime === t2, s"refresh didn't happen in $refresher")
  }

  /**
   * Wait for the refresh count to increment by at least one iteration
   * @param provider provider
   * @param timeout timeout
   * @return the successful listing
   */
  def awaitRefreshMessageProcessed (
      provider: TimeManagedHistoryProvider,
      initialCount: Long,
      timeout: Long,
      text: String): Unit = {
    val refresher = provider.refresher

    def listingProbe(): Outcome = {
      outcomeFromBool(refresher.messagesProcessed > initialCount)
    }

    def failure(outcome: Outcome, attempts: Int, timedOut: Boolean): Unit = {
      val outcome = if (timedOut) "timeout" else "failed"
      fail(s"$text -$outcome after $attempts attempts," +
          s" refresh count is <= $initialCount in: $refresher")
    }
    require(provider.isRefreshThreadRunning, s"refresher is not running in $provider")
    spinForState("await refresh message count", SLEEP_INTERVAL, timeout, listingProbe, failure)
  }

}
