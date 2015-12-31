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

package org.apache.spark.deploy.history.yarn.publish

import org.apache.spark.deploy.history.yarn.YarnEventListener
import org.apache.spark.deploy.history.yarn.integration.AbstractHistoryIntegrationTests
import org.apache.spark.deploy.history.yarn.testtools.YarnTestUtils._

class TimelinePostSuite extends AbstractHistoryIntegrationTests {

  test("Round Trip App Stop") {
    historyService = startHistoryService(sc)
    val outcome = postEvent(appStartEvent(), now())
    historyService.stop()
    awaitEmptyQueue(historyService, TEST_STARTUP_DELAY)
  }

  test("App Start Via Event Listener") {
    historyService = startHistoryService(sc)
    val listener = new YarnEventListener(sc, historyService)
    val sparkEvt = appStartEvent()
    listener.onApplicationStart(sparkEvt)
  }

}
