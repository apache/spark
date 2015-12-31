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

import java.io.File

import org.apache.spark.deploy.history.yarn.integration.AbstractHistoryIntegrationTests
import org.apache.spark.deploy.history.yarn.testtools.{HistoryServiceListeningToSparkContext, TimelineSingleEntryBatchSize}
import org.apache.spark.deploy.history.yarn.testtools.YarnTestUtils._

/**
 * hookup from spark context to timeline.
 */
class ContextEventPublishingSuite
    extends AbstractHistoryIntegrationTests
    with HistoryServiceListeningToSparkContext
    with TimelineSingleEntryBatchSize {

  test ("Publish events via Context") {
    describe("Publish events via Context")
    // hook up to spark context
    historyService = startHistoryService(sc)
    assert(historyService.listening, s"listening $historyService")
    assert(1 === historyService.batchSize, s"batch size in $historyService")
    assert(historyService.bondedToATS, s"not bonded to ATS: $historyService")
    // post in an app start
    var flushes = 0
    logDebug("posting app start")
    enqueue(appStartEvent())
    flushes += 1
    awaitEmptyQueue(historyService, TEST_STARTUP_DELAY)

    // add a local file to the context, so generate another event
    val tempFile = File.createTempFile("test", ".txt")
    sc.addFile(tempFile.toURI.toString)
    tempFile.delete()
    flushes += 1

    // closing context generates an application stop
    logDebug("stopping context")
    sc.stop()
    awaitEmptyQueue(historyService, TEST_STARTUP_DELAY)

    flushes += 1
    logDebug(s"status: $historyService")

    // seeing intermittent failure
    awaitFlushCount(historyService, flushes, TEST_STARTUP_DELAY)
    flushHistoryServiceToSuccess()
  }

}
