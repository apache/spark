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

package org.apache.spark.deploy.master

import org.scalatest.{FunSuite, Matchers}

class ApplicationFailureDetectorSuite extends FunSuite with Matchers {

  test("initially, the application should not be failed") {
    val failureDetector = new ApplicationFailureDetector("testApp", "testAppId", 10)
    assert(!failureDetector.isFailed)
  }

  test("normal operation (no executor failures)") {
    val failureDetector = new ApplicationFailureDetector("testApp", "testAppId", 10)
    for (execId <- 1 to 100) {
      failureDetector.updateExecutorStatus(hasRegisteredExecutors = true)
      assert(!failureDetector.isFailed)
    }
    assert(!failureDetector.isFailed)
  }

  test("every other executor launch fails") {
    val failureDetector = new ApplicationFailureDetector("testApp", "testAppId", 10)
    for (execId <- 1 to 100) {
      failureDetector.updateExecutorStatus(hasRegisteredExecutors = true)
      assert(!failureDetector.isFailed)
      if (execId % 2 == 0) {
        failureDetector.onFailedExecutorExit(execId)
        assert(!failureDetector.isFailed)
      }
    }
    assert(!failureDetector.isFailed)
  }

  test("every executor fails after launch") {
    val failureDetector = new ApplicationFailureDetector("testApp", "testAppId", 10)
    var failed: Boolean = false
    for (execId <- 1 to 100) {
      failureDetector.updateExecutorStatus(hasRegisteredExecutors = false)
      failureDetector.onFailedExecutorExit(execId)
      if (failureDetector.isFailed) {
        failed = true
      }
    }
    assert(failed, "Expected the application to be marked as failed")
  }
}
