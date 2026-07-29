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

package org.apache.spark.sql.test

import org.scalatest.OneInstancePerTest

import org.apache.spark.sql.Row
import org.apache.spark.sql.classic.SparkSession

/**
 * Demonstrates binding a per-test [[SparkSession]] instead of the single shared session, by
 * overriding [[SparkSessionBinderBase.classicSpark]].
 *
 * With `OneInstancePerTest`, ScalaTest runs each test in a fresh instance of the suite that
 * executes only `beforeEach` -- never `beforeAll` / `initializeSession`. The shared session bound
 * by the base binder therefore stays `null` in that per-test instance, so the inherited
 * `beforeEach` / `afterEach` hooks cannot reach it through the private field. Overriding
 * `classicSpark` to return a per-test session (a clone of the shared one, set up here in
 * `beforeEach` before `super.beforeEach()` runs) lets the inherited hooks operate on the live
 * session. This is the seam a parallelized / isolated-session test runner builds on.
 */
class PerTestSessionBinderSuite
  extends SharedSparkSession
  with OneInstancePerTest {

  // The session bound for the currently running test. Set in beforeEach on the per-test instance.
  private var perTestSpark: SparkSession = _

  // Route the inherited before/after hooks (and the suite's `spark`) at the per-test session.
  override protected def classicSpark: SparkSession = perTestSpark
  override protected def spark: SparkSession = perTestSpark

  override protected def beforeEach(): Unit = {
    // Clone the shared session so each test gets an isolated one; must be set before the base
    // beforeEach runs, since that hook now reads `classicSpark`.
    perTestSpark = SparkSession.getActiveSession
      .orElse(SparkSession.getDefaultSession)
      .getOrElse(createSparkSession)
      .cloneSession()
    SparkSession.setActiveSession(perTestSpark)
    super.beforeEach()
  }

  override protected def afterEach(): Unit = {
    super.afterEach()
    SparkSession.clearActiveSession()
  }

  test("per-test session is bound so the inherited lifecycle hooks operate on a live session") {
    // A successful run proves the inherited beforeEach/afterEach reached a live session through
    // the overridden `classicSpark` rather than NPEing on the null shared field.
    assert(perTestSpark != null)
    checkAnswer(perTestSpark.sql("SELECT 1"), Row(1))
  }
}
