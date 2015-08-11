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

package org.apache.spark.sql.hive.test

import org.scalatest.BeforeAndAfterAll

import org.apache.spark.SparkFunSuite


/**
 * Helper trait for hive test suites where all tests share a single [[TestHiveContext]].
 * This is analogous to [[org.apache.spark.sql.test.SharedSQLContext]].
 */
private[spark] trait SharedHiveContext extends SparkFunSuite with BeforeAndAfterAll {

  /**
   * The [[TestHiveContext]] to use for all tests in this suite.
   *
   * By default, the underlying [[org.apache.spark.SparkContext]] will be run in local
   * mode with the default test configurations.
   */
  private var _ctx: TestHiveContext = null

  /**
   * Initialize the [[TestHiveContext]].
   * This is a no-op if the user explicitly switched to a custom context before this is called.
   */
  protected override def beforeAll(): Unit = {
    if (_ctx == null) {
      _ctx = new TestHiveContext
    }
  }

  /**
   * Stop the underlying [[org.apache.spark.SparkContext]], if any.
   */
  protected override def afterAll(): Unit = {
    if (_ctx != null) {
      _ctx.sparkContext.stop()
      _ctx = null
    }
    super.afterAll()
  }

  /**
   * The [[TestHiveContext]] to use for all tests in this suite.
   */
  protected def ctx: TestHiveContext = _ctx
  protected def hiveContext: TestHiveContext = _ctx

  /**
   * Switch a custom [[TestHiveContext]].
   *
   * This stops the underlying [[org.apache.spark.SparkContext]] and expects a new one
   * to be created. This is needed because only one [[org.apache.spark.SparkContext]]
   * is allowed per JVM.
   */
  protected def switchHiveContext(newContext: () => TestHiveContext): Unit = {
    if (_ctx != null) {
      _ctx.sparkContext.stop()
      _ctx = newContext()
    }
  }

  /**
   * Execute the given block of code with a custom [[TestHiveContext]].
   * At the end of the method, the default [[TestHiveContext]] will be restored.
   */
  protected def withHiveContext[T](newContext: () => TestHiveContext)(body: => T) {
    switchHiveContext(newContext)
    try {
      body
    } finally {
      switchHiveContext(() => new TestHiveContext)
    }
  }

}
