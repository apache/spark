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

import org.scalatest.BeforeAndAfterAll

import org.apache.spark.SparkFunSuite

/**
 * Helper trait for SQL test suites where all tests share a single [[TestSQLContext]].
 */
private[sql] trait SharedSQLContext extends SparkFunSuite with BeforeAndAfterAll {

  /**
   * The [[TestSQLContext]] to use for all tests in this suite.
   *
   * By default, the underlying [[org.apache.spark.SparkContext]] will be run in local
   * mode with the default test configurations.
   */
  private var _ctx: TestSQLContext = new TestSQLContext

  /**
   * The [[TestSQLContext]] to use for all tests in this suite.
   */
  protected def sqlContext: TestSQLContext = _ctx

  /**
   * Switch to a custom [[TestSQLContext]].
   *
   * This stops the underlying [[org.apache.spark.SparkContext]] and expects a new one
   * to be created. This is necessary because only one [[org.apache.spark.SparkContext]]
   * is allowed per JVM.
   */
  protected def switchSQLContext(newContext: () => TestSQLContext): Unit = {
    if (_ctx != null) {
      _ctx.sparkContext.stop()
      _ctx = newContext()
    }
  }

  /**
   * Execute the given block of code with a custom [[TestSQLContext]].
   * At the end of the method, the default [[TestSQLContext]] will be restored.
   */
  protected def withSQLContext[T](newContext: () => TestSQLContext)(body: => T) {
    switchSQLContext(newContext)
    try {
      body
    } finally {
      switchSQLContext(() => new TestSQLContext)
    }
  }

  protected override def afterAll(): Unit = {
    if (_ctx != null) {
      _ctx.sparkContext.stop()
      _ctx = null
    }
    super.afterAll()
  }

}
