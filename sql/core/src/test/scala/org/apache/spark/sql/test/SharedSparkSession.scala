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

import scala.concurrent.duration._

import org.scalatest.{BeforeAndAfterEach, Suite}
import org.scalatest.concurrent.Eventually

import org.apache.spark.{DebugFilesystem, SparkConf}
import org.apache.spark.internal.config.UNSAFE_EXCEPTION_ON_MEMORY_LEAK
import org.apache.spark.sql.{SparkSession, SQLContext}
import org.apache.spark.sql.catalyst.expressions.CodegenObjectFactoryMode
import org.apache.spark.sql.catalyst.optimizer.ConvertToLocalRelation
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}

trait SharedSparkSession extends SQLTestUtils with SharedSparkSessionBase {

  /**
   * Suites extending [[SharedSparkSession]] are sharing resources (e.g. SparkSession) in their
   * tests. That trait initializes the spark session in its [[beforeAll()]] implementation before
   * the automatic thread snapshot is performed, so the audit code could fail to report threads
   * leaked by that shared session.
   *
   * The behavior is overridden here to take the snapshot before the spark session is initialized.
   */
  override protected val enableAutoThreadAudit = false

  protected override def beforeAll(): Unit = {
    doThreadPreAudit()
    super.beforeAll()
  }

  protected override def afterAll(): Unit = {
    try {
      super.afterAll()
    } finally {
      doThreadPostAudit()
    }
  }
}

/**
 * Helper trait for SQL test suites where all tests share a single [[TestSparkSession]].
 */
trait SharedSparkSessionBase
  extends SQLTestUtilsBase
  with BeforeAndAfterEach
  with Eventually { self: Suite =>

  protected def sparkConf = {
    val conf = new SparkConf()
      .set("spark.hadoop.fs.file.impl", classOf[DebugFilesystem].getName)
      .set(UNSAFE_EXCEPTION_ON_MEMORY_LEAK, true)
      .set(SQLConf.CODEGEN_FALLBACK.key, "false")
      .set(SQLConf.CODEGEN_FACTORY_MODE.key, CodegenObjectFactoryMode.CODEGEN_ONLY.toString)
      // Disable ConvertToLocalRelation for better test coverage. Test cases built on
      // LocalRelation will exercise the optimization rules better by disabling it as
      // this rule may potentially block testing of other optimization rules such as
      // ConstantPropagation etc.
      .set(SQLConf.OPTIMIZER_EXCLUDED_RULES.key, ConvertToLocalRelation.ruleName)
    conf.set(
      StaticSQLConf.WAREHOUSE_PATH,
      conf.get(StaticSQLConf.WAREHOUSE_PATH) + "/" + getClass.getCanonicalName)
  }

  /**
   * The [[TestSparkSession]] to use for all tests in this suite.
   *
   * By default, the underlying [[org.apache.spark.SparkContext]] will be run in local
   * mode with the default test configurations.
   */
  private var _spark: TestSparkSession = null

  /**
   * The [[TestSparkSession]] to use for all tests in this suite.
   */
  protected implicit def spark: SparkSession = _spark

  /**
   * The [[TestSQLContext]] to use for all tests in this suite.
   */
  protected implicit def sqlContext: SQLContext = _spark.sqlContext

  protected def createSparkSession: TestSparkSession = {
    SparkSession.cleanupAnyExistingSession()
    new TestSparkSession(sparkConf)
  }

  /**
   * Initialize the [[TestSparkSession]].  Generally, this is just called from
   * beforeAll; however, in test using styles other than FunSuite, there is
   * often code that relies on the session between test group constructs and
   * the actual tests, which may need this session.  It is purely a semantic
   * difference, but semantically, it makes more sense to call
   * 'initializeSession' between a 'describe' and an 'it' call than it does to
   * call 'beforeAll'.
   */
  protected def initializeSession(): Unit = {
    if (_spark == null) {
      _spark = createSparkSession
    }
  }

  /**
   * Make sure the [[TestSparkSession]] is initialized before any tests are run.
   */
  protected override def beforeAll(): Unit = {
    initializeSession()

    // Ensure we have initialized the context before calling parent code
    super.beforeAll()
  }

  /**
   * Stop the underlying [[org.apache.spark.SparkContext]], if any.
   */
  protected override def afterAll(): Unit = {
    try {
      super.afterAll()
    } finally {
      try {
        if (_spark != null) {
          try {
            _spark.sessionState.catalog.reset()
          } finally {
            _spark.stop()
            _spark = null
          }
        }
      } finally {
        SparkSession.clearActiveSession()
        SparkSession.clearDefaultSession()
      }
    }
  }

  protected override def beforeEach(): Unit = {
    super.beforeEach()
    DebugFilesystem.clearOpenStreams()
  }

  protected override def afterEach(): Unit = {
    super.afterEach()
    // Clear all persistent datasets after each test
    spark.sharedState.cacheManager.clearCache()
    // files can be closed from other threads, so wait a bit
    // normally this doesn't take more than 1s
    eventually(timeout(10.seconds), interval(2.seconds)) {
      DebugFilesystem.assertNoOpenStreams()
    }
  }
}
