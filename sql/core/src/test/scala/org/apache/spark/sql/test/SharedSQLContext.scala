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

import scala.collection.JavaConverters._

import org.scalatest.BeforeAndAfterEach

import org.apache.spark.{DebugFilesystem, SparkConf}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{SparkSession, SQLContext}


/**
 * Helper trait for SQL test suites where all tests share a single [[TestSparkSession]].
 */
trait SharedSQLContext extends SQLTestUtils with BeforeAndAfterEach with Logging {

  protected val sparkConf = new SparkConf()

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

  /**
   * Initialize the [[TestSparkSession]].
   */
  protected override def beforeAll(): Unit = {
    SparkSession.sqlListener.set(null)
    if (_spark == null) {
      _spark = new TestSparkSession(
        sparkConf.set("spark.hadoop.fs.file.impl", classOf[DebugFilesystem].getName))
    }
    // Ensure we have initialized the context before calling parent code
    super.beforeAll()
  }

  /**
   * Stop the underlying [[org.apache.spark.SparkContext]], if any.
   */
  protected override def afterAll(): Unit = {
    try {
      if (_spark != null) {
        _spark.stop()
        _spark = null
      }
    } finally {
      super.afterAll()
    }
  }

  protected override def beforeEach(): Unit = {
    super.beforeEach()
    DebugFilesystem.openStreams.clear()
  }

  protected override def afterEach(): Unit = {
    super.afterEach()
    val numOpen = DebugFilesystem.openStreams.size
    if (numOpen > 0) {
      for (exc <- DebugFilesystem.openStreams.values.asScala) {
        logWarning("Leaked filesystem connection created at:")
        exc.printStackTrace()
      }
      throw new RuntimeException(s"There are $numOpen possibly leaked file streams.")
    }
  }
}
