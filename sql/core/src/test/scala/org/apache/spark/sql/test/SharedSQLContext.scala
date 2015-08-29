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

import org.apache.spark.sql.{ColumnName, SQLContext}


/**
 * Helper trait for SQL test suites where all tests share a single [[TestSQLContext]].
 */
trait SharedSQLContext extends SQLTestUtils {

  /**
   * The [[TestSQLContext]] to use for all tests in this suite.
   *
   * By default, the underlying [[org.apache.spark.SparkContext]] will be run in local
   * mode with the default test configurations.
   */
  private var _ctx: TestSQLContext = null

  /**
   * The [[TestSQLContext]] to use for all tests in this suite.
   */
  protected def ctx: TestSQLContext = _ctx
  protected def sqlContext: TestSQLContext = _ctx
  protected override def _sqlContext: SQLContext = _ctx

  /**
   * Initialize the [[TestSQLContext]].
   */
  protected override def beforeAll(): Unit = {
    if (_ctx == null) {
      _ctx = new TestSQLContext
    }
    // Ensure we have initialized the context before calling parent code
    super.beforeAll()
  }

  /**
   * Stop the underlying [[org.apache.spark.SparkContext]], if any.
   */
  protected override def afterAll(): Unit = {
    try {
      if (_ctx != null) {
        _ctx.sparkContext.stop()
        _ctx = null
      }
    } finally {
      super.afterAll()
    }
  }

  /**
   * Converts $"col name" into an [[Column]].
   * @since 1.3.0
   */
  // This must be duplicated here to preserve binary compatibility with Spark < 1.5.
  implicit class StringToColumn(val sc: StringContext) {
    def $(args: Any*): ColumnName = {
      new ColumnName(sc.s(args: _*))
    }
  }
}
