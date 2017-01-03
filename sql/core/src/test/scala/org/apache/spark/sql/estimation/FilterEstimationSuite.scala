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

package org.apache.spark.sql.estimation

import org.apache.spark.sql.catalyst.expressions.AttributeMap
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans.logical.estimation.EstimationUtils._
import org.apache.spark.sql.test.SharedSQLContext

/**
 * In this test suite, we test the proedicates containing the following operators:
 * =, <, <=, >, >=, AND, OR, IS NULL, IS NOT NULL, IN, NOT IN
 */

class FilterEstimationSuite extends QueryTest with SharedSQLContext {
  import testImplicits._

  private val data1 = Seq[Long](1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
  private val table1 = "filter_estimation_test1"

  test("filter estimation with equality comparison") {
    val df1 = data1.toDF("key1")
    withTable(table1) {
      df1.write.saveAsTable(table1)

      /** Collect statistics */
      sql(s"analyze table $table1 compute STATISTICS FOR COLUMNS key1")

      /** Validate statistics */
      val sqlStmt = s"SELECT * FROM $table1 WHERE key1 = 2"
      val colStats = Seq("key1" -> ColumnStat(1, Some(2L), Some(2L), 0, 8, 8))
      validateEstimatedStats(sqlStmt, colStats, Some(1L))
    }
  }

  test("filter estimation with less than comparison") {
    val df1 = data1.toDF("key1")
    withTable(table1) {
      df1.write.saveAsTable(table1)

      /** Collect statistics */
      sql(s"analyze table $table1 compute STATISTICS FOR COLUMNS key1")

      /** Validate statistics */
      val sqlStmt = s"SELECT * FROM $table1 WHERE key1 < 3"
      val colStats = Seq("key1" -> ColumnStat(2, Some(1L), Some(3L), 0, 8, 8))
      validateEstimatedStats(sqlStmt, colStats, Some(3L))
    }
  }

  test("filter estimation with less than or equal to comparison") {
    val df1 = data1.toDF("key1")
    withTable(table1) {
      df1.write.saveAsTable(table1)

      /** Collect statistics */
      sql(s"analyze table $table1 compute STATISTICS FOR COLUMNS key1")

      /** Validate statistics */
      val sqlStmt = s"SELECT * FROM $table1 WHERE key1 <= 3"
      val colStats = Seq("key1" -> ColumnStat(2, Some(1L), Some(3L), 0, 8, 8))
      validateEstimatedStats(sqlStmt, colStats, Some(3L))
    }
  }

  test("filter estimation with greater than comparison") {
    val df1 = data1.toDF("key1")
    withTable(table1) {
      df1.write.saveAsTable(table1)

      /** Collect statistics */
      sql(s"analyze table $table1 compute STATISTICS FOR COLUMNS key1")

      /** Validate statistics */
      val sqlStmt = s"SELECT * FROM $table1 WHERE key1 > 6"
      val colStats = Seq("key1" -> ColumnStat(4, Some(6L), Some(10L), 0, 8, 8))
      validateEstimatedStats(sqlStmt, colStats, Some(5L))
    }
  }

  test("filter estimation with greater than or equal to comparison") {
    val df1 = data1.toDF("key1")
    withTable(table1) {
      df1.write.saveAsTable(table1)

      /** Collect statistics */
      sql(s"analyze table $table1 compute STATISTICS FOR COLUMNS key1")

      /** Validate statistics */
      val sqlStmt = s"SELECT * FROM $table1 WHERE key1 >= 6"
      val colStats = Seq("key1" -> ColumnStat(4, Some(6L), Some(10L), 0, 8, 8))
      validateEstimatedStats(sqlStmt, colStats, Some(5L))
    }
  }

  test("filter estimation with IS NULL comparison") {
    val df1 = data1.toDF("key1")
    withTable(table1) {
      df1.write.saveAsTable(table1)

      /** Collect statistics */
      sql(s"analyze table $table1 compute STATISTICS FOR COLUMNS key1")

      /** Validate statistics */
      val sqlStmt = s"SELECT * FROM $table1 WHERE key1 IS NULL"
      val colStats = Seq("key1" -> ColumnStat(0, None, None, 0, 8, 8))
      validateEstimatedStats(sqlStmt, colStats, Some(0L))
    }
  }

  test("filter estimation with IS NOT NULL comparison") {
    val df1 = data1.toDF("key1")
    withTable(table1) {
      df1.write.saveAsTable(table1)

      /** Collect statistics */
      sql(s"analyze table $table1 compute STATISTICS FOR COLUMNS key1")

      /** Validate statistics */
      val sqlStmt = s"SELECT * FROM $table1 WHERE key1 IS NOT NULL"
      val colStats = Seq("key1" -> ColumnStat(10, Some(1L), Some(10L), 0, 8, 8))
      validateEstimatedStats(sqlStmt, colStats, Some(10L))
    }
  }

  test("filter estimation with logical AND operator") {
    val df1 = data1.toDF("key1")
    withTable(table1) {
      df1.write.saveAsTable(table1)

      /** Collect statistics */
      sql(s"analyze table $table1 compute STATISTICS FOR COLUMNS key1")

      /** Validate statistics */
      val sqlStmt = s"SELECT * FROM $table1 WHERE key1 > 3 AND key1 <= 6"
      val colStats = Seq("key1" -> ColumnStat(3, Some(3L), Some(6L), 0, 8, 8))
      validateEstimatedStats(sqlStmt, colStats, Some(4L))
    }
  }

  test("filter estimation with logical OR operator") {
    val df1 = data1.toDF("key1")
    withTable(table1) {
      df1.write.saveAsTable(table1)

      /** Collect statistics */
      sql(s"analyze table $table1 compute STATISTICS FOR COLUMNS key1")

      /** Validate statistics */
      val sqlStmt = s"SELECT * FROM $table1 WHERE key1 = 3 OR key1 = 6"
      val colStats = Seq("key1" -> ColumnStat(10, Some(1L), Some(10L), 0, 8, 8))
      validateEstimatedStats(sqlStmt, colStats, Some(2L))
    }
  }

  test("filter estimation with IN operator") {
    val df1 = data1.toDF("key1")
    withTable(table1) {
      df1.write.saveAsTable(table1)

      /** Collect statistics */
      sql(s"analyze table $table1 compute STATISTICS FOR COLUMNS key1")

      /** Validate statistics */
      val sqlStmt = s"SELECT * FROM $table1 WHERE key1 IN (3, 4, 5)"
      val colStats = Seq("key1" -> ColumnStat(3, Some(3L), Some(5L), 0, 8, 8))
      validateEstimatedStats(sqlStmt, colStats, Some(3L))
    }
  }

  test("filter estimation with logical NOT operator") {
    val df1 = data1.toDF("key1")
    withTable(table1) {
      df1.write.saveAsTable(table1)

      /** Collect statistics */
      sql(s"analyze table $table1 compute STATISTICS FOR COLUMNS key1")

      /** Validate statistics */
      val sqlStmt = s"SELECT * FROM $table1 WHERE key1 NOT IN (3, 4, 5)"
      val colStats = Seq("key1" -> ColumnStat(10, Some(1L), Some(10L), 0, 8, 8))
      validateEstimatedStats(sqlStmt, colStats, Some(7L))
    }
  }

  private def validateEstimatedStats(
      sqlStmt: String,
      expectedColStats: Seq[(String, ColumnStat)],
      rowCount: Option[Long] = None)
  : Unit = {
    val logicalPlan = sql(sqlStmt).queryExecution.optimizedPlan
    val operNode = logicalPlan.collect {
      case oper: Filter =>
        oper
    }.head
    val expectedRowCount = rowCount.getOrElse(sql(sqlStmt).collect().head.getLong(0))
    val nameToAttr = operNode.output.map(a => (a.name, a)).toMap
    val expectedAttrStats =
      AttributeMap(expectedColStats.map(kv => nameToAttr(kv._1) -> kv._2))
    val expectedStats = Statistics(
      sizeInBytes = expectedRowCount * getRowSize(operNode.output, expectedAttrStats),
      rowCount = Some(expectedRowCount),
      attributeStats = expectedAttrStats,
      isBroadcastable = false)

    val filterStats = operNode.statistics
    assert(filterStats.sizeInBytes == expectedStats.sizeInBytes)
    assert(filterStats.rowCount == expectedStats.rowCount)
    assert(filterStats.isBroadcastable == expectedStats.isBroadcastable)
  }

}