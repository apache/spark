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

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.plans.logical._
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
      val logicalPlan =
        sql(s"SELECT * FROM $table1 WHERE key1 = 2").queryExecution.optimizedPlan
      val expectedFilterStats = Statistics(
        sizeInBytes = 1 * 8, rowCount = Some(1),
        colStats = Map("key1" -> ColumnStat(1, Some(2L), Some(2L), 0, 8, 8)),
        isBroadcastable = false)

      val filterNodes = logicalPlan.collect {
        case filter: Filter =>
          val filterStats = filter.statistics
          assert(filterStats == expectedFilterStats)
          filter
      }
      assert(filterNodes.size == 1)
    }
  }

  test("filter estimation with less than comparison") {
    val df1 = data1.toDF("key1")
    withTable(table1) {
      df1.write.saveAsTable(table1)

      /** Collect statistics */
      sql(s"analyze table $table1 compute STATISTICS FOR COLUMNS key1")

      /** Validate statistics */
      val logicalPlan =
        sql(s"SELECT * FROM $table1 WHERE key1 < 3").queryExecution.optimizedPlan
      val expectedFilterStats = Statistics(
        sizeInBytes = 3 * 8, rowCount = Some(3),
        colStats = Map("key1" -> ColumnStat(2, Some(1L), Some(3L), 0, 8, 8)),
        isBroadcastable = false)

      val filterNodes = logicalPlan.collect {
        case filter: Filter =>
          val filterStats = filter.statistics
          assert(filterStats == expectedFilterStats)
          filter
      }
      assert(filterNodes.size == 1)
    }
  }

  test("filter estimation with less than or equal to comparison") {
    val df1 = data1.toDF("key1")
    withTable(table1) {
      df1.write.saveAsTable(table1)

      /** Collect statistics */
      sql(s"analyze table $table1 compute STATISTICS FOR COLUMNS key1")

      /** Validate statistics */
      val logicalPlan =
        sql(s"SELECT * FROM $table1 WHERE key1 <= 3").queryExecution.optimizedPlan
      val expectedFilterStats = Statistics(
        sizeInBytes = 3 * 8, rowCount = Some(3),
        colStats = Map("key1" -> ColumnStat(2, Some(1L), Some(3L), 0, 8, 8)),
        isBroadcastable = false)

      val filterNodes = logicalPlan.collect {
        case filter: Filter =>
          val filterStats = filter.statistics
          assert(filterStats == expectedFilterStats)
          filter
      }
      assert(filterNodes.size == 1)
    }
  }

  test("filter estimation with greater than comparison") {
    val df1 = data1.toDF("key1")
    withTable(table1) {
      df1.write.saveAsTable(table1)

      /** Collect statistics */
      sql(s"analyze table $table1 compute STATISTICS FOR COLUMNS key1")

      /** Validate statistics */
      val logicalPlan =
        sql(s"SELECT * FROM $table1 WHERE key1 > 6").queryExecution.optimizedPlan
      val expectedFilterStats = Statistics(
        sizeInBytes = 5 * 8, rowCount = Some(5),
        colStats = Map("key1" -> ColumnStat(4, Some(6L), Some(10L), 0, 8, 8)),
        isBroadcastable = false)

      val filterNodes = logicalPlan.collect {
        case filter: Filter =>
          val filterStats = filter.statistics
          assert(filterStats == expectedFilterStats)
          filter
      }
      assert(filterNodes.size == 1)
    }
  }

  test("filter estimation with greater than or equal to comparison") {
    val df1 = data1.toDF("key1")
    withTable(table1) {
      df1.write.saveAsTable(table1)

      /** Collect statistics */
      sql(s"analyze table $table1 compute STATISTICS FOR COLUMNS key1")

      /** Validate statistics */
      val logicalPlan =
        sql(s"SELECT * FROM $table1 WHERE key1 >= 6").queryExecution.optimizedPlan
      val expectedFilterStats = Statistics(
        sizeInBytes = 5 * 8, rowCount = Some(5),
        colStats = Map("key1" -> ColumnStat(4, Some(6L), Some(10L), 0, 8, 8)),
        isBroadcastable = false)

      val filterNodes = logicalPlan.collect {
        case filter: Filter =>
          val filterStats = filter.statistics
          assert(filterStats == expectedFilterStats)
          filter
      }
      assert(filterNodes.size == 1)
    }
  }

  test("filter estimation with IS NULL comparison") {
    val df1 = data1.toDF("key1")
    withTable(table1) {
      df1.write.saveAsTable(table1)

      /** Collect statistics */
      sql(s"analyze table $table1 compute STATISTICS FOR COLUMNS key1")

      /** Validate statistics */
      val logicalPlan =
        sql(s"SELECT * FROM $table1 WHERE key1 IS NULL").queryExecution.optimizedPlan
      val expectedFilterStats = Statistics(
        sizeInBytes = 0, rowCount = Some(0),
        colStats = Map("key1" -> ColumnStat(0, None, None, 0, 8, 8)),
        isBroadcastable = false)

      val filterNodes = logicalPlan.collect {
        case filter: Filter =>
          val filterStats = filter.statistics
          assert(filterStats == expectedFilterStats)
          filter
      }
      assert(filterNodes.size == 1)
    }
  }

  test("filter estimation with IS NOT NULL comparison") {
    val df1 = data1.toDF("key1")
    withTable(table1) {
      df1.write.saveAsTable(table1)

      /** Collect statistics */
      sql(s"analyze table $table1 compute STATISTICS FOR COLUMNS key1")

      /** Validate statistics */
      val logicalPlan =
        sql(s"SELECT * FROM $table1 WHERE key1 IS NOT NULL").queryExecution.optimizedPlan
      val expectedFilterStats = Statistics(
        sizeInBytes = 10 * 8, rowCount = Some(10),
        colStats = Map("key1" -> ColumnStat(10, Some(1L), Some(10L), 0, 8, 8)),
        isBroadcastable = false)

      val filterNodes = logicalPlan.collect {
        case filter: Filter =>
          val filterStats = filter.statistics
          assert(filterStats == expectedFilterStats)
          filter
      }
      assert(filterNodes.size == 1)
    }
  }

  test("filter estimation with logical AND operator") {
    val df1 = data1.toDF("key1")
    withTable(table1) {
      df1.write.saveAsTable(table1)

      /** Collect statistics */
      sql(s"analyze table $table1 compute STATISTICS FOR COLUMNS key1")

      /** Validate statistics */
      val logicalPlan =
        sql(s"SELECT * FROM $table1 WHERE key1 > 3 AND key1 <= 6").queryExecution.optimizedPlan
      val expectedFilterStats = Statistics(
        sizeInBytes = 4 * 8, rowCount = Some(4),
        colStats = Map("key1" -> ColumnStat(3, Some(3L), Some(6L), 0, 8, 8)),
        isBroadcastable = false)

      val filterNodes = logicalPlan.collect {
        case filter: Filter =>
          val filterStats = filter.statistics
          assert(filterStats == expectedFilterStats)
          filter
      }
      assert(filterNodes.size == 1)
    }
  }

  test("filter estimation with logical OR operator") {
    val df1 = data1.toDF("key1")
    withTable(table1) {
      df1.write.saveAsTable(table1)

      /** Collect statistics */
      sql(s"analyze table $table1 compute STATISTICS FOR COLUMNS key1")

      /** Validate statistics */
      val logicalPlan =
        sql(s"SELECT * FROM $table1 WHERE key1 = 3 OR key1 = 6").queryExecution.optimizedPlan
      val expectedFilterStats = Statistics(
        sizeInBytes = 2 * 8, rowCount = Some(2),
        colStats = Map("key1" -> ColumnStat(10, Some(1L), Some(10L), 0, 8, 8)),
        isBroadcastable = false)

      val filterNodes = logicalPlan.collect {
        case filter: Filter =>
          val filterStats = filter.statistics
          assert(filterStats == expectedFilterStats)
          filter
      }
      assert(filterNodes.size == 1)
    }
  }

  test("filter estimation with IN operator") {
    val df1 = data1.toDF("key1")
    withTable(table1) {
      df1.write.saveAsTable(table1)

      /** Collect statistics */
      sql(s"analyze table $table1 compute STATISTICS FOR COLUMNS key1")

      /** Validate statistics */
      val logicalPlan =
        sql(s"SELECT * FROM $table1 WHERE key1 IN (3, 4, 5)").queryExecution.optimizedPlan
      val expectedFilterStats = Statistics(
        sizeInBytes = 3 * 8, rowCount = Some(3),
        colStats = Map("key1" -> ColumnStat(3, Some(3L), Some(5L), 0, 8, 8)),
        isBroadcastable = false)

      val filterNodes = logicalPlan.collect {
        case filter: Filter =>
          val filterStats = filter.statistics
          assert(filterStats == expectedFilterStats)
          filter
      }
      assert(filterNodes.size == 1)
    }
  }

  test("filter estimation with logical NOT operator") {
    val df1 = data1.toDF("key1")
    withTable(table1) {
      df1.write.saveAsTable(table1)

      /** Collect statistics */
      sql(s"analyze table $table1 compute STATISTICS FOR COLUMNS key1")

      /** Validate statistics */
      val logicalPlan =
        sql(s"SELECT * FROM $table1 WHERE key1 NOT IN (3, 4, 5)").queryExecution.optimizedPlan
      val expectedFilterStats = Statistics(
        sizeInBytes = 7 * 8, rowCount = Some(7),
        colStats = Map("key1" -> ColumnStat(10, Some(1L), Some(10L), 0, 8, 8)),
        isBroadcastable = false)

      val filterNodes = logicalPlan.collect {
        case filter: Filter =>
          val filterStats = filter.statistics
          assert(filterStats == expectedFilterStats)
          filter
      }
      assert(filterNodes.size == 1)
    }
  }

}
