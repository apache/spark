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

package org.apache.spark.sql.hive

import org.apache.spark.sql.{DataFrame, QueryTest}
import org.apache.spark.sql.catalyst.plans.logical.{ColumnStats, Statistics}
import org.apache.spark.sql.execution.command.{AnalyzeColumnCommand, AnalyzeTableCommand}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.sql.types._

trait StatisticsTest extends QueryTest with TestHiveSingleton with SQLTestUtils {

  def assertAnalyzeCommand(analyzeCommand: String, c: Class[_]) {
    val parsed = spark.sessionState.sqlParser.parsePlan(analyzeCommand)
    val operators = parsed.collect {
      case a: AnalyzeTableCommand => a
      case b: AnalyzeColumnCommand => b
    }

    assert(operators.size === 1)
    if (operators(0).getClass() != c) {
      fail(
        s"""$analyzeCommand expected command: $c, but got ${operators(0)}
           |parsed command:
           |$parsed
         """.stripMargin)
    }
  }

  def checkTableStats(
      stats: Option[Statistics],
      hasSizeInBytes: Boolean,
      expectedRowCounts: Option[Int]): Unit = {
    if (hasSizeInBytes || expectedRowCounts.nonEmpty) {
      assert(stats.isDefined)
      assert(stats.get.sizeInBytes >= 0)
      assert(stats.get.rowCount === expectedRowCounts)
    } else {
      assert(stats.isEmpty)
    }
  }

  def checkTableStats(
      tableName: String,
      isDataSourceTable: Boolean,
      hasSizeInBytes: Boolean,
      expectedRowCounts: Option[Int]): Option[Statistics] = {
    val df = sql(s"SELECT * FROM $tableName")
    val stats = df.queryExecution.analyzed.collect {
      case rel: MetastoreRelation =>
        checkTableStats(rel.catalogTable.stats, hasSizeInBytes, expectedRowCounts)
        assert(!isDataSourceTable, "Expected a Hive serde table, but got a data source table")
        rel.catalogTable.stats
      case rel: LogicalRelation =>
        checkTableStats(rel.catalogTable.get.stats, hasSizeInBytes, expectedRowCounts)
        assert(isDataSourceTable, "Expected a data source table, but got a Hive serde table")
        rel.catalogTable.get.stats
    }
    assert(stats.size == 1)
    stats.head
  }

  def checkColStats(
      df: DataFrame,
      expectedColStatsSeq: Seq[(String, ColumnStats)]): Unit = {
    val table = "tbl"
    withTable(table) {
      df.write.format("json").saveAsTable(table)
      val columns = expectedColStatsSeq.map(_._1).mkString(", ")
      sql(s"ANALYZE TABLE $table COMPUTE STATISTICS FOR COLUMNS $columns")
      val readback = sql(s"SELECT * FROM $table")
      val stats = readback.queryExecution.analyzed.collect {
        case rel: LogicalRelation =>
          expectedColStatsSeq.foreach { expected =>
            assert(rel.catalogTable.get.stats.get.colStats.contains(expected._1))
            checkColStats(colStats = rel.catalogTable.get.stats.get.colStats(expected._1),
              expectedColStats = expected._2)
          }
      }
      assert(stats.size == 1)
    }
  }

  def checkColStats(colStats: ColumnStats, expectedColStats: ColumnStats): Unit = {
    assert(colStats.dataType == expectedColStats.dataType)
    assert(colStats.numNulls == expectedColStats.numNulls)
    colStats.dataType match {
      case _: IntegralType | DateType | TimestampType =>
        assert(colStats.max.map(_.toString.toLong) == expectedColStats.max.map(_.toString.toLong))
        assert(colStats.min.map(_.toString.toLong) == expectedColStats.min.map(_.toString.toLong))
      case _: FractionalType =>
        assert(colStats.max.map(_.toString.toDouble) == expectedColStats
          .max.map(_.toString.toDouble))
        assert(colStats.min.map(_.toString.toDouble) == expectedColStats
          .min.map(_.toString.toDouble))
      case _ =>
        // other types don't have max and min stats
        assert(colStats.max.isEmpty)
        assert(colStats.min.isEmpty)
    }
    colStats.dataType match {
      case BinaryType | BooleanType => assert(colStats.ndv.isEmpty)
      case _ =>
        // ndv is an approximate value, so we make sure we have the value, and it should be
        // within 3*SD's of the given rsd.
        assert(colStats.ndv.get >= 0)
        if (expectedColStats.ndv.get == 0) {
          assert(colStats.ndv.get == 0)
        } else if (expectedColStats.ndv.get > 0) {
          val rsd = spark.sessionState.conf.ndvMaxError
          val error = math.abs((colStats.ndv.get / expectedColStats.ndv.get.toDouble) - 1.0d)
          assert(error <= rsd * 3.0d, "Error should be within 3 std. errors.")
        }
    }
    assert(colStats.avgColLen == expectedColStats.avgColLen)
    assert(colStats.maxColLen == expectedColStats.maxColLen)
    assert(colStats.numTrues == expectedColStats.numTrues)
    assert(colStats.numFalses == expectedColStats.numFalses)
  }

}
