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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.plans.logical.{BasicColStats, Statistics}
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
      case o => o
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
      assert(stats.get.sizeInBytes > 0)
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
      rowRDD: RDD[Row],
      schema: StructType,
      expectedColStatsSeq: Seq[(String, BasicColStats)]): Unit = {
    val table = "tbl"
    withTable(table) {
      var df = spark.createDataFrame(rowRDD, schema)
      df.write.format("json").saveAsTable(table)
      val columns = expectedColStatsSeq.map(_._1).mkString(", ")
      sql(s"ANALYZE TABLE $table COMPUTE STATISTICS FOR COLUMNS $columns")
      df = sql(s"SELECT * FROM $table")
      val stats = df.queryExecution.analyzed.collect {
        case rel: LogicalRelation =>
          expectedColStatsSeq.foreach { expected =>
            assert(rel.catalogTable.get.stats.get.basicColStats.contains(expected._1))
            checkColStats(colStats = rel.catalogTable.get.stats.get.basicColStats(expected._1),
              expectedColStats = expected._2)
          }
      }
      assert(stats.size == 1)
    }
  }

  def checkColStats(colStats: BasicColStats, expectedColStats: BasicColStats): Unit = {
    assert(colStats.dataType == expectedColStats.dataType)
    assert(colStats.numNulls == expectedColStats.numNulls)
    colStats.dataType match {
      case ByteType | ShortType | IntegerType | LongType =>
        assert(colStats.max.map(_.toString.toLong) == expectedColStats.max.map(_.toString.toLong))
        assert(colStats.min.map(_.toString.toLong) == expectedColStats.min.map(_.toString.toLong))
      case FloatType | DoubleType =>
        assert(colStats.max.map(_.toString.toDouble) == expectedColStats.max
          .map(_.toString.toDouble))
        assert(colStats.min.map(_.toString.toDouble) == expectedColStats.min
          .map(_.toString.toDouble))
      case DecimalType.SYSTEM_DEFAULT =>
        assert(colStats.max.map(i => Decimal(i.toString)) == expectedColStats.max
          .map(i => Decimal(i.toString)))
        assert(colStats.min.map(i => Decimal(i.toString)) == expectedColStats.min
          .map(i => Decimal(i.toString)))
      case DateType | TimestampType =>
        if (expectedColStats.max.isDefined) {
          // just check the difference to exclude the influence of timezones
          assert(colStats.max.get.toString.toLong - colStats.min.get.toString.toLong ==
            expectedColStats.max.get.toString.toLong - expectedColStats.min.get.toString.toLong)
        } else {
          assert(colStats.max.isEmpty && colStats.min.isEmpty)
        }
      case _ => // only numeric types, date type and timestamp type have max and min stats
    }
    colStats.dataType match {
      case BinaryType => assert(colStats.ndv.isEmpty)
      case BooleanType => assert(colStats.ndv.contains(2))
      case _ =>
        // ndv is an approximate value, so we just make sure we have the value
        assert(colStats.ndv.get >= 0)
    }
    assert(colStats.avgColLen == expectedColStats.avgColLen)
    assert(colStats.maxColLen == expectedColStats.maxColLen)
    assert(colStats.numTrues == expectedColStats.numTrues)
    assert(colStats.numFalses == expectedColStats.numFalses)
  }

}
