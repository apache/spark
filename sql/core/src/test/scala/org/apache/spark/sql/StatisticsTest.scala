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

package org.apache.spark.sql

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.plans.logical.{ColumnStat, Statistics}
import org.apache.spark.sql.execution.command.AnalyzeColumnCommand
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types._

trait StatisticsTest extends QueryTest with SharedSQLContext {

  def checkColStats(
      df: DataFrame,
      expectedColStatsSeq: Seq[(StructField, ColumnStat)]): Unit = {
    val table = "tbl"
    withTable(table) {
      df.write.format("json").saveAsTable(table)
      val columns = expectedColStatsSeq.map(_._1)
      val tableIdent = TableIdentifier(table, Some("default"))
      val relation = spark.sessionState.catalog.lookupRelation(tableIdent)
      val columnStats =
        AnalyzeColumnCommand(tableIdent, columns.map(_.name)).computeColStats(spark, relation)._2
      expectedColStatsSeq.foreach { expected =>
        assert(columnStats.contains(expected._1.name))
        val colStat = columnStats(expected._1.name)
        StatisticsTest.checkColStat(
          dataType = expected._1.dataType,
          colStat = colStat,
          expectedColStat = expected._2,
          rsd = spark.sessionState.conf.ndvMaxError)

        // check if we get the same colStat after encoding and decoding
        val encodedCS = colStat.toString
        val decodedCS = ColumnStat(expected._1.dataType, encodedCS)
        StatisticsTest.checkColStat(
          dataType = expected._1.dataType,
          colStat = decodedCS,
          expectedColStat = expected._2,
          rsd = spark.sessionState.conf.ndvMaxError)
      }
    }
  }

  def checkTableStats(tableName: String, expectedRowCount: Option[Int]): Option[Statistics] = {
    val df = spark.table(tableName)
    val stats = df.queryExecution.analyzed.collect { case rel: LogicalRelation =>
      assert(rel.catalogTable.get.stats.flatMap(_.rowCount) === expectedRowCount)
      rel.catalogTable.get.stats
    }
    assert(stats.size == 1)
    stats.head
  }
}

object StatisticsTest {
  def checkColStat(
      dataType: DataType,
      colStat: ColumnStat,
      expectedColStat: ColumnStat,
      rsd: Double): Unit = {
    dataType match {
      case StringType =>
        val cs = colStat.forString
        val expectedCS = expectedColStat.forString
        assert(cs.numNulls == expectedCS.numNulls)
        assert(cs.avgColLen == expectedCS.avgColLen)
        assert(cs.maxColLen == expectedCS.maxColLen)
        checkNdv(ndv = cs.ndv, expectedNdv = expectedCS.ndv, rsd = rsd)
      case BinaryType =>
        val cs = colStat.forBinary
        val expectedCS = expectedColStat.forBinary
        assert(cs.numNulls == expectedCS.numNulls)
        assert(cs.avgColLen == expectedCS.avgColLen)
        assert(cs.maxColLen == expectedCS.maxColLen)
      case BooleanType =>
        val cs = colStat.forBoolean
        val expectedCS = expectedColStat.forBoolean
        assert(cs.numNulls == expectedCS.numNulls)
        assert(cs.numTrues == expectedCS.numTrues)
        assert(cs.numFalses == expectedCS.numFalses)
      case atomicType: AtomicType =>
        checkNumericColStats(
          dataType = atomicType, colStat = colStat, expectedColStat = expectedColStat, rsd = rsd)
    }
  }

  private def checkNumericColStats(
      dataType: AtomicType,
      colStat: ColumnStat,
      expectedColStat: ColumnStat,
      rsd: Double): Unit = {
    val cs = colStat.forNumeric(dataType)
    val expectedCS = expectedColStat.forNumeric(dataType)
    assert(cs.numNulls == expectedCS.numNulls)
    assert(cs.max == expectedCS.max)
    assert(cs.min == expectedCS.min)
    checkNdv(ndv = cs.ndv, expectedNdv = expectedCS.ndv, rsd = rsd)
  }

  private def checkNdv(ndv: Long, expectedNdv: Long, rsd: Double): Unit = {
    // ndv is an approximate value, so we make sure we have the value, and it should be
    // within 3*SD's of the given rsd.
    if (expectedNdv == 0) {
      assert(ndv == 0)
    } else if (expectedNdv > 0) {
      assert(ndv > 0)
      val error = math.abs((ndv / expectedNdv.toDouble) - 1.0d)
      assert(error <= rsd * 3.0d, "Error should be within 3 std. errors.")
    }
  }
}
