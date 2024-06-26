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

package org.apache.spark.sql.collation

import org.apache.spark.sql.{DataFrame, QueryTest}
import org.apache.spark.sql.execution.ExplainMode
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class CollatedFilterPushDownToReadersSuite extends QueryTest
  with SharedSparkSession
  with AdaptiveSparkPlanHelper {

  private val tblName = "tbl"
  private val nonCollatedCol = "c0"
  private val collatedCol = "c1"
  private val collatedStructCol = "c2"
  private val collatedStructNestedCol = "f1"
  private val collatedStructFieldAccess = s"$collatedStructCol.$collatedStructNestedCol"
  private val collatedArrayCol = "c3"
  private val collatedMapCol = "c4"

  private val lcaseCollation = "'UTF8_LCASE'"
  private val dataSources = Seq("parquet")

  def testV1AndV2PushDown(
      filterString: String,
      expectedPushedFilters: Seq[String],
      expectedRowCount: Int): Unit = {
    def testPushDown(dataSource: String, useV1: Boolean): Unit = {
      test(s"collation push down filter: $filterString, source: $dataSource, isV1: $useV1") {
        val v1Source = if (useV1) dataSource else ""
        withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> v1Source) {
          withTestTable(dataSource) {
            val df = sql(s"SELECT * FROM $tblName WHERE $filterString")
            val actualPushedFilters = getPushedFilters(df)
            assert(actualPushedFilters.sorted === expectedPushedFilters.sorted)
            assert(df.count() === expectedRowCount)
          }
        }
      }
    }

    dataSources.foreach { source =>
      testPushDown(source, useV1 = true)
      testPushDown(source, useV1 = false)
    }
  }

  def withTestTable(dataSource: String)(fn: => Unit): Unit = {
    withTable(tblName) {
      sql(s"""
           |CREATE TABLE $tblName USING $dataSource AS
           |SELECT
           |  c as $nonCollatedCol,
           |  COLLATE(c, $lcaseCollation) as $collatedCol,
           |  named_struct('$collatedStructNestedCol',
           |    COLLATE(c, $lcaseCollation)) as $collatedStructCol,
           |  array(COLLATE(c, $lcaseCollation)) as $collatedArrayCol,
           |  map(COLLATE(c, $lcaseCollation), 1) as $collatedMapCol
           |FROM VALUES ('aaa'), ('AAA'), ('bbb')
           |as data(c)
           |""".stripMargin)

      fn
    }
  }

  def getPushedFilters(df: DataFrame): Seq[String] = {
    val explain = df.queryExecution.explainString(ExplainMode.fromString("extended"))

    // Regular expression to extract text inside the brackets
    val pattern = "PushedFilters: \\[(.*?)\\]".r

    pattern.findFirstMatchIn(explain) match {
      case Some(m) =>
        m.group(1)
          .split(", ")
          .toSeq.filterNot(_ == "")
      case None => Seq.empty
    }
  }

  testV1AndV2PushDown(
    filterString = s"'aaa' COLLATE UNICODE = 'bbb' COLLATE UNICODE",
    expectedPushedFilters = Seq.empty,
    expectedRowCount = 0)

  testV1AndV2PushDown(
    filterString = s"$collatedCol = 'aaa'",
    expectedPushedFilters = Seq(s"IsNotNull($collatedCol)"),
    expectedRowCount = 2)

  testV1AndV2PushDown(
    filterString = s"$collatedCol = 'aaa' AND $nonCollatedCol = 'aaa'",
    expectedPushedFilters = Seq(
      s"IsNotNull($collatedCol)", s"IsNotNull($nonCollatedCol)", s"EqualTo($nonCollatedCol,aaa)"),
    expectedRowCount = 1)

  testV1AndV2PushDown(
    filterString = s"$collatedCol = 'aaa' OR $nonCollatedCol = 'aaa'",
    expectedPushedFilters = Seq.empty,
    expectedRowCount = 2)

  testV1AndV2PushDown(
    filterString = s"$collatedCol != 'aaa'",
    expectedPushedFilters = Seq(s"IsNotNull($collatedCol)"),
    expectedRowCount = 1)

  testV1AndV2PushDown(
    filterString = s"NOT($collatedCol == 'aaa')",
    expectedPushedFilters = Seq(s"IsNotNull($collatedCol)"),
    expectedRowCount = 1)

  testV1AndV2PushDown(
    filterString = s"$collatedStructFieldAccess = 'aaa'",
    expectedPushedFilters = Seq(s"IsNotNull($collatedStructFieldAccess)"),
    expectedRowCount = 2)

  testV1AndV2PushDown(
    filterString = s"$collatedArrayCol = array(collate('aaa', $lcaseCollation))",
    expectedPushedFilters = Seq(s"IsNotNull($collatedArrayCol)"),
    expectedRowCount = 2)

  testV1AndV2PushDown(
    filterString = s"map_keys($collatedMapCol) != array(collate('aaa', $lcaseCollation))",
    expectedPushedFilters = Seq(s"IsNotNull($collatedMapCol)"),
    expectedRowCount = 1)
}
