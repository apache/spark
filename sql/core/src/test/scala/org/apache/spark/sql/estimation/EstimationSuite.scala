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
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeMap}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans.logical.estimation.EstimationUtils._
import org.apache.spark.sql.test.SharedSQLContext


/**
 * End-to-end suite testing statistics estimation for logical operators.
 */
class EstimationSuite extends QueryTest with SharedSQLContext {
  import testImplicits._

  /** Table info: all estimation tests are conducted on these tables. */
  private val estimationTestData = Seq(
    ("estimation_test1", Seq("key11", "key12"), Seq[(Int, Int)]((1, 10), (2, 10))),
    ("estimation_test2", Seq("key21", "key22"),
      Seq[(Int, Int)]((1, 10), (1, 20), (2, 30), (2, 40))),
    ("estimation_test3", Seq("key31", "key32"),
      Seq[(Int, Int)]((1, 10), (1, 10), (1, 20), (2, 20), (2, 10), (2, 10))))

  /** Original column stats */
  val colStatForKey11 = ColumnStat(2, Some(1), Some(2), 0, 4, 4)
  val colStatForKey12 = ColumnStat(1, Some(10), Some(10), 0, 4, 4)
  val colStatForKey21 = colStatForKey11
  val colStatForKey22 = ColumnStat(4, Some(10), Some(40), 0, 4, 4)
  val colStatForKey31 = colStatForKey11
  val colStatForKey32 = ColumnStat(2, Some(10), Some(20), 0, 4, 4)

  override def beforeAll(): Unit = {
    super.beforeAll()
    // Create tables and collect statistics
    estimationTestData.foreach { case (table, columns, data) =>
      data.toDF(columns: _*).write.saveAsTable(table)
      sql(s"ANALYZE TABLE $table COMPUTE STATISTICS FOR COLUMNS ${columns.mkString(", ")}")
    }
  }

  override def afterAll(): Unit = {
    estimationTestData.foreach { case (table, _, _) => sql(s"DROP TABLE IF EXISTS $table") }
    super.afterAll()
  }

  test("estimate agg with a primary key") {
    // There's a primary key, so row count = ndv of that key
    checkAggStats(
      testSql = "select * from estimation_test1 group by key11, key12",
      expectedRowCount = colStatForKey11.distinctCount,
      expectedColStats = Seq("key11" -> colStatForKey11, "key12" -> colStatForKey12))
  }

  test("estimate agg when child's row count is smaller than the product of ndv's") {
    checkAggStats(
      testSql = "select * from estimation_test2 group by key21, key22",
      expectedRowCount = 4,
      expectedColStats = Seq("key21" -> colStatForKey21, "key22" -> colStatForKey22))
  }

  test("estimate agg when data contains all combinations of distinct values of group by columns.") {
    checkAggStats(
      testSql = "select * from estimation_test3 group by key31, key32",
      expectedRowCount = colStatForKey31.distinctCount * colStatForKey32.distinctCount,
      expectedColStats = Seq("key31" -> colStatForKey31, "key32" -> colStatForKey32))
  }

  test("estimate agg without group by column") {
    checkAggStats(
      testSql = "select count(1) from estimation_test3",
      expectedRowCount = 1,
      expectedColStats = Seq.empty)
  }

  private def checkAggStats(
      testSql: String,
      expectedRowCount: BigInt,
      expectedColStats: Seq[(String, ColumnStat)]): Unit = {

    // Make sure the given row count is correct.
    assert(expectedRowCount == computeRowCount(s"select count(1) from ($testSql)"))

    val agg = findPlan(testSql, { case agg: Aggregate => agg })
    val expectedAttrStats = toAttributeMap(expectedColStats, agg)
    val expectedStats = Statistics(
      sizeInBytes = expectedRowCount * getRowSize(agg.output, expectedAttrStats),
      rowCount = Some(expectedRowCount),
      attributeStats = expectedAttrStats)
    assert(agg.statistics == expectedStats)
  }

  private def computeRowCount(countSql: String): Long = sql(countSql).collect().head.getLong(0)

  /** Find required plan from query. */
  private def findPlan(query: String, findFunc: PartialFunction[LogicalPlan, LogicalPlan])
    : LogicalPlan = {
    sql(query).queryExecution.optimizedPlan.collect(findFunc).head
  }

  /** Convert (column name, column stat) pairs to an AttributeMap based on plan output. */
  private def toAttributeMap(colStats: Seq[(String, ColumnStat)], plan: LogicalPlan)
    : AttributeMap[ColumnStat] = {
    val nameToAttr: Map[String, Attribute] = plan.output.map(a => (a.name, a)).toMap
    AttributeMap(colStats.map(kv => nameToAttr(kv._1) -> kv._2))
  }
}
