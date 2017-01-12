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
import org.apache.spark.sql.catalyst.expressions.AttributeMap
import org.apache.spark.sql.catalyst.plans.logical.{ColumnStat, Join, Statistics}
import org.apache.spark.sql.catalyst.plans.logical.estimation.EstimationUtils._
import org.apache.spark.sql.test.SharedSQLContext


class JoinEstimationSuite extends QueryTest with SharedSQLContext {
  import testImplicits._

  /** Data for one-column tables */
  private val joinEstimationTestData1 = Seq(
    ("join_est_test1", "key1", Seq[Int](1, 1, 2, 2, 3, 3, 3)),
    ("join_est_test2", "key2", Seq[Int](5, 6)))

  /** Data for two-column tables */
  private val joinEstimationTestData2 = Seq(
    ("join_est_test3", Seq("key31", "key32"),
      Seq[(Int, Int)]((1, 9), (2, 8), (3, 7), (4, 6), (5, 5))),
    ("join_est_test4", Seq("key41", "key42"),
      Seq[(Int, Int)]((1, 3), (2, 4))))

  /** Original column stats */
  val colStatForKey1 = ColumnStat(3, Some(1), Some(3), 0, 4, 4)
  val colStatForKey2 = ColumnStat(2, Some(5), Some(6), 0, 4, 4)
  val colStatForKey31 = ColumnStat(5, Some(1), Some(5), 0, 4, 4)
  val colStatForKey32 = ColumnStat(5, Some(5), Some(9), 0, 4, 4)
  val colStatForKey41 = ColumnStat(2, Some(1), Some(2), 0, 4, 4)
  val colStatForKey42 = ColumnStat(2, Some(3), Some(4), 0, 4, 4)

  override def beforeAll(): Unit = {
    super.beforeAll()
    // Create tables and collect statistics
    joinEstimationTestData1.foreach { case (table, column, data) =>
      data.toDF(column).write.saveAsTable(table)
      sql(s"analyze table $table compute STATISTICS FOR COLUMNS $column")
    }
    joinEstimationTestData2.foreach { case (table, columns, data) =>
      data.toDF(columns: _*).write.saveAsTable(table)
      sql(s"analyze table $table compute STATISTICS FOR COLUMNS ${columns.mkString(", ")}")
    }
  }

  override def afterAll(): Unit = {
    joinEstimationTestData1.foreach { case (table, _, _) => sql(s"DROP TABLE IF EXISTS $table") }
    joinEstimationTestData2.foreach { case (table, _, _) => sql(s"DROP TABLE IF EXISTS $table") }
    super.afterAll()
  }


  test("estimate inner join") {
    val innerJoinSql =
      "select count(1) from join_est_test1 join join_est_test3 on key1 = key31"
    // Update column stats from both sides.
    val joinedColStat = ColumnStat(3, Some(1), Some(3), 0, 4, 4)
    val colStats = Seq("key1" -> joinedColStat, "key31" -> joinedColStat)
    validateEstimatedStats(innerJoinSql, colStats)
  }

  test("update column stats for join keys and non-join keys") {
    val innerJoinSql =
      "select count(1) from join_est_test3 join join_est_test4 on key31 = key41 and key32 > key42"
    // Update column stats for both join keys.
    // Update non-join column stat if #outputRow / #sideRow < 1, otherwise keep it unchanged.
    val joinedColStat = ColumnStat(2, Some(1), Some(2), 0, 4, 4)
    val colStats = Seq("key31" -> joinedColStat, "key41" -> joinedColStat,
      "key32" -> colStatForKey32.copy(distinctCount = 2), "key42" -> colStatForKey42)
    validateEstimatedStats(innerJoinSql, colStats)
  }

  test("estimate disjoint inner join") {
    val innerJoinSql =
      "select count(1) from join_est_test1 join join_est_test2 on key1 = key2"
    // Empty column stats for both sides.
    val emptyColStat = ColumnStat(0, None, None, 0, 4, 4)
    val colStats = Seq("key1" -> emptyColStat, "key2" -> emptyColStat)
    validateEstimatedStats(innerJoinSql, colStats)
  }

  test("estimate cross join without equal conditions") {
    val crossJoinSql =
      "select count(1) from join_est_test1 cross join join_est_test2 on key1 < key2"
    // Keep the column stat from both sides unchanged.
    val colStats = Seq("key1" -> colStatForKey1, "key2" -> colStatForKey2)
    validateEstimatedStats(crossJoinSql, colStats)
  }

  test("estimate left outer join") {
    val leftOuterJoinSql =
      "select count(1) from join_est_test3 left join join_est_test4 on key31 = key41"
    // Keep the column stat from left side unchanged.
    val joinedColStat = ColumnStat(2, Some(1), Some(2), 0, 4, 4)
    val colStats = Seq("key31" -> colStatForKey31, "key41" -> joinedColStat)
    validateEstimatedStats(leftOuterJoinSql, colStats)
  }

  test("estimate right outer join") {
    val rightOuterJoinSql =
      "select count(1) from join_est_test4 right join join_est_test3 on key41 = key31"
    // Keep the column stat from right side unchanged.
    val joinedColStat = ColumnStat(2, Some(1), Some(2), 0, 4, 4)
    val colStats = Seq("key41" -> joinedColStat, "key31" -> colStatForKey31)
    validateEstimatedStats(rightOuterJoinSql, colStats)
  }

  test("estimate full outer join") {
    val fullOuterJoinSql =
      "select count(1) from join_est_test3 full join join_est_test4 on key31 = key41"
    // Keep the column stat from both sides unchanged.
    val colStats = Seq("key31" -> colStatForKey31, "key41" -> colStatForKey41)
    validateEstimatedStats(fullOuterJoinSql, colStats)
  }

  test("estimate left semi/anti join") {
    val joinTypeStrings = Seq("left semi", "left anti")
    joinTypeStrings.foreach { str =>
      val joinSql =
        s"select count(1) from join_est_test3 $str join join_est_test4 on key31 = key41"
      // For now we just propagate the statistics from left side for left semi/anti join.
      val colStats = Seq("key31" -> colStatForKey31)
      validateEstimatedStats(joinSql, colStats, Some(5))
    }
  }

  private def validateEstimatedStats(
      joinSql: String,
      expectedColStats: Seq[(String, ColumnStat)],
      rowCount: Option[Long] = None) : Unit = {
    val logicalPlan = sql(joinSql).queryExecution.optimizedPlan
    val joinNode = logicalPlan.collect { case join: Join => join }.head
    val expectedRowCount = rowCount.getOrElse(sql(joinSql).collect().head.getLong(0))
    val nameToAttr = joinNode.output.map(a => (a.name, a)).toMap
    val expectedAttrStats =
      AttributeMap(expectedColStats.map(kv => nameToAttr(kv._1) -> kv._2))
    val expectedStats = Statistics(
      sizeInBytes = expectedRowCount * getRowSize(joinNode.output, expectedAttrStats),
      rowCount = Some(expectedRowCount),
      attributeStats = expectedAttrStats,
      isBroadcastable = false)
    assert(joinNode.statistics == expectedStats)
  }
}
