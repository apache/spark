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
    ("estimation_test1", Seq("key11", "key12"), Seq[(Int, Int)]((1, 10), (2, 10))))

  /** Original column stats */
  val colStatForKey11 = ColumnStat(2, Some(1), Some(2), 0, 4, 4)
  val colStatForKey12 = ColumnStat(1, Some(10), Some(10), 0, 4, 4)

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

  test("estimate project with alias") {
    val project = findPlan(
      query = "select key11, key12 as abc from estimation_test1",
      findFunc = { case proj: Project => proj })
    val expectedColStats = Seq("key11" -> colStatForKey11, "abc" -> colStatForKey12)
    val expectedAttrStats = toAttributeMap(expectedColStats, project)
    // The number of rows won't change for project.
    val expectedStats = Statistics(
      sizeInBytes = 2 * getRowSize(project.output, expectedAttrStats),
      rowCount = Some(2),
      attributeStats = expectedAttrStats)
    assert(project.statistics == expectedStats)
  }

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
