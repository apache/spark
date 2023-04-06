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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{StructField, StructType, TimestampType}

class ResolveDefaultColumnsSuite extends QueryTest with SharedSparkSession {
  val rule = ResolveDefaultColumns(catalog = null)
  // This is the internal storage for the timestamp 2020-12-31 00:00:00.0.
  val literal = Literal(1609401600000000L, TimestampType)
  val table = UnresolvedInlineTable(
    names = Seq("attr1"),
    rows = Seq(Seq(literal)))
  val localRelation = ResolveInlineTables(table).asInstanceOf[LocalRelation]

  def asLocalRelation(result: LogicalPlan): LocalRelation = result match {
    case r: LocalRelation => r
    case _ => fail(s"invalid result operator type: $result")
  }

  test("SPARK-43018: Add DEFAULTs for INSERT from VALUES list with user-defined columns") {
    // Call the 'addMissingDefaultValuesForInsertFromInlineTable' method with one user-specified
    // column. We add a default value of NULL to the row as a result.
    val insertTableSchemaWithoutPartitionColumns = StructType(Seq(
      StructField("c1", TimestampType),
      StructField("c2", TimestampType)))
    val (result: LogicalPlan, _: Boolean) =
      rule.addMissingDefaultValuesForInsertFromInlineTable(
        localRelation, insertTableSchemaWithoutPartitionColumns, numUserSpecifiedColumns = 1)
    val relation = asLocalRelation(result)
    assert(relation.output.map(_.name) == Seq("c1", "c2"))
    val data: Seq[Seq[Any]] = relation.data.map { row =>
      row.toSeq(StructType(relation.output.map(col => StructField(col.name, col.dataType))))
    }
    assert(data == Seq(Seq(literal.value, null)))
  }

  test("SPARK-43018: Add no DEFAULTs for INSERT from VALUES list with no user-defined columns") {
    // Call the 'addMissingDefaultValuesForInsertFromInlineTable' method with zero user-specified
    // columns. The table is unchanged because there are no default columns to add in this case.
    val insertTableSchemaWithoutPartitionColumns = StructType(Seq(
      StructField("c1", TimestampType),
      StructField("c2", TimestampType)))
    val (result: LogicalPlan, _: Boolean) =
      rule.addMissingDefaultValuesForInsertFromInlineTable(
        localRelation, insertTableSchemaWithoutPartitionColumns, numUserSpecifiedColumns = 0)
    assert(asLocalRelation(result) == localRelation)
  }

  test("SPARK-43018: INSERT timestamp values into a table with column DEFAULTs") {
    withTable("t") {
      sql("create table t(id int, ts timestamp) using parquet")
      sql("insert into t (ts) values (timestamp'2020-12-31')")
      checkAnswer(spark.table("t"),
        sql("select null, timestamp'2020-12-31'").collect().head)
    }
  }
}
