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
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{StructField, StructType, TimestampType}

class ResolveDefaultColumnsSuite extends QueryTest with SharedSparkSession {
  val rule = ResolveDefaultColumns(catalog = null)

  test("SPARK-43018: Assign correct types for DEFAULTs with INSERT from a VALUES list") {
    // This is the internal storage for the timestamp 2020-12-31 00:00:00.0.
    val literal = Literal(1609401600000000L, TimestampType)
    val table = UnresolvedInlineTable(
      names = Seq("attr1"),
      rows = Seq(Seq(literal)))
    val node = ResolveInlineTables(table).asInstanceOf[LocalRelation]

    assert(node.output.map(_.dataType) == Seq(TimestampType))
    assert(node.data.size == 1)

    val insertTableSchemaWithoutPartitionColumns = StructType(Seq(
      StructField("c1", TimestampType),
      StructField("c2", TimestampType)))
    val result = rule.addMissingDefaultValuesForInsertFromInlineTable(
      node, insertTableSchemaWithoutPartitionColumns, numUserSpecifiedColumns = 1)
    val relation: LocalRelation = result match {
      case r: LocalRelation => r
      case _ => fail(s"invalid result operator type: $result")
    }
    assert(relation.output.map(_.name) == Seq("c1", "c2"))
    val data: Seq[Seq[Any]] = relation.data.map { row =>
      row.toSeq(StructType(relation.output.map(col => StructField(col.name, col.dataType))))
    }
    assert(data == Seq(Seq(literal.value, null)))
  }

  test("SPARK-43018: INSERT timestamp values into a table with column DEFAULTs") {
    withTable("t") {
      sql("create table t(id int, ts timestamp) using parquet")
      sql("insert into t (ts) values (timestamp'2023-01-01')")
      val rows = sql("select id, ts from t").collect()
      assert(rows.size == 1)
      assert(rows.head.toString == "[42,2023-01-01 00:00:00.0]")
    }
  }
}
