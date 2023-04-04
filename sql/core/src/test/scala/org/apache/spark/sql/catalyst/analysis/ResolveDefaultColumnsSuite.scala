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

  test("Assign correct types when adding DEFAULTs for inserting from a VALUES list") {
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
    val inlineTable: UnresolvedInlineTable = result match {
      case u: UnresolvedInlineTable => u
      case _ => fail(s"invalid result operator type: $result")
    }
    assert(inlineTable.names == Seq("c1", "c2"))
    assert(inlineTable.rows == Seq(
      Seq(literal, UnresolvedAttribute("DEFAULT"))))
  }
}
