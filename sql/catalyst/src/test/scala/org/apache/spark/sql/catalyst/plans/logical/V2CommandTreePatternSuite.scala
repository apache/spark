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

package org.apache.spark.sql.catalyst.plans.logical

import java.util

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.ProjectingInternalRow
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.trees.TreePattern
import org.apache.spark.sql.catalyst.util.{ReplaceDataProjections, WriteDeltaProjections}
import org.apache.spark.sql.connector.catalog.{Column, Table, TableCapability}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.types.{IntegerType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * Pins the tree-pattern identity contract for the DSv2 row-level command nodes: each node carries
 * both the shared `COMMAND` bit and its own identity bit, so rules can prune on either.
 */
class V2CommandTreePatternSuite extends SparkFunSuite {

  private val target = LocalRelation($"a".int, $"b".int)
  private val source = LocalRelation($"c".int, $"d".int)

  // A minimal `NamedRelation` for the row-level write nodes, whose `nodePatternsInternal()`
  // returns a compile-time constant, so no resolution is needed to pin the identity bit.
  private val v2Relation: DataSourceV2Relation = {
    val table = new Table {
      override def name(): String = "t"
      override def columns(): Array[Column] = Array(Column.create("a", IntegerType))
      override def capabilities(): util.Set[TableCapability] = util.Set.of[TableCapability]()
    }
    DataSourceV2Relation.create(table, None, None, CaseInsensitiveStringMap.empty())
  }

  private val emptyRow = ProjectingInternalRow(new StructType(), IndexedSeq.empty)

  test("DeleteFromTable declares COMMAND and DELETE_FROM_TABLE") {
    val plan = DeleteFromTable(target, Literal.TrueLiteral)
    assert(plan.containsAllPatterns(TreePattern.COMMAND, TreePattern.DELETE_FROM_TABLE))
  }

  test("UpdateTable declares COMMAND and UPDATE_TABLE") {
    val plan = UpdateTable(target, Seq.empty, Some(Literal.TrueLiteral))
    assert(plan.containsAllPatterns(TreePattern.COMMAND, TreePattern.UPDATE_TABLE))
  }

  test("MergeIntoTable declares COMMAND and MERGE_INTO_TABLE") {
    val plan = MergeIntoTable(
      target,
      source,
      mergeCondition = $"a" === $"c",
      matchedActions = Seq(DeleteAction(None)),
      notMatchedActions = Seq(InsertAction(None,
        Seq(Assignment($"a", $"c"), Assignment($"b", $"d")))),
      notMatchedBySourceActions = Seq(DeleteAction(None)),
      withSchemaEvolution = false)
    assert(plan.containsAllPatterns(TreePattern.COMMAND, TreePattern.MERGE_INTO_TABLE))
  }

  test("ReplaceData declares COMMAND and REPLACE_DATA") {
    val plan = ReplaceData(
      v2Relation,
      Literal.TrueLiteral,
      source,
      v2Relation,
      ReplaceDataProjections(emptyRow, None))
    assert(plan.containsAllPatterns(TreePattern.COMMAND, TreePattern.REPLACE_DATA))
  }

  test("WriteDelta declares COMMAND and WRITE_DELTA") {
    val plan = WriteDelta(
      v2Relation,
      Literal.TrueLiteral,
      source,
      v2Relation,
      WriteDeltaProjections(None, emptyRow, None))
    assert(plan.containsAllPatterns(TreePattern.COMMAND, TreePattern.WRITE_DELTA))
  }
}
