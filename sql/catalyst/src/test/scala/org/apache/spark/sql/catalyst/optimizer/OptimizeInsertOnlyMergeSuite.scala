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

package org.apache.spark.sql.catalyst.optimizer

import java.util.Collections

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.{Alias, If, IntegralDivide, Literal}
import org.apache.spark.sql.catalyst.expressions.Literal.{FalseLiteral, TrueLiteral}
import org.apache.spark.sql.catalyst.plans.{LeftAnti, PlanTest}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, InsertOnlyMerge, Join, JoinHint, LocalRelation, LogicalPlan, MergeRows, Project}
import org.apache.spark.sql.catalyst.plans.logical.MergeRows.{Insert, Instruction, Keep}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.connector.catalog.InMemoryTable
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{LongType, MetadataBuilder, StructType}

class OptimizeInsertOnlyMergeSuite extends PlanTest {
  private object Optimize extends RuleExecutor[LogicalPlan] {
    override val batches: Seq[Batch] = Seq(
      Batch("Optimize insert-only MERGE", Once, ConstantFolding, OptimizeInsertOnlyMerge))
  }

  private val source = LocalRelation($"sourceId".long, $"unused".int)
  private val key = source.output.head
  private val table = new InMemoryTable(
    "target", new StructType().add("id", LongType), Array.empty, Collections.emptyMap())
  private val target = DataSourceV2Relation.create(table, None, None)
  private val join = Join(source, target, LeftAnti, Some(key === target.output.head), JoinHint.NONE)
  private val metadata = new MetadataBuilder().putString("comment", "target column").build()

  private def insert(
      instructions: Seq[Instruction],
      child: LogicalPlan = join): InsertOnlyMerge = {
    val attrs = target.output.map(_.newInstance().withMetadata(metadata).withQualifier(Seq("t")))
    InsertOnlyMerge(target, MergeRows(TrueLiteral, FalseLiteral, Nil, instructions, Nil,
      checkCardinality = false, attrs, child))
  }

  private def optimize(plan: InsertOnlyMerge): InsertOnlyMerge = {
    Optimize.execute(plan).asInstanceOf[InsertOnlyMerge]
  }

  test("fold constants and preserve the write output attributes") {
    val plan = insert(Seq(Keep(Insert, key > (Literal(1L) + Literal(1L)), Seq(key + 1L))))
    val result = optimize(plan)

    val project = result.query.asInstanceOf[Project]
    assert(project.output.map(_.exprId) == plan.query.output.map(_.exprId))
    assert(project.output.map(_.metadata) == plan.query.output.map(_.metadata))
    assert(project.output.map(_.qualifier) == plan.query.output.map(_.qualifier))
    assert(project.projectList.head.asInstanceOf[Alias].child.semanticEquals(key + 1L))
    comparePlans(project.child, join.copy(left = Filter(key > 2L, source)))
  }

  gridTest("preserve a failed constant fold")(Seq("condition", "value")) { position =>
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      val bad = IntegralDivide(Literal(1L), Literal(0L))
      val instruction = if (position == "condition") {
        Keep(Insert, bad === 1L, Seq(key))
      } else {
        Keep(Insert, TrueLiteral, Seq(bad))
      }
      val plan = insert(Seq(instruction))
      assert(optimize(plan).query.isInstanceOf[MergeRows])
    }
  }

  test("failures inside a conditional expression remain protected") {
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      val value = If(key > 0L, IntegralDivide(Literal(1L), Literal(0L)), Literal(1L))
      val result = optimize(insert(Seq(Keep(Insert, TrueLiteral, Seq(value)))))
      val project = result.query.asInstanceOf[Project]
      assert(project.projectList.head.asInstanceOf[Alias].child.semanticEquals(value))
    }
  }

  test("preserve multiple insert actions") {
    val plan = insert(Seq(
      Keep(Insert, key < 0L, Seq(Literal(0L))),
      Keep(Insert, TrueLiteral, Seq(key))))
    assert(optimize(plan).query.isInstanceOf[MergeRows])
  }

  test("do not remove MergeRows before its constants have been folded") {
    val plan = insert(Seq(Keep(Insert, TrueLiteral, Seq(Literal(1L) + Literal(2L)))))
    comparePlans(OptimizeInsertOnlyMerge(plan), plan)
  }

  test("push the condition through a pruning projection above the anti join") {
    val pruned = Project(Seq(Alias(key, "renamed")()), join)
    val renamed = pruned.output.head
    val result = optimize(insert(Seq(Keep(Insert, renamed > 0L, Seq(renamed))), pruned))
    val filteredJoin = result.query.collectFirst { case j: Join => j }.get
    comparePlans(filteredJoin, join.copy(left = Filter(key > 0L, source)))
  }

  test("keep source-dependent conditions below the anti join") {
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      val condition = IntegralDivide(Literal(1L), key) > 0L
      val result = optimize(insert(Seq(Keep(Insert, condition, Seq(key)))))
      comparePlans(result.query.children.head,
        join.copy(left = Filter(condition, source)))
    }
  }

  test("do not rewrite MergeRows outside an insert-only write") {
    val rows = insert(Seq(Keep(Insert, TrueLiteral, Seq(key)))).query
    comparePlans(Optimize.execute(rows), rows)
  }
}
