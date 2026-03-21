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

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.{
  Alias, AttributeReference, BaseGroupingSets,
  GroupingID, Literal, VirtualColumn
}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{
  Aggregate, Expand, LocalRelation, LogicalPlan, Union
}
import org.apache.spark.sql.catalyst.rules.RuleExecutor

class SplitEmptyGroupingSetSuite extends PlanTest {

  object Split extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Split", Once, SplitEmptyGroupingSet) :: Nil
  }

  private def buildRollupExpand(
      child: LocalRelation,
      groupByColNames: Seq[String]): Expand = {
    val childOutput = child.output
    val groupByAliases = groupByColNames.map { name =>
      val attr = childOutput.find(_.name == name).get
      Alias(attr, name)()
    }
    val groupByAttrs = groupByAliases.map(_.toAttribute)
    val gidAttr = AttributeReference(
      VirtualColumn.groupingIdName,
      GroupingID.dataType,
      nullable = false)()
    val sets = BaseGroupingSets.rollupExprs(
      groupByAttrs.map(Seq(_)))
    val setsAttrs = sets.map(_.map(e =>
      groupByAttrs.find(_.semanticEquals(e)).get))
    Expand(
      setsAttrs, groupByAliases,
      groupByAttrs, gidAttr, child)
  }

  private def buildGroupingSetsExpand(
      child: LocalRelation,
      groupByColNames: Seq[String],
      sets: Seq[Seq[String]]): Expand = {
    val childOutput = child.output
    val groupByAliases = groupByColNames.map { name =>
      val attr = childOutput.find(_.name == name).get
      Alias(attr, name)()
    }
    val groupByAttrs = groupByAliases.map(_.toAttribute)
    val gidAttr = AttributeReference(
      VirtualColumn.groupingIdName,
      GroupingID.dataType,
      nullable = false)()
    val setsAttrs = sets.map { s =>
      s.map(n =>
        groupByAttrs.find(_.name == n).get)
    }
    Expand(
      setsAttrs, groupByAliases,
      groupByAttrs, gidAttr, child)
  }

  test("SPARK-53565: ROLLUP produces Union split") {
    val child = LocalRelation($"a".int, $"b".int)
    val expand = buildRollupExpand(child, Seq("a"))
    val expandOutput = expand.output
    val grpAttr = expandOutput(expandOutput.length - 2)
    val gidAttr = expandOutput.last

    val agg = Aggregate(
      Seq(grpAttr, gidAttr),
      Seq(grpAttr, gidAttr),
      expand)

    val result = Split.execute(agg)
    assert(result.isInstanceOf[Union],
      s"Expected Union but got ${result.getClass.getSimpleName}")
    val union = result.asInstanceOf[Union]
    assert(union.children.size == 2)

    // First branch: Aggregate with grouping expressions
    union.children.head match {
      case Aggregate(ge, _, _, _) =>
        assert(ge.nonEmpty,
          "Non-empty branch should have grouping exprs")
      case other =>
        fail(s"Expected Aggregate, got $other")
    }

    // Second branch: no-group Aggregate (grand total)
    union.children(1) match {
      case Aggregate(ge, _, _, _) =>
        assert(ge.isEmpty,
          "Grand total should have empty grouping exprs")
      case other =>
        fail(s"Expected Aggregate, got $other")
    }
  }

  test("SPARK-53565: no split without empty grouping set") {
    val child = LocalRelation($"a".int, $"b".int)
    val expand = buildGroupingSetsExpand(
      child, Seq("a"), Seq(Seq("a")))
    val expandOutput = expand.output
    val grpAttr = expandOutput(expandOutput.length - 2)
    val gidAttr = expandOutput.last

    val agg = Aggregate(
      Seq(grpAttr, gidAttr),
      Seq(grpAttr, gidAttr),
      expand)

    val result = Split.execute(agg)
    assert(result.isInstanceOf[Aggregate],
      "Should remain Aggregate without empty set")
  }

  test("SPARK-53565: only empty set produces grand total") {
    val child = LocalRelation($"a".int, $"b".int)
    val expand = buildGroupingSetsExpand(
      child, Seq("a"), Seq(Seq()))
    val expandOutput = expand.output
    val grpAttr = expandOutput(expandOutput.length - 2)
    val gidAttr = expandOutput.last

    val agg = Aggregate(
      Seq(grpAttr, gidAttr),
      Seq(grpAttr, gidAttr),
      expand)

    val result = Split.execute(agg)
    // Only empty set -> just grand total, no Union
    result match {
      case Aggregate(ge, _, _, _) =>
        assert(ge.isEmpty,
          "Only-empty-set should produce no-group Aggregate")
      case other =>
        fail(s"Expected Aggregate, got $other")
    }
  }

  test("SPARK-53565: grand total preserves grouping ID") {
    val child = LocalRelation($"a".int, $"b".int)
    val expand = buildRollupExpand(child, Seq("a"))
    val expandOutput = expand.output
    val grpAttr = expandOutput(expandOutput.length - 2)
    val gidAttr = expandOutput.last

    val agg = Aggregate(
      Seq(grpAttr, gidAttr),
      Seq(grpAttr, gidAttr),
      expand)

    val result = Split.execute(agg)
    val union = result.asInstanceOf[Union]
    val grandTotal = union.children(1).asInstanceOf[Aggregate]

    // The grand total aggregate expressions should contain
    // a non-null literal for the grouping ID (bitmask=1)
    val gidLiterals = grandTotal.aggregateExpressions
      .flatMap(_.collect {
        case lit: Literal if lit.value != null => lit
      })
    assert(gidLiterals.nonEmpty,
      "Grand total should have non-null grouping ID literal")
  }
}
