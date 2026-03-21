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

import org.apache.spark.sql.catalyst.expressions.{
  Alias, AttributeReference, Expression, ExprId,
  Literal, NamedExpression, VirtualColumn
}
import org.apache.spark.sql.catalyst.plans.logical.{
  Aggregate, Expand, LogicalPlan, Union
}
import org.apache.spark.sql.catalyst.rules.Rule

/**
 * Splits an Aggregate over Expand that contains an empty grouping
 * set into a Union of two branches:
 *  - Non-empty branch: Aggregate over Expand with only the
 *    non-empty grouping sets
 *  - Grand total branch: no-group Aggregate that always produces
 *    exactly one row
 *
 * SQL standard requires that the empty grouping set (grand total)
 * in ROLLUP/CUBE/GROUPING SETS always produces one row, even when
 * the input relation is empty at runtime. A no-group Aggregate
 * guarantees this because SQL defines that an aggregate without
 * GROUP BY on empty input returns one row.
 *
 * This rule must run before [[PropagateEmptyRelation]] so that the
 * grand total branch (a no-group Aggregate) is preserved when the
 * input is empty, while the non-empty branch (Aggregate with
 * grouping expressions) can still be eliminated.
 */
object SplitEmptyGroupingSet extends Rule[LogicalPlan] {

  def apply(plan: LogicalPlan): LogicalPlan = {
    plan.transformUp {
      case a @ Aggregate(ge, ae, expand: Expand, _)
        if ge.nonEmpty && !a.isStreaming &&
          findEmptyGroupingSetIndices(expand).nonEmpty =>
        splitAggregate(a, ge, ae, expand)
    }
  }

  /**
   * Find the indices of all empty grouping set projections in
   * the Expand. The empty grouping set nulls out all N group-by
   * columns, so its grouping ID bitmask is (1 << N) - 1.
   * Returns empty if none found.
   */
  private def findEmptyGroupingSetIndices(
      expand: Expand): Seq[Int] = {
    val gidIndex = expand.output.indexWhere(
      _.name == VirtualColumn.groupingIdName)
    if (gidIndex < 0) return Seq.empty

    val childExprIds = expand.child.output.map(_.exprId).toSet
    val numGroupByCols = expand.output.take(gidIndex)
      .count(attr => !childExprIds.contains(attr.exprId))
    if (numGroupByCols <= 0) return Seq.empty

    // Empty set bitmask: all N bits set.
    // N >= 64 is unreachable (Analyzer rejects it) but handled
    // for consistency with Expand.buildBitmask.
    val expectedMask: Long =
      if (numGroupByCols >= 64) -1L
      else (1L << numGroupByCols) - 1

    expand.projections.indices.filter { idx =>
      val proj = expand.projections(idx)
      proj(gidIndex) match {
        case Literal(v: Int, _) =>
          Integer.toUnsignedLong(v) == expectedMask
        case Literal(v: Long, _) => v == expectedMask
        case _ => false
      }
    }
  }

  private def splitAggregate(
      agg: Aggregate,
      ge: Seq[Expression],
      ae: Seq[NamedExpression],
      expand: Expand): LogicalPlan = {
    val emptySetIndices = findEmptyGroupingSetIndices(expand)
    // Use the first empty set projection as the template
    val emptySetProj = expand.projections(emptySetIndices.head)

    // Build grand total branch: replace Expand-produced
    // attributes with their literal values from the empty
    // grouping set projection, then wrap in a no-group Aggregate.
    val attrToLiteral: Map[ExprId, Expression] =
      expand.output.zipWithIndex.collect {
        case (attr, idx)
          if emptySetProj(idx).isInstanceOf[Literal] =>
          attr.exprId -> emptySetProj(idx)
      }.toMap
    val modifiedAe = ae.map { expr =>
      val transformed = expr.transformUp {
        case ref: AttributeReference
          if attrToLiteral.contains(ref.exprId) =>
          attrToLiteral(ref.exprId)
      }
      transformed match {
        case ne: NamedExpression => ne
        case other =>
          Alias(other, expr.name)(expr.exprId)
      }
    }

    // For duplicate empty sets (e.g. GROUPING SETS ((a,b),(),(),()))
    // each empty set should produce one grand total row. Build an
    // Expand over the grand total to replicate the row, or just
    // use a single Aggregate if there's only one empty set.
    val grandTotal = if (emptySetIndices.size == 1) {
      Aggregate(Seq.empty, modifiedAe, expand.child)
    } else {
      // Multiple empty sets: create a Union of grand totals,
      // one per empty set. Each has the same output but may
      // differ in _gen_grouping_pos value.
      val grandTotals = emptySetIndices.map { idx =>
        val proj = expand.projections(idx)
        val localAttrToLiteral: Map[ExprId, Expression] =
          expand.output.zipWithIndex.collect {
            case (attr, i)
              if proj(i).isInstanceOf[Literal] =>
              attr.exprId -> proj(i)
          }.toMap
        val localAe = ae.map { expr =>
          val transformed = expr.transformUp {
            case ref: AttributeReference
              if localAttrToLiteral.contains(ref.exprId) =>
              localAttrToLiteral(ref.exprId)
          }
          transformed match {
            case ne: NamedExpression => ne
            case other =>
              Alias(other, expr.name)(expr.exprId)
          }
        }
        Aggregate(Seq.empty, localAe, expand.child)
      }
      if (grandTotals.size == 1) grandTotals.head
      else Union(grandTotals)
    }

    // Grand total's child is the Expand's child. Aggregate
    // functions reference pass-through attributes whose exprIds
    // match the child's output.

    // Build non-empty branch: remove all empty set projections
    val emptySetIdxSet = emptySetIndices.toSet
    val nonEmptyProjs = expand.projections.zipWithIndex
      .filterNot { case (_, idx) => emptySetIdxSet.contains(idx) }
      .map(_._1)
    if (nonEmptyProjs.isEmpty) {
      // Only empty sets existed -- just the grand total(s)
      return grandTotal
    }
    val nonEmptyExpand =
      expand.copy(projections = nonEmptyProjs)
    val nonEmptyAgg = agg.copy(child = nonEmptyExpand)

    Union(Seq(nonEmptyAgg, grandTotal))
  }
}
