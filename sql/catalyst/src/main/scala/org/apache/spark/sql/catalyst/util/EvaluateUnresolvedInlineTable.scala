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
package org.apache.spark.sql.catalyst.util

import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.expressions.{AliasHelper, CommonExpressionDef, EvalHelper, Expression, Generator, LeafExpression, Literal, OuterReference, ScalarSubquery, SubqueryExpression, Unevaluable, WindowExpression, WindowFunction}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, AggregateFunction}
import org.apache.spark.sql.catalyst.optimizer.EvalInlineTables
import org.apache.spark.sql.catalyst.plans.logical.{Expand, LogicalPlan, OneRowRelation}
import org.apache.spark.sql.catalyst.trees.TreePattern.CURRENT_LIKE
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.catalyst.util.TypeUtils.{toSQLExpr, toSQLId}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{StringHelper, StringType, StructField, StructType}

/**
 * Utility object used to replace [[UnresolvedInlineTable]] with [[ResolvedInlineTable]] or
 * (whenever possible) with [[LocalRelation]]. Typically, [[UnresolvedInlineTable]] is
 * a child of [[InsertIntoStatement]].
 * Use the method: [[EvaluateUnresolvedInlineTable.evaluate]] as the entry point for this
 * transformation.
 *
 * If [[SQLConf.INLINE_TABLE_NON_FOLDABLE_EXPRESSIONS_ENABLED]] is true, an inline table with
 * expressions that can only be evaluated when the query runs, such as `rand()`, is replaced with
 * an [[Expand]] over a [[OneRowRelation]] instead, with one projection per row.
 */
object EvaluateUnresolvedInlineTable extends SQLConfHelper
  with AliasHelper with EvalHelper with CastSupport {

  def evaluate(
      plan: UnresolvedInlineTable,
      ignoreCollation: Boolean = false): LogicalPlan =
    if (plan.expressionsResolved) evaluateUnresolvedInlineTable(plan, ignoreCollation) else plan

  def evaluateUnresolvedInlineTable(
      table: UnresolvedInlineTable,
      ignoreCollation: Boolean = false): LogicalPlan = {
    validateInputDimension(table)
    validateInputEvaluable(table)
    val resolvedTable = findCommonTypesAndCast(table, ignoreCollation)
    if (table.rows.forall(_.forall(canEvalDuringAnalysis))) {
      earlyEvalIfPossible(resolvedTable)
    } else {
      toExpand(resolvedTable)
    }
  }

  /**
   * This function attempts to early evaluate rows in inline table.
   * If evaluation doesn't rely on non-deterministic expressions (e.g. current_like)
   * expressions will be evaluated and inlined as [[LocalRelation]]
   * This is package visible for unit testing.
   */
  private def earlyEvalIfPossible(table: ResolvedInlineTable): LogicalPlan = {
    val earlyEvalPossible = table.rows.flatten.forall(!_.containsPattern(CURRENT_LIKE))
    if (earlyEvalPossible) EvalInlineTables.eval(table) else table
  }

  /**
   * Turns an inline table with expressions that are evaluated when the query runs into an
   * [[Expand]] over a [[OneRowRelation]], which produces one output row per projection. The
   * foldable expressions are evaluated now, except CURRENT_LIKE expressions, which are replaced
   * with literals at the end of the analysis phase.
   */
  private def toExpand(table: ResolvedInlineTable): Expand = {
    val projections = table.rows.map(_.map {
      case e if !e.containsPattern(CURRENT_LIKE) && trimAliases(prepareForEval(e)).foldable =>
        Literal(EvalInlineTables.evalExpression(table, e), e.dataType)
      case e => e
    })
    Expand(projections, table.output, OneRowRelation())
  }

  /**
   * Returns whether the expression can be evaluated during the analysis phase: it must be
   * foldable, or contain CURRENT_LIKE expressions, which are replaced by literals at the end of
   * the analysis phase, but no subqueries, which can only be evaluated when the query runs.
   * Inline tables with only such expressions are evaluated during analysis.
   */
  private def canEvalDuringAnalysis(e: Expression): Boolean =
    (e.containsPattern(CURRENT_LIKE) && !SubqueryExpression.hasSubquery(e)) ||
      (e.resolved && trimAliases(prepareForEval(e)).foldable)

  /**
   * Validates the input data dimension:
   * 1. All rows have the same cardinality.
   * 2. The number of column aliases defined is consistent with the number of columns in data.
   *
   * This is package visible for unit testing.
   */
  def validateInputDimension(table: UnresolvedInlineTable): Unit = {
    if (table.rows.nonEmpty) {
      val numCols = table.names.size
      table.rows.zipWithIndex.foreach { case (row, rowIndex) =>
        if (row.size != numCols) {
          table.failAnalysis(
            errorClass = "INVALID_INLINE_TABLE.NUM_COLUMNS_MISMATCH",
            messageParameters = Map(
              "expectedNumCols" -> numCols.toString,
              "actualNumCols" -> row.size.toString,
              "rowIndex" -> rowIndex.toString))
        }
      }
    }
  }

  /**
   * Validates that all inline table data are valid expressions that can be evaluated: during the
   * analysis phase (see [[canEvalDuringAnalysis]]), or, if
   * [[SQLConf.INLINE_TABLE_NON_FOLDABLE_EXPRESSIONS_ENABLED]] is true, when the query runs (see
   * [[isRuntimeEvaluable]]).
   * This is package visible for unit testing.
   */
  def validateInputEvaluable(table: UnresolvedInlineTable): Unit = {
    val allowRuntimeEvaluation =
      conf.getConf(SQLConf.INLINE_TABLE_NON_FOLDABLE_EXPRESSIONS_ENABLED)
    table.rows.foreach { row =>
      row.foreach { e =>
        if (!canEvalDuringAnalysis(e) &&
            !(allowRuntimeEvaluation && e.resolved && isRuntimeEvaluable(e))) {
          e.failAnalysis(
            errorClass = "INVALID_INLINE_TABLE.CANNOT_EVALUATE_EXPRESSION_IN_INLINE_TABLE",
            messageParameters = Map("expr" -> toSQLExpr(e)))
        }
      }
    }
  }

  /**
   * Returns whether the resolved expression can be evaluated by itself when the query runs.
   * Aggregate, window and generator functions are not meaningful in a VALUES list. Outer
   * references and subqueries other than uncorrelated scalar subqueries are not supported, and
   * neither are the remaining unevaluable expressions, such as SQL and Python UDFs, which would
   * have to be rewritten into separate operators.
   */
  private def isRuntimeEvaluable(e: Expression): Boolean = {
    !trimAliases(prepareForEval(e)).exists {
      case _: AggregateExpression | _: AggregateFunction | _: WindowExpression |
           _: WindowFunction | _: Generator | _: OuterReference => true
      case s: ScalarSubquery => s.isCorrelated
      case _: SubqueryExpression => true
      // Replaced with literals at the end of the analysis phase.
      case l: LeafExpression if l.containsPattern(CURRENT_LIKE) => false
      // Evaluated by the enclosing `With`.
      case _: CommonExpressionDef => false
      case _: Unevaluable => true
      case _ => false
    }
  }

  /**
   * This function attempts to coerce inputs into consistent types.
   *
   * This is package visible for unit testing.
   */
  def findCommonTypesAndCast(
      table: UnresolvedInlineTable,
      ignoreCollation: Boolean = false): ResolvedInlineTable = {
    // For each column, traverse all the values and find a common data type and nullability.
    val (fields, columns) = table.rows.transpose.zip(table.names).map { case (column, name) =>
      val inputTypes = column.map(_.dataType)
      // In INSERT INTO ... VALUES, collations are ignored because the target table's column
      // collation takes precedence - the INSERT coercion will cast each value to the target
      // column's type, including collation.
      val strippedInputTypes = if (ignoreCollation) {
        inputTypes.map(_.transformRecursively {
          case st: StringType => StringHelper.removeCollation(st)
        })
      } else {
        inputTypes
      }
      val tpe = TypeCoercion.findWiderTypeWithoutStringPromotion(strippedInputTypes).getOrElse {
        table.failAnalysis(
          errorClass = "INVALID_INLINE_TABLE.INCOMPATIBLE_TYPES_IN_INLINE_TABLE",
          messageParameters = Map("colName" -> toSQLId(name)))
      }
      val newColumn = column.map {
        case expr if DataTypeUtils.sameType(expr.dataType, tpe) =>
          expr
        case expr =>
          cast(expr, tpe)
      }
      (StructField(name, tpe, nullable = column.exists(_.nullable)), newColumn)
    }.unzip
    assert(fields.size == table.names.size)
    val attributes = DataTypeUtils.toAttributes(StructType(fields))
    val castedRows: Seq[Seq[Expression]] = columns.transpose

    ResolvedInlineTable(castedRows, attributes)
  }
}
