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
package org.apache.spark.sql.catalyst

import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.expressions.{AliasHelper, EvalHelper, Expression}
import org.apache.spark.sql.catalyst.optimizer.EvalInlineTables
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.trees.TreePattern.CURRENT_LIKE
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.catalyst.util.TypeUtils.{toSQLExpr, toSQLId}
import org.apache.spark.sql.types.{StructField, StructType}

/**
 * Utility object used to replace [[UnresolvedInlineTable]] with [[ResolvedInlineTable]] or
 * (whenever possible) with [[LocalRelation]]. Typically, [[UnresolvedInlineTable]] is
 * a child of [[InsertIntoStatement]].
 * Use the method: [[EvaluateUnresolvedInlineTable.evaluate]] as the entry point for this
 * transformation.
 */
object EvaluateUnresolvedInlineTable extends SQLConfHelper
  with AliasHelper with EvalHelper with CastSupport {

  def evaluate(plan: UnresolvedInlineTable): LogicalPlan =
    if (plan.expressionsResolved) evaluateUnresolvedInlineTable(plan) else plan

  def evaluateUnresolvedInlineTable(table: UnresolvedInlineTable): LogicalPlan = {
    validateInputDimension(table)
    validateInputEvaluable(table)
    val resolvedTable = findCommonTypesAndCast(table)
    earlyEvalIfPossible(resolvedTable)
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
   * Validates that all inline table data are valid expressions that can be evaluated
   * (in this they must be foldable).
   * Note that nondeterministic expressions are not supported since they are not foldable.
   * Exception are CURRENT_LIKE expressions, which are replaced by a literal in later stages.
   * This is package visible for unit testing.
   */
  def validateInputEvaluable(table: UnresolvedInlineTable): Unit = {
    table.rows.foreach { row =>
      row.foreach { e =>
        if (e.containsPattern(CURRENT_LIKE)) {
          // Do nothing.
        } else if (!e.resolved || !trimAliases(prepareForEval(e)).foldable) {
          e.failAnalysis(
            errorClass = "INVALID_INLINE_TABLE.CANNOT_EVALUATE_EXPRESSION_IN_INLINE_TABLE",
            messageParameters = Map("expr" -> toSQLExpr(e)))
        }
      }
    }
  }

  /**
   * This function attempts to coerce inputs into consistent types.
   *
   * This is package visible for unit testing.
   */
  def findCommonTypesAndCast(table: UnresolvedInlineTable): ResolvedInlineTable = {
    // For each column, traverse all the values and find a common data type and nullability.
    val (fields, columns) = table.rows.transpose.zip(table.names).map { case (column, name) =>
      val inputTypes = column.map(_.dataType)
      val tpe = TypeCoercion.findWiderTypeWithoutStringPromotion(inputTypes).getOrElse {
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
