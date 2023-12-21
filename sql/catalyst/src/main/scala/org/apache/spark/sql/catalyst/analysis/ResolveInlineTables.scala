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

import org.apache.spark.sql.catalyst.expressions.{AliasHelper, EvalHelper, Expression}
import org.apache.spark.sql.catalyst.optimizer.EvalInlineTables
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.AlwaysProcess
import org.apache.spark.sql.catalyst.trees.TreePattern.CURRENT_LIKE
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.catalyst.util.TypeUtils.{toSQLExpr, toSQLId}
import org.apache.spark.sql.types.{StructField, StructType}

/**
 * An analyzer rule that replaces [[UnresolvedInlineTable]] with [[ResolvedInlineTable]].
 */
object ResolveInlineTables extends Rule[LogicalPlan]
  with CastSupport with AliasHelper with EvalHelper {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan.resolveOperatorsWithPruning(AlwaysProcess.fn, ruleId) {
      case table: UnresolvedInlineTable if table.expressionsResolved =>
        validateInputDimension(table)
        validateInputEvaluable(table)
        val resolvedTable = findCommonTypesAndCast(table)
        earlyEvalIfPossible(resolvedTable)
    }
  }

  /**
   * Validates the input data dimension:
   * 1. All rows have the same cardinality.
   * 2. The number of column aliases defined is consistent with the number of columns in data.
   *
   * This is package visible for unit testing.
   */
  private[analysis] def validateInputDimension(table: UnresolvedInlineTable): Unit = {
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
   *
   * This is package visible for unit testing.
   */
  private[analysis] def validateInputEvaluable(table: UnresolvedInlineTable): Unit = {
    table.rows.foreach { row =>
      row.foreach { e =>
        // Note that nondeterministic expressions are not supported since they are not foldable.
        // Only exception are CURRENT_LIKE expressions, which are replaced by a literal
        // In later stages.
        if ((!e.resolved && !e.containsPattern(CURRENT_LIKE))
          || !trimAliases(prepareForEval(e)).foldable) {
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
  private[analysis] def findCommonTypesAndCast(table: UnresolvedInlineTable):
    ResolvedInlineTable = {
    // For each column, traverse all the values and find a common data type and nullability.
    val fields = table.rows.transpose.zip(table.names).map { case (column, name) =>
      val inputTypes = column.map(_.dataType)
      val tpe = TypeCoercion.findWiderTypeWithoutStringPromotion(inputTypes).getOrElse {
        table.failAnalysis(
          errorClass = "INVALID_INLINE_TABLE.INCOMPATIBLE_TYPES_IN_INLINE_TABLE",
          messageParameters = Map("colName" -> toSQLId(name)))
      }
      StructField(name, tpe, nullable = column.exists(_.nullable))
    }
    val attributes = DataTypeUtils.toAttributes(StructType(fields))
    assert(fields.size == table.names.size)

    val castedRows: Seq[Seq[Expression]] = table.rows.map { row =>
      row.zipWithIndex.map {
        case (e, ci) =>
          val targetType = fields(ci).dataType
          val castedExpr = if (DataTypeUtils.sameType(e.dataType, targetType)) {
            e
          } else {
            cast(e, targetType)
          }
          castedExpr
      }
    }

    ResolvedInlineTable(castedRows, attributes)
  }

  /**
   * This function attempts to early evaluate rows in inline table.
   * If evaluation doesn't rely on non-deterministic expressions (e.g. current_like)
   * expressions will be evaluated and inlined as [[LocalRelation]]
   * This is package visible for unit testing.
   */
  private[analysis] def earlyEvalIfPossible(table: ResolvedInlineTable): LogicalPlan = {
      val earlyEvalPossible = table.rows.flatten.forall(!_.containsPattern(CURRENT_LIKE))
      if (earlyEvalPossible) EvalInlineTables(table) else table
  }
}
