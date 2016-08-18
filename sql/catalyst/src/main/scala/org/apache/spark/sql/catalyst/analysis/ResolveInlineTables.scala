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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Cast, InterpretedProjection, Unevaluable}
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.types.{StructField, StructType}

/**
 * An analyzer rule that replaces [[UnresolvedInlineTable]] with [[LocalRelation]].
 */
object ResolveInlineTables extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
    case table: UnresolvedInlineTable if table.expressionsResolved =>
      validateInputDimension(table)
      validateInputEvaluable(table)
      convert(table)
  }

  /**
   * Validates that all inline table data are foldable expressions.
   *
   * This is package visible for unit testing.
   */
  private[analysis] def validateInputEvaluable(table: UnresolvedInlineTable): Unit = {
    table.rows.foreach { row =>
      row.foreach { e =>
        if (!e.resolved || e.isInstanceOf[Unevaluable]) {
          e.failAnalysis(s"cannot evaluate expression ${e.sql} in inline table definition")
        }
      }
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
      val numCols = table.rows.head.size
      table.rows.zipWithIndex.foreach { case (row, ri) =>
        if (row.size != numCols) {
          table.failAnalysis(s"expected $numCols columns but found ${row.size} columns in row $ri")
        }
      }

      if (table.names.size != numCols) {
        table.failAnalysis(s"expected ${table.names.size} columns but found $numCols in first row")
      }
    }
  }

  /**
   * Convert a valid (with right shape and foldable inputs) [[UnresolvedInlineTable]]
   * into a [[LocalRelation]].
   *
   * This function attempts to coerce inputs into consistent types.
   *
   * This is package visible for unit testing.
   */
  private[analysis] def convert(table: UnresolvedInlineTable): LocalRelation = {
    val numCols = table.rows.head.size

    // For each column, traverse all the values and find a common data type.
    val targetTypes = table.rows.transpose.zip(table.names).map { case (column, name) =>
      val inputTypes = column.map(_.dataType)
      TypeCoercion.findWiderTypeWithoutStringPromotion(inputTypes).getOrElse {
        table.failAnalysis(s"incompatible types found in column $name for inline table")
      }
    }
    assert(targetTypes.size == table.names.size)

    val newRows: Seq[InternalRow] = table.rows.map { row =>
      new InterpretedProjection(row.zipWithIndex.map { case (e, ci) =>
        val targetType = targetTypes(ci)
        if (e.dataType.sameType(targetType)) e else Cast(e, targetType)
      }).apply(null)
    }

    val attributes = StructType(targetTypes.zip(table.names)
      .map { case (typ, name) => StructField(name, typ) }).toAttributes
    LocalRelation(attributes, newRows)
  }
}
