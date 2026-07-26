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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions.{AnalysisAwareExpression, AttributeReference, Cast, Expression, UnaryExpression, Unevaluable}
import org.apache.spark.sql.catalyst.trees.TreePattern.{ANALYSIS_AWARE_EXPRESSION, PLAN_EXPRESSION, TreePattern, VARIABLE_REFERENCE}
import org.apache.spark.sql.catalyst.util.V2ExpressionBuilder
import org.apache.spark.sql.connector.catalog.GenerationExpression
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, TimeType}
import org.apache.spark.sql.util.SchemaUtils

/**
 * A wrapper expression to hold the generation expression and its original SQL text.
 */
case class GeneratedColumnExpression(
    child: Expression,
    originalSQL: String,
    analyzedChild: Option[Expression] = None)
  extends UnaryExpression
  with Unevaluable
  with AnalysisAwareExpression[GeneratedColumnExpression] {

  final override val nodePatterns: Seq[TreePattern] = Seq(ANALYSIS_AWARE_EXPRESSION)

  override def dataType: DataType = child.dataType

  override def stringArgs: Iterator[Any] = Iterator(child, originalSQL)

  override def markAsAnalyzed(): GeneratedColumnExpression =
    copy(analyzedChild = Some(child))

  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)

  /**
   * Validate the generation expression and throw an AnalysisException if invalid.
   * Validations include:
   * - The expression cannot reference itself
   * - The expression cannot reference other generated columns
   * - The expression must be deterministic
   * - The expression data type can be safely up-cast to the destination column data type
   * - No subquery expressions
   * - No references to session (temporary) variables
   * - No non-UTF8 binary collation
   */
  def validate(
      fieldName: String,
      targetDataType: DataType,
      allColumns: Seq[ColumnDefinition]): Unit = {
    def unsupportedExpressionError(reason: String): AnalysisException = {
      new AnalysisException(
        errorClass = "UNSUPPORTED_EXPRESSION_GENERATED_COLUMN",
        messageParameters = Map(
          "fieldName" -> fieldName,
          "expressionStr" -> originalSQL,
          "reason" -> reason))
    }

    // Don't allow subquery expressions
    if (child.containsPattern(PLAN_EXPRESSION)) {
      throw unsupportedExpressionError("subquery expressions are not allowed for generated columns")
    }

    // Don't allow references to session (temporary) variables. They are session-scoped mutable
    // state, so persisting them into a generation expression would be ill-defined.
    if (child.containsPattern(VARIABLE_REFERENCE)) {
      throw unsupportedExpressionError(
        "generation expression cannot reference temporary variables")
    }

    // Use the resolver to respect case sensitivity settings
    val resolver = SQLConf.get.resolver

    // Check for self-reference - the expression cannot reference itself
    val referencedColumns = child.collect {
      case a: AttributeReference => a.name
    }
    if (referencedColumns.exists(resolver(_, fieldName))) {
      throw unsupportedExpressionError("generation expression cannot reference itself")
    }

    // Check for references to other generated columns
    val generatedColumnNames = allColumns
      .filter(col => col.generationExpression.isDefined && !resolver(col.name, fieldName))
      .map(_.name)
    if (referencedColumns.exists(ref => generatedColumnNames.exists(resolver(ref, _)))) {
      throw unsupportedExpressionError(
        "generation expression cannot reference another generated column")
    }

    if (!child.deterministic) {
      throw unsupportedExpressionError("generation expression is not deterministic")
    }

    if (targetDataType.existsRecursively(_.isInstanceOf[TimeType]) ||
        child.exists(_.dataType.existsRecursively(_.isInstanceOf[TimeType]))) {
      throw unsupportedExpressionError("TIME type is not supported in generated columns")
    }

    if (!Cast.canUpCast(child.dataType, targetDataType)) {
      throw unsupportedExpressionError(
        s"generation expression data type ${child.dataType.simpleString} " +
          s"is incompatible with column data type ${targetDataType.simpleString}")
    }

    if (child.exists(e => SchemaUtils.hasNonUTF8BinaryCollation(e.dataType))) {
      throw unsupportedExpressionError(
        "generation expression cannot contain non utf8 binary collated string type")
    }
  }

  // Convert the generation expression to V2 GenerationExpression. The V2 expression is built from
  // the analyzed (pre-optimization) child so that context-dependent functions such as
  // CURRENT_TIMESTAMP are not frozen into definition-time literals. When the analyzed child cannot
  // be represented as a V2 expression (e.g. CURRENT_TIMESTAMP), the V2 expression is null and the
  // connector falls back to the SQL-string form.
  def toV2: GenerationExpression = {
    val v2Expr = analyzedChild.flatMap(new V2ExpressionBuilder(_).build()).orNull
    new GenerationExpression(originalSQL, v2Expr)
  }
}
