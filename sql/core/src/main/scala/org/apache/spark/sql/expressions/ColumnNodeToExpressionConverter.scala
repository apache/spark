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
package org.apache.spark.sql.expressions

import org.apache.spark.SparkException
import org.apache.spark.sql.{column, SparkSession}
import org.apache.spark.sql.catalyst.analysis.{MultiAlias, UnresolvedAttribute, UnresolvedDataFrameStar, UnresolvedExtractValue, UnresolvedFunction, UnresolvedRegex, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.{Alias, Ascending, CaseWhen, Cast, CurrentRow, Descending, EvalMode, Expression, LambdaFunction, Literal, NullsFirst, NullsLast, RangeFrame, RowFrame, SortOrder, SpecifiedWindowFrame, UnboundedFollowing, UnboundedPreceding, UnresolvedNamedLambdaVariable, UnspecifiedFrame, UpdateFields, WindowExpression, WindowSpecDefinition}
import org.apache.spark.sql.catalyst.parser.{ParserInterface, ParserUtils}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.trees.CurrentOrigin
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.internal.SQLConf

/**
 * Convert a [[column.ColumnNode]] into an [[Expression]].
 */
private[sql] trait ColumnNodeToExpressionConverter extends (column.ColumnNode => Expression) {

  protected def parser: ParserInterface
  protected def conf: SQLConf

  override def apply(node: column.ColumnNode): Expression = CurrentOrigin.withOrigin(node.origin) {
    node match {
      case column.Literal(value, Some(dataType), _) =>
        Literal.create(value, dataType)

      case column.Literal(value, None, _) =>
        Literal(value)

      case column.UnresolvedAttribute(unparsedIdentifier, planId, isMetadataColumn, _) =>
        convertUnresolvedAttribute(unparsedIdentifier, planId, isMetadataColumn)

      case column.UnresolvedStar(unparsedTarget, None, _) =>
        UnresolvedStar(unparsedTarget.map(UnresolvedAttribute.parseAttributeName))

      case column.UnresolvedStar(None, Some(planId), _) =>
        UnresolvedDataFrameStar(planId)

      case column.UnresolvedRegex(ParserUtils.escapedIdentifier(columnNameRegex), _, _) =>
        UnresolvedRegex(columnNameRegex, None, conf.caseSensitiveAnalysis)

      case column.UnresolvedRegex(
          ParserUtils.qualifiedEscapedIdentifier(nameParts, columnNameRegex), _, _) =>
        UnresolvedRegex(columnNameRegex, Some(nameParts), conf.caseSensitiveAnalysis)

      case column.UnresolvedRegex(unparsedIdentifier, planId, _) =>
        convertUnresolvedAttribute(unparsedIdentifier, planId, isMetadataColumn = false)

      case column.UnresolvedFunction(functionName, arguments, isDistinct, isUDF, _) =>
        val nameParts = if (isUDF) {
          parser.parseMultipartIdentifier(functionName)
        } else {
          Seq(functionName)
        }
        UnresolvedFunction(nameParts, arguments.map(apply), isDistinct)

      case column.Alias(child, Seq(name), metadata, _) =>
        Alias(apply(child), name)(explicitMetadata = metadata)

      case column.Alias(child, names, None, _) if names.nonEmpty =>
        MultiAlias(apply(child), names)

      case column.Cast(child, dataType, evalMode, _) =>
        val convertedEvalMode = evalMode match {
          case Some(column.Cast.EvalMode.Ansi) => EvalMode.ANSI
          case Some(column.Cast.EvalMode.Legacy) => EvalMode.LEGACY
          case Some(column.Cast.EvalMode.Try) => EvalMode.TRY
          case _ => EvalMode.fromSQLConf(conf)
        }
        val cast = Cast(
          apply(child),
          CharVarcharUtils.replaceCharVarcharWithStringForCast(dataType),
          None,
          convertedEvalMode)
        cast.setTagValue(Cast.USER_SPECIFIED_CAST, ())
        cast

      case column.SqlExpression(expression, _) =>
        parser.parseExpression(expression)

      case sortOrder: column.SortOrder =>
        convertSortOrder(sortOrder)

      case column.Window(function, spec, _) =>
        val frame = spec.frame match {
          case Some(column.WindowFrame(frameType, lower, upper)) =>
            val convertedFrameType = frameType match {
              case column.WindowFrame.FrameType.Range => RangeFrame
              case column.WindowFrame.FrameType.Row => RowFrame
            }
            val convertedLower = lower match {
              case column.WindowFrame.CurrentRow => CurrentRow
              case column.WindowFrame.Unbounded => UnboundedPreceding
              case column.WindowFrame.Value(node) => apply(node)
            }
            val convertedUpper = upper match {
              case column.WindowFrame.CurrentRow => CurrentRow
              case column.WindowFrame.Unbounded => UnboundedFollowing
              case column.WindowFrame.Value(node) => apply(node)
            }
            SpecifiedWindowFrame(convertedFrameType, convertedLower, convertedUpper)
          case None =>
            UnspecifiedFrame
        }
        WindowExpression(
          apply(function),
          WindowSpecDefinition(
            partitionSpec = spec.partitionColumns.map(apply),
            orderSpec = spec.sortColumns.map(convertSortOrder),
            frameSpecification = frame))

      case column.LambdaFunction(function, arguments, _) =>
        LambdaFunction(
          apply(function),
          arguments.map(convertUnresolvedNamedLambdaVariable))

      case v: column.UnresolvedNamedLambdaVariable =>
        convertUnresolvedNamedLambdaVariable(v)

      case column.UnresolvedExtractValue(child, extraction, _) =>
        UnresolvedExtractValue(apply(child), apply(extraction))

      case column.UpdateFields(struct, field, Some(value), _) =>
        UpdateFields(apply(struct), field, apply(value))

      case column.UpdateFields(struct, field, None, _) =>
        UpdateFields(apply(struct), field)

      case column.CaseWhenOtherwise(branches, otherwise, _) =>
        CaseWhen(
          branches = branches.map { case (condition, value) =>
            (apply(condition), apply(value))
          },
          elseValue = otherwise.map(apply))

      case column.Extension(expression: Expression, _) =>
        expression

      case node =>
        throw SparkException.internalError("Unsupported ColumnNode: " + node)
    }
  }

  private def convertUnresolvedNamedLambdaVariable(
      v: column.UnresolvedNamedLambdaVariable): UnresolvedNamedLambdaVariable = {
    UnresolvedNamedLambdaVariable(Seq(v.name))
  }

  private def convertSortOrder(sortOrder: column.SortOrder): SortOrder = {
    val sortDirection = sortOrder.sortDirection match {
      case column.SortOrder.SortDirection.Ascending => Ascending
      case column.SortOrder.SortDirection.Descending => Descending
    }
    val nullOrdering = sortOrder.nullOrdering match {
      case column.SortOrder.NullOrdering.NullsFirst => NullsFirst
      case column.SortOrder.NullOrdering.NullsLast => NullsLast
    }
    SortOrder(apply(sortOrder.child), sortDirection, nullOrdering, Nil)
  }

  private def convertUnresolvedAttribute(
      unparsedIdentifier: String,
      planId: Option[Long],
      isMetadataColumn: Boolean): UnresolvedAttribute = {
    val attribute = UnresolvedAttribute.quotedString(unparsedIdentifier)
    if (planId.isDefined) {
      attribute.setTagValue(LogicalPlan.PLAN_ID_TAG, planId.get)
    }
    if (isMetadataColumn) {
      attribute.setTagValue(LogicalPlan.IS_METADATA_COL, ())
    }
    attribute
  }
}

object ColumnNodeToExpressionConverter extends ColumnNodeToExpressionConverter {
  override protected def parser: ParserInterface = {
    SparkSession.getActiveSession.map(_.sessionState.sqlParser).getOrElse {
      new SparkSqlParser()
    }
  }

  override protected def conf: SQLConf = SQLConf.get
}
