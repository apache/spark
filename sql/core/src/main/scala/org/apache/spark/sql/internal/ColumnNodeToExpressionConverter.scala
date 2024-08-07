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
package org.apache.spark.sql.internal

import org.apache.spark.SparkException
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders.UnboundEncoder
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.expressions.{Expression, ScalaUDF}
import org.apache.spark.sql.catalyst.parser.{ParserInterface, ParserUtils}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.trees.CurrentOrigin
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.execution.aggregate.{ScalaAggregator, TypedAggregateExpression}
import org.apache.spark.sql.expressions.Aggregator

/**
 * Convert a [[ColumnNode]] into an [[Expression]].
 */
private[sql] trait ColumnNodeToExpressionConverter extends (ColumnNode => Expression) {

  protected def parser: ParserInterface
  protected def conf: SQLConf

  override def apply(node: ColumnNode): Expression = CurrentOrigin.withOrigin(node.origin) {
    node match {
      case Literal(value, Some(dataType), _) =>
        expressions.Literal.create(value, dataType)

      case Literal(value, None, _) =>
        expressions.Literal(value)

      case UnresolvedAttribute(unparsedIdentifier, planId, isMetadataColumn, _) =>
        convertUnresolvedAttribute(unparsedIdentifier, planId, isMetadataColumn)

      case UnresolvedStar(unparsedTarget, None, _) =>
        analysis.UnresolvedStar(unparsedTarget.map(analysis.UnresolvedAttribute.parseAttributeName))

      case UnresolvedStar(None, Some(planId), _) =>
        analysis.UnresolvedDataFrameStar(planId)

      case UnresolvedRegex(ParserUtils.escapedIdentifier(columnNameRegex), _, _) =>
        analysis.UnresolvedRegex(columnNameRegex, None, conf.caseSensitiveAnalysis)

      case UnresolvedRegex(
          ParserUtils.qualifiedEscapedIdentifier(nameParts, columnNameRegex), _, _) =>
        analysis.UnresolvedRegex(columnNameRegex, Some(nameParts), conf.caseSensitiveAnalysis)

      case UnresolvedRegex(unparsedIdentifier, planId, _) =>
        convertUnresolvedAttribute(unparsedIdentifier, planId, isMetadataColumn = false)

      case UnresolvedFunction(functionName, arguments, isDistinct, isUDF, _) =>
        val nameParts = if (isUDF) {
          parser.parseMultipartIdentifier(functionName)
        } else {
          Seq(functionName)
        }
        analysis.UnresolvedFunction(nameParts, arguments.map(apply), isDistinct)

      case Alias(child, Seq(name), metadata, _) =>
        expressions.Alias(apply(child), name)(explicitMetadata = metadata)

      case Alias(child, names, None, _) if names.nonEmpty =>
        analysis.MultiAlias(apply(child), names)

      case Cast(child, dataType, evalMode, _) =>
        val convertedEvalMode = evalMode match {
          case Some(Cast.EvalMode.Ansi) => expressions.EvalMode.ANSI
          case Some(Cast.EvalMode.Legacy) => expressions.EvalMode.LEGACY
          case Some(Cast.EvalMode.Try) => expressions.EvalMode.TRY
          case _ => expressions.EvalMode.fromSQLConf(conf)
        }
        val cast = expressions.Cast(
          apply(child),
          CharVarcharUtils.replaceCharVarcharWithStringForCast(dataType),
          None,
          convertedEvalMode)
        cast.setTagValue(expressions.Cast.USER_SPECIFIED_CAST, ())
        cast

      case SqlExpression(expression, _) =>
        parser.parseExpression(expression)

      case sortOrder: SortOrder =>
        convertSortOrder(sortOrder)

      case Window(function, spec, _) =>
        val frame = spec.frame match {
          case Some(WindowFrame(frameType, lower, upper)) =>
            val convertedFrameType = frameType match {
              case WindowFrame.FrameType.Range => expressions.RangeFrame
              case WindowFrame.FrameType.Row => expressions.RowFrame
            }
            val convertedLower = lower match {
              case WindowFrame.CurrentRow => expressions.CurrentRow
              case WindowFrame.Unbounded => expressions.UnboundedPreceding
              case WindowFrame.Value(node) => apply(node)
            }
            val convertedUpper = upper match {
              case WindowFrame.CurrentRow => expressions.CurrentRow
              case WindowFrame.Unbounded => expressions.UnboundedFollowing
              case WindowFrame.Value(node) => apply(node)
            }
            expressions.SpecifiedWindowFrame(convertedFrameType, convertedLower, convertedUpper)
          case None =>
            expressions.UnspecifiedFrame
        }
        expressions.WindowExpression(
          apply(function),
          expressions.WindowSpecDefinition(
            partitionSpec = spec.partitionColumns.map(apply),
            orderSpec = spec.sortColumns.map(convertSortOrder),
            frameSpecification = frame))

      case LambdaFunction(function, arguments, _) =>
        expressions.LambdaFunction(
          apply(function),
          arguments.map(convertUnresolvedNamedLambdaVariable))

      case v: UnresolvedNamedLambdaVariable =>
        convertUnresolvedNamedLambdaVariable(v)

      case UnresolvedExtractValue(child, extraction, _) =>
        analysis.UnresolvedExtractValue(apply(child), apply(extraction))

      case UpdateFields(struct, field, Some(value), _) =>
        expressions.UpdateFields(apply(struct), field, apply(value))

      case UpdateFields(struct, field, None, _) =>
        expressions.UpdateFields(apply(struct), field)

      case CaseWhenOtherwise(branches, otherwise, _) =>
        expressions.CaseWhen(
          branches = branches.map { case (condition, value) =>
            (apply(condition), apply(value))
          },
          elseValue = otherwise.map(apply))

      case InvokeInlineUserDefinedFunction(f, arguments, _) =>
        // This code is a bit clunky, it will stay this way until we have moved everything to
        // sql/api and we can actually use the SparkUserDefinedFunction and
        // UserDefinedAggregator classes.
        (f.function, arguments.map(apply)) match {
          case (a: Aggregator[Any @unchecked, Any @unchecked, Any @unchecked], Nil) =>
            TypedAggregateExpression(a)(a.bufferEncoder, a.outputEncoder).toAggregateExpression()

          case (a: Aggregator[Any @unchecked, Any  @unchecked, Any  @unchecked], children) =>
            ScalaAggregator(
              agg = a,
              children = children,
              inputEncoder = ExpressionEncoder(f.inputEncoders.head),
              bufferEncoder = ExpressionEncoder(f.resultEncoder),
              aggregatorName = f.name,
              nullable = !f.nonNullable && f.resultEncoder.nullable,
              isDeterministic = f.deterministic).toAggregateExpression()

          case (function, children) =>
            ScalaUDF(
              function = function,
              dataType = f.resultEncoder.dataType,
              children = children,
              inputEncoders = f.inputEncoders.map {
                case UnboundEncoder => None
                case encoder => Option(ExpressionEncoder(encoder))
              },
              outputEncoder = Option(ExpressionEncoder(f.resultEncoder)),
              udfName = f.name,
              nullable = !f.nonNullable && f.resultEncoder.nullable,
              udfDeterministic = f.deterministic)
        }

      case Extension(expression: Expression, _) =>
        expression

      case node =>
        throw SparkException.internalError("Unsupported ColumnNode: " + node)
    }
  }

  private def convertUnresolvedNamedLambdaVariable(
      v: UnresolvedNamedLambdaVariable): expressions.UnresolvedNamedLambdaVariable = {
    expressions.UnresolvedNamedLambdaVariable(Seq(v.name))
  }

  private def convertSortOrder(sortOrder: SortOrder): expressions.SortOrder = {
    val sortDirection = sortOrder.sortDirection match {
      case SortOrder.SortDirection.Ascending => expressions.Ascending
      case SortOrder.SortDirection.Descending => expressions.Descending
    }
    val nullOrdering = sortOrder.nullOrdering match {
      case SortOrder.NullOrdering.NullsFirst => expressions.NullsFirst
      case SortOrder.NullOrdering.NullsLast => expressions.NullsLast
    }
    expressions.SortOrder(apply(sortOrder.child), sortDirection, nullOrdering, Nil)
  }

  private def convertUnresolvedAttribute(
      unparsedIdentifier: String,
      planId: Option[Long],
      isMetadataColumn: Boolean): analysis.UnresolvedAttribute = {
    val attribute = analysis.UnresolvedAttribute.quotedString(unparsedIdentifier)
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
