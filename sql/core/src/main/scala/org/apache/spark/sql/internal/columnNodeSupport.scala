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

import UserDefinedFunctionUtils.toScalaUDF

import org.apache.spark.SparkException
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.catalyst.{analysis, expressions, CatalystTypeConverters}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression}
import org.apache.spark.sql.catalyst.parser.{ParserInterface, ParserUtils}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.trees.{CurrentOrigin, Origin}
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.execution.aggregate.{ScalaAggregator, ScalaUDAF, TypedAggregateExpression}
import org.apache.spark.sql.execution.analysis.DetectAmbiguousSelfJoin
import org.apache.spark.sql.expressions.{Aggregator, SparkUserDefinedFunction, UserDefinedAggregateFunction, UserDefinedAggregator}

/**
 * Convert a [[ColumnNode]] into an [[Expression]].
 */
private[sql] trait ColumnNodeToExpressionConverter extends (ColumnNode => Expression) {

  protected def parser: ParserInterface
  protected def conf: SQLConf

  override def apply(node: ColumnNode): Expression = CurrentOrigin.withOrigin(node.origin) {
    node match {
      case Literal(value, Some(dataType), _) =>
        val converter = CatalystTypeConverters.createToCatalystConverter(dataType)
        expressions.Literal(converter(value), dataType)

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

      case UnresolvedFunction(functionName, arguments, isDistinct, isUDF, isInternal, _) =>
        val nameParts = if (isUDF) {
          parser.parseMultipartIdentifier(functionName)
        } else {
          Seq(functionName)
        }
        analysis.UnresolvedFunction(
          nameParts = nameParts,
          arguments = arguments.map(apply),
          isDistinct = isDistinct,
          isInternal = isInternal)

      case Alias(child, Seq(name), None, _) =>
        expressions.Alias(apply(child), name)(
          nonInheritableMetadataKeys = Seq(Dataset.DATASET_ID_KEY, Dataset.COL_POS_KEY))

      case Alias(child, Seq(name), metadata, _) =>
        expressions.Alias(apply(child), name)(explicitMetadata = metadata)

      case Alias(child, names, None, _) if names.nonEmpty =>
        analysis.MultiAlias(apply(child), names)

      case Cast(child, dataType, evalMode, _) =>
        val convertedEvalMode = evalMode match {
          case Some(Cast.Ansi) => expressions.EvalMode.ANSI
          case Some(Cast.Legacy) => expressions.EvalMode.LEGACY
          case Some(Cast.Try) => expressions.EvalMode.TRY
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
              case WindowFrame.Range => expressions.RangeFrame
              case WindowFrame.Row => expressions.RowFrame
            }
            expressions.SpecifiedWindowFrame(
              convertedFrameType,
              convertWindowFrameBoundary(lower),
              convertWindowFrameBoundary(upper))
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

      case InvokeInlineUserDefinedFunction(
          a: Aggregator[Any @unchecked, Any @unchecked, Any @unchecked], Nil, isDistinct, _) =>
        TypedAggregateExpression(a)(a.bufferEncoder, a.outputEncoder)
          .toAggregateExpression(isDistinct)

      case InvokeInlineUserDefinedFunction(
          a: UserDefinedAggregator[Any @unchecked, Any @unchecked, Any @unchecked],
          arguments, isDistinct, _) =>
        ScalaAggregator(a, arguments.map(apply)).toAggregateExpression(isDistinct)

      case InvokeInlineUserDefinedFunction(
          a: UserDefinedAggregateFunction, arguments, isDistinct, _) =>
        ScalaUDAF(udaf = a, children = arguments.map(apply)).toAggregateExpression(isDistinct)

      case InvokeInlineUserDefinedFunction(udf: SparkUserDefinedFunction, arguments, _, _) =>
        toScalaUDF(udf, arguments.map(apply))

      case ExpressionColumnNode(expression, _) =>
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
      case SortOrder.Ascending => expressions.Ascending
      case SortOrder.Descending => expressions.Descending
    }
    val nullOrdering = sortOrder.nullOrdering match {
      case SortOrder.NullsFirst => expressions.NullsFirst
      case SortOrder.NullsLast => expressions.NullsLast
    }
    expressions.SortOrder(apply(sortOrder.child), sortDirection, nullOrdering, Nil)
  }

  private def convertWindowFrameBoundary(boundary: WindowFrame.FrameBoundary): Expression = {
    boundary match {
      case WindowFrame.CurrentRow => expressions.CurrentRow
      case WindowFrame.UnboundedPreceding => expressions.UnboundedPreceding
      case WindowFrame.UnboundedFollowing => expressions.UnboundedFollowing
      case WindowFrame.Value(node) => apply(node)
    }
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

private[sql] object ColumnNodeToExpressionConverter extends ColumnNodeToExpressionConverter {
  override protected def parser: ParserInterface = {
    SparkSession.getActiveSession.map(_.sessionState.sqlParser).getOrElse {
      new SparkSqlParser()
    }
  }

  override protected def conf: SQLConf = SQLConf.get
}

/**
 * [[ColumnNode]] wrapper for an [[Expression]].
 */
private[sql] case class ExpressionColumnNode(
    expression: Expression,
    override val origin: Origin = CurrentOrigin.get) extends ColumnNode {
  override def normalize(): ExpressionColumnNode = {
    val updated = expression.transform {
      case a: AttributeReference =>
        DetectAmbiguousSelfJoin.stripColumnReferenceMetadata(a)
    }
    copy(updated, ColumnNode.NO_ORIGIN)
  }

  override def sql: String = expression.sql
}
