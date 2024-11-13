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

import scala.language.implicitConversions

import UserDefinedFunctionUtils.toScalaUDF

import org.apache.spark.SparkException
import org.apache.spark.sql.{Column, Dataset, SparkSession}
import org.apache.spark.sql.catalyst.{analysis, expressions, CatalystTypeConverters}
import org.apache.spark.sql.catalyst.analysis.{MultiAlias, UnresolvedAlias}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression, Generator, NamedExpression, Unevaluable}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, AggregateFunction}
import org.apache.spark.sql.catalyst.parser.{ParserInterface, ParserUtils}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.trees.{CurrentOrigin, Origin}
import org.apache.spark.sql.catalyst.util.{toPrettySQL, CharVarcharUtils}
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.execution.aggregate.{ScalaAggregator, ScalaUDAF, TypedAggregateExpression}
import org.apache.spark.sql.execution.analysis.DetectAmbiguousSelfJoin
import org.apache.spark.sql.expressions.{Aggregator, SparkUserDefinedFunction, UserDefinedAggregateFunction, UserDefinedAggregator}
import org.apache.spark.sql.types.{DataType, NullType}

/**
 * Convert a [[ColumnNode]] into an [[Expression]].
 */
private[sql] trait ColumnNodeToExpressionConverter extends (ColumnNode => Expression) {

  protected def parser: ParserInterface
  protected def conf: SQLConf

  override def apply(node: ColumnNode): Expression = SQLConf.withExistingConf(conf) {
    CurrentOrigin.withOrigin(node.origin) {
      node match {
        case Literal(value, Some(dataType), _) =>
          val converter = CatalystTypeConverters.createToCatalystConverter(dataType)
          expressions.Literal(converter(value), dataType)

        case Literal(value, None, _) =>
          expressions.Literal(value)

        case UnresolvedAttribute(nameParts, planId, isMetadataColumn, _) =>
          convertUnresolvedAttribute(nameParts, planId, isMetadataColumn)

        case UnresolvedStar(unparsedTarget, None, _) =>
          val target = unparsedTarget.map { t =>
            analysis.UnresolvedAttribute.parseAttributeName(t.stripSuffix(".*"))
          }
          analysis.UnresolvedStar(target)

        case UnresolvedStar(None, Some(planId), _) =>
          analysis.UnresolvedDataFrameStar(planId)

        case UnresolvedRegex(ParserUtils.escapedIdentifier(columnNameRegex), _, _) =>
          analysis.UnresolvedRegex(columnNameRegex, None, conf.caseSensitiveAnalysis)

        case UnresolvedRegex(
        ParserUtils.qualifiedEscapedIdentifier(nameParts, columnNameRegex), _, _) =>
          analysis.UnresolvedRegex(columnNameRegex, Some(nameParts), conf.caseSensitiveAnalysis)

        case UnresolvedRegex(unparsedIdentifier, planId, _) =>
          convertUnresolvedRegex(unparsedIdentifier, planId)

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

        case LazyOuterReference(nameParts, planId, _) =>
          convertLazyOuterReference(nameParts, planId)

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
          val transformed = expression.transformDown {
            case ColumnNodeExpression(node) => apply(node)
          }
          transformed match {
            case f: AggregateFunction => f.toAggregateExpression()
            case _ => transformed
          }

        case node =>
          throw SparkException.internalError("Unsupported ColumnNode: " + node)
      }
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
      nameParts: Seq[String],
      planId: Option[Long],
      isMetadataColumn: Boolean): analysis.UnresolvedAttribute = {
    val attribute = analysis.UnresolvedAttribute(nameParts)
    if (planId.isDefined) {
      attribute.setTagValue(LogicalPlan.PLAN_ID_TAG, planId.get)
    }
    if (isMetadataColumn) {
      attribute.setTagValue(LogicalPlan.IS_METADATA_COL, ())
    }
    attribute
  }

  private def convertUnresolvedRegex(
      unparsedIdentifier: String,
      planId: Option[Long]): analysis.UnresolvedAttribute = {
    val attribute = analysis.UnresolvedAttribute.quotedString(unparsedIdentifier)
    if (planId.isDefined) {
      attribute.setTagValue(LogicalPlan.PLAN_ID_TAG, planId.get)
    }
    attribute
  }

  private def convertLazyOuterReference(
      nameParts: Seq[String],
      planId: Option[Long]): analysis.LazyOuterReference = {
    val lazyOuterReference = analysis.LazyOuterReference(nameParts)
    if (planId.isDefined) {
      lazyOuterReference.setTagValue(LogicalPlan.PLAN_ID_TAG, planId.get)
    }
    lazyOuterReference
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
private[sql] case class ExpressionColumnNode private(
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

private[sql] object ExpressionColumnNode {
  def apply(e: Expression): ColumnNode = e match {
    case ColumnNodeExpression(node) => node
    case _ => new ExpressionColumnNode(e)
  }
}

private[internal] case class ColumnNodeExpression private(node: ColumnNode) extends Unevaluable {
  override def nullable: Boolean = true
  override def dataType: DataType = NullType
  override def children: Seq[Expression] = Nil
  override protected def withNewChildrenInternal(c: IndexedSeq[Expression]): Expression = this
}

private[sql] object ColumnNodeExpression {
  def apply(node: ColumnNode): Expression = node match {
    case ExpressionColumnNode(e, _) => e
    case _ => new ColumnNodeExpression(node)
  }
}

private[spark] object ExpressionUtils {
  /**
   * Create an Expression backed Column.
   */
  implicit def column(e: Expression): Column = Column(ExpressionColumnNode(e))

  /**
   * Create an ColumnNode backed Expression. Please not that this has to be converted to an actual
   * Expression before it is used.
   */
  implicit def expression(c: Column): Expression = ColumnNodeExpression(c.node)

  /**
   * Returns the expression either with an existing or auto assigned name.
   */
  def toNamed(expr: Expression): NamedExpression = expr match {
    case expr: NamedExpression => expr

    // Leave an unaliased generator with an empty list of names since the analyzer will generate
    // the correct defaults after the nested expression's type has been resolved.
    case g: Generator => MultiAlias(g, Nil)

    // If we have a top level Cast, there is a chance to give it a better alias, if there is a
    // NamedExpression under this Cast.
    case c: expressions.Cast =>
      c.transformUp {
        case c @ expressions.Cast(_: NamedExpression, _, _, _) => UnresolvedAlias(c)
      } match {
        case ne: NamedExpression => ne
        case _ => UnresolvedAlias(expr, Some(generateAlias))
      }

    case expr: Expression => UnresolvedAlias(expr, Some(generateAlias))
  }

  def generateAlias(e: Expression): String = {
    e match {
      case AggregateExpression(f: TypedAggregateExpression, _, _, _, _) => f.toString
      case expr => toPrettySQL(expr)
    }
  }
}
