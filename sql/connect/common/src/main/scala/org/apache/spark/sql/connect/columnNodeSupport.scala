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
package org.apache.spark.sql.connect

import scala.jdk.CollectionConverters._

import org.apache.spark.SparkException
import org.apache.spark.connect.proto
import org.apache.spark.connect.proto.Expression
import org.apache.spark.connect.proto.Expression.SortOrder.NullOrdering.{SORT_NULLS_FIRST, SORT_NULLS_LAST}
import org.apache.spark.connect.proto.Expression.SortOrder.SortDirection.{SORT_DIRECTION_ASCENDING, SORT_DIRECTION_DESCENDING}
import org.apache.spark.connect.proto.Expression.Window.WindowFrame.{FrameBoundary, FrameType}
import org.apache.spark.sql.{functions, Column, Encoder}
import org.apache.spark.sql.catalyst.trees.{CurrentOrigin, Origin}
import org.apache.spark.sql.connect.common.DataTypeProtoConverter
import org.apache.spark.sql.connect.common.LiteralValueProtoConverter.toLiteralProtoBuilder
import org.apache.spark.sql.expressions.{Aggregator, UserDefinedAggregator, UserDefinedFunction}
import org.apache.spark.sql.internal.{Alias, CaseWhenOtherwise, Cast, ColumnNode, ColumnNodeLike, InvokeInlineUserDefinedFunction, LambdaFunction, LazyExpression, Literal, SortOrder, SqlExpression, UnresolvedAttribute, UnresolvedExtractValue, UnresolvedFunction, UnresolvedNamedLambdaVariable, UnresolvedRegex, UnresolvedStar, UpdateFields, Window, WindowFrame}

/**
 * Converter for [[ColumnNode]] to [[proto.Expression]] conversions.
 */
object ColumnNodeToProtoConverter extends (ColumnNode => proto.Expression) {
  def toExpr(column: Column): proto.Expression = apply(column.node, None)

  def toLiteral(v: Any): proto.Expression = apply(functions.lit(v).node, None)

  def toTypedExpr[I](column: Column, encoder: Encoder[I]): proto.Expression = {
    apply(column.node, Option(encoder))
  }

  override def apply(node: ColumnNode): Expression = apply(node, None)

  private def apply(node: ColumnNode, e: Option[Encoder[_]]): proto.Expression = {
    val builder = proto.Expression.newBuilder()
    node match {
      case Literal(value, None, _) =>
        builder.setLiteral(toLiteralProtoBuilder(value))

      case Literal(value, Some(dataType), _) =>
        builder.setLiteral(toLiteralProtoBuilder(value, dataType))

      case u @ UnresolvedAttribute(unparsedIdentifier, planId, isMetadataColumn, _) =>
        val escapedName = u.sql
        val b = builder.getUnresolvedAttributeBuilder
          .setUnparsedIdentifier(escapedName)
        if (isMetadataColumn) {
          // We only set this field when it is needed. If we would always set it,
          // too many of the verbatims we use for testing would have to be regenerated.
          b.setIsMetadataColumn(true)
        }
        planId.foreach(b.setPlanId)

      case UnresolvedStar(unparsedTarget, planId, _) =>
        val b = builder.getUnresolvedStarBuilder
        unparsedTarget.foreach(b.setUnparsedTarget)
        planId.foreach(b.setPlanId)

      case UnresolvedRegex(regex, planId, _) =>
        val b = builder.getUnresolvedRegexBuilder
          .setColName(regex)
        planId.foreach(b.setPlanId)

      case UnresolvedFunction(
            functionName,
            arguments,
            isDistinct,
            isUserDefinedFunction,
            isInternal,
            _) =>
        builder.getUnresolvedFunctionBuilder
          .setFunctionName(functionName)
          .setIsUserDefinedFunction(isUserDefinedFunction)
          .setIsDistinct(isDistinct)
          .addAllArguments(arguments.map(apply(_, e)).asJava)
          .setIsInternal(isInternal)

      case Alias(child, name, metadata, _) =>
        val b = builder.getAliasBuilder.setExpr(apply(child, e))
        name.foreach(b.addName)
        metadata.foreach(m => b.setMetadata(m.json))

      case Cast(child, dataType, evalMode, _) =>
        val b = builder.getCastBuilder
          .setExpr(apply(child, e))
          .setType(DataTypeProtoConverter.toConnectProtoType(dataType))
        evalMode.foreach { mode =>
          val convertedMode = mode match {
            case Cast.Try => proto.Expression.Cast.EvalMode.EVAL_MODE_TRY
            case Cast.Ansi => proto.Expression.Cast.EvalMode.EVAL_MODE_ANSI
            case Cast.Legacy => proto.Expression.Cast.EvalMode.EVAL_MODE_LEGACY
          }
          b.setEvalMode(convertedMode)
        }

      case SqlExpression(expression, _) =>
        builder.getExpressionStringBuilder.setExpression(expression)

      case s: SortOrder =>
        builder.setSortOrder(convertSortOrder(s, e))

      case Window(windowFunction, windowSpec, _) =>
        val b = builder.getWindowBuilder
          .setWindowFunction(apply(windowFunction, e))
          .addAllPartitionSpec(windowSpec.partitionColumns.map(apply(_, e)).asJava)
          .addAllOrderSpec(windowSpec.sortColumns.map(convertSortOrder(_, e)).asJava)
        windowSpec.frame.foreach { frame =>
          b.getFrameSpecBuilder
            .setFrameType(frame.frameType match {
              case WindowFrame.Row => FrameType.FRAME_TYPE_ROW
              case WindowFrame.Range => FrameType.FRAME_TYPE_RANGE
            })
            .setLower(convertFrameBoundary(frame.lower, e))
            .setUpper(convertFrameBoundary(frame.upper, e))
        }

      case UnresolvedExtractValue(child, extraction, _) =>
        builder.getUnresolvedExtractValueBuilder
          .setChild(apply(child, e))
          .setExtraction(apply(extraction, e))

      case UpdateFields(structExpression, fieldName, valueExpression, _) =>
        val b = builder.getUpdateFieldsBuilder
          .setStructExpression(apply(structExpression, e))
          .setFieldName(fieldName)
        valueExpression.foreach(v => b.setValueExpression(apply(v, e)))

      case v: UnresolvedNamedLambdaVariable =>
        builder.setUnresolvedNamedLambdaVariable(convertNamedLambdaVariable(v))

      case LambdaFunction(function, arguments, _) =>
        builder.getLambdaFunctionBuilder
          .setFunction(apply(function, e))
          .addAllArguments(arguments.map(convertNamedLambdaVariable).asJava)

      case InvokeInlineUserDefinedFunction(
            a: Aggregator[Any @unchecked, Any @unchecked, Any @unchecked],
            Nil,
            false,
            _) =>
        // TODO we should probably 'just' detect this particular scenario
        //  in the planner instead of wrapping it in a separate method.
        val protoUdf = UdfToProtoUtils.toProto(UserDefinedAggregator(a, e.get))
        builder.getTypedAggregateExpressionBuilder.setScalarScalaUdf(protoUdf.getScalarScalaUdf)

      case InvokeInlineUserDefinedFunction(udf: UserDefinedFunction, args, false, _) =>
        builder.setCommonInlineUserDefinedFunction(
          UdfToProtoUtils.toProto(udf, args.map(apply(_, e))))

      case CaseWhenOtherwise(branches, otherwise, _) =>
        val b = builder.getUnresolvedFunctionBuilder
          .setFunctionName("when")
          .setIsInternal(false)
        branches.foreach { case (condition, value) =>
          b.addArguments(apply(condition, e))
          b.addArguments(apply(value, e))
        }
        otherwise.foreach { value =>
          b.addArguments(apply(value, e))
        }

      case LazyExpression(child, _) =>
        builder.getLazyExpressionBuilder.setChild(apply(child, e))

      case SubqueryExpressionNode(relation, subqueryType, _) =>
        val b = builder.getSubqueryExpressionBuilder
        b.setSubqueryType(subqueryType match {
          case SubqueryType.SCALAR => proto.SubqueryExpression.SubqueryType.SUBQUERY_TYPE_SCALAR
          case SubqueryType.EXISTS => proto.SubqueryExpression.SubqueryType.SUBQUERY_TYPE_EXISTS
        })
        assert(relation.hasCommon && relation.getCommon.hasPlanId)
        b.setPlanId(relation.getCommon.getPlanId)

      case ProtoColumnNode(e, _) =>
        return e

      case node =>
        throw SparkException.internalError("Unsupported ColumnNode: " + node)
    }
    if (node.origin != Origin()) {
      builder.setCommon(proto.ExpressionCommon.newBuilder().setOrigin(convertOrigin(node.origin)))
    }
    builder.build()
  }

  private def convertOrigin(origin: Origin): proto.Origin = {
    val jvmOrigin = proto.JvmOrigin.newBuilder()
    origin.line.map(jvmOrigin.setLine)
    origin.startPosition.map(jvmOrigin.setStartPosition)
    origin.startIndex.map(jvmOrigin.setStartIndex)
    origin.stopIndex.map(jvmOrigin.setStopIndex)
    origin.sqlText.map(jvmOrigin.setSqlText)
    origin.objectType.map(jvmOrigin.setObjectType)
    origin.objectName.map(jvmOrigin.setObjectName)

    origin.stackTrace
      .map(_.map(convertStackTraceElement).toSeq.asJava)
      .map(jvmOrigin.addAllStackTrace)

    proto.Origin.newBuilder().setJvmOrigin(jvmOrigin).build()
  }

  private def convertStackTraceElement(stack: StackTraceElement): proto.StackTraceElement = {
    val builder = proto.StackTraceElement.newBuilder()
    Option(stack.getClassLoaderName).map(builder.setClassLoaderName)
    Option(stack.getModuleName).map(builder.setModuleName)
    Option(stack.getModuleVersion).map(builder.setModuleVersion)
    Option(stack.getClassName).map(builder.setDeclaringClass)
    Option(stack.getMethodName).map(builder.setMethodName)
    Option(stack.getFileName).map(builder.setFileName)
    Option(stack.getLineNumber).map(builder.setLineNumber)
    builder.build()
  }

  private def convertSortOrder(
      s: SortOrder,
      e: Option[Encoder[_]]): proto.Expression.SortOrder = {
    proto.Expression.SortOrder
      .newBuilder()
      .setChild(apply(s.child, e))
      .setDirection(s.sortDirection match {
        case SortOrder.Ascending => SORT_DIRECTION_ASCENDING
        case SortOrder.Descending => SORT_DIRECTION_DESCENDING
      })
      .setNullOrdering(s.nullOrdering match {
        case SortOrder.NullsFirst => SORT_NULLS_FIRST
        case SortOrder.NullsLast => SORT_NULLS_LAST
      })
      .build()
  }

  private def convertFrameBoundary(
      boundary: WindowFrame.FrameBoundary,
      e: Option[Encoder[_]]): FrameBoundary = {
    val builder = FrameBoundary.newBuilder()
    boundary match {
      case WindowFrame.UnboundedPreceding => builder.setUnbounded(true)
      case WindowFrame.UnboundedFollowing => builder.setUnbounded(true)
      case WindowFrame.CurrentRow => builder.setCurrentRow(true)
      case WindowFrame.Value(value) => builder.setValue(apply(value, e))
    }
    builder.build()
  }

  private def convertNamedLambdaVariable(
      v: UnresolvedNamedLambdaVariable): proto.Expression.UnresolvedNamedLambdaVariable = {
    proto.Expression.UnresolvedNamedLambdaVariable.newBuilder().addNameParts(v.name).build()
  }
}

case class ProtoColumnNode(
    expr: proto.Expression,
    override val origin: Origin = CurrentOrigin.get)
    extends ColumnNode {
  override def sql: String = expr.toString
  override def children: Seq[ColumnNodeLike] = Seq.empty
}

sealed trait SubqueryType

object SubqueryType {
  case object SCALAR extends SubqueryType
  case object EXISTS extends SubqueryType
}

case class SubqueryExpressionNode(
    relation: proto.Relation,
    subqueryType: SubqueryType,
    override val origin: Origin = CurrentOrigin.get)
    extends ColumnNode {
  override def sql: String = subqueryType match {
    case SubqueryType.SCALAR => s"($relation)"
    case _ => s"$subqueryType ($relation)"
  }
  override def children: Seq[ColumnNodeLike] = Seq.empty
}
