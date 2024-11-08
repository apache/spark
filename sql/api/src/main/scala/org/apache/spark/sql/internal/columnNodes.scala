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

import java.util.concurrent.atomic.AtomicLong

import ColumnNode._

import org.apache.spark.sql.catalyst.trees.{CurrentOrigin, Origin}
import org.apache.spark.sql.catalyst.util.AttributeNameParser
import org.apache.spark.sql.errors.DataTypeErrorsBase
import org.apache.spark.sql.types.{DataType, IntegerType, LongType, Metadata}
import org.apache.spark.util.SparkClassUtils

/**
 * AST for constructing columns. This API is implementation agnostic and allows us to build a
 * single Column implementation that can be shared between implementations. Consequently a
 * Dataframe API implementations will have to provide conversions from this AST to its
 * implementation specific form (e.g. Catalyst expressions, or Connect protobuf messages).
 *
 * This API is a mirror image of Connect's expression.proto. There are a couple of extensions to
 * make constructing nodes easier (e.g. [[CaseWhenOtherwise]]). We could not use the actual
 * connect protobuf messages because of classpath clashes (e.g. Guava & gRPC) and Maven shading
 * issues.
 */
private[sql] trait ColumnNode extends ColumnNodeLike {

  /**
   * Origin where the node was created.
   */
  def origin: Origin

  /**
   * A normalized version of this node. This is stripped of dataset related (contextual) metadata.
   * This is mostly used to power Column.equals and Column.hashcode.
   */
  lazy val normalized: ColumnNode = {
    val transformed = normalize()
    if (this != transformed) {
      transformed
    } else {
      this
    }
  }

  override private[internal] def normalize(): ColumnNode = this

  /**
   * Return a SQL-a-like representation of the node.
   *
   * This is best effort; there are no guarantees that the returned SQL is valid.
   */
  def sql: String
}

trait ColumnNodeLike {
  private[internal] def normalize(): ColumnNodeLike = this
  private[internal] def sql: String
}

private[internal] object ColumnNode {
  val NO_ORIGIN: Origin = Origin()
  def normalize[T <: ColumnNodeLike](option: Option[T]): Option[T] =
    option.map(_.normalize().asInstanceOf[T])
  def normalize[T <: ColumnNodeLike](nodes: Seq[T]): Seq[T] =
    nodes.map(_.normalize().asInstanceOf[T])
  def argumentsToSql(nodes: Seq[ColumnNodeLike]): String =
    textArgumentsToSql(nodes.map(_.sql))
  def textArgumentsToSql(parts: Seq[String]): String = parts.mkString("(", ", ", ")")
  def elementsToSql(elements: Seq[ColumnNodeLike], prefix: String = ""): String = {
    if (elements.nonEmpty) {
      elements.map(_.sql).mkString(prefix, ", ", "")
    } else {
      ""
    }
  }
  def optionToSql(option: Option[ColumnNodeLike]): String = {
    option.map(_.sql).getOrElse("")
  }
}

/**
 * A literal column.
 *
 * @param value
 *   of the literal. This is the unconverted input value.
 * @param dataType
 *   of the literal. If none is provided the dataType is inferred.
 */
private[sql] case class Literal(
    value: Any,
    dataType: Option[DataType] = None,
    override val origin: Origin = CurrentOrigin.get)
    extends ColumnNode
    with DataTypeErrorsBase {
  override private[internal] def normalize(): Literal = copy(origin = NO_ORIGIN)

  override def sql: String = value match {
    case null => "NULL"
    case v: String => toSQLValue(v)
    case v: Long => toSQLValue(v)
    case v: Float => toSQLValue(v)
    case v: Double => toSQLValue(v)
    case v: Short => toSQLValue(v)
    case _ => value.toString
  }
}

/**
 * Reference to an attribute produced by one of the underlying DataFrames.
 *
 * @param nameParts
 *   name of the attribute.
 * @param planId
 *   id of the plan (Dataframe) that produces the attribute.
 * @param isMetadataColumn
 *   whether this is a metadata column.
 */
private[sql] case class UnresolvedAttribute(
    nameParts: Seq[String],
    planId: Option[Long] = None,
    isMetadataColumn: Boolean = false,
    override val origin: Origin = CurrentOrigin.get)
    extends ColumnNode {

  override private[internal] def normalize(): UnresolvedAttribute =
    copy(planId = None, origin = NO_ORIGIN)

  override def sql: String = nameParts.map(n => if (n.contains(".")) s"`$n`" else n).mkString(".")
}

private[sql] object UnresolvedAttribute {
  def apply(
      unparsedIdentifier: String,
      planId: Option[Long],
      isMetadataColumn: Boolean,
      origin: Origin): UnresolvedAttribute = UnresolvedAttribute(
    AttributeNameParser.parseAttributeName(unparsedIdentifier),
    planId = planId,
    isMetadataColumn = isMetadataColumn,
    origin = origin)

  def apply(
      unparsedIdentifier: String,
      planId: Option[Long],
      isMetadataColumn: Boolean): UnresolvedAttribute =
    apply(unparsedIdentifier, planId, isMetadataColumn, CurrentOrigin.get)

  def apply(unparsedIdentifier: String, planId: Option[Long]): UnresolvedAttribute =
    apply(unparsedIdentifier, planId, false, CurrentOrigin.get)

  def apply(unparsedIdentifier: String): UnresolvedAttribute =
    apply(unparsedIdentifier, None, false, CurrentOrigin.get)
}

/**
 * Reference to all columns in a namespace (global, a Dataframe, or a nested struct).
 *
 * @param unparsedTarget
 *   name of the namespace. None if the global namespace is supposed to be used.
 * @param planId
 *   id of the plan (Dataframe) that produces the attribute.
 */
private[sql] case class UnresolvedStar(
    unparsedTarget: Option[String],
    planId: Option[Long] = None,
    override val origin: Origin = CurrentOrigin.get)
    extends ColumnNode {
  override private[internal] def normalize(): UnresolvedStar =
    copy(planId = None, origin = NO_ORIGIN)
  override def sql: String = unparsedTarget.map(_ + ".*").getOrElse("*")
}

/**
 * Call a function. This can either be a built-in function, a UDF, or a UDF registered in the
 * Catalog.
 *
 * @param functionName
 *   of the function to invoke.
 * @param arguments
 *   to pass into the function.
 * @param isDistinct
 *   (aggregate only) whether the input of the aggregate function should be de-duplicated.
 */
private[sql] case class UnresolvedFunction(
    functionName: String,
    arguments: Seq[ColumnNode],
    isDistinct: Boolean = false,
    isUserDefinedFunction: Boolean = false,
    isInternal: Boolean = false,
    override val origin: Origin = CurrentOrigin.get)
    extends ColumnNode {
  override private[internal] def normalize(): UnresolvedFunction =
    copy(arguments = ColumnNode.normalize(arguments), origin = NO_ORIGIN)

  override def sql: String = functionName + argumentsToSql(arguments)
}

/**
 * Evaluate a SQL expression.
 *
 * @param expression
 *   text to execute.
 */
private[sql] case class SqlExpression(
    expression: String,
    override val origin: Origin = CurrentOrigin.get)
    extends ColumnNode {
  override private[internal] def normalize(): SqlExpression = copy(origin = NO_ORIGIN)
  override def sql: String = expression
}

/**
 * Name a column, and (optionally) modify its metadata.
 *
 * @param child
 *   to name
 * @param name
 *   to use
 * @param metadata
 *   (optional) metadata to add.
 */
private[sql] case class Alias(
    child: ColumnNode,
    name: Seq[String],
    metadata: Option[Metadata] = None,
    override val origin: Origin = CurrentOrigin.get)
    extends ColumnNode {
  override private[internal] def normalize(): Alias =
    copy(child = child.normalize(), origin = NO_ORIGIN)

  override def sql: String = {
    val alias = name match {
      case Seq(single) => single
      case multiple => textArgumentsToSql(multiple)
    }
    s"${child.sql} AS $alias"
  }
}

/**
 * Cast the value of a Column to a different [[DataType]]. The behavior of the cast can be
 * influenced by the `evalMode`.
 *
 * @param child
 *   that produces the input value.
 * @param dataType
 *   to cast to.
 * @param evalMode
 *   (try/ansi/legacy) to use for the cast.
 */
private[sql] case class Cast(
    child: ColumnNode,
    dataType: DataType,
    evalMode: Option[Cast.EvalMode] = None,
    override val origin: Origin = CurrentOrigin.get)
    extends ColumnNode {
  override private[internal] def normalize(): Cast =
    copy(child = child.normalize(), origin = NO_ORIGIN)

  override def sql: String = {
    s"${optionToSql(evalMode)}CAST(${child.sql} AS ${dataType.sql})"
  }
}

private[sql] object Cast {
  sealed abstract class EvalMode(override val sql: String = "") extends ColumnNodeLike
  object Legacy extends EvalMode
  object Ansi extends EvalMode
  object Try extends EvalMode("TRY_")
}

/**
 * Reference to all columns in the global namespace in that match a regex.
 *
 * @param regex
 *   name of the namespace. None if the global namespace is supposed to be used.
 * @param planId
 *   id of the plan (Dataframe) that produces the attribute.
 */
private[sql] case class UnresolvedRegex(
    regex: String,
    planId: Option[Long] = None,
    override val origin: Origin = CurrentOrigin.get)
    extends ColumnNode {
  override private[internal] def normalize(): UnresolvedRegex =
    copy(planId = None, origin = NO_ORIGIN)
  override def sql: String = regex
}

/**
 * Sort the input column.
 *
 * @param child
 *   to sort.
 * @param sortDirection
 *   to sort in, either Ascending or Descending.
 * @param nullOrdering
 *   where to place nulls, either at the begin or the end.
 */
private[sql] case class SortOrder(
    child: ColumnNode,
    sortDirection: SortOrder.SortDirection,
    nullOrdering: SortOrder.NullOrdering,
    override val origin: Origin = CurrentOrigin.get)
    extends ColumnNode {
  override private[internal] def normalize(): SortOrder =
    copy(child = child.normalize(), origin = NO_ORIGIN)

  override def sql: String = s"${child.sql} ${sortDirection.sql} ${nullOrdering.sql}"
}

private[sql] object SortOrder {
  sealed abstract class SortDirection(override val sql: String) extends ColumnNodeLike
  object Ascending extends SortDirection("ASC")
  object Descending extends SortDirection("DESC")
  sealed abstract class NullOrdering(override val sql: String) extends ColumnNodeLike
  object NullsFirst extends NullOrdering("NULLS FIRST")
  object NullsLast extends NullOrdering("NULLS LAST")
}

/**
 * Evaluate a function within a window.
 *
 * @param windowFunction
 *   function to execute.
 * @param windowSpec
 *   of the window.
 */
private[sql] case class Window(
    windowFunction: ColumnNode,
    windowSpec: WindowSpec,
    override val origin: Origin = CurrentOrigin.get)
    extends ColumnNode {
  override private[internal] def normalize(): Window = copy(
    windowFunction = windowFunction.normalize(),
    windowSpec = windowSpec.normalize(),
    origin = NO_ORIGIN)

  override def sql: String = s"${windowFunction.sql} OVER (${windowSpec.sql})"
}

private[sql] case class WindowSpec(
    partitionColumns: Seq[ColumnNode],
    sortColumns: Seq[SortOrder],
    frame: Option[WindowFrame] = None)
    extends ColumnNodeLike {
  override private[internal] def normalize(): WindowSpec = copy(
    partitionColumns = ColumnNode.normalize(partitionColumns),
    sortColumns = ColumnNode.normalize(sortColumns),
    frame = ColumnNode.normalize(frame))
  override private[internal] def sql: String = {
    val parts = Seq(
      elementsToSql(partitionColumns, "PARTITION BY "),
      elementsToSql(sortColumns, "ORDER BY "),
      optionToSql(frame))
    parts.filter(_.nonEmpty).mkString(" ")
  }
}

private[sql] case class WindowFrame(
    frameType: WindowFrame.FrameType,
    lower: WindowFrame.FrameBoundary,
    upper: WindowFrame.FrameBoundary)
    extends ColumnNodeLike {
  override private[internal] def normalize(): WindowFrame =
    copy(lower = lower.normalize(), upper = upper.normalize())
  override private[internal] def sql: String =
    s"${frameType.sql} BETWEEN ${lower.sql} AND ${upper.sql}"
}

private[sql] object WindowFrame {
  sealed abstract class FrameType(override val sql: String) extends ColumnNodeLike
  object Row extends FrameType("ROWS")
  object Range extends FrameType("RANGE")

  sealed abstract class FrameBoundary extends ColumnNodeLike {
    override private[internal] def normalize(): FrameBoundary = this
  }
  object CurrentRow extends FrameBoundary {
    override private[internal] def sql = "CURRENT ROW"
  }
  object UnboundedPreceding extends FrameBoundary {
    override private[internal] def sql = "UNBOUNDED PRECEDING"
  }
  object UnboundedFollowing extends FrameBoundary {
    override private[internal] def sql = "UNBOUNDED FOLLOWING"
  }
  case class Value(value: ColumnNode) extends FrameBoundary {
    override private[internal] def normalize(): Value = copy(value.normalize())
    override private[internal] def sql: String = value.sql
  }
  def value(i: Int): Value = Value(Literal(i, Some(IntegerType)))
  def value(l: Long): Value = Value(Literal(l, Some(LongType)))
}

/**
 * Lambda function to execute. This typically passed as an argument to a function.
 *
 * @param function
 *   to execute.
 * @param arguments
 *   the bound lambda variables.
 */
private[sql] case class LambdaFunction(
    function: ColumnNode,
    arguments: Seq[UnresolvedNamedLambdaVariable],
    override val origin: Origin)
    extends ColumnNode {

  override private[internal] def normalize(): LambdaFunction = copy(
    function = function.normalize(),
    arguments = ColumnNode.normalize(arguments),
    origin = NO_ORIGIN)

  override def sql: String = {
    val argumentsSql = arguments match {
      case Seq(arg) => arg.sql
      case _ => argumentsToSql(arguments)
    }
    argumentsSql + " -> " + function.sql
  }
}

object LambdaFunction {
  def apply(function: ColumnNode, arguments: Seq[UnresolvedNamedLambdaVariable]): LambdaFunction =
    new LambdaFunction(function, arguments, CurrentOrigin.get)
}

/**
 * Variable used in a [[LambdaFunction]].
 *
 * @param name
 *   of the variable.
 */
private[sql] case class UnresolvedNamedLambdaVariable(
    name: String,
    override val origin: Origin = CurrentOrigin.get)
    extends ColumnNode {
  override private[internal] def normalize(): UnresolvedNamedLambdaVariable =
    copy(origin = NO_ORIGIN)

  override def sql: String = name
}

object UnresolvedNamedLambdaVariable {
  private val nextId = new AtomicLong()

  /**
   * Create a lambda variable with a unique name.
   */
  def apply(name: String): UnresolvedNamedLambdaVariable = {
    // Generate a unique name because we reuse lambda variable names (e.g. x, y, or z).
    new UnresolvedNamedLambdaVariable(s"${name}_${nextId.incrementAndGet()}")
  }

  /**
   * Reset the ID generator. For testing purposes only!
   */
  private[sql] def resetIdGenerator(): Unit = {
    nextId.set(0)
  }
}

/**
 * Extract a value from a complex type. This can be a field from a struct, a value from a map, or
 * an element from an array.
 *
 * @param child
 *   that produces a complex value.
 * @param extraction
 *   that is used to access the complex type. This needs to be a string type for structs and maps,
 *   and it needs to be an integer for arrays.
 */
private[sql] case class UnresolvedExtractValue(
    child: ColumnNode,
    extraction: ColumnNode,
    override val origin: Origin = CurrentOrigin.get)
    extends ColumnNode {
  override private[internal] def normalize(): UnresolvedExtractValue =
    copy(child = child.normalize(), extraction = extraction.normalize(), origin = NO_ORIGIN)

  override def sql: String = s"${child.sql}[${extraction.sql}]"
}

/**
 * Update or drop the field of a struct.
 *
 * @param structExpression
 *   that will be updated.
 * @param fieldName
 *   name of the field to update.
 * @param valueExpression
 *   new value of the field. If this is None the field will be dropped.
 */
private[sql] case class UpdateFields(
    structExpression: ColumnNode,
    fieldName: String,
    valueExpression: Option[ColumnNode] = None,
    override val origin: Origin = CurrentOrigin.get)
    extends ColumnNode {
  override private[internal] def normalize(): UpdateFields = copy(
    structExpression = structExpression.normalize(),
    valueExpression = ColumnNode.normalize(valueExpression),
    origin = NO_ORIGIN)
  override def sql: String = valueExpression match {
    case Some(value) => s"update_field(${structExpression.sql}, $fieldName, ${value.sql})"
    case None => s"drop_field(${structExpression.sql}, $fieldName)"
  }
}

/**
 * Evaluate one or more conditional branches. The value of the first branch for which the
 * predicate evalutes to true is returned. If none of the branches evaluate to true, the value of
 * `otherwise` is returned.
 *
 * @param branches
 *   to evaluate. Each entry if a pair of condition and value.
 * @param otherwise
 *   (optional) to evaluate when none of the branches evaluate to true.
 */
private[sql] case class CaseWhenOtherwise(
    branches: Seq[(ColumnNode, ColumnNode)],
    otherwise: Option[ColumnNode] = None,
    override val origin: Origin = CurrentOrigin.get)
    extends ColumnNode {
  assert(branches.nonEmpty)
  override private[internal] def normalize(): CaseWhenOtherwise = copy(
    branches = branches.map(kv => (kv._1.normalize(), kv._2.normalize())),
    otherwise = ColumnNode.normalize(otherwise),
    origin = NO_ORIGIN)

  override def sql: String =
    "CASE" +
      branches.map(cv => s" WHEN ${cv._1.sql} THEN ${cv._2.sql}").mkString +
      otherwise.map(o => s" ELSE ${o.sql}").getOrElse("") +
      " END"
}

/**
 * Invoke an inline user defined function.
 *
 * @param function
 *   to invoke.
 * @param arguments
 *   to pass into the user defined function.
 */
private[sql] case class InvokeInlineUserDefinedFunction(
    function: UserDefinedFunctionLike,
    arguments: Seq[ColumnNode],
    isDistinct: Boolean = false,
    override val origin: Origin = CurrentOrigin.get)
    extends ColumnNode {
  override private[internal] def normalize(): InvokeInlineUserDefinedFunction =
    copy(arguments = ColumnNode.normalize(arguments), origin = NO_ORIGIN)

  override def sql: String =
    function.name + argumentsToSql(arguments)
}

private[sql] trait UserDefinedFunctionLike {
  def name: String = SparkClassUtils.getFormattedClassName(this)
}
