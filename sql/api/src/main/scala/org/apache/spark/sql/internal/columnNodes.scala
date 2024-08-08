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

import org.apache.spark.sql.catalyst.encoders.AgnosticEncoder
import org.apache.spark.sql.catalyst.trees.{CurrentOrigin, Origin}
import org.apache.spark.sql.types.{DataType, Metadata}

/**
 * AST for constructing columns. This API is implementation agnostic and allows us to build a
 * single Column implementation that can be shared between implementations. Consequently a
 * Dataframe API implementations will have to provide conversions from this AST to its
 * implementation specific form (e.g. Catalyst expressions, or Connect protobuf messages).
 *
 * This API is a mirror image of Connect's expression.proto. There are a couple of extensions to
 * make constructing nodes easier (e.g. [[CaseWhenOtherwise]]). We could not use the actual connect
 * protobuf messages because of classpath clashes (e.g. Guava & gRPC) and Maven shading issues.
 */
private[sql] trait ColumnNode {
  /**
   * Origin where the node was created.
   */
  def origin: Origin
}

/**
 * A literal column.
 *
 * @param value of the literal. This is the unconverted input value.
 * @param dataType of the literal. If none is provided the dataType is inferred.
 */
private[sql] case class Literal(
    value: Any,
    dataType: Option[DataType] = None,
    override val origin: Origin = CurrentOrigin.get) extends ColumnNode

/**
 * Reference to an attribute produced by one of the underlying DataFrames.
 *
 * @param unparsedIdentifier name of the attribute.
 * @param planId id of the plan (Dataframe) that produces the attribute.
 * @param isMetadataColumn whether this is a metadata column.
 */
private[sql] case class UnresolvedAttribute(
    unparsedIdentifier: String,
    planId: Option[Long] = None,
    isMetadataColumn: Boolean = false,
    override val origin: Origin = CurrentOrigin.get)
  extends ColumnNode

/**
 * Reference to all columns in a namespace (global, a Dataframe, or a nested struct).
 *
 * @param unparsedTarget name of the namespace. None if the global namespace is supposed to be used.
 * @param planId id of the plan (Dataframe) that produces the attribute.
 */
private[sql] case class UnresolvedStar(
    unparsedTarget: Option[String],
    planId: Option[Long] = None,
    override val origin: Origin = CurrentOrigin.get)
  extends ColumnNode

/**
 * Call a function. This can either be a built-in function, a UDF, or a UDF registered in the
 * Catalog.
 *
 * @param functionName of the function to invoke.
 * @param arguments to pass into the function.
 * @param isDistinct (aggregate only) whether the input of the aggregate function should be
 *                   de-duplicated.
 */
private[sql] case class UnresolvedFunction(
    functionName: String,
    arguments: Seq[ColumnNode],
    isDistinct: Boolean = false,
    isUserDefinedFunction: Boolean = false,
    override val origin: Origin = CurrentOrigin.get)
  extends ColumnNode

/**
 * Evaluate a SQL expression.
 *
 * @param expression text to execute.
 */
private[sql] case class SqlExpression(
    expression: String,
    override val origin: Origin = CurrentOrigin.get) extends ColumnNode

/**
 * Name a column, and (optionally) modify its metadata.
 *
 * @param child to name
 * @param name to use
 * @param metadata (optional) metadata to add.
 */
private[sql] case class Alias(
    child: ColumnNode,
    name: Seq[String],
    metadata: Option[Metadata] = None,
    override val origin: Origin = CurrentOrigin.get) extends ColumnNode

/**
 * Cast the value of a Column to a different [[DataType]]. The behavior of the cast can be
 * influenced by the `evalMode`.
 *
 * @param child that produces the input value.
 * @param dataType to cast to.
 * @param evalMode (try/ansi/legacy) to use for the cast.
 */
private[sql] case class Cast(
    child: ColumnNode,
    dataType: DataType,
    evalMode: Option[Cast.EvalMode.Value] = None,
    override val origin: Origin = CurrentOrigin.get) extends ColumnNode

private[sql] object Cast {
  object EvalMode extends Enumeration {
    type EvalMode = Value
    val Legacy, Ansi, Try = Value
  }
}

/**
 * Reference to all columns in the global namespace in that match a regex.
 *
 * @param regex name of the namespace. None if the global namespace is supposed to be used.
 * @param planId id of the plan (Dataframe) that produces the attribute.
 */
private[sql] case class UnresolvedRegex(
    regex: String,
    planId: Option[Long] = None,
    override val origin: Origin = CurrentOrigin.get) extends ColumnNode

/**
 * Sort the input column.
 *
 * @param child to sort.
 * @param sortDirection to sort in, either Ascending or Descending.
 * @param nullOrdering where to place nulls, either at the begin or the end.
 */
private[sql] case class SortOrder(
    child: ColumnNode,
    sortDirection: SortOrder.SortDirection.Value,
    nullOrdering: SortOrder.NullOrdering.Value,
    override val origin: Origin = CurrentOrigin.get)
  extends ColumnNode

private[sql] object SortOrder {
  object SortDirection extends Enumeration {
    type SortDirection = Value
    val Ascending, Descending = Value
  }
  object NullOrdering extends  Enumeration {
    type NullOrdering = Value
    val NullsFirst, NullsLast = Value
  }
}

/**
 * Evaluate a function within a window.
 *
 * @param windowFunction function to execute.
 * @param windowSpec of the window.
 */
private[sql] case class Window(
    windowFunction: ColumnNode,
    windowSpec: WindowSpec,
    override val origin: Origin = CurrentOrigin.get)
  extends ColumnNode

private[sql] case class WindowSpec(
    partitionColumns: Seq[ColumnNode],
    sortColumns: Seq[SortOrder],
    frame: Option[WindowFrame] = None)

private[sql] case class WindowFrame(
    frameType: WindowFrame.FrameType.Value,
    lower: WindowFrame.FrameBoundary,
    upper: WindowFrame.FrameBoundary)

private[sql] object WindowFrame {
  object FrameType extends Enumeration {
    type FrameType = this.Value
    val Row, Range = this.Value
  }

  sealed trait FrameBoundary
  object CurrentRow extends FrameBoundary
  object Unbounded extends FrameBoundary
  case class Value(value: ColumnNode) extends FrameBoundary
}

/**
 * Lambda function to execute. This typically passed as an argument to a function.
 *
 * @param function to execute.
 * @param arguments the bound lambda variables.
 */
private[sql] case class LambdaFunction(
    function: ColumnNode,
    arguments: Seq[UnresolvedNamedLambdaVariable],
    override val origin: Origin = CurrentOrigin.get) extends ColumnNode

/**
 * Variable used in a [[LambdaFunction]].
 *
 * @param name of the variable.
 */
private[sql] case class UnresolvedNamedLambdaVariable(
    name: String,
    override val origin: Origin = CurrentOrigin.get) extends ColumnNode

/**
 * Extract a value from a complex type. This can be a field from a struct, a value from a map,
 * or an element from an array.
 *
 * @param child that produces a complex value.
 * @param extraction that is used to access the complex type. This needs to be a string type for
 *                   structs and maps, and it needs to be an integer for arrays.
 */
private[sql] case class UnresolvedExtractValue(
    child: ColumnNode,
    extraction: ColumnNode,
    override val origin: Origin = CurrentOrigin.get) extends ColumnNode

/**
 * Update or drop the field of a struct.
 *
 * @param structExpression that will be updated.
 * @param fieldName name of the field to update.
 * @param valueExpression new value of the field. If this is None the field will be dropped.
 */
private[sql] case class UpdateFields(
    structExpression: ColumnNode,
    fieldName: String,
    valueExpression: Option[ColumnNode] = None,
    override val origin: Origin = CurrentOrigin.get) extends ColumnNode

/**
 * Evaluate one or more conditional branches. The value of the first branch for which the predicate
 * evalutes to true is returned. If none of the branches evaluate to true, the value of `otherwise`
 * is returned.
 *
 * @param branches to evaluate. Each entry if a pair of condition and value.
 * @param otherwise (optional) to evaluate when none of the branches evaluate to true.
 */
private[sql] case class CaseWhenOtherwise(
    branches: Seq[(ColumnNode, ColumnNode)],
    otherwise: Option[ColumnNode] = None,
    override val origin: Origin = CurrentOrigin.get)
  extends ColumnNode

/**
 * Invoke an inline user defined function.
 *
 * @param function to invoke.
 * @param arguments to pass into the user defined function.
 */
private[sql] case class InvokeInlineUserDefinedFunction(
    function: UserDefinedFunction,
    arguments: Seq[ColumnNode],
    override val origin: Origin = CurrentOrigin.get)
  extends ColumnNode

// This is a temporary class until we move the actual interfaces
private[sql] case class UserDefinedFunction(
    function: AnyRef,
    resultEncoder: AgnosticEncoder[Any],
    inputEncoders: Seq[AgnosticEncoder[Any]],
    name: Option[String],
    nonNullable: Boolean,
    deterministic: Boolean)

/**
 * Extension point that allows an implementation to use its column representation to be used in a
 * generic column expression. This should only be used when the Column constructed is used within
 * the implementation.
 */
private[sql] case class Extension(
    value: Any,
    override val origin: Origin = CurrentOrigin.get) extends ColumnNode
