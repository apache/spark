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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.types._

/**
 * A Scala extractor that builds a [[org.apache.spark.sql.types.StructField]] from a Catalyst
 * complex type extractor. For example, consider a relation with the following schema:
 *
 * {{{
 * root
 * |-- name: struct (nullable = true)
 * |    |-- first: string (nullable = true)
 * |    |-- last: string (nullable = true)
 * }}}
 *
 * Further, suppose we take the select expression `name.first`. This will parse into an
 * `Alias(child, "first")`. Ignoring the alias, `child` matches the following pattern:
 *
 * {{{
 * GetStructFieldObject(
 *   AttributeReference("name", StructType(_), _, _),
 *   StructField("first", StringType, _, _))
 * }}}
 *
 * [[SelectedField]] converts that expression into
 *
 * {{{
 * StructField("name", StructType(Array(StructField("first", StringType))))
 * }}}
 *
 * by mapping each complex type extractor to a [[org.apache.spark.sql.types.StructField]] with the
 * same name as its child (or "parent" going right to left in the select expression) and a data
 * type appropriate to the complex type extractor. In our example, the name of the child expression
 * is "name" and its data type is a [[org.apache.spark.sql.types.StructType]] with a single string
 * field named "first".
 */
object SelectedField {
  def unapply(expr: Expression): Option[StructField] = {
    // If this expression is an alias, work on its child instead
    val unaliased = expr match {
      case Alias(child, _) => child
      case expr => expr
    }
    selectField(unaliased, None)
  }

  /**
   * Like unapply, but returns all fields from expressions that combine multiple field accesses.
   * This is needed for expressions like ArraysZip and NestedArraysZip that merge multiple
   * array field extractions into a single output.
   *
   * For example, `ArraysZip([arr.f1, arr.f2], names)` accesses both f1 and f2 from arr.
   * Regular unapply would return None, but unapplySeq returns both fields.
   *
   * @return None if no fields are accessed, Some(Seq[StructField]) otherwise
   */
  def unapplySeq(expr: Expression): Option[Seq[StructField]] = {
    val unaliased = expr match {
      case Alias(child, _) => child
      case e => e
    }
    unaliased match {
      // ArraysZip combines multiple array field extractions
      // Use unapplySeq recursively to handle nested ArraysZip/NestedArraysZip
      case ArraysZip(children, _) =>
        val fields = children.flatMap(c => unapplySeq(c).getOrElse(Seq.empty))
        if (fields.nonEmpty) Some(fields) else None

      // NestedArraysZip combines nested array field extractions
      // Use unapplySeq recursively to handle further nested expressions
      case NestedArraysZip(children, _, _) =>
        val fields = children.flatMap(c => unapplySeq(c).getOrElse(Seq.empty))
        if (fields.nonEmpty) Some(fields) else None

      // For other expressions, delegate to regular unapply
      case _ =>
        unapply(expr).map(Seq(_))
    }
  }

  /**
   * Convert an expression into the parts of the schema (the field) it accesses.
   */
  @scala.annotation.tailrec
  private def selectField(expr: Expression, dataTypeOpt: Option[DataType]): Option[StructField] = {
    expr match {
      case a: Attribute =>
        dataTypeOpt.map { dt =>
          StructField(a.name, dt, a.nullable)
        }
      case c: GetStructField =>
        val field = c.childSchema(c.ordinal)
        val newField = field.copy(dataType = dataTypeOpt.getOrElse(field.dataType))
        selectField(c.child, Option(struct(newField)))
      case GetArrayStructFields(child, _, ordinal, _, containsNull) =>
        // For case-sensitivity aware field resolution, we should take `ordinal` which
        // points to correct struct field.
        val field = child.dataType.asInstanceOf[ArrayType]
          .elementType.asInstanceOf[StructType](ordinal)
        val newFieldDataType = dataTypeOpt match {
          case None =>
            // GetArrayStructFields is the top level extractor. This means its result is
            // not pruned and we need to use the element type of the array its producing.
            field.dataType
          case Some(ArrayType(dataType, _)) =>
            // GetArrayStructFields is part of a chain of extractors and its result is pruned
            // by a parent expression. In this case need to use the parent element type.
            dataType
          case Some(x) =>
            // This should not happen.
            throw QueryCompilationErrors.dataTypeUnsupportedByClassError(x, "GetArrayStructFields")
        }
        val newField = StructField(field.name, newFieldDataType, field.nullable)
        selectField(child, Option(ArrayType(struct(newField), containsNull)))
      case GetNestedArrayStructFields(child, field, ordinal, _, containsNull) =>
        // GetNestedArrayStructFields extracts a field from the innermost struct of a
        // nested array like array<array<struct>>. We need to find the innermost struct
        // field and rebuild the full nested array schema.
        val innermostField = findInnermostStructField(child.dataType, ordinal)
        val newFieldDataType = dataTypeOpt match {
          case None =>
            // Top level extractor - use the field's type
            innermostField.dataType
          case Some(dt) =>
            // Part of a chain - peel off only the parent's array layers, not the field's own
            // For example, if child is array<array<struct<..., field: array<struct<...>>>>>,
            // the parent chain contributes 2 array levels, and field contributes 1 more.
            // We should peel only the parent's 2 levels, keeping the field's array type.
            val parentArrayDepth = arrayDepth(child.dataType)
            peelNArrayLayers(dt, parentArrayDepth)
        }
        val newField = StructField(innermostField.name, newFieldDataType, innermostField.nullable)
        val wrappedType = wrapInArrays(child.dataType, struct(newField), containsNull)
        selectField(child, Option(wrappedType))
      case GetMapValue(child, key) if key.foldable =>
        // GetMapValue does not select a field from a struct (i.e. prune the struct) so it can't be
        // the top-level extractor. However it can be part of an extractor chain.
        // See comment on GetArrayItem regarding the need for key.foldable
        val MapType(keyType, _, valueContainsNull) = child.dataType
        val opt = dataTypeOpt.map(dt => MapType(keyType, dt, valueContainsNull))
        selectField(child, opt)
      case MapValues(child) =>
        val MapType(keyType, _, valueContainsNull) = child.dataType
        // MapValues does not select a field from a struct (i.e. prune the struct) so it can't be
        // the top-level extractor. However it can be part of an extractor chain.
        val opt = dataTypeOpt.map {
          case ArrayType(dataType, _) => MapType(keyType, dataType, valueContainsNull)
          case x =>
            // This should not happen.
            throw QueryCompilationErrors.dataTypeUnsupportedByClassError(x, "MapValues")
        }
        selectField(child, opt)
      case MapKeys(child) =>
        val MapType(_, valueType, valueContainsNull) = child.dataType
        // MapKeys does not select a field from a struct (i.e. prune the struct) so it can't be
        // the top-level extractor. However it can be part of an extractor chain.
        val opt = dataTypeOpt.map {
          case ArrayType(dataType, _) => MapType(dataType, valueType, valueContainsNull)
          case x =>
            // This should not happen.
            throw QueryCompilationErrors.dataTypeUnsupportedByClassError(x, "MapKeys")
        }
        selectField(child, opt)
      case GetArrayItem(child, index, _) if index.foldable =>
        // GetArrayItem does not select a field from a struct (i.e. prune the struct) so it can't be
        // the top-level extractor. However it can be part of an extractor chain.
        // If index is not foldable, we'd need to also return the field selected by index, which
        // the SelectedField interface doesn't support, so only allow a foldable index for now.
        val ArrayType(_, containsNull) = child.dataType
        val opt = dataTypeOpt.map(dt => ArrayType(dt, containsNull))
        selectField(child, opt)
      case ElementAt(left, right, _, _) if right.foldable =>
        // ElementAt does not select a field from a struct (i.e. prune the struct) so it can't be
        // the top-level extractor. However it can be part of an extractor chain.
        // For example:
        // For a column schema: `c: array<struct<s1: int, s2: string>>`
        // With the query: `SELECT element_at(c, 1).s1`
        // The final pruned schema should be `c: array<struct<s1: int>>`
        left.dataType match {
          case ArrayType(_, containsNull) =>
            val opt = dataTypeOpt.map(dt => ArrayType(dt, containsNull))
            selectField(left, opt)
          case MapType(keyType, _, valueContainsNull) =>
            val opt = dataTypeOpt.map(dt => MapType(keyType, dt, valueContainsNull))
            selectField(left, opt)
        }
      case _ =>
        None
    }
  }

  private def struct(field: StructField): StructType = StructType(Array(field))

  /**
   * Finds the struct field at the given ordinal in the innermost struct of a nested array type.
   * For example, for `array<array<struct<a, b, c>>>` with ordinal 1, returns field `b`.
   */
  @scala.annotation.tailrec
  private def findInnermostStructField(dt: DataType, ordinal: Int): StructField = dt match {
    case ArrayType(elementType: ArrayType, _) => findInnermostStructField(elementType, ordinal)
    case ArrayType(st: StructType, _) => st(ordinal)
    case _ => throw new IllegalArgumentException(s"Expected nested array of struct, got: $dt")
  }

  /**
   * Removes all ArrayType wrappers from a data type, returning the innermost element type.
   * For example, `array<array<int>>` becomes `int`.
   */
  @scala.annotation.tailrec
  private def peelArrayLayers(dt: DataType): DataType = dt match {
    case ArrayType(elementType, _) => peelArrayLayers(elementType)
    case other => other
  }

  /**
   * Counts the number of array layers in a data type.
   * For example, `array<array<struct>>` returns 2.
   */
  private def arrayDepth(dt: DataType): Int = {
    @scala.annotation.tailrec
    def loop(dt: DataType, depth: Int): Int = dt match {
      case ArrayType(elementType, _) => loop(elementType, depth + 1)
      case _ => depth
    }
    loop(dt, 0)
  }

  /**
   * Removes exactly N ArrayType wrappers from a data type.
   * For example, `peelNArrayLayers(array<array<array<int>>>, 2)` returns `array<int>`.
   */
  @scala.annotation.tailrec
  private def peelNArrayLayers(dt: DataType, n: Int): DataType = {
    if (n <= 0) dt
    else dt match {
      case ArrayType(elementType, _) => peelNArrayLayers(elementType, n - 1)
      case other => other
    }
  }

  /**
   * Wraps an innermost struct type in the same array nesting as the source type.
   * For example, if sourceType is `array<array<struct<a, b>>>` and innerStruct is `struct<a>`,
   * returns `array<array<struct<a>>>`.
   */
  private def wrapInArrays(sourceType: DataType, innerStruct: StructType,
      containsNull: Boolean): DataType = sourceType match {
    case ArrayType(elementType: ArrayType, outerContainsNull) =>
      ArrayType(wrapInArrays(elementType, innerStruct, containsNull), outerContainsNull)
    case ArrayType(_: StructType, _) =>
      ArrayType(innerStruct, containsNull)
    case _ =>
      throw new IllegalArgumentException(s"Expected nested array of struct, got: $sourceType")
  }
}
