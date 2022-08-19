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
      case GetMapValue(child, _) =>
        // GetMapValue does not select a field from a struct (i.e. prune the struct) so it can't be
        // the top-level extractor. However it can be part of an extractor chain.
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
      case GetArrayItem(child, _, _) =>
        // GetArrayItem does not select a field from a struct (i.e. prune the struct) so it can't be
        // the top-level extractor. However it can be part of an extractor chain.
        val ArrayType(_, containsNull) = child.dataType
        val opt = dataTypeOpt.map(dt => ArrayType(dt, containsNull))
        selectField(child, opt)
      case _ =>
        None
    }
  }

  private def struct(field: StructField): StructType = StructType(Array(field))
}
