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

package org.apache.spark.sql.execution

import org.apache.spark.sql.catalyst.expressions._
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
 *
 * @param expr the top-level complex type extractor
 */
private[execution] object SelectedField {
  def unapply(expr: Expression): Option[StructField] = {
    // If this expression is an alias, work on its child instead
    val unaliased = expr match {
      case Alias(child, _) => child
      case expr => expr
    }
    selectField(unaliased, None)
  }

  private def selectField(expr: Expression, fieldOpt: Option[StructField]): Option[StructField] = {
    expr match {
      // No children. Returns a StructField with the attribute name or None if fieldOpt is None.
      case AttributeReference(name, dataType, nullable, metadata) =>
        fieldOpt.map(field =>
          StructField(name, wrapStructType(dataType, field), nullable, metadata))
      // Handles case "expr0.field[n]", where "expr0" is of struct type and "expr0.field" is of
      // array type.
      case GetArrayItem(x @ GetStructFieldObject(child, field @ StructField(name,
          dataType, nullable, metadata)), _) =>
        val childField = fieldOpt.map(field => StructField(name,
          wrapStructType(dataType, field), nullable, metadata)).getOrElse(field)
        selectField(child, Some(childField))
      // Handles case "expr0.field[n]", where "expr0.field" is of array type.
      case GetArrayItem(child, _) =>
        selectField(child, fieldOpt)
      // Handles case "expr0.field.subfield", where "expr0" and "expr0.field" are of array type.
      case GetArrayStructFields(child: GetArrayStructFields,
          field @ StructField(name, dataType, nullable, metadata), _, _, _) =>
        val childField = fieldOpt.map(field => StructField(name,
            wrapStructType(dataType, field),
            nullable, metadata)).orElse(Some(field))
        selectField(child, childField)
      // Handles case "expr0.field", where "expr0" is of array type.
      case GetArrayStructFields(child,
          field @ StructField(name, dataType, nullable, metadata), _, _, _) =>
        val childField =
          fieldOpt.map(field => StructField(name,
            wrapStructType(dataType, field),
            nullable, metadata)).orElse(Some(field))
        selectField(child, childField)
      // Handles case "expr0.field[key]", where "expr0" is of struct type and "expr0.field" is of
      // map type.
      case GetMapValue(x @ GetStructFieldObject(child, field @ StructField(name,
          dataType,
          nullable, metadata)), _) =>
        val childField = fieldOpt.map(field => StructField(name,
          wrapStructType(dataType, field),
          nullable, metadata)).orElse(Some(field))
        selectField(child, childField)
      // Handles case "expr0.field[key]", where "expr0.field" is of map type.
      case GetMapValue(child, _) =>
        selectField(child, fieldOpt)
      // Handles case "expr0.field", where expr0 is of struct type.
      case GetStructFieldObject(child,
        field @ StructField(name, dataType, nullable, metadata)) =>
        val childField = fieldOpt.map(field => StructField(name,
          wrapStructType(dataType, field),
          nullable, metadata)).orElse(Some(field))
        selectField(child, childField)
      case _ =>
        None
    }
  }

  // Constructs a composition of complex types with a StructType(Array(field)) at its core. Returns
  // a StructType for a StructType, an ArrayType for an ArrayType and a MapType for a MapType.
  private def wrapStructType(dataType: DataType, field: StructField): DataType = {
    dataType match {
      case _: StructType =>
        StructType(Array(field))
      case ArrayType(elementType, containsNull) =>
        ArrayType(wrapStructType(elementType, field), containsNull)
      case MapType(keyType, valueType, valueContainsNull) =>
        MapType(keyType, wrapStructType(valueType, field), valueContainsNull)
    }
  }
}
