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

package org.apache.spark.sql.catalyst.planning

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._

/**
 * A Scala extractor that builds a [[org.apache.spark.sql.types.StructField]] from a Catalyst
 * complex type extractor. For example, consider a relation with the following schema:
 *
 *   {{{
 *   root
 *    |-- name: struct (nullable = true)
 *    |    |-- first: string (nullable = true)
 *    |    |-- last: string (nullable = true)
 *    }}}
 *
 * Further, suppose we take the select expression `name.first`. This will parse into an
 * `Alias(child, "first")`. Ignoring the alias, `child` matches the following pattern:
 *
 *   {{{
 *   GetStructFieldObject(
 *     AttributeReference("name", StructType(_), _, _),
 *     StructField("first", StringType, _, _))
 *   }}}
 *
 * [[SelectedField]] converts that expression into
 *
 *   {{{
 *   StructField("name", StructType(Array(StructField("first", StringType))))
 *   }}}
 *
 * by mapping each complex type extractor to a [[org.apache.spark.sql.types.StructField]] with the
 * same name as its child (or "parent" going right to left in the select expression) and a data
 * type appropriate to the complex type extractor. In our example, the name of the child expression
 * is "name" and its data type is a [[org.apache.spark.sql.types.StructType]] with a single string
 * field named "first".
 *
 * @param expr the top-level complex type extractor
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

  private def selectField(expr: Expression, fieldOpt: Option[StructField]): Option[StructField] =
    expr match {
      // No children. Returns a StructField with the attribute name or None if fieldOpt is None.
      case AttributeReference(name, _, nullable, metadata) =>
        fieldOpt.map(field => StructField(name, StructType(Array(field)), nullable, metadata))
      // Handles case "col.field[n]", where "field" is an array type. Returns
      // StructField("col",
      //   StructType(Array(StructField("field", ArrayType(elementType, containsNull)))))
      // where "elementType" is the element data type of "field".
      case GetArrayItem(GetStructFieldObject(child, field @ StructField(name,
          ArrayType(_, containsNull), fieldNullable, fieldMetadata)), _) =>
        val childField = fieldOpt.map(field => StructField(name, ArrayType(
          StructType(Array(field)), containsNull), fieldNullable, fieldMetadata)).getOrElse(field)
        selectField(child, Some(childField))
      // Handles case "col.field.subfield", where "field" and "subfield" are array types. Returns
      // StructField("col", StructType(Array(
      //   StructField("field", ArrayType(elementType, containsNull)))))
      // where "elementType" is the element data type of "field".
      case GetArrayStructFields(child: GetArrayStructFields,
          field @ StructField(name, _, nullable, metadata), _, _, containsNull) =>
        val childField =
          fieldOpt.map(field => StructField(name, StructType(Array(field)), nullable, metadata))
            .getOrElse(field)
        selectField(child, Some(childField))
      // Handles case "col.field", where "field" is an array type. Returns
      // StructField("col", StructType(Array(
      //   StructField("field", ArrayType(elementType, containsNull)))))
      // where "elementType" is the element data type of "field".
      case GetArrayStructFields(child,
          field @ StructField(name, _, nullable, metadata), _, _, containsNull) =>
        val childField =
          fieldOpt.map(field => StructField(name, StructType(Array(field)), nullable, metadata))
            .getOrElse(field)
        selectField(child, Some(childField)).map {
          case StructField(name,
              StructType(Array(StructField(name2, elementType, nullable2, metadata2))),
                nullable, metadata) =>
            StructField(name, StructType(Array(StructField(name2,
                ArrayType(elementType, containsNull), nullable2, metadata2))), nullable, metadata)
        }
      // Handles case "col.field[key]", where "field" is a map type. Returns
      // StructField("col",
      //   StructType(Array(StructField("field", MapType(keyType, valueType, valueContainsNull)))))
      // where "keyType" and "valueType" are the data types of keys and values of "field",
      // respectively.
      case GetMapValue(GetStructFieldObject(child, field @ StructField(name,
          MapType(keyType, _, valueContainsNull), fieldNullable, fieldMetadata)), _) =>
        val childField = fieldOpt.map(field => StructField(name, MapType(keyType,
          StructType(Array(field)), valueContainsNull), fieldNullable, fieldMetadata))
            .getOrElse(field)
        selectField(child, Some(childField))
      // Handles case "col.field". Returns
      // StructField("col", StructType(Array(
      //   StructField("field", fieldType))))
      // where "fieldType" is the data type of "field".
      case GetStructFieldObject(child, field @ StructField(name, _, nullable, metadata)) =>
        val childField = fieldOpt.map(field => StructField(name,
          StructType(Array(field)), nullable, metadata)).getOrElse(field)
        selectField(child, Some(childField))
      case _ =>
        None
    }
}
