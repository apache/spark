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

package org.apache.spark.sql.avro

import scala.collection.JavaConverters._

import org.apache.avro.JsonProperties.{NULL_VALUE => nullInstance}
import org.apache.avro.Schema
import org.apache.avro.Schema.Field
import org.apache.avro.Schema.Type._

object SchemaMerge {
  private implicit class RichSchema(val schema: Schema) extends AnyVal {
    def withAliases(aliases: Set[String]): Schema = {
      aliases.foreach{ alias => schema.addAlias(alias) }
      schema
    }
  }

  private def optional(field: Field): Field = {
    require(field.defaultVal == null || field.defaultVal == nullInstance,
      s"optional field ${field} has non-null default value")
    new Field(field.name, optional(field.schema), null, nullInstance)
  }

  private def optional(schema: Schema): Schema =
    if (schema.getType == UNION) {
      Schema.createUnion((Schema.create(NULL) +: schema.getTypes.asScala.filter(_.getType != NULL))
        .asJava)
    } else {
      Schema.createUnion(Schema.create(NULL), schema)
    }

  private def isNullableOf(schema: Schema): Option[Schema] =
    if (schema.getType == UNION) {
      val types = schema.getTypes.asScala
      if (types.size == 2 && types.head.getType == NULL) {
        Some(types.last)
      } else if (types.size == 2 && types.last.getType == NULL) {
        Some(types.head)
      } else if (types.size == 1 && types.head.getType != NULL) {
        Some(types.head)
      } else {
        None
      }
    } else {
      Some(schema)
    }

  private def defaultType(schema: Schema): Schema.Type =
    if (schema.getType == UNION) {
      schema.getTypes.asScala.head.getType
    } else {
      schema.getType
    }

  private def spaceNameAliases(x: Schema, y: Schema): (String, String, Set[String]) = {
    val space = x.getNamespace
    val name = x.getName
    val aliases = x.getAliases.asScala.toSet ++ y.getAliases.asScala.toSet +
      y.getFullName - x.getFullName
    (space, name, aliases)
  }

  def mergeSchemas(x: Schema, y: Schema): Schema =
    if (x == y) {
      x
    } else {
      try {
        mergeWithLogical(x, y)
      } catch {
        case e: Exception =>
          throw new IllegalArgumentException(s"failed to merge ${x} and ${y}", e)
      }
    }

  private def mergeWithLogical(x: Schema, y: Schema): Schema = {
    val xLogical = x.getLogicalType
    val yLogical = y.getLogicalType
    require(xLogical == null || yLogical == null || xLogical == yLogical,
      s"inconsistent logical types ${xLogical} and ${yLogical} for ${x} and ${y}")
    val merged = merge(x, y)
    if (merged.getType == UNION) {
      merged
    } else if (xLogical != null) {
      xLogical.addToSchema(merged)
    } else if (yLogical != null) {
      yLogical.addToSchema(merged)
    } else {
      merged
    }
  }

  private def merge(x: Schema, y: Schema, swapped: Boolean = false): Schema =
    (x.getType, y.getType) match {
      case (INT, INT) => Schema.create(INT)
      case (INT, LONG) => Schema.create(LONG)
      case (INT, FLOAT) => Schema.create(FLOAT)
      case (INT, DOUBLE) => Schema.create(DOUBLE)

      case (LONG, LONG) => Schema.create(LONG)
      case (LONG, FLOAT) => Schema.create(FLOAT)
      case (LONG, DOUBLE) => Schema.create(DOUBLE)

      case (FLOAT, FLOAT) => Schema.create(FLOAT)
      case (FLOAT, DOUBLE) => Schema.create(DOUBLE)

      case (DOUBLE, DOUBLE) => Schema.create(DOUBLE)

      case (STRING, STRING) => Schema.create(STRING)
      case (STRING, BYTES) => Schema.create(BYTES)

      case (BYTES, BYTES) => Schema.create(BYTES)

      case (FIXED, FIXED) =>
        val (space, name, aliases) = spaceNameAliases(x, y)
        require(x.getFixedSize == y.getFixedSize,
          s"${x} and ${y} do not have same size")
        val fixedSize = x.getFixedSize
        Schema.createFixed(name, null, space, fixedSize)
          .withAliases(aliases)

      case (ENUM, ENUM) =>
        val (space, name, aliases) = spaceNameAliases(x, y)
        val enumSymbols = (x.getEnumSymbols.asScala ++ y.getEnumSymbols.asScala).distinct
        Schema.createEnum(name, null, space, enumSymbols.asJava)
          .withAliases(aliases)

      case (MAP, MAP) =>
        val valueType = mergeWithLogical(x.getValueType, y.getValueType)
        Schema.createMap(valueType)

      case (ARRAY, ARRAY) =>
        val elementType = mergeWithLogical(x.getElementType, y.getElementType)
        Schema.createArray(elementType)

      case (UNION, UNION) =>
        isNullableOf(x).flatMap{ x1 =>
          isNullableOf(y).flatMap{ y1 =>
            // logic for when union is just used for nullable fields
            Option(mergeWithLogical(x1, y1)).filter(_.getType != UNION).map{ s =>
              Schema.createUnion(Schema.create(NULL), s)
            }
          }
        }.getOrElse{
          val unionTypes = (x.getTypes.asScala ++ y.getTypes.asScala).distinct
          Schema.createUnion(unionTypes.asJava)
        }

      case (UNION, _) =>
        mergeWithLogical(x, Schema.createUnion(y))

      case (RECORD, RECORD) =>
        val (space, name, aliases) = spaceNameAliases(x, y)
        val fields = (x.getFields.asScala.map(_.name) ++ y.getFields.asScala.map(_.name))
          .distinct.map{ field =>
            if (x.getField(field) == null) {
              optional(y.getField(field))
            } else if (y.getField(field) == null) {
              optional(x.getField(field))
            } else {
              val left = x.getField(field)
              val right = y.getField(field)
              assert(left.name == right.name)
              require(
                left.defaultVal == null ||
                  right.defaultVal == null ||
                  left.defaultVal == right.defaultVal,
                s"inconsistent default values ${left.defaultVal} and ${right.defaultVal}"
              )
              val name = left.name
              val schema = mergeWithLogical(left.schema, right.schema)
              val defaultVal = Seq(left.defaultVal, right.defaultVal)
                .filter(_ != null).headOption.orNull
              require(
                defaultVal == null ||
                  (defaultType(schema) == NULL && defaultVal == nullInstance) ||
                  (defaultType(schema) == defaultType(left.schema) &&
                    defaultType(schema) == defaultType(right.schema)),
                s"effective schema type cannot change with default value"
              )
              new Field(name, schema, null, defaultVal)
            }
          }
        Schema.createRecord(name, null, space, false, fields.asJava)
          .withAliases(aliases)

      case (_, _) if !swapped =>
        merge(y, x, swapped = true)

      case (_, _) =>
        // flipped around because we always try swapped so this unswaps
        Schema.createUnion(y, x)
    }
}
