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

import org.apache.spark.SparkException
import org.apache.spark.sql.types._

/**
 * A Scala extractor that projects an expression over a given schema. Data types,
 * field indexes and field counts of complex type extractors and attributes
 * are adjusted to fit the schema. All other expressions are left as-is. This
 * class is motivated by columnar nested schema pruning.
 *
 * @param schema nested column schema
 * @param output output attributes of the data source relation. They are used to filter out
 *               attributes in the schema that do not belong to the current relation.
 */
case class ProjectionOverSchema(schema: StructType, output: AttributeSet) {
  private val fieldNames = schema.fieldNames.toSet

  def unapply(expr: Expression): Option[Expression] = getProjection(expr)

  private def getProjection(expr: Expression): Option[Expression] =
    expr match {
      case a: AttributeReference if fieldNames.contains(a.name) && output.contains(a) =>
        Some(a.copy(dataType = schema(a.name).dataType)(a.exprId, a.qualifier))
      case GetArrayItem(child, arrayItemOrdinal, failOnError) =>
        getProjection(child).map {
          projection => GetArrayItem(projection, arrayItemOrdinal, failOnError)
        }
      case a: GetArrayStructFields =>
        getProjection(a.child).map(p => (p, p.dataType)).map {
          case (projection, ArrayType(projSchema @ StructType(_), _)) =>
            // For case-sensitivity aware field resolution, we should take `ordinal` which
            // points to correct struct field, because `ExtractValue` actually does column
            // name resolving correctly.
            val selectedField = a.child.dataType.asInstanceOf[ArrayType]
              .elementType.asInstanceOf[StructType](a.ordinal)
            val prunedField = projSchema(selectedField.name)
            GetArrayStructFields(projection,
              prunedField.copy(name = a.field.name),
              projSchema.fieldIndex(selectedField.name),
              projSchema.size,
              a.containsNull)
          case (_, projSchema) =>
            throw SparkException.internalError(
              s"unmatched child schema for GetArrayStructFields: ${projSchema.toString}"
            )
        }
      case MapKeys(child) =>
        getProjection(child).map { projection => MapKeys(projection) }
      case MapValues(child) =>
        getProjection(child).map { projection => MapValues(projection) }
      case GetMapValue(child, key) =>
        getProjection(child).map { projection => GetMapValue(projection, key) }
      case GetStructFieldObject(child, field: StructField) =>
        getProjection(child).map(p => (p, p.dataType)).map {
          case (projection, projSchema: StructType) =>
            GetStructField(projection, projSchema.fieldIndex(field.name))
          case (_, projSchema) =>
            throw SparkException.internalError(
              s"unmatched child schema for GetStructField: ${projSchema.toString}"
            )
        }
      case ElementAt(left, right, defaultValueOutOfBound, failOnError) if right.foldable =>
        getProjection(left).map(p => ElementAt(p, right, defaultValueOutOfBound, failOnError))
      case _ =>
        None
    }
}
