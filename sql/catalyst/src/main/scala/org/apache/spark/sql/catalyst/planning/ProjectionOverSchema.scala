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
 * A Scala extractor that projects an expression over a given schema. Data types,
 * field indexes and field counts of complex type extractors and attributes
 * are adjusted to fit the schema. All other expressions are left as-is. This
 * class is motivated by columnar nested schema pruning.
 */
case class ProjectionOverSchema(schema: StructType) {
  private val fieldNames = schema.fieldNames.toSet

  def unapply(expr: Expression): Option[Expression] = getProjection(expr)

  private def getProjection(expr: Expression): Option[Expression] =
    expr match {
      case a @ AttributeReference(name, _, _, _) if (fieldNames.contains(name)) =>
        Some(a.copy(dataType = schema(name).dataType)(a.exprId, a.qualifier))
      case GetArrayItem(child, arrayItemOrdinal) =>
        getProjection(child).map {
          case projection =>
            GetArrayItem(projection, arrayItemOrdinal)
        }
      case GetArrayStructFields(child, StructField(name, _, _, _), _, numFields, containsNull) =>
        getProjection(child).map(p => (p, p.dataType)).map {
          case (projection, ArrayType(projSchema @ StructType(_), _)) =>
            GetArrayStructFields(projection,
              projSchema(name), projSchema.fieldIndex(name), projSchema.size, containsNull)
        }
      case GetMapValue(child, key) =>
        getProjection(child).map {
          case projection =>
            GetMapValue(projection, key)
        }
      case GetStructFieldObject(child, StructField(name, _, _, _)) =>
        getProjection(child).map(p => (p, p.dataType)).map {
          case (projection, projSchema @ StructType(_)) =>
            GetStructField(projection, projSchema.fieldIndex(name))
        }
      case _ =>
        None
    }
}
