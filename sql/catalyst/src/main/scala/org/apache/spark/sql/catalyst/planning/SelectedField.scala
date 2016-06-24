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
 * A Scala extractor that builds a [[StructField]] from a Catalyst complex type
 * extractor. This is like the opposite of [[ExtractValue#apply]].
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
   * Converts some chain of complex type extractors into a [[StructField]].
   *
   * @param expr the top-level complex type extractor
   * @param fieldOpt the subfield of [[expr]], where relevent
   */
  private def selectField(expr: Expression, fieldOpt: Option[StructField]): Option[StructField] =
    expr match {
      case AttributeReference(name, _, nullable, _) =>
        fieldOpt.map(field => StructField(name, StructType(Array(field)), nullable))
      case GetArrayItem(GetStructField2(child, field @ StructField(name,
          ArrayType(_, arrayNullable), fieldNullable, _)), _) =>
        val childField = fieldOpt.map(field => StructField(name, ArrayType(
          StructType(Array(field)), arrayNullable), fieldNullable)).getOrElse(field)
        selectField(child, Some(childField))
      case GetArrayStructFields(child,
          field @ StructField(name, _, nullable, _), _, _, containsNull) =>
        val childField =
          fieldOpt.map(field => StructField(name, StructType(Array(field)), nullable))
            .getOrElse(field)
        selectField(child, Some(childField)).map {
          case StructField(name,
              StructType(Array(StructField(name2, dataType, nullable2, _))), nullable, _) =>
            StructField(name,
              StructType(Array(StructField(name2,
                ArrayType(dataType, containsNull), nullable2))), nullable)
        }
      case GetMapValue(GetStructField2(child, field @ StructField(name,
          MapType(keyType, _, keyNullable), fieldNullable, _)), _) =>
        val childField = fieldOpt.map(field => StructField(name, MapType(keyType,
          StructType(Array(field)), keyNullable), fieldNullable)).getOrElse(field)
        selectField(child, Some(childField))
      case GetStructField2(child, field @ StructField(name, _, nullable, _)) =>
        val childField = fieldOpt.map(field => StructField(name,
          StructType(Array(field)), nullable)).getOrElse(field)
        selectField(child, Some(childField))
      case _ =>
        None
    }
}
