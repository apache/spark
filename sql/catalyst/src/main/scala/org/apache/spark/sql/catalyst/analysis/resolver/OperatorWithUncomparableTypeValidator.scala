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

package org.apache.spark.sql.catalyst.analysis.resolver

import com.databricks.sql.DatabricksSQLConf

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{Distinct, LogicalPlan, SetOperation}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, GeographyType, GeometryType, MapType, VariantType}

/**
 * [[OperatorWithUncomparableTypeValidator]] performs the validation of a logical plan to ensure
 * that it (if it is [[Distinct]] or [[SetOperation]]) does not contain any uncomparable types:
 * [[VariantType]], [[MapType]], [[GeometryType]] or [[GeographyType]].
 */
object OperatorWithUncomparableTypeValidator {

  /**
   * Validates that the provided logical plan does not contain any uncomparable types:
   * [[VariantType]], [[MapType]], [[GeometryType]] or [[GeographyType]] (throws a specific
   * user-facing error if it does). Operators that are not supported are [[Distinct]] and
   * [[SetOperation]] ([[Union]], [[Except]], [[Intersect]]).
   */
  def validate(operator: LogicalPlan, output: Seq[Attribute]): Unit = {
    operator match {
      case unsupportedOperator @ (_: SetOperation | _: Distinct) =>
        val enableUndefinedVariantGroupingBehavior =
          SQLConf.get.getConf(DatabricksSQLConf.ENABLE_UNDEFINED_VARIANT_GROUPING_BEHAVIOR)

        output.foreach { element =>
          if (hasMapType(element.dataType)) {
            throwUnsupportedSetOperationOnMapType(element, unsupportedOperator)
          }

          if (!enableUndefinedVariantGroupingBehavior && hasVariantType(element.dataType)) {
            throwUnsupportedSetOperationOnVariantType(element, unsupportedOperator)
          }
          // BEGIN-EDGE

          if (hasGeoType(element.dataType)) {
            throwUnsupportedOperationForGeoType(element, unsupportedOperator.nodeName)
          }
        // END-EDGE
        }
      case _ =>
    }
  }

  private def hasMapType(dt: DataType): Boolean = {
    dt.existsRecursively(_.isInstanceOf[MapType])
  }

  private def hasVariantType(dt: DataType): Boolean = {
    dt.existsRecursively(_.isInstanceOf[VariantType])
  }
  // BEGIN-EDGE

  private def hasGeoType(dt: DataType): Boolean = {
    dt.existsRecursively(dt => dt.isInstanceOf[GeometryType] || dt.isInstanceOf[GeographyType])
  }
  // END-EDGE

  private def throwUnsupportedSetOperationOnMapType(
      mapCol: Attribute,
      unresolvedPlan: LogicalPlan): Unit = {
    throw QueryCompilationErrors.unsupportedSetOperationOnMapType(
      mapCol = mapCol,
      origin = unresolvedPlan.origin
    )
  }

  private def throwUnsupportedSetOperationOnVariantType(
      variantCol: Attribute,
      unresolvedPlan: LogicalPlan): Unit = {
    throw QueryCompilationErrors.unsupportedSetOperationOnVariantType(
      variantCol = variantCol,
      origin = unresolvedPlan.origin
    )
  }
  // BEGIN-EDGE

  private def throwUnsupportedOperationForGeoType(col: Attribute, operatorName: String): Unit = {
    throw QueryCompilationErrors.unsupportedOperationForGeoType(
      dataType = col.dataType,
      operation = operatorName
    )
  }
  // END-EDGE
}
