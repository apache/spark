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

package org.apache.spark.sql.catalyst.expressions.st

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

private[sql] object STExpressionUtils {

  /**
   * Checks if the given data type is a geospatial type (i.e. GeometryType or GeographyType).
   */
  def isGeoSpatialType(dt: DataType): Boolean = dt match {
    case _: GeometryType | _: GeographyType => true
    case _ => false
  }

  /**
   * Returns the input GEOMETRY or GEOGRAPHY value with the specified SRID. Only geospatial types
   * are allowed as the source type, and calls are delegated to the corresponding helper methods.
   */
  def geospatialTypeWithSrid(sourceType: DataType, srid: Expression): DataType = {
    sourceType match {
      case _ if !SQLConf.get.geospatialEnabled =>
        throw new AnalysisException(
          errorClass = "UNSUPPORTED_FEATURE.GEOSPATIAL_DISABLED",
          messageParameters = Map.empty
        )
      case _: GeometryType =>
        geometryTypeWithSrid(srid)
      case _: GeographyType =>
        geographyTypeWithSrid(srid)
      case _ =>
        throw new IllegalArgumentException(s"Unexpected data type: $sourceType.")
    }
  }

  /**
   * Returns the input GEOMETRY value with the specified SRID. If the SRID expression is a literal,
   * the SRID value can be directly extracted. Otherwise, only the mixed SRID value can be used.
   */
  private def geometryTypeWithSrid(srid: Expression): GeometryType = {
    srid match {
      case Literal(sridValue: Int, IntegerType) =>
        // If the SRID expression is a literal, the SRID value can be directly extracted.
        GeometryType(sridValue)
      case _ =>
        // Otherwise, only the mixed SRID value can be used for the output GEOMETRY value.
        GeometryType("ANY")
    }
  }

  /**
   * Returns the input GEOGRAPHY value with the specified SRID. If the SRID expression is a literal,
   * the SRID value can be directly extracted. Otherwise, only the mixed SRID value can be used.
   */
  private def geographyTypeWithSrid(srid: Expression): GeographyType = {
    srid match {
      case Literal(sridValue: Int, IntegerType) =>
        // If the SRID expression is a literal, the SRID value can be directly extracted.
        GeographyType(sridValue)
      case _ =>
        // Otherwise, only the mixed SRID value can be used for the output GEOMETRY value.
        GeographyType("ANY")
    }
  }

}
