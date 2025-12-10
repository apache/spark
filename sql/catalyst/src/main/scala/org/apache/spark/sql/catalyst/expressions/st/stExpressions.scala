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
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.objects._
import org.apache.spark.sql.catalyst.trees._
import org.apache.spark.sql.catalyst.util.{Geography, Geometry, STUtils}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

/**
 * ST expressions are behind a feature flag while the geospatial module is under development.
 */

sealed trait GeospatialInputTypes extends ImplicitCastInputTypes {
  override def checkInputDataTypes(): TypeCheckResult = {
    if (!SQLConf.get.geospatialEnabled) {
      throw new AnalysisException(
        errorClass = "UNSUPPORTED_FEATURE.GEOSPATIAL_DISABLED",
        messageParameters = Map.empty
      )
    }
    super.checkInputDataTypes()
  }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
// This file defines expressions for geospatial operations.
////////////////////////////////////////////////////////////////////////////////////////////////////


// Useful constants for ST expressions.
private[sql] object ExpressionDefaults {
  val DEFAULT_GEOGRAPHY_SRID: Int = Geography.DEFAULT_SRID
  val DEFAULT_GEOMETRY_SRID: Int = Geometry.DEFAULT_SRID
}

/** ST writer expressions. */

/**
 * Returns the input GEOGRAPHY or GEOMETRY value in WKB format.
 * See https://en.wikipedia.org/wiki/Well-known_text_representation_of_geometry#Well-known_binary
 * for more details on the WKB format.
 */
@ExpressionDescription(
  usage = "_FUNC_(geo) - Returns the geospatial value (value of type GEOGRAPHY or GEOMETRY) "
    + "in WKB format.",
  arguments = """
    Arguments:
      * geo - A geospatial value, either a GEOGRAPHY or a GEOMETRY.
  """,
  examples = """
    Examples:
      > SELECT hex(_FUNC_(st_geogfromwkb(X'0101000000000000000000F03F0000000000000040')));
       0101000000000000000000F03F0000000000000040
      > SELECT hex(_FUNC_(st_geomfromwkb(X'0101000000000000000000F03F0000000000000040')));
       0101000000000000000000F03F0000000000000040
  """,
  since = "4.1.0",
  group = "st_funcs"
)
case class ST_AsBinary(geo: Expression)
    extends RuntimeReplaceable
    with GeospatialInputTypes
    with UnaryLike[Expression] {

  override def inputTypes: Seq[AbstractDataType] = Seq(
    TypeCollection(GeographyType, GeometryType)
  )

  override lazy val replacement: Expression = StaticInvoke(
    classOf[STUtils],
    BinaryType,
    "stAsBinary",
    Seq(geo),
    returnNullable = false
  )

  override def prettyName: String = "st_asbinary"

  override def child: Expression = geo

  override protected def withNewChildInternal(newChild: Expression): ST_AsBinary =
    copy(geo = newChild)
}

/** ST reader expressions. */

/**
 * Parses the WKB description of a geography and returns the corresponding GEOGRAPHY value. The SRID
 * value of the returned GEOGRAPHY value is 4326.
 * See https://en.wikipedia.org/wiki/Well-known_text_representation_of_geometry#Well-known_binary
 * for more details on the WKB format.
 */
@ExpressionDescription(
  usage = "_FUNC_(wkb) - Parses the WKB description of a geography and returns the corresponding "
    + "GEOGRAPHY value.",
  arguments = """
    Arguments:
      * wkb - A BINARY value in WKB format, representing a GEOGRAPHY value.
  """,
  examples = """
    Examples:
      > SELECT hex(st_asbinary(_FUNC_(X'0101000000000000000000F03F0000000000000040')));
       0101000000000000000000F03F0000000000000040
  """,
  since = "4.1.0",
  group = "st_funcs"
)
case class ST_GeogFromWKB(wkb: Expression)
    extends RuntimeReplaceable
    with GeospatialInputTypes
    with UnaryLike[Expression] {

  override def inputTypes: Seq[AbstractDataType] = Seq(BinaryType)

  override lazy val replacement: Expression = StaticInvoke(
    classOf[STUtils],
    GeographyType(ExpressionDefaults.DEFAULT_GEOGRAPHY_SRID),
    "stGeogFromWKB",
    Seq(wkb),
    returnNullable = false
  )

  override def prettyName: String = "st_geogfromwkb"

  override def child: Expression = wkb

  override protected def withNewChildInternal(newChild: Expression): ST_GeogFromWKB =
    copy(wkb = newChild)
}

/**
 * Parses the WKB description of a geometry and returns the corresponding GEOMETRY value. The SRID
 * value of the returned GEOMETRY value is 0.
 * See https://en.wikipedia.org/wiki/Well-known_text_representation_of_geometry#Well-known_binary
 * for more details on the WKB format.
 */
@ExpressionDescription(
  usage = "_FUNC_(wkb) - Parses the WKB description of a geometry and returns the corresponding "
    + "GEOMETRY value.",
  arguments = """
    Arguments:
      * wkb - A BINARY value in WKB format, representing a GEOMETRY value.
  """,
  examples = """
    Examples:
      > SELECT hex(st_asbinary(_FUNC_(X'0101000000000000000000F03F0000000000000040')));
       0101000000000000000000F03F0000000000000040
  """,
  since = "4.1.0",
  group = "st_funcs"
)
case class ST_GeomFromWKB(wkb: Expression)
    extends RuntimeReplaceable
    with GeospatialInputTypes
    with UnaryLike[Expression] {

  override def inputTypes: Seq[AbstractDataType] = Seq(BinaryType)

  override lazy val replacement: Expression = StaticInvoke(
    classOf[STUtils],
    GeometryType(ExpressionDefaults.DEFAULT_GEOMETRY_SRID),
    "stGeomFromWKB",
    Seq(wkb),
    returnNullable = false
  )

  override def prettyName: String = "st_geomfromwkb"

  override def child: Expression = wkb

  override protected def withNewChildInternal(newChild: Expression): ST_GeomFromWKB =
    copy(wkb = newChild)
}

/** ST accessor expressions. */

/**
 * Returns the SRID of the input GEOGRAPHY or GEOMETRY value. Returns NULL if the input is NULL.
 * See https://en.wikipedia.org/wiki/Spatial_reference_system#Identifier for more details on the
 * Spatial Reference System Identifier (SRID).
 */
@ExpressionDescription(
  usage = "_FUNC_(geo) - Returns the SRID of the input GEOGRAPHY or GEOMETRY value.",
  arguments = """
    Arguments:
      * geo - A GEOGRAPHY or GEOMETRY value.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(st_geogfromwkb(X'0101000000000000000000F03F0000000000000040'));
       4326
      > SELECT _FUNC_(st_geomfromwkb(X'0101000000000000000000F03F0000000000000040'));
       0
      > SELECT _FUNC_(NULL);
       NULL
  """,
  since = "4.1.0",
  group = "st_funcs"
)
case class ST_Srid(geo: Expression)
    extends RuntimeReplaceable
    with GeospatialInputTypes
    with UnaryLike[Expression] {

  override def inputTypes: Seq[AbstractDataType] = Seq(
    TypeCollection(GeographyType, GeometryType)
  )

  override lazy val replacement: Expression = StaticInvoke(
    classOf[STUtils],
    IntegerType,
    "stSrid",
    Seq(geo),
    returnNullable = false
  )

  override def prettyName: String = "st_srid"

  override def child: Expression = geo

  override protected def withNewChildInternal(newChild: Expression): ST_Srid =
    copy(geo = newChild)
}

/** ST modifier expressions. */

/**
 * Returns a new GEOGRAPHY or GEOMETRY value whose SRID is the specified SRID value.
 */
@ExpressionDescription(
  usage = "_FUNC_(geo, srid) - Returns a new GEOGRAPHY or GEOMETRY value whose SRID is " +
    "the specified SRID value.",
  arguments = """
    Arguments:
      * geo - A GEOGRAPHY or GEOMETRY value.
      * srid - The new SRID value of the geography or geometry.
  """,
  examples = """
    Examples:
      > SELECT st_srid(_FUNC_(ST_GeogFromWKB(X'0101000000000000000000F03F0000000000000040'), 4326));
       4326
      > SELECT st_srid(_FUNC_(ST_GeomFromWKB(X'0101000000000000000000F03F0000000000000040'), 3857));
       3857
  """,
  since = "4.1.0",
  group = "st_funcs"
)
case class ST_SetSrid(geo: Expression, srid: Expression)
    extends RuntimeReplaceable
    with GeospatialInputTypes
    with BinaryLike[Expression] {

  override def inputTypes: Seq[AbstractDataType] =
    Seq(
      TypeCollection(GeographyType, GeometryType),
      IntegerType
    )

  override lazy val replacement: Expression = StaticInvoke(
    classOf[STUtils],
    STExpressionUtils.geospatialTypeWithSrid(geo.dataType, srid),
    "stSetSrid",
    Seq(geo, srid),
    returnNullable = false
  )

  override def prettyName: String = "st_setsrid"

  override def left: Expression = geo

  override def right: Expression = srid

  override protected def withNewChildrenInternal(
      newLeft: Expression,
      newRight: Expression): ST_SetSrid = copy(geo = newLeft, srid = newRight)
}
