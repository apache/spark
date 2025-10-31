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

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.objects._
import org.apache.spark.sql.catalyst.trees._
import org.apache.spark.sql.catalyst.util.{Geography, Geometry, STUtils}
import org.apache.spark.sql.types._


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
    with ImplicitCastInputTypes
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
    with ImplicitCastInputTypes
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
    with ImplicitCastInputTypes
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
