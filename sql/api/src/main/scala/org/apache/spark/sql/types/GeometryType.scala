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

package org.apache.spark.sql.types

import org.json4s.JsonAST.{JString, JValue}

import org.apache.spark.{SparkIllegalArgumentException, SparkRuntimeException}
import org.apache.spark.annotation.Unstable
import org.apache.spark.sql.internal.types.CartesianSpatialReferenceSystemMapper

/**
 * The data type representing GEOMETRY values which are spatial objects, as defined in the Open
 * Geospatial Consortium (OGC) Simple Feature Access specification
 * (https://portal.ogc.org/files/?artifact_id=25355), with a Cartesian coordinate system.
 */
@Unstable
class GeometryType private (val crs: String) extends AtomicType with Serializable {

  /**
   * Spatial Reference Identifier (SRID) value of the geometry type.
   */
  val srid: Int = GeometryType.toSrid(crs)

  /**
   * The default size of a value of the GeometryType is 2048 bytes, which can store roughly 120 2D
   * points.
   */
  override def defaultSize: Int = 2048

  /**
   * The GeometryType is a mixed SRID type iff the SRID is MIXED_SRID. Semantically, this means
   * that different SRID values per row are allowed.
   */
  def isMixedSrid: Boolean = srid == GeometryType.MIXED_SRID

  /**
   * Type name that is displayed to users.
   */
  override def typeName: String = {
    if (isMixedSrid) {
      // The mixed SRID type is displayed with a special specifier value "ANY".
      "geometry(any)"
    } else {
      // The fixed SRID type is always displayed with the appropriate SRID value.
      s"geometry($srid)"
    }
  }

  /**
   * String representation of the GeometryType, which uses SRID for fixed SRID types and "ANY" for
   * mixed SRID types, providing a clear and concise user-friendly format for this type.
   */
  override def toString: String = {
    if (isMixedSrid) {
      // The mixed SRID type is displayed with a special specifier value "ANY".
      "GeometryType(ANY)"
    } else {
      // The fixed SRID type is always displayed with the appropriate SRID value.
      s"GeometryType($srid)"
    }
  }

  /**
   * JSON representation of the GeometryType, which uses the CRS string, in line with the current
   * storage specifications (e.g. Parquet, Delta, Iceberg). Note that mixed SRID is disallowed,
   * and only fixed SRID types can be stored. This is also in accordance to storage formats.
   */
  override def jsonValue: JValue = JString(s"geometry($crs)")

  private[spark] override def asNullable: GeometryType = this

  /**
   * Two types are considered equal iff they are both GeometryTypes and have the same SRID value.
   * For the GEOMETRY type, the SRID value uniquely identifies its type information.
   */
  override def equals(obj: Any): Boolean = {
    obj match {
      case g: GeometryType =>
        // Iff two GeometryTypes have the same SRID, they are considered equal.
        g.srid == srid
      case _ =>
        // In all other cases, the two types are considered not equal.
        false
    }
  }

  /**
   * The hash code of the GeometryType is derived from its SRID value.
   */
  override def hashCode(): Int = srid.hashCode

  /**
   * The GeometryType can only accept another type if the other type is also a GeometryType, and
   * the SRID values are compatible (see `acceptsGeometryType` below for more details).
   */
  override private[sql] def acceptsType(other: DataType): Boolean = {
    other match {
      case gt: GeometryType =>
        // For GeometryType, we need to check the SRID values.
        acceptsGeometryType(gt)
      case _ =>
        // In all other cases, the two types are considered different.
        false
    }
  }

  /**
   * The GeometryType with a mixed SRID can accept any other GeometryType, i.e. either a fixed
   * SRID GeometryType or another mixed SRID GeometryType. Conversely, a GeometryType with a fixed
   * SRID can only accept another GeometryType with the same fixed SRID value, and not a mixed
   * SRID.
   */
  def acceptsGeometryType(gt: GeometryType): Boolean = {
    // If the SRID is mixed, we can accept any other GeometryType.
    // If the SRID is not mixed, we can only accept the same SRID.
    isMixedSrid || gt.srid == srid
  }

  private[sql] def assertSridAllowedForType(otherSrid: Int): Unit = {
    // If SRID is not mixed, SRIDs must match.
    if (!isMixedSrid && otherSrid != srid) {
      throw new SparkRuntimeException(
        errorClass = "GEO_ENCODER_SRID_MISMATCH_ERROR",
        messageParameters = Map(
          "type" -> "GEOMETRY",
          "valueSrid" -> otherSrid.toString,
          "typeSrid" -> srid.toString))
    } else if (isMixedSrid) {
      // For fixed SRID geom types, we have a check that value matches the type srid.
      // For mixed SRID we need to do that check explicitly, as MIXED SRID can accept any SRID.
      // However it should accept only valid SRIDs.
      if (!GeometryType.isSridSupported(otherSrid)) {
        throw new SparkIllegalArgumentException(
          errorClass = "ST_INVALID_SRID_VALUE",
          messageParameters = Map("srid" -> otherSrid.toString))
      }
    }
  }
}

@Unstable
object GeometryType extends SpatialType {

  /**
   * The default coordinate reference system (CRS) value used for geometries, as specified by the
   * Parquet, Delta, and Iceberg specifications. If crs is omitted, it should always default to
   * this.
   */
  final val GEOMETRY_DEFAULT_SRID = 4326
  final val GEOMETRY_DEFAULT_CRS = "OGC:CRS84"

  /**
   * The default concrete GeometryType in SQL.
   */
  private final lazy val GEOMETRY_MIXED_TYPE: GeometryType =
    GeometryType(MIXED_CRS)

  /** Returns whether the given SRID is supported. */
  def isSridSupported(srid: Int): Boolean = {
    CartesianSpatialReferenceSystemMapper.getStringId(srid) != null
  }

  /**
   * Constructors for GeometryType.
   */
  def apply(srid: Int): GeometryType = {
    val crs = CartesianSpatialReferenceSystemMapper.getStringId(srid)
    if (crs == null) {
      throw new SparkIllegalArgumentException(
        errorClass = "ST_INVALID_SRID_VALUE",
        messageParameters = Map("srid" -> srid.toString))
    }
    new GeometryType(crs)
  }

  def apply(crs: String): GeometryType = {
    crs match {
      case "ANY" =>
        // Special value "ANY" is used for mixed SRID values.
        // This should be available to users in the Scala API.
        new GeometryType(MIXED_CRS)
      case _ =>
        // Otherwise, we need to further check the CRS value.
        // This shouldn't be available to users in the Scala API.
        new GeometryType(crs)
    }
  }

  override private[sql] def defaultConcreteType: DataType = GEOMETRY_MIXED_TYPE

  override private[sql] def acceptsType(other: DataType): Boolean =
    other.isInstanceOf[GeometryType]

  override private[sql] def simpleString: String = "geometry"

  /**
   * Converts a CRS string to its corresponding SRID integer value.
   */
  private[types] def toSrid(crs: String): Int = {
    // The special value "SRID:ANY" is used to represent mixed SRID values.
    if (crs.equalsIgnoreCase(GeometryType.MIXED_CRS)) {
      return GeometryType.MIXED_SRID
    }
    // For all other CRS values, we need to look up the corresponding SRID.
    val srid = CartesianSpatialReferenceSystemMapper.getSrid(crs)
    if (srid == null) {
      // If the CRS value is not recognized, we throw an exception.
      throw new SparkIllegalArgumentException(
        errorClass = "ST_INVALID_CRS_VALUE",
        messageParameters = Map("crs" -> crs))
    }
    srid
  }
}
