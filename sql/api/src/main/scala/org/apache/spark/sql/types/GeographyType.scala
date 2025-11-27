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
import org.apache.spark.sql.internal.types.GeographicSpatialReferenceSystemMapper

/**
 * The data type representing GEOGRAPHY values which are spatial objects, as defined in the Open
 * Geospatial Consortium (OGC) Simple Feature Access specification
 * (https://portal.ogc.org/files/?artifact_id=25355), with a geographic coordinate system.
 */
@Unstable
class GeographyType private (val crs: String, val algorithm: EdgeInterpolationAlgorithm)
    extends AtomicType
    with Serializable {

  /**
   * Spatial Reference Identifier (SRID) value of the geography type.
   */
  val srid: Int = GeographyType.toSrid(crs)

  /**
   * The default size of a value of the GeographyType is 2048 bytes, which can store roughly 120
   * 2D points.
   */
  override def defaultSize: Int = 2048

  /**
   * The GeographyType is a mixed SRID type iff the SRID is MIXED_SRID. Semantically, this means
   * that different SRID values per row are allowed.
   */
  def isMixedSrid: Boolean = srid == GeographyType.MIXED_SRID

  /**
   * Type name that is displayed to users.
   */
  override def typeName: String = {
    if (isMixedSrid) {
      // The mixed SRID type is displayed with a special specifier value "ANY".
      "geography(any)"
    } else {
      // The fixed SRID type is always displayed with the appropriate SRID value.
      s"geography($srid)"
    }
  }

  /**
   * String representation of the GeographyType, which uses SRID for fixed SRID types and "ANY"
   * for mixed SRID types, providing a clear and concise user-friendly format for this type.
   */
  override def toString: String = {
    if (isMixedSrid) {
      // The mixed SRID type is displayed with a special specifier value "ANY".
      "GeographyType(ANY)"
    } else {
      // The fixed SRID type is always displayed with the appropriate SRID value.
      s"GeographyType($srid)"
    }
  }

  /**
   * JSON representation of the GeographyType, which uses the CRS string and edge interpolation
   * algorithm string, in line with the current storage specifications (e.g. Parquet, Delta,
   * Iceberg). Note that mixed SRID is disallowed, and only fixed SRID types can be stored. This
   * is also in accordance to storage formats.
   */
  override def jsonValue: JValue = JString(s"geography($crs, $algorithm)")

  private[spark] override def asNullable: GeographyType = this

  /**
   * Two types are considered equal iff they are both GeographyTypes and have the same type info.
   * For the GEOGRAPHY type, the SRID value and algorithm uniquely identify its type information.
   */
  override def equals(obj: Any): Boolean = {
    obj match {
      case g: GeographyType =>
        // Iff two GeographyTypes have the same SRID and algorithm, they are considered equal.
        g.srid == srid && g.algorithm == algorithm
      case _ =>
        // In all other cases, the two types are considered not equal.
        false
    }
  }

  /**
   * The hash code of the GeographyType is derived from its SRID value.
   */
  override def hashCode(): Int = srid.hashCode

  /**
   * The GeographyType can only accept another type if the other type is also a GeographyType, and
   * the SRID values are compatible (see `acceptsGeographyType` below for more details).
   */
  override private[sql] def acceptsType(other: DataType): Boolean = {
    other match {
      case gt: GeographyType =>
        // For GeographyType, we need to check the SRID values.
        acceptsGeographyType(gt)
      case _ =>
        // In all other cases, the two types are considered different.
        false
    }
  }

  /**
   * The GeographyType with mixed SRID can accept any other GeographyType, i.e. either a fixed
   * SRID GeographyType or another mixed SRID GeographyType. Conversely, a GeographyType with
   * fixed SRID can only accept another GeographyType with the same fixed SRID value, and not a
   * mixed SRID.
   */
  def acceptsGeographyType(gt: GeographyType): Boolean = {
    // If the SRID is mixed, we can accept any other GeographyType.
    // If the SRID is not mixed, we can only accept the same SRID.
    isMixedSrid || gt.srid == srid
  }

  private[sql] def assertSridAllowedForType(otherSrid: Int): Unit = {
    // If SRID is not mixed, SRIDs must match.
    if (!isMixedSrid && otherSrid != srid) {
      throw new SparkRuntimeException(
        errorClass = "GEO_ENCODER_SRID_MISMATCH_ERROR",
        messageParameters = Map(
          "type" -> "GEOGRAPHY",
          "valueSrid" -> otherSrid.toString,
          "typeSrid" -> srid.toString))
    } else if (isMixedSrid) {
      // For fixed SRID geom types, we have a check that value matches the type srid.
      // For mixed SRID we need to do that check explicitly, as MIXED SRID can accept any SRID.
      // However it should accept only valid SRIDs.
      if (!GeographyType.isSridSupported(otherSrid)) {
        throw new SparkIllegalArgumentException(
          errorClass = "ST_INVALID_SRID_VALUE",
          messageParameters = Map("srid" -> otherSrid.toString))
      }
    }
  }
}

@Unstable
object GeographyType extends SpatialType {

  /**
   * Default CRS value for GeographyType depends on storage specification. Parquet and Iceberg use
   * OGC:CRS84, which translates to SRID 4326 here.
   */
  final val GEOGRAPHY_DEFAULT_SRID = 4326
  final val GEOGRAPHY_DEFAULT_CRS = "OGC:CRS84"

  // The default edge interpolation algorithm value for GeographyType.
  final val GEOGRAPHY_DEFAULT_ALGORITHM = EdgeInterpolationAlgorithm.SPHERICAL

  // Another way to represent the default parquet crs value (OGC:CRS84).
  final val GEOGRAPHY_DEFAULT_EPSG_CRS = s"EPSG:$GEOGRAPHY_DEFAULT_SRID"

  /**
   * The default concrete GeographyType in SQL.
   */
  private final lazy val GEOGRAPHY_MIXED_TYPE: GeographyType =
    GeographyType(MIXED_CRS, GEOGRAPHY_DEFAULT_ALGORITHM)

  /** Returns whether the given SRID is supported. */
  def isSridSupported(srid: Int): Boolean = {
    GeographicSpatialReferenceSystemMapper.getStringId(srid) != null
  }

  /**
   * Constructors for GeographyType.
   */
  def apply(srid: Int): GeographyType = {
    val crs = GeographicSpatialReferenceSystemMapper.getStringId(srid)
    if (crs == null) {
      throw new SparkIllegalArgumentException(
        errorClass = "ST_INVALID_SRID_VALUE",
        messageParameters = Map("srid" -> srid.toString))
    }
    new GeographyType(GEOGRAPHY_DEFAULT_CRS, GEOGRAPHY_DEFAULT_ALGORITHM)
  }

  def apply(crs: String): GeographyType = {
    crs match {
      case "ANY" =>
        // Special value "ANY" is used for mixed SRID values.
        // This should be available to users in the Scala API.
        new GeographyType(MIXED_CRS, GEOGRAPHY_DEFAULT_ALGORITHM)
      case _ =>
        // Otherwise, we need to further check the CRS value.
        // This shouldn't be available to users in the Scala API.
        GeographyType(crs, GEOGRAPHY_DEFAULT_ALGORITHM.toString)
    }
  }

  def apply(crs: String, algorithm: String): GeographyType = {
    EdgeInterpolationAlgorithm.fromString(algorithm) match {
      case Some(alg) => GeographyType(crs, alg)
      case None =>
        throw new SparkIllegalArgumentException(
          errorClass = "ST_INVALID_ALGORITHM_VALUE",
          messageParameters = Map("alg" -> algorithm))
    }
  }

  def apply(crs: String, algorithm: EdgeInterpolationAlgorithm): GeographyType = {
    new GeographyType(crs, algorithm)
  }

  override private[sql] def defaultConcreteType: DataType = GEOGRAPHY_MIXED_TYPE

  override private[sql] def acceptsType(other: DataType): Boolean =
    other.isInstanceOf[GeographyType]

  override private[sql] def simpleString: String = "geography"

  /**
   * Converts a CRS string to its corresponding SRID integer value.
   */
  private[types] def toSrid(crs: String): Int = {
    // The special value "SRID:ANY" is used to represent mixed SRID values.
    if (crs.equalsIgnoreCase(GeographyType.MIXED_CRS)) {
      return GeographyType.MIXED_SRID
    }
    // For all other CRS values, we need to look up the corresponding SRID.
    val srid = GeographicSpatialReferenceSystemMapper.getSrid(crs)
    if (srid == null) {
      // If the CRS value is not recognized, we throw an exception.
      throw new SparkIllegalArgumentException(
        errorClass = "ST_INVALID_CRS_VALUE",
        messageParameters = Map("crs" -> crs))
    }
    srid
  }
}

/**
 * Edge interpolation algorithm for Geography logical type. Currently, Spark only supports
 * spherical algorithm.
 */
@Unstable
sealed abstract class EdgeInterpolationAlgorithm

@Unstable
object EdgeInterpolationAlgorithm {
  case object SPHERICAL extends EdgeInterpolationAlgorithm

  val values: Seq[EdgeInterpolationAlgorithm] =
    Seq(SPHERICAL)

  def fromString(s: String): Option[EdgeInterpolationAlgorithm] =
    values.find(_.toString.equalsIgnoreCase(s))
}
