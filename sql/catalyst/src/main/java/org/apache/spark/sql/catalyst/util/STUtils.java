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
package org.apache.spark.sql.catalyst.util;

import org.apache.spark.sql.errors.QueryExecutionErrors;
import org.apache.spark.sql.types.GeographyType;
import org.apache.spark.sql.types.GeometryType;
import org.apache.spark.unsafe.types.GeographyVal;
import org.apache.spark.unsafe.types.GeometryVal;

// This class defines static methods that used to implement ST expressions using `StaticInvoke`.
public final class STUtils {

  /** Conversion methods from physical values to Geography/Geometry objects. */

  // Converts a GEOGRAPHY from its physical value to the corresponding `Geography` object
  static Geography fromPhysVal(GeographyVal value) {
    return Geography.fromBytes(value.getBytes());
  }

  // Converts a GEOMETRY from its physical value to the corresponding `Geometry` object
  static Geometry fromPhysVal(GeometryVal value) {
    return Geometry.fromBytes(value.getBytes());
  }

  /** Conversion methods from Geography/Geometry objects to physical values. */

  // Converts a `Geography` object to the corresponding GEOGRAPHY physical value.
  static GeographyVal toPhysVal(Geography g) {
    return g.getValue();
  }

  // Converts a `Geometry` object to the corresponding GEOMETRY physical value.
  static GeometryVal toPhysVal(Geometry g) {
    return g.getValue();
  }

  /** Geospatial type casting utility methods. */

  // Cast geography to geometry.
  public static GeometryVal geographyToGeometry(GeographyVal geographyVal) {
    // Geographic SRID is always a valid SRID for geometry, so we don't need to check it.
    // Also, all geographic coordinates are valid for geometry, so no need to check bounds.
    return toPhysVal(Geometry.fromBytes(geographyVal.getBytes()));
  }

  /** Geospatial type encoder/decoder utilities. */

  public static GeometryVal serializeGeomFromWKB(org.apache.spark.sql.types.Geometry geometry,
      GeometryType gt) {
    int geometrySrid = geometry.getSrid();
    gt.assertSridAllowedForType(geometrySrid);
    return toPhysVal(Geometry.fromWkb(geometry.getBytes(), geometrySrid));
  }

  public static GeographyVal serializeGeogFromWKB(org.apache.spark.sql.types.Geography geography,
      GeographyType gt) {
    int geographySrid = geography.getSrid();
    gt.assertSridAllowedForType(geographySrid);
    return toPhysVal(Geography.fromWkb(geography.getBytes(), geographySrid));
  }

  public static org.apache.spark.sql.types.Geometry deserializeGeom(
      GeometryVal geometry, GeometryType gt) {
    int geometrySrid = stSrid(geometry);
    gt.assertSridAllowedForType(geometrySrid);
    byte[] wkb = stAsBinary(geometry);
    return org.apache.spark.sql.types.Geometry.fromWKB(wkb, geometrySrid);
  }

  public static org.apache.spark.sql.types.Geography deserializeGeog(
      GeographyVal geography, GeographyType gt) {
    int geographySrid = stSrid(geography);
    gt.assertSridAllowedForType(geographySrid);
    byte[] wkb = stAsBinary(geography);
    return org.apache.spark.sql.types.Geography.fromWKB(wkb, geographySrid);
  }

  /** Methods for implementing ST expressions. */

  // ST_AsBinary
  public static byte[] stAsBinary(GeographyVal geo) {
    return fromPhysVal(geo).toWkb();
  }

  public static byte[] stAsBinary(GeometryVal geo) {
    return fromPhysVal(geo).toWkb();
  }

  // ST_GeogFromWKB
  public static GeographyVal stGeogFromWKB(byte[] wkb) {
    return toPhysVal(Geography.fromWkb(wkb));
  }

  // ST_GeomFromWKB
  public static GeometryVal stGeomFromWKB(byte[] wkb) {
    return toPhysVal(Geometry.fromWkb(wkb));
  }

  public static GeometryVal stGeomFromWKB(byte[] wkb, int srid) {
    return toPhysVal(Geometry.fromWkb(wkb, srid));
  }

  // ST_SetSrid
  public static GeographyVal stSetSrid(GeographyVal geo, int srid) {
    // We only allow setting the SRID to geographic values.
    if(!GeographyType.isSridSupported(srid)) {
      throw QueryExecutionErrors.stInvalidSridValueError(srid);
    }
    // Create a copy of the input geography.
    Geography copy = fromPhysVal(geo).copy();
    // Set the SRID of the copy to the specified value.
    copy.setSrid(srid);
    return toPhysVal(copy);
  }

  public static GeometryVal stSetSrid(GeometryVal geo, int srid) {
    // We only allow setting the SRID to valid values.
    if(!GeometryType.isSridSupported(srid)) {
      throw QueryExecutionErrors.stInvalidSridValueError(srid);
    }
    // Create a copy of the input geometry.
    Geometry copy = fromPhysVal(geo).copy();
    // Set the SRID of the copy to the specified value.
    copy.setSrid(srid);
    return toPhysVal(copy);
  }

  // ST_Srid
  public static int stSrid(GeographyVal geog) {
    return fromPhysVal(geog).srid();
  }

  public static int stSrid(GeometryVal geom) {
    return fromPhysVal(geom).srid();
  }

}
