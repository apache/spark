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

import org.apache.spark.sql.catalyst.util.geo.WkbParseException;
import org.apache.spark.sql.catalyst.util.geo.WkbReader;
import org.apache.spark.sql.errors.QueryExecutionErrors;
import org.apache.spark.sql.types.GeographyType;
import org.apache.spark.sql.types.GeometryType;
import org.apache.spark.unsafe.types.BinaryView;
import org.apache.spark.unsafe.types.UTF8String;

import java.nio.ByteOrder;

// This class defines static methods that used to implement ST expressions using `StaticInvoke`.
public final class STUtils {

  /** Conversion methods from physical values to Geography/Geometry objects. */

  // Converts a GEOGRAPHY physical value to the corresponding `Geography` object. The returned
  // Geography may share its backing array with {@code value} (see {@link BinaryView#getBytes}),
  // so callers that need to mutate it (e.g. via `setSrid`) must first call `Geography.copy()`.
  static Geography geogFromPhysVal(BinaryView value) {
    return Geography.fromBytes(value.getBytes());
  }

  // Converts a GEOMETRY physical value to the corresponding `Geometry` object. The returned
  // Geometry may share its backing array with {@code value} (see {@link BinaryView#getBytes}),
  // so callers that need to mutate it (e.g. via `setSrid`) must first call `Geometry.copy()`.
  static Geometry geomFromPhysVal(BinaryView value) {
    return Geometry.fromBytes(value.getBytes());
  }

  /** Conversion methods from Geography/Geometry objects to physical values. */

  // Converts a `Geography` object to its physical GEOGRAPHY value.
  static BinaryView toPhysVal(Geography g) {
    return g.getValue();
  }

  // Converts a `Geometry` object to its physical GEOMETRY value.
  static BinaryView toPhysVal(Geometry g) {
    return g.getValue();
  }

  /** Geospatial type casting utility methods. */

  // Cast geometry to geography.
  public static BinaryView geometryToGeography(BinaryView geometryVal) {
    // We first need to check whether the input geometry has a geographic SRID.
    int srid = stGeomSrid(geometryVal);
    if(!GeographyType.isSridSupported(srid)) {
      throw QueryExecutionErrors.stInvalidSridValueError(String.valueOf(srid));
    }
    // We also need to check whether the input geometry has coordinates in geography bounds.
    try {
      byte[] wkb = stGeomAsBinary(geometryVal);
      new WkbReader(true).read(wkb, srid);
    } catch (WkbParseException e) {
      throw QueryExecutionErrors.wkbParseError(e.getParseError(), e.getPosition());
    }
    return toPhysVal(Geography.fromBytes(geometryVal.getBytes()));
  }

  // Cast geography to geometry.
  public static BinaryView geographyToGeometry(BinaryView geographyVal) {
    // Geographic SRID is always a valid SRID for geometry, so we don't need to check it.
    // Also, all geographic coordinates are valid for geometry, so no need to check bounds.
    return toPhysVal(Geometry.fromBytes(geographyVal.getBytes()));
  }

  /** Geospatial type encoder/decoder utilities. */

  public static BinaryView serializeGeomFromWKB(org.apache.spark.sql.types.Geometry geometry,
      GeometryType gt) {
    int geometrySrid = geometry.getSrid();
    gt.assertSridAllowedForType(geometrySrid);
    return toPhysVal(Geometry.fromWkb(geometry.getBytes(), geometrySrid));
  }

  public static BinaryView serializeGeogFromWKB(org.apache.spark.sql.types.Geography geography,
      GeographyType gt) {
    int geographySrid = geography.getSrid();
    gt.assertSridAllowedForType(geographySrid);
    return toPhysVal(Geography.fromWkb(geography.getBytes(), geographySrid));
  }

  public static org.apache.spark.sql.types.Geometry deserializeGeom(
      BinaryView geometry, GeometryType gt) {
    int geometrySrid = stGeomSrid(geometry);
    gt.assertSridAllowedForType(geometrySrid);
    byte[] wkb = stGeomAsBinary(geometry);
    return org.apache.spark.sql.types.Geometry.fromWKB(wkb, geometrySrid);
  }

  public static org.apache.spark.sql.types.Geography deserializeGeog(
      BinaryView geography, GeographyType gt) {
    int geographySrid = stGeogSrid(geography);
    gt.assertSridAllowedForType(geographySrid);
    byte[] wkb = stGeogAsBinary(geography);
    return org.apache.spark.sql.types.Geography.fromWKB(wkb, geographySrid);
  }

  /** Methods for implementing ST expressions.
   *
   * The ST_AsBinary, ST_AsEWKT, ST_Srid, and ST_SetSrid expression families each have a
   * {@code stGeog*} and a {@code stGeom*} variant. The variants exist for
   * {@link org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke} dispatch only:
   * the GEOMETRY/GEOGRAPHY physical type is the same opaque byte chunk, but the WKB
   * validation rules and SRID checks differ, so the ST expressions pick the variant from
   * the input's logical {@link org.apache.spark.sql.types.DataType}.
   */

  private static ByteOrder parseEndianness(UTF8String endianness) {
    String endiannessString = endianness.toString();
    if (endiannessString.equalsIgnoreCase("NDR")) return ByteOrder.LITTLE_ENDIAN;
    if (endiannessString.equalsIgnoreCase("XDR")) return ByteOrder.BIG_ENDIAN;
    throw QueryExecutionErrors.stInvalidArgumentErrorInvalidEndiannessValue(endiannessString);
  }

  // ST_AsBinary
  public static byte[] stGeogAsBinary(BinaryView geo) {
    return geogFromPhysVal(geo).toWkb(ByteOrder.LITTLE_ENDIAN);
  }

  public static byte[] stGeogAsBinary(BinaryView geo, UTF8String endianness) {
    return geogFromPhysVal(geo).toWkb(parseEndianness(endianness));
  }

  public static byte[] stGeomAsBinary(BinaryView geo) {
    return geomFromPhysVal(geo).toWkb(ByteOrder.LITTLE_ENDIAN);
  }

  public static byte[] stGeomAsBinary(BinaryView geo, UTF8String endianness) {
    return geomFromPhysVal(geo).toWkb(parseEndianness(endianness));
  }

  // ST_AsEWKT
  public static UTF8String stGeogAsEwkt(BinaryView geo) {
    return UTF8String.fromBytes(geogFromPhysVal(geo).toEwkt());
  }

  public static UTF8String stGeomAsEwkt(BinaryView geo) {
    return UTF8String.fromBytes(geomFromPhysVal(geo).toEwkt());
  }

  // ST_GeogFromWKB
  public static BinaryView stGeogFromWKB(byte[] wkb) {
    return toPhysVal(Geography.fromWkb(wkb));
  }

  public static BinaryView stGeogFromWKB(byte[] wkb, int srid) {
    // We only allow setting the SRID to geographic values.
    if(!GeographyType.isSridSupported(srid)) {
      throw QueryExecutionErrors.stInvalidSridValueError(srid);
    }
    return toPhysVal(Geography.fromWkb(wkb, srid));
  }

  // ST_GeomFromWKB
  public static BinaryView stGeomFromWKB(byte[] wkb) {
    return toPhysVal(Geometry.fromWkb(wkb));
  }

  public static BinaryView stGeomFromWKB(byte[] wkb, int srid) {
    // We only allow setting the SRID to valid values.
    if(!GeometryType.isSridSupported(srid)) {
      throw QueryExecutionErrors.stInvalidSridValueError(srid);
    }
    return toPhysVal(Geometry.fromWkb(wkb, srid));
  }

  // ST_SetSrid
  public static BinaryView stGeogSetSrid(BinaryView geo, int srid) {
    // We only allow setting the SRID to geographic values.
    if(!GeographyType.isSridSupported(srid)) {
      throw QueryExecutionErrors.stInvalidSridValueError(srid);
    }
    // Create a copy of the input geography.
    Geography copy = geogFromPhysVal(geo).copy();
    // Set the SRID of the copy to the specified value.
    copy.setSrid(srid);
    return toPhysVal(copy);
  }

  public static BinaryView stGeomSetSrid(BinaryView geo, int srid) {
    // We only allow setting the SRID to valid values.
    if(!GeometryType.isSridSupported(srid)) {
      throw QueryExecutionErrors.stInvalidSridValueError(srid);
    }
    // Create a copy of the input geometry.
    Geometry copy = geomFromPhysVal(geo).copy();
    // Set the SRID of the copy to the specified value.
    copy.setSrid(srid);
    return toPhysVal(copy);
  }

  // ST_Srid
  public static int stGeogSrid(BinaryView geog) {
    return geogFromPhysVal(geog).srid();
  }

  public static int stGeomSrid(BinaryView geom) {
    return geomFromPhysVal(geom).srid();
  }

}
