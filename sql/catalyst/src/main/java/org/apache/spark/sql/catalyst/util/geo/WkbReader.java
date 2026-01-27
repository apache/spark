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
package org.apache.spark.sql.catalyst.util.geo;


import org.apache.spark.sql.catalyst.util.Geometry;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

/**
 * Reader for parsing Well-Known Binary (WKB) format geometries.
 * This class implements the OGC Simple Features specification for WKB parsing.
 * This class is not thread-safe. Create a new instance for each thread.
 * This class should be catalyst-internal.
 */
public class WkbReader {
  private ByteBuffer buffer;
  private final int validationLevel;
  private byte[] currentWkb;

  /**
   * Constructor for WkbReader with default validation level (1 = basic validation).
   */
  public WkbReader() {
    this(1);
  }

  /**
   * Constructor for WkbReader with specified validation level.
   * @param validationLevel validation level (0 = no validation, 1 = basic validation)
   */
  public WkbReader(int validationLevel) {
    this.validationLevel = validationLevel;
  }

  // ========== Coordinate Validation Helpers ==========

  /**
   * Returns true if the coordinate value is valid for a non-empty point.
   * A valid coordinate is finite (not NaN and not Infinity).
   */
  private static boolean isValidCoordinate(double value) {
    return Double.isFinite(value);
  }

  /**
   * Returns true if the coordinate value is valid for a point that may be empty.
   * A valid coordinate is either finite or NaN (for empty points).
   * Infinity values are not allowed.
   */
  private static boolean isValidCoordinateAllowEmpty(double value) {
    return Double.isFinite(value) || Double.isNaN(value);
  }

  /**
   * Reads a geometry from WKB bytes.
   */
  public GeometryModel read(byte[] wkb) {
    try {
      currentWkb = wkb;
      return readGeometry(Geometry.DEFAULT_SRID);
    } finally {
      // Clear references to allow garbage collection
      buffer = null;
      currentWkb = null;
    }
  }

  /**
   * Reads a geometry from WKB bytes with a specified SRID.
   */
  public GeometryModel read(byte[] wkb, int srid) {
    try {
      currentWkb = wkb;
      return readGeometry(srid);
    } finally {
      // Clear references to allow garbage collection
      buffer = null;
      currentWkb = null;
    }
  }

  private void checkNotAtEnd(long pos) {
    if (buffer.position() >= buffer.limit()) {
      throw new WkbParseException("Unexpected end of WKB buffer", pos, currentWkb);
    }
  }

  private ByteOrder readEndianness() {
    checkNotAtEnd(buffer.position());
    byte endianValue = buffer.get();
    if (endianValue != WkbUtil.BIG_ENDIAN && endianValue != WkbUtil.LITTLE_ENDIAN) {
      throw new WkbParseException("Invalid byte order " + endianValue, buffer.position() - 1,
        currentWkb);
    }
    return endianValue == WkbUtil.LITTLE_ENDIAN ?
      ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN;
  }

  private int readInt() {
    if (buffer.remaining() < WkbUtil.INT_SIZE) {
      throw new WkbParseException("Unexpected end of WKB buffer", buffer.position(), currentWkb);
    }
    return buffer.getInt();
  }

  /**
   * Reads a double coordinate value, allowing NaN for empty points.
   */
  private double readDoubleAllowEmpty() {
    if (buffer.remaining() < WkbUtil.DOUBLE_SIZE) {
      throw new WkbParseException("Unexpected end of WKB buffer", buffer.position(), currentWkb);
    }
    double value = buffer.getDouble();
    if (!isValidCoordinateAllowEmpty(value)) {
      throw new WkbParseException("Invalid coordinate value found", buffer.position() - 8,
        currentWkb);
    }
    return value;
  }

  /**
   * Reads a double coordinate value, not allowing NaN (for non-point coordinates like rings).
   */
  private double readDoubleNoEmpty() {
    if (buffer.remaining() < WkbUtil.DOUBLE_SIZE) {
      throw new WkbParseException("Unexpected end of WKB buffer", buffer.position(), currentWkb);
    }
    double value = buffer.getDouble();
    if (!isValidCoordinate(value)) {
      throw new WkbParseException("Invalid coordinate value found", buffer.position() - 8,
        currentWkb);
    }
    return value;
  }

  /**
   * Reads a geometry from WKB bytes with a specified SRID.
   *
   * @param defaultSrid srid to use if not specified in WKB
   * @return Geometry object
   */
  private GeometryModel readGeometry(int defaultSrid) {
    // Check that we have data
    if (currentWkb == null || currentWkb.length < 1) {
      throw new WkbParseException("WKB data is empty or null", 0, currentWkb);
    }

    // Check that we have enough bytes for header (endianness byte + 4-byte type)
    if (currentWkb.length < WkbUtil.BYTE_SIZE + WkbUtil.TYPE_SIZE) {
      throw new WkbParseException("WKB data too short", 0, currentWkb);
    }

    // Create buffer wrapping the entire byte array
    buffer = ByteBuffer.wrap(currentWkb);

    // Read endianness and set byte order
    ByteOrder byteOrder = readEndianness();
    buffer.order(byteOrder);

    return readGeometryInternal(defaultSrid);
  }

  /**
   * Reads a nested geometry and validates that its dimensions match the parent's dimensions.
   * The check happens immediately after reading the type, before reading coordinate data.
   */
  private GeometryModel readNestedGeometryWithDimensionCheck(
      int defaultSrid,
      boolean expectedHasZ,
      boolean expectedHasM) {
    return readNestedGeometryInternal(defaultSrid, true, expectedHasZ, expectedHasM);
  }

  /**
   * Internal method to read a nested geometry with optional dimension validation.
   */
  private GeometryModel readNestedGeometryInternal(
      int defaultSrid,
      boolean validateDimensions,
      boolean expectedHasZ,
      boolean expectedHasM) {
    // Read endianness from the current buffer position
    ByteOrder byteOrder = readEndianness();

    // Save the current byte order and temporarily set to the nested geometry's byte order
    ByteOrder savedByteOrder = buffer.order();
    buffer.order(byteOrder);
    long typeStartPos = buffer.position();

    // Read type and dimension
    int typeAndDim = readInt();

    // Parse WKB format (geotype and dimension)
    if (!WkbUtil.isValidWkbType(typeAndDim)) {
      throw new WkbParseException("Invalid or unsupported type " + typeAndDim, typeStartPos,
        currentWkb);
    }

    GeoTypeId geoType = WkbUtil.getBaseType(typeAndDim);
    int dimensionCount = WkbUtil.getDimensionCount(typeAndDim);
    boolean hasZ = WkbUtil.hasZ(typeAndDim);
    boolean hasM = WkbUtil.hasM(typeAndDim);

    // Validate dimensions match parent if required
    if (validateDimensions && (hasZ != expectedHasZ || hasM != expectedHasM)) {
      throw new WkbParseException(
          "Invalid or unsupported type " + typeAndDim, typeStartPos, currentWkb);
    }

    // Dispatch to appropriate reader based on geometry type
    GeometryModel result = readGeometryData(geoType, defaultSrid, dimensionCount, hasZ, hasM,
        typeStartPos);

    // Restore the saved byte order
    buffer.order(savedByteOrder);

    return result;
  }

  /**
   * Internal method to read geometry with optional endianness reading.
   *
   * @param defaultSrid srid to use if not specified in WKB
   * @return Geometry object
   */
  private GeometryModel readGeometryInternal(int defaultSrid) {
    long typeStartPos = buffer.position();

    // Read type and dimension
    int typeAndDim = readInt();

    GeoTypeId geoType;
    int dimensionCount;

    // Parse WKB format (geotype and dimension)
    if (!WkbUtil.isValidWkbType(typeAndDim)) {
      throw new WkbParseException("Invalid or unsupported type " + typeAndDim, typeStartPos,
        currentWkb);
    }

    geoType = WkbUtil.getBaseType(typeAndDim);
    dimensionCount = WkbUtil.getDimensionCount(typeAndDim);
    boolean hasZ = WkbUtil.hasZ(typeAndDim);
    boolean hasM = WkbUtil.hasM(typeAndDim);

    // Dispatch to appropriate reader based on geometry type
    return readGeometryData(geoType, defaultSrid, dimensionCount, hasZ, hasM,
        typeStartPos);
  }

  /**
   * Reads geometry data based on the geometry type.
   */
  private GeometryModel readGeometryData(
      GeoTypeId geoType,
      int defaultSrid,
      int dimensionCount,
      boolean hasZ,
      boolean hasM,
      long typeStartPos) {
    switch (geoType) {
      case POINT:
        return readPoint(defaultSrid, dimensionCount, hasZ, hasM);
      case LINESTRING:
        return readLineString(defaultSrid, dimensionCount, hasZ, hasM);
      case POLYGON:
        return readPolygon(defaultSrid, dimensionCount, hasZ, hasM);
      case MULTI_POINT:
        return readMultiPoint(defaultSrid, hasZ, hasM);
      case MULTI_LINESTRING:
        return readMultiLineString(defaultSrid, hasZ, hasM);
      case MULTI_POLYGON:
        return readMultiPolygon(defaultSrid, hasZ, hasM);
      case GEOMETRY_COLLECTION:
        return readGeometryCollection(defaultSrid, hasZ, hasM);
      default:
        throw new WkbParseException("Unsupported geometry type: " + geoType, typeStartPos,
          currentWkb);
    }
  }

  /**
   * Reads a top-level point geometry (allows empty points with NaN coordinates).
   */
  private Point readPoint(int srid, int dimensionCount, boolean hasZ, boolean hasM) {
    double[] coords = new double[dimensionCount];
    for (int i = 0; i < dimensionCount; i++) {
      coords[i] = readDoubleAllowEmpty();
    }
    return new Point(coords, srid, hasZ, hasM);
  }

  /**
   * Reads point coordinates for non-point geometries (does not allow empty/NaN).
   * SRID is set from the parent.
   */
  private Point readInternalPoint(int srid, int dimensionCount, boolean hasZ,
                                  boolean hasM) {
    double[] coords = new double[dimensionCount];
    for (int i = 0; i < dimensionCount; i++) {
      coords[i] = readDoubleNoEmpty();
    }
    return new Point(coords, srid, hasZ, hasM);
  }

  private LineString readLineString(int srid, int dimensionCount, boolean hasZ, boolean hasM) {
    long numPointsPos = buffer.position();
    int numPoints = readInt();

    if (validationLevel > 0 && numPoints == 1) {
      throw new WkbParseException("Too few points in linestring", numPointsPos, currentWkb);
    }

    List<Point> points = new ArrayList<>(numPoints);
    for (int i = 0; i < numPoints; i++) {
      points.add(readInternalPoint(srid, dimensionCount, hasZ, hasM));
    }

    return new LineString(points, srid, hasZ, hasM);
  }

  private Ring readRing(int srid, int dimensionCount, boolean hasZ, boolean hasM) {
    long numPointsPos = buffer.position();
    int numPoints = readInt();

    List<Point> points = new ArrayList<>(numPoints);

    for (int i = 0; i < numPoints; i++) {
      Point p = readInternalPoint(srid, dimensionCount, hasZ, hasM);
      points.add(p);
    }

    Ring ring = new Ring(points);

    // Validate that the ring is closed (first and last points are the same)
    // and has at least 4 points
    if (validationLevel > 0 && !ring.isClosed()) {
      throw new WkbParseException(
          numPoints < 4 ? "Too few points in ring" : "Ring is not closed",
          numPointsPos, currentWkb);
    }

    return ring;
  }

  private Polygon readPolygon(int srid, int dimensionCount, boolean hasZ, boolean hasM) {
    int numRings = readInt();
    List<Ring> rings = new ArrayList<>(numRings);

    for (int i = 0; i < numRings; i++) {
      rings.add(readRing(srid, dimensionCount, hasZ, hasM));
    }

    return new Polygon(rings, srid, hasZ, hasM);
  }

  private MultiPoint readMultiPoint(int srid, boolean hasZ, boolean hasM) {
    int numPoints = readInt();
    List<Point> points = new ArrayList<>(numPoints);

    for (int i = 0; i < numPoints; i++) {
      GeometryModel geom = readNestedGeometryWithDimensionCheck(srid, hasZ, hasM);
      if (!(geom instanceof Point)) {
        throw new WkbParseException("Expected Point in MultiPoint", buffer.position(),
          currentWkb);
      }
      points.add((Point) geom);
    }

    return new MultiPoint(points, srid, hasZ, hasM);
  }

  private MultiLineString readMultiLineString(int srid, boolean hasZ, boolean hasM) {
    int numLineStrings = readInt();
    List<LineString> lineStrings = new ArrayList<>(numLineStrings);

    for (int i = 0; i < numLineStrings; i++) {
      GeometryModel geom = readNestedGeometryWithDimensionCheck(srid, hasZ, hasM);
      if (!(geom instanceof LineString)) {
        throw new WkbParseException("Expected LineString in MultiLineString", buffer.position(),
          currentWkb);
      }
      lineStrings.add((LineString) geom);
    }

    return new MultiLineString(lineStrings, srid, hasZ, hasM);
  }

  private MultiPolygon readMultiPolygon(int srid, boolean hasZ, boolean hasM) {
    int numPolygons = readInt();
    List<Polygon> polygons = new ArrayList<>(numPolygons);

    for (int i = 0; i < numPolygons; i++) {
      GeometryModel geom = readNestedGeometryWithDimensionCheck(srid, hasZ, hasM);
      if (!(geom instanceof Polygon)) {
        throw new WkbParseException("Expected Polygon in MultiPolygon", buffer.position(),
          currentWkb);
      }
      polygons.add((Polygon) geom);
    }

    return new MultiPolygon(polygons, srid, hasZ, hasM);
  }

  private GeometryCollection readGeometryCollection(int srid, boolean hasZ, boolean hasM) {
    int numGeometries = readInt();
    List<GeometryModel> geometries = new ArrayList<>(numGeometries);

    for (int i = 0; i < numGeometries; i++) {
      geometries.add(readNestedGeometryWithDimensionCheck(srid, hasZ, hasM));
    }

    return new GeometryCollection(geometries, srid, hasZ, hasM);
  }
}
