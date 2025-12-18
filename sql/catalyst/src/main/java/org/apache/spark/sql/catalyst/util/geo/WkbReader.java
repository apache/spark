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


import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.catalyst.util.geo.WkbConstants.DEFAULT_SRID;

/**
 * Reader for parsing Well-Known Binary (WKB) format geometries.
 * This class implements the OGC Simple Features specification for WKB parsing.
 */
public class WkbReader {
  private ByteBuffer buffer;
  private final int validationLevel;
  private byte[] currentWkb;

  public WkbReader() {
    this(1);
  }

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
      return readGeometry(DEFAULT_SRID);
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
    if (endianValue != WkbConstants.BIG_ENDIAN && endianValue != WkbConstants.LITTLE_ENDIAN) {
      throw new WkbParseException("Invalid byte order " + endianValue, buffer.position() - 1,
        currentWkb);
    }
    return endianValue == WkbConstants.LITTLE_ENDIAN ?
      ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN;
  }

  private int readInt() {
    if (buffer.remaining() < WkbConstants.INT_SIZE) {
      checkNotAtEnd(buffer.limit());
    }
    return buffer.getInt();
  }

  /**
   * Reads a double coordinate value, allowing NaN for empty points.
   */
  private double readDoubleAllowEmpty() {
    if (buffer.remaining() < WkbConstants.DOUBLE_SIZE) {
      checkNotAtEnd(buffer.limit());
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
    if (buffer.remaining() < WkbConstants.DOUBLE_SIZE) {
      checkNotAtEnd(buffer.limit());
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
    // Check that we have at least one byte for endianness
    if (currentWkb == null || currentWkb.length < 1) {
      throw new WkbParseException("WKB data is empty or null", 0, currentWkb);
    }

    // Read endianness directly from the first byte
    byte endianValue = currentWkb[0];
    if (endianValue > 1) {
      throw new WkbParseException("Invalid byte order " + endianValue, 0, currentWkb);
    }
    ByteOrder byteOrder = endianValue == 1 ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN;

    // Check that we have enough bytes for the rest of the data
    if (currentWkb.length < 5) {
      throw new WkbParseException("WKB data too short", 0, currentWkb);
    }

    // Create a new buffer wrapping the rest of the byte array (after the endianness byte)
    buffer = ByteBuffer.wrap(currentWkb, 1, currentWkb.length - 1);
    buffer.order(byteOrder);

    return readGeometryInternal(defaultSrid, true);
  }

  /**
   * Reads a nested geometry from the current buffer position.
   * Used by multi-geometry types to read child geometries.
   */
  private GeometryModel readNestedGeometry(int defaultSrid) {
    return readGeometryInternal(defaultSrid, false);
  }

  /**
   * Internal method to read geometry with optional endianness reading.
   *
   * @param defaultSrid srid to use if not specified in WKB
   * @param isRootGeometry if true, assumes endianness has already been read and buffer is set up;
   *                       if false, reads endianness from current buffer position
   * @return Geometry object
   */
  private GeometryModel readGeometryInternal(int defaultSrid, boolean isRootGeometry) {
    ByteOrder savedByteOrder = null;
    long typeStartPos;

    if (isRootGeometry) {
      // For root geometry, endianness has already been read and buffer is set up
      typeStartPos = 1;
    } else {
      // For nested geometry, read endianness from the current buffer position
      ByteOrder byteOrder = readEndianness();

      // Save the current byte order and temporarily set to the nested geometry's byte order
      savedByteOrder = buffer.order();
      buffer.order(byteOrder);
      typeStartPos = buffer.position();
    }

    // Read type and dimension
    int typeAndDim = readInt();

    GeoTypeId geoType;
    int dimensionCount;

    // Parse WKB format (geotype and dimension)
    if (!WkbConstants.isValidWkbType(typeAndDim)) {
      throw new WkbParseException("Invalid or unsupported type " + typeAndDim, typeStartPos,
        currentWkb);
    }

    int baseType = WkbConstants.getBaseType(typeAndDim);
    geoType = GeoTypeId.fromValue(baseType);
    dimensionCount = WkbConstants.getDimensionCount(typeAndDim);
    boolean hasZ = WkbConstants.hasZ(typeAndDim);
    boolean hasM = WkbConstants.hasM(typeAndDim);

    // Dispatch to appropriate reader based on geometry type
    GeometryModel result;
    switch (geoType) {
      case POINT:
        result = readPoint(defaultSrid, dimensionCount, hasZ, hasM);
        break;
      case LINESTRING:
        result = readLineString(defaultSrid, dimensionCount, hasZ, hasM);
        break;
      case POLYGON:
        result = readPolygon(defaultSrid, dimensionCount, hasZ, hasM);
        break;
      case MULTI_POINT:
        result = readMultiPoint(defaultSrid, dimensionCount, hasZ, hasM);
        break;
      case MULTI_LINESTRING:
        result = readMultiLineString(defaultSrid, dimensionCount, hasZ, hasM);
        break;
      case MULTI_POLYGON:
        result = readMultiPolygon(defaultSrid, dimensionCount, hasZ, hasM);
        break;
      case GEOMETRY_COLLECTION:
        result = readGeometryCollection(defaultSrid, dimensionCount, hasZ, hasM);
        break;
      default:
        throw new WkbParseException("Unsupported geometry type: " + geoType, typeStartPos,
          currentWkb);
    }

    // Restore the saved byte order if this was a nested geometry
    if (!isRootGeometry && savedByteOrder != null) {
      buffer.order(savedByteOrder);
    }

    return result;
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
   */
  private Point readPointCoordinatesNoEmpty(int dimensionCount, boolean hasZ, boolean hasM) {
    double[] coords = new double[dimensionCount];
    for (int i = 0; i < dimensionCount; i++) {
      coords[i] = readDoubleNoEmpty();
    }
    return new Point(coords, 0, hasZ, hasM); // SRID will be set by parent
  }

  private LineString readLineString(int srid, int dimensionCount, boolean hasZ, boolean hasM) {
    long numPointsPos = buffer.position();
    int numPoints = readInt();

    if (validationLevel > 0 && numPoints == 1) {
      throw new WkbParseException("Too few points in linestring", numPointsPos, currentWkb);
    }

    List<Point> points = new ArrayList<>(numPoints);
    for (int i = 0; i < numPoints; i++) {
      points.add(readPointCoordinatesNoEmpty(dimensionCount, hasZ, hasM));
    }

    return new LineString(points, srid, hasZ, hasM);
  }

  private Ring readRing(int dimensionCount, boolean hasZ, boolean hasM) {
    long numPointsPos = buffer.position();
    int numPoints = readInt();

    if (validationLevel > 0 && numPoints < 4) {
      throw new WkbParseException("Too few points in ring", numPointsPos, currentWkb);
    }

    List<Point> points = new ArrayList<>(numPoints);

    for (int i = 0; i < numPoints; i++) {
      Point p = readPointCoordinatesNoEmpty(dimensionCount, hasZ, hasM);
      points.add(p);
    }

    // Validate that the ring is closed (first and last points are the same)
    if (validationLevel > 0 && numPoints > 0) {
      Point first = points.get(0);
      Point last = points.get(numPoints - 1);
      if (!coordinatesEqual(first.getCoordinates(), last.getCoordinates())) {
        throw new WkbParseException("Ring is not closed", numPointsPos, currentWkb);
      }
    }

    return new Ring(points, hasZ, hasM);
  }

  /**
   * Compares two coordinate arrays for equality.
   */
  private static boolean coordinatesEqual(double[] coords1, double[] coords2) {
    if (coords1.length != coords2.length) {
      return false;
    }
    for (int i = 0; i < coords1.length; i++) {
      if (coords1[i] != coords2[i]) {
        return false;
      }
    }
    return true;
  }

  private Polygon readPolygon(int srid, int dimensionCount, boolean hasZ, boolean hasM) {
    int numRings = readInt();
    List<Ring> rings = new ArrayList<>(numRings);

    for (int i = 0; i < numRings; i++) {
      rings.add(readRing(dimensionCount, hasZ, hasM));
    }

    return new Polygon(rings, srid, hasZ, hasM);
  }

  private MultiPoint readMultiPoint(int srid, int dimensionCount, boolean hasZ, boolean hasM) {
    int numPoints = readInt();
    List<Point> points = new ArrayList<>(numPoints);

    for (int i = 0; i < numPoints; i++) {
      GeometryModel geom = readNestedGeometry(srid);
      if (!(geom instanceof Point)) {
        throw new WkbParseException("Expected Point in MultiPoint", buffer.position(),
          currentWkb);
      }
      points.add((Point) geom);
    }

    return new MultiPoint(points, srid, hasZ, hasM);
  }

  private MultiLineString readMultiLineString(
    int srid,
    int dimensionCount,
    boolean hasZ,
    boolean hasM) {
    int numLineStrings = readInt();
    List<LineString> lineStrings = new ArrayList<>(numLineStrings);

    for (int i = 0; i < numLineStrings; i++) {
      GeometryModel geom = readNestedGeometry(srid);
      if (!(geom instanceof LineString)) {
        throw new WkbParseException("Expected LineString in MultiLineString", buffer.position(),
          currentWkb);
      }
      lineStrings.add((LineString) geom);
    }

    return new MultiLineString(lineStrings, srid, hasZ, hasM);
  }

  private MultiPolygon readMultiPolygon(int srid, int dimensionCount, boolean hasZ, boolean hasM) {
    int numPolygons = readInt();
    List<Polygon> polygons = new ArrayList<>(numPolygons);

    for (int i = 0; i < numPolygons; i++) {
      GeometryModel geom = readNestedGeometry(srid);
      if (!(geom instanceof Polygon)) {
        throw new WkbParseException("Expected Polygon in MultiPolygon", buffer.position(),
          currentWkb);
      }
      polygons.add((Polygon) geom);
    }

    return new MultiPolygon(polygons, srid, hasZ, hasM);
  }

  private GeometryCollection readGeometryCollection(int srid, int dimensionCount,
      boolean hasZ, boolean hasM) {
    int numGeometries = readInt();
    List<GeometryModel> geometries = new ArrayList<>(numGeometries);

    for (int i = 0; i < numGeometries; i++) {
      geometries.add(readNestedGeometry(srid));
    }

    return new GeometryCollection(geometries, srid, hasZ, hasM);
  }
}

