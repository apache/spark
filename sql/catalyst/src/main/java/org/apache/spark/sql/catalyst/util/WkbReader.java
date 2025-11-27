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


import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

/**
 * Reader for parsing Well-Known Binary (WKB) format geometries.
 * This class implements the OGC Simple Features specification for WKB parsing.
 */
class WkbReader {
  private static final int DEFAULT_SRID = 0;
  private static final int EWKB_SRID_FLAG = 0x20000000;
  private static final int EWKB_Z_FLAG = 0x80000000;
  private static final int EWKB_M_FLAG = 0x40000000;
  private static final int EWKB_TYPE_MASK = 0x000000FF;

  private ByteBuffer buffer;
  private boolean readEWKB;
  private int validationLevel;
  private byte[] currentWkb;

  WkbReader() {
    this(false, 1);
  }

  WkbReader(boolean readEWKB, int validationLevel) {
    this.readEWKB = readEWKB;
    this.validationLevel = validationLevel;
  }

  /**
   * Reads a geometry from WKB bytes.
   */
  public Geometry read(byte[] wkb) {
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
  public Geometry read(byte[] wkb, int srid) {
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
    if (endianValue > 1) {
      throw new WkbParseException("Invalid byte order " + endianValue,
        buffer.position() - 1, currentWkb);
    }
    return endianValue == 1 ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN;
  }

  private int readInt() {
    if (buffer.remaining() < 4) {
      checkNotAtEnd(buffer.limit());
    }
    return buffer.getInt();
  }

  private double readDouble() {
    if (buffer.remaining() < 8) {
      checkNotAtEnd(buffer.limit());
    }
    double value = buffer.getDouble();
    if (!Double.isFinite(value) && !Double.isNaN(value)) {
      throw new WkbParseException("Invalid coordinate value found",
        buffer.position() - 8, currentWkb);
    }
    return value;
  }

  /**
   * Reads a geometry from WKB bytes with a specified SRID.
   *
   * @param defaultSrid srid to use if not specified in WKB
   * @return Geometry object
   */
  private Geometry readGeometry(int defaultSrid) {
    // We map negative SRID values to 0.
    defaultSrid = Math.max(defaultSrid, 0);

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

    // Read type and dimension
    long typeStartPos = 1;
    int typeAndDim = readInt();

    int srid = defaultSrid;
    GeoTypeId geoType;
    int dimensionCount;

    if (readEWKB) {
      // Parse EWKB format
      boolean hasSrid = (typeAndDim & EWKB_SRID_FLAG) != 0;
      boolean hasZ = (typeAndDim & EWKB_Z_FLAG) != 0;
      boolean hasM = (typeAndDim & EWKB_M_FLAG) != 0;

      if (hasSrid) {
        srid = readInt();
      }

      int rawType = typeAndDim & EWKB_TYPE_MASK;
      if (rawType < 1 || rawType > 7) {
        throw new WkbParseException("Invalid or unsupported type " + typeAndDim, typeStartPos,
          currentWkb);
      }

      geoType = GeoTypeId.fromValue(rawType);
      dimensionCount = 2 + (hasZ ? 1 : 0) + (hasM ? 1 : 0);
    } else {
      // Determine dimension from WKB type
      if (typeAndDim >= 1 && typeAndDim <= 7) {
        // 2D geometry
        dimensionCount = 2;
        geoType = GeoTypeId.fromValue(typeAndDim);
      } else if (typeAndDim >= 1000 && typeAndDim <= 1007) {
        // 3DZ geometry
        dimensionCount = 3;
        geoType = GeoTypeId.fromValue(typeAndDim - 1000);
      } else if (typeAndDim >= 2000 && typeAndDim <= 2007) {
        // 3DM geometry
        dimensionCount = 3;
        geoType = GeoTypeId.fromValue(typeAndDim - 2000);
      } else if (typeAndDim >= 3000 && typeAndDim <= 3007) {
        // 4D geometry
        dimensionCount = 4;
        geoType = GeoTypeId.fromValue(typeAndDim - 3000);
      } else {
        throw new WkbParseException("Invalid or unsupported type " + typeAndDim, typeStartPos,
          currentWkb);
      }
    }

    // Set GeometryVal byte representation
    Geometry result = readType(geoType, srid, dimensionCount, typeStartPos);
    result.setVal(STUtils.physicalValFromWKB(currentWkb, srid));
    return result;
  }

  /**
   * Reads a nested geometry from the current buffer position.
   * Used by multi-geometry types to read child geometries.
   */
  private Geometry readNestedGeometry(int defaultSrid) {
    // Read endianness from the current buffer position
    ByteOrder byteOrder = readEndianness();

    // Save the current byte order and temporarily set to the nested geometry's byte order
    ByteOrder savedByteOrder = buffer.order();
    buffer.order(byteOrder);

    // Read type and dimension
    long typeStartPos = buffer.position();
    int typeAndDim = readInt();

    int srid = defaultSrid;
    GeoTypeId geoType;
    int dimensionCount;

    if (readEWKB) {
      // Parse EWKB format
      boolean hasSrid = (typeAndDim & EWKB_SRID_FLAG) != 0;
      boolean hasZ = (typeAndDim & EWKB_Z_FLAG) != 0;
      boolean hasM = (typeAndDim & EWKB_M_FLAG) != 0;

      if (hasSrid) {
        srid = readInt();
      }

      int rawType = typeAndDim & EWKB_TYPE_MASK;
      if (rawType < 1 || rawType > 7) {
        throw new WkbParseException("Invalid or unsupported type " + typeAndDim, typeStartPos,
          currentWkb);
      }

      geoType = GeoTypeId.fromValue(rawType);
      dimensionCount = 2 + (hasZ ? 1 : 0) + (hasM ? 1 : 0);
    } else {
      // Determine dimension from WKB type
      if (typeAndDim >= 1 && typeAndDim <= 7) {
        // 2D geometry
        dimensionCount = 2;
        geoType = GeoTypeId.fromValue(typeAndDim);
      } else if (typeAndDim >= 1000 && typeAndDim <= 1007) {
        // 3DZ geometry
        dimensionCount = 3;
        geoType = GeoTypeId.fromValue(typeAndDim - 1000);
      } else if (typeAndDim >= 2000 && typeAndDim <= 2007) {
        // 3DM geometry
        dimensionCount = 3;
        geoType = GeoTypeId.fromValue(typeAndDim - 2000);
      } else if (typeAndDim >= 3000 && typeAndDim <= 3007) {
        // 4D geometry
        dimensionCount = 4;
        geoType = GeoTypeId.fromValue(typeAndDim - 3000);
      } else {
        throw new WkbParseException("Invalid or unsupported type " + typeAndDim, typeStartPos,
          currentWkb);
      }
    }

    Geometry result = readType(geoType, srid, dimensionCount, typeStartPos);

    // Restore the saved byte order
    buffer.order(savedByteOrder);

    return result;
  }

  private Geometry readType(GeoTypeId geoType, int srid, int dimensionCount, long startPos) {
    switch (geoType) {
      case POINT:
        return readPoint(srid, dimensionCount);
      case LINESTRING:
        return readLineString(srid, dimensionCount);
      case POLYGON:
        return readPolygon(srid, dimensionCount);
      case MULTI_POINT:
        return readMultiPoint(srid, dimensionCount);
      case MULTI_LINESTRING:
        return readMultiLineString(srid, dimensionCount);
      case MULTI_POLYGON:
        return readMultiPolygon(srid, dimensionCount);
      case GEOMETRY_COLLECTION:
        return readGeometryCollection(srid, dimensionCount);
      default:
        throw new WkbParseException("Unsupported geometry type: " + geoType, startPos, currentWkb);
    }
  }

  private Point readPoint(int srid, int dimensionCount) {
    double[] coords = new double[dimensionCount];
    for (int i = 0; i < dimensionCount; i++) {
      coords[i] = readDouble();
    }
    return new Point(coords, srid);
  }

  private Point readPointCoordinates(int dimensionCount) {
    double[] coords = new double[dimensionCount];
    for (int i = 0; i < dimensionCount; i++) {
      coords[i] = readDouble();
    }
    return new Point(coords, 0); // SRID will be set by parent
  }

  private LineString readLineString(int srid, int dimensionCount) {
    long numPointsPos = buffer.position();
    int numPoints = readInt();

    if (validationLevel > 0 && numPoints == 1) {
      throw new WkbParseException("Too few points in linestring", numPointsPos, currentWkb);
    }

    List<Point> points = new ArrayList<>(numPoints);
    for (int i = 0; i < numPoints; i++) {
      points.add(readPointCoordinates(dimensionCount));
    }

    return new LineString(points, srid);
  }

  private Ring readRing(int dimensionCount) {
    long numPointsPos = buffer.position();
    int numPoints = readInt();

    if (validationLevel > 0 && numPoints < 4) {
      throw new WkbParseException("Too few points in ring", numPointsPos, currentWkb);
    }

    List<Point> points = new ArrayList<>(numPoints);
    Point first = null;
    Point last = null;

    for (int i = 0; i < numPoints; i++) {
      Point p = readPointCoordinates(dimensionCount);
      points.add(p);
      if (i == 0) {
        first = p;
      }
      if (i == numPoints - 1) {
        last = p;
      }
    }

    if (validationLevel > 0 && first != null && last != null) {
      if (first.getX() != last.getX() || first.getY() != last.getY()) {
        throw new WkbParseException("Found non-closed ring", buffer.position(), currentWkb);
      }
    }

    return new Ring(points);
  }

  private Polygon readPolygon(int srid, int dimensionCount) {
    int numRings = readInt();
    List<Ring> rings = new ArrayList<>(numRings);

    for (int i = 0; i < numRings; i++) {
      rings.add(readRing(dimensionCount));
    }

    return new Polygon(rings, srid);
  }

  private MultiPoint readMultiPoint(int srid, int dimensionCount) {
    int numPoints = readInt();
    List<Point> points = new ArrayList<>(numPoints);

    for (int i = 0; i < numPoints; i++) {
      Geometry geom = readNestedGeometry(srid);
      if (!(geom instanceof Point)) {
        throw new WkbParseException("Expected Point in MultiPoint", buffer.position(), currentWkb);
      }
      points.add((Point) geom);
    }

    return new MultiPoint(points, srid);
  }

  private MultiLineString readMultiLineString(int srid, int dimensionCount) {
    int numLineStrings = readInt();
    List<LineString> lineStrings = new ArrayList<>(numLineStrings);

    for (int i = 0; i < numLineStrings; i++) {
      Geometry geom = readNestedGeometry(srid);
      if (!(geom instanceof LineString)) {
        throw new WkbParseException("Expected LineString in MultiLineString", buffer.position(),
          currentWkb);
      }
      lineStrings.add((LineString) geom);
    }

    return new MultiLineString(lineStrings, srid);
  }

  private MultiPolygon readMultiPolygon(int srid, int dimensionCount) {
    int numPolygons = readInt();
    List<Polygon> polygons = new ArrayList<>(numPolygons);

    for (int i = 0; i < numPolygons; i++) {
      Geometry geom = readNestedGeometry(srid);
      if (!(geom instanceof Polygon)) {
        throw new WkbParseException("Expected Polygon in MultiPolygon", buffer.position(),
          currentWkb);
      }
      polygons.add((Polygon) geom);
    }

    return new MultiPolygon(polygons, srid);
  }

  private GeometryCollection readGeometryCollection(int srid, int dimensionCount) {
    int numGeometries = readInt();
    List<Geometry> geometries = new ArrayList<>(numGeometries);

    for (int i = 0; i < numGeometries; i++) {
      geometries.add(readNestedGeometry(srid));
    }

    return new GeometryCollection(geometries, srid);
  }
}

