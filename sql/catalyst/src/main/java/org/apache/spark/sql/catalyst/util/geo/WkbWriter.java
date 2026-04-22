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

/**
 * Utility class for converting geometries to Well-Known Binary (WKB) format.
 * This class implements the OGC Simple Features specification for WKB writing.
 * <p>
 * This class is designed to support reuse of a single instance to write multiple
 * geometries efficiently. This class is NOT thread-safe; each thread should create
 * its own instance.
 * This class should be catalyst-internal.
 */
public class WkbWriter {

  /**
   * Gets the WKB type code for a geometry, including dimension offset.
   * For example: Point 2D = 1, Point Z = 1001, Point M = 2001, Point ZM = 3001
   */
  private static int getWkbType(GeometryModel geometry) {
    int baseType = (int) geometry.getTypeId().getValue();
    boolean hasZ = geometry.hasZ();
    boolean hasM = geometry.hasM();

    // Determine dimension offset based on hasZ and hasM flags
    if (hasZ && hasM) {
      return baseType + WkbUtil.DIM_OFFSET_ZM;
    } else if (hasZ) {
      return baseType + WkbUtil.DIM_OFFSET_Z;
    } else if (hasM) {
      return baseType + WkbUtil.DIM_OFFSET_M;
    } else {
      return baseType + WkbUtil.DIM_OFFSET_2D;
    }
  }

  // ========== Public Write Methods ==========

  /**
   * Writes a geometry to WKB format.
   */
  public byte[] write(GeometryModel geometry) {
    return write(geometry, ByteOrder.LITTLE_ENDIAN);
  }

  /**
   * Writes a geometry to WKB format with specified byte order.
   * <p>
   * This method reuses an internal buffer to reduce GC pressure when writing
   * many geometries. The returned byte array is a copy of the internal buffer.
   */
  public byte[] write(GeometryModel geometry, ByteOrder byteOrder) {
    // Calculate size first
    int size = calculateSize(geometry);
    ByteBuffer buffer = ByteBuffer.allocate(size);
    buffer.order(byteOrder);
    writeGeometry(buffer, geometry, byteOrder);
    // Return a copy of exactly the right size
    return buffer.array();
  }

  // ========== Size Calculation ==========

  private int calculateSize(GeometryModel geometry) {
    int dimCount = geometry.getDimensionCount();

    // Header: endianness (1 byte) + type (4 bytes)
    int size = WkbUtil.BYTE_SIZE + WkbUtil.INT_SIZE;

    if (geometry instanceof Point) {
      Point point = (Point) geometry;
      size += point.getCoordinates().length * WkbUtil.DOUBLE_SIZE;
    } else if (geometry instanceof LineString) {
      LineString lineString = (LineString) geometry;
      // Number of points (4 bytes) + coordinates
      size += WkbUtil.INT_SIZE;
      size += lineString.getNumPoints() * dimCount * WkbUtil.DOUBLE_SIZE;
    } else if (geometry instanceof Polygon) {
      Polygon polygon = (Polygon) geometry;
      // Number of rings (4 bytes)
      size += WkbUtil.INT_SIZE;
      for (Ring ring : polygon.getRings()) {
        int numPoints = ring.getNumPoints();
        // Number of points in ring (4 bytes) + coordinates
        size += WkbUtil.INT_SIZE;
        size += numPoints * dimCount * WkbUtil.DOUBLE_SIZE;
      }
    } else if (geometry instanceof MultiPoint) {
      MultiPoint mp = (MultiPoint) geometry;
      // Number of geometries (4 bytes)
      size += WkbUtil.INT_SIZE;
      for (Point p : mp.getPoints()) {
        size += calculateSize(p);
      }
    } else if (geometry instanceof MultiLineString) {
      MultiLineString mls = (MultiLineString) geometry;
      // Number of geometries (4 bytes)
      size += WkbUtil.INT_SIZE;
      for (LineString ls : mls.getLineStrings()) {
        size += calculateSize(ls);
      }
    } else if (geometry instanceof MultiPolygon) {
      MultiPolygon mpoly = (MultiPolygon) geometry;
      // Number of geometries (4 bytes)
      size += WkbUtil.INT_SIZE;
      for (Polygon poly : mpoly.getPolygons()) {
        size += calculateSize(poly);
      }
    } else if (geometry instanceof GeometryCollection) {
      GeometryCollection gc = (GeometryCollection) geometry;
      // Number of geometries (4 bytes)
      size += WkbUtil.INT_SIZE;
      for (GeometryModel geom : gc.getGeometries()) {
        size += calculateSize(geom);
      }
    }

    return size;
  }

  // ========== Write Methods ==========

  private void writeGeometry(ByteBuffer buffer, GeometryModel geometry, ByteOrder byteOrder) {
    // Write endianness
    buffer.put(byteOrder == ByteOrder.LITTLE_ENDIAN
      ? WkbUtil.LITTLE_ENDIAN : WkbUtil.BIG_ENDIAN);

    // Write type
    int type = getWkbType(geometry);
    buffer.putInt(type);

    // Write geometry-specific data
    if (geometry instanceof Point) {
      writePoint(buffer, (Point) geometry);
    } else if (geometry instanceof LineString) {
      writeLineString(buffer, (LineString) geometry);
    } else if (geometry instanceof Polygon) {
      writePolygon(buffer, (Polygon) geometry);
    } else if (geometry instanceof MultiPoint) {
      writeMultiPoint(buffer, (MultiPoint) geometry, byteOrder);
    } else if (geometry instanceof MultiLineString) {
      writeMultiLineString(buffer, (MultiLineString) geometry, byteOrder);
    } else if (geometry instanceof MultiPolygon) {
      writeMultiPolygon(buffer, (MultiPolygon) geometry, byteOrder);
    } else if (geometry instanceof GeometryCollection) {
      writeGeometryCollection(buffer, (GeometryCollection) geometry, byteOrder);
    }
  }

  private void writePoint(ByteBuffer buffer, Point point) {
    for (double coord : point.getCoordinates()) {
      buffer.putDouble(coord);
    }
  }

  private void writeLineString(ByteBuffer buffer, LineString lineString) {
    buffer.putInt(lineString.getNumPoints());
    for (Point point : lineString.getPoints()) {
      writePoint(buffer, point);
    }
  }

  private void writeRing(ByteBuffer buffer, Ring ring) {
    int numPoints = ring.getNumPoints();
    buffer.putInt(numPoints);
    for (Point point : ring.getPoints()) {
      writePoint(buffer, point);
    }
  }

  private void writePolygon(ByteBuffer buffer, Polygon polygon) {
    buffer.putInt(polygon.getRings().size());
    for (Ring ring : polygon.getRings()) {
      writeRing(buffer, ring);
    }
  }

  private void writeMultiPoint(ByteBuffer buffer, MultiPoint multiPoint, ByteOrder byteOrder) {
    buffer.putInt(multiPoint.getNumGeometries());
    for (Point point : multiPoint.getPoints()) {
      writeGeometry(buffer, point, byteOrder);
    }
  }

  private void writeMultiLineString(ByteBuffer buffer, MultiLineString multiLineString,
                                    ByteOrder byteOrder) {
    buffer.putInt(multiLineString.getNumGeometries());
    for (LineString lineString : multiLineString.getLineStrings()) {
      writeGeometry(buffer, lineString, byteOrder);
    }
  }

  private void writeMultiPolygon(ByteBuffer buffer, MultiPolygon multiPolygon,
                                 ByteOrder byteOrder) {
    buffer.putInt(multiPolygon.getNumGeometries());
    for (Polygon polygon : multiPolygon.getPolygons()) {
      writeGeometry(buffer, polygon, byteOrder);
    }
  }

  private void writeGeometryCollection(ByteBuffer buffer, GeometryCollection collection,
                                       ByteOrder byteOrder) {
    buffer.putInt(collection.getNumGeometries());
    for (GeometryModel geometry : collection.getGeometries()) {
      writeGeometry(buffer, geometry, byteOrder);
    }
  }
}
