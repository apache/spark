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

/**
 * Utility class for converting geometries to Well-Known Binary (WKB) format.
 * This class implements the OGC Simple Features specification for WKB writing.
 */
public class WkbWriter {
  private static final int DEFAULT_SRID = 0;
  private static final int EWKB_SRID_FLAG = 0x20000000;

  /**
   * Writes a geometry to WKB format.
   */
  public byte[] write(Geometry geometry) {
    return write(geometry, ByteOrder.LITTLE_ENDIAN);
  }

  /**
   * Writes a geometry to WKB format with specified byte order.
   */
  public byte[] write(Geometry geometry, ByteOrder byteOrder) {
    // Calculate size first
    int size = calculateSize(geometry);
    ByteBuffer buffer = ByteBuffer.allocate(size);
    buffer.order(byteOrder);

    writeGeometry(buffer, geometry, byteOrder);

    return buffer.array();
  }

  private int calculateSize(Geometry geometry) {
    int size = 1 + 4; // Endianness + type

    if (geometry instanceof Point) {
      Point point = (Point) geometry;
      size += point.getCoordinates().length * 8;
    } else if (geometry instanceof LineString) {
      LineString lineString = (LineString) geometry;
      size += 4; // Number of points
      size += lineString.getNumPoints() * (1 + 4 + 2 * 8); // Assume 2D for simplicity
    } else if (geometry instanceof Polygon) {
      Polygon polygon = (Polygon) geometry;
      size += 4; // Number of rings
      for (Ring ring : polygon.getRings()) {
        size += 4; // Number of points
        size += ring.getNumPoints() * 2 * 8; // Assume 2D
      }
    } else if (geometry instanceof MultiPoint) {
      MultiPoint mp = (MultiPoint) geometry;
      size += 4; // Number of geometries
      for (Point p : mp.getPoints()) {
        size += calculateSize(p);
      }
    } else if (geometry instanceof MultiLineString) {
      MultiLineString mls = (MultiLineString) geometry;
      size += 4; // Number of geometries
      for (LineString ls : mls.getLineStrings()) {
        size += calculateSize(ls);
      }
    } else if (geometry instanceof MultiPolygon) {
      MultiPolygon mpoly = (MultiPolygon) geometry;
      size += 4; // Number of geometries
      for (Polygon poly : mpoly.getPolygons()) {
        size += calculateSize(poly);
      }
    } else if (geometry instanceof GeometryCollection) {
      GeometryCollection gc = (GeometryCollection) geometry;
      size += 4; // Number of geometries
      for (Geometry geom : gc.getGeometries()) {
        size += calculateSize(geom);
      }
    }

    return size;
  }

  private void writeGeometry(ByteBuffer buffer, Geometry geometry, ByteOrder byteOrder) {
    // Write endianness
    buffer.put((byte) (byteOrder == ByteOrder.LITTLE_ENDIAN ? 1 : 0));

    // Write type
    int type = (int) geometry.getTypeId().getValue();
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
      for (double coord : point.getCoordinates()) {
        buffer.putDouble(coord);
      }
    }
  }

  private void writePolygon(ByteBuffer buffer, Polygon polygon) {
    buffer.putInt(polygon.getRings().size());
    for (Ring ring : polygon.getRings()) {
      buffer.putInt(ring.getNumPoints());
      for (Point point : ring.getPoints()) {
        for (double coord : point.getCoordinates()) {
          buffer.putDouble(coord);
        }
      }
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
    for (Geometry geometry : collection.getGeometries()) {
      writeGeometry(buffer, geometry, byteOrder);
    }
  }
}

