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

package org.apache.spark.sql.util;

import org.apache.spark.sql.errors.QueryExecutionErrors;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateXYM;
import org.locationtech.jts.geom.CoordinateXYZM;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;
import org.locationtech.jts.io.WKBWriter;
import org.locationtech.jts.io.ByteOrderValues;

public class GeoWkbReader {

  /** Common constants for geospatial standard format manipulation. */

  // The default endianness for in-memory geo objects is Little Endian (NDR).
  private static final int DEFAULT_ENDIANNESS = ByteOrderValues.LITTLE_ENDIAN;

  /** Utility methods for standard format parsing using JTS. */

  // Private helper to determine dimension of geometry.
  private static int getDimension(Geometry geometry) {
    Coordinate[] coords = geometry.getCoordinates();
    if (coords.length == 0) return 2;
    Coordinate c = coords[0];
    if (c instanceof CoordinateXYZM) return 4;
    if (c instanceof CoordinateXYM) return 3;
    if (!Double.isNaN(c.getZ())) return 3;
    return 2;
  }

  // WKB parser (private helper method).
  private static Geometry parseWkb(byte[] bytes) {
    try {
      WKBReader reader = new WKBReader();
      return reader.read(bytes);
    } catch (ParseException e) {
      throw QueryExecutionErrors.wkbParseError();
    }
  }

  /** Public geospatial standard format reader API. */

  // WKB reader.
  public static byte[] readWkb(byte[] bytes) {
    // Parse the input WKB bytes to a Geometry object.
    Geometry geometry = parseWkb(bytes);
    // Write the Geometry object back to WKB format with the default endianness.
    return new WKBWriter(getDimension(geometry), DEFAULT_ENDIANNESS).write(geometry);
  }

}
