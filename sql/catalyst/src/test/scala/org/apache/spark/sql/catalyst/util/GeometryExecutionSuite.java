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

import org.apache.spark.unsafe.types.GeometryVal;
import org.junit.jupiter.api.Test;

import java.nio.ByteOrder;
import java.util.HexFormat;

import static org.junit.jupiter.api.Assertions.*;


/**
 * Test suite for the Geometry server-side execution class.
 */
class GeometryExecutionSuite {

  // A sample Geometry byte array for testing purposes, representing a POINT(1 2) with SRID 4326.
  private final byte[] testGeometryVal = new byte[] {
    (byte)0xE6, 0x10, 0x00, 0x00,
    0x01, 0x01, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, (byte)0xF0,
    0x3F, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00,
    0x40
  };

  /** Tests for Geometry factory methods and getters. */

  @Test
  void testFromBytes() {
    Geometry geometry = Geometry.fromBytes(testGeometryVal);
    assertNotNull(geometry);
    assertArrayEquals(testGeometryVal, geometry.getBytes());
  }

  @Test
  void testFromValue() {
    GeometryVal value = GeometryVal.fromBytes(testGeometryVal);
    Geometry geometry = Geometry.fromValue(value);
    assertNotNull(geometry);
    assertEquals(value, geometry.getValue());
  }

  @Test
  void testGetBytes() {
    Geometry geometry = Geometry.fromBytes(testGeometryVal);
    assertArrayEquals(testGeometryVal, geometry.getBytes());
  }

  @Test
  void testCopy() {
    Geometry geometry = Geometry.fromBytes(testGeometryVal);
    Geometry geometryCopy = geometry.copy();
    assertNotNull(geometryCopy);
    assertArrayEquals(geometry.getBytes(), geometryCopy.getBytes());
  }

  /** Tests for Geometry constants. */

  @Test
  void testDefaultSrid() {
    assertEquals(0, Geometry.DEFAULT_SRID);
  }

  /** Tests for Geometry WKB parsing. */

  @Test
  void testFromWkbWithSridRudimentary() {
    byte[] wkb = new byte[]{1, 2, 3};
    // Note: This is a rudimentary WKB handling test; actual WKB parsing is not yet implemented.
    // Once we implement the appropriate parsing logic, this test should be updated accordingly.
    Geometry geometry = Geometry.fromWkb(wkb, 4326);
    assertNotNull(geometry);
    assertArrayEquals(wkb, geometry.toWkb());
    assertEquals(4326, geometry.srid());
  }

  @Test
  void testFromWkbNoSridRudimentary() {
    byte[] wkb = new byte[]{1, 2, 3};
    // Note: This is a rudimentary WKB handling test; actual WKB parsing is not yet implemented.
    // Once we implement the appropriate parsing logic, this test should be updated accordingly.
    Geometry geometry = Geometry.fromWkb(wkb);
    assertNotNull(geometry);
    assertArrayEquals(wkb, geometry.toWkb());
    assertEquals(0, geometry.srid());
  }

  /** Tests for Geometry EWKB parsing. */

  @Test
  void testFromEwkbUnsupported() {
    byte[] ewkb = new byte[]{1, 2, 3};
    UnsupportedOperationException exception = assertThrows(
      UnsupportedOperationException.class,
      () -> Geometry.fromEwkb(ewkb)
    );
    assertEquals("Geometry EWKB parsing is not yet supported.", exception.getMessage());
  }

  /** Tests for Geometry WKT parsing. */

  @Test
  void testFromWktWithSridUnsupported() {
    byte[] wkt = new byte[]{4, 5, 5};
    UnsupportedOperationException exception = assertThrows(
      UnsupportedOperationException.class,
      () -> Geometry.fromWkt(wkt, 0)
    );
    assertEquals("Geometry WKT parsing is not yet supported.", exception.getMessage());
  }

  @Test
  void testFromWktNoSridUnsupported() {
    byte[] wkt = new byte[]{4, 5, 5};
    UnsupportedOperationException exception = assertThrows(
      UnsupportedOperationException.class,
      () -> Geometry.fromWkt(wkt)
    );
    assertEquals("Geometry WKT parsing is not yet supported.", exception.getMessage());
  }

  /** Tests for Geometry EWKT parsing. */

  @Test
  void testFromEwktUnsupported() {
    byte[] ewkt = new byte[]{4, 5, 5};
    UnsupportedOperationException exception = assertThrows(
      UnsupportedOperationException.class,
      () -> Geometry.fromEwkt(ewkt)
    );
    assertEquals("Geometry EWKT parsing is not yet supported.", exception.getMessage());
  }

  /** Tests for Geometry WKB and EWKB converters. */

  @Test
  void testToWkb() {
    Geometry geometry = Geometry.fromBytes(testGeometryVal);
    // WKB value (endianness: NDR) corresponding to WKT: POINT(1 2).
    byte[] wkb = HexFormat.of().parseHex("0101000000000000000000f03f0000000000000040");
    assertArrayEquals(wkb, geometry.toWkb());
  }

  @Test
  void testToWkbEndiannessNDR() {
    Geometry geometry = Geometry.fromBytes(testGeometryVal);
    // WKB value (endianness: NDR) corresponding to WKT: POINT(1 2).
    byte[] wkb = HexFormat.of().parseHex("0101000000000000000000f03f0000000000000040");
    assertArrayEquals(wkb, geometry.toWkb(ByteOrder.LITTLE_ENDIAN));
  }

  @Test
  void testToWkbEndiannessXDR() {
    Geometry geometry = Geometry.fromBytes(testGeometryVal);
    UnsupportedOperationException exception = assertThrows(
      UnsupportedOperationException.class,
      () -> geometry.toWkb(ByteOrder.BIG_ENDIAN)
    );
    assertEquals("Geometry WKB endianness is not yet supported.", exception.getMessage());
  }

  @Test
  void testToEwkbUnsupported() {
    Geometry geometry = Geometry.fromBytes(testGeometryVal);
    UnsupportedOperationException exception = assertThrows(
      UnsupportedOperationException.class,
      geometry::toEwkb
    );
    assertEquals("Geometry EWKB conversion is not yet supported.", exception.getMessage());
  }

  @Test
  void testToEwkbEndiannessXDRUnsupported() {
    Geometry geometry = Geometry.fromBytes(testGeometryVal);
    UnsupportedOperationException exception = assertThrows(
      UnsupportedOperationException.class,
      () -> geometry.toEwkb(ByteOrder.BIG_ENDIAN)
    );
    assertEquals("Geometry EWKB endianness is not yet supported.", exception.getMessage());
  }

  @Test
  void testToEwkbEndiannessNDRUnsupported() {
    Geometry geometry = Geometry.fromBytes(testGeometryVal);
    UnsupportedOperationException exception = assertThrows(
      UnsupportedOperationException.class,
      () -> geometry.toEwkb(ByteOrder.LITTLE_ENDIAN)
    );
    assertEquals("Geometry EWKB endianness is not yet supported.", exception.getMessage());
  }

  /** Tests for Geometry WKT and EWKT converters. */

  @Test
  void testToWktUnsupported() {
    Geometry geometry = Geometry.fromBytes(testGeometryVal);
    UnsupportedOperationException exception = assertThrows(
      UnsupportedOperationException.class,
      geometry::toWkt
    );
    assertEquals("Geometry WKT conversion is not yet supported.", exception.getMessage());
  }

  @Test
  void testToEwktUnsupported() {
    Geometry geometry = Geometry.fromBytes(testGeometryVal);
    UnsupportedOperationException exception = assertThrows(
      UnsupportedOperationException.class,
      geometry::toEwkt
    );
    assertEquals("Geometry EWKT conversion is not yet supported.", exception.getMessage());
  }

  /** Tests for other Geometry methods. */

  @Test
  void testSrid() {
    Geometry geometry = Geometry.fromBytes(testGeometryVal);
    assertEquals(4326, geometry.srid());
  }
}
