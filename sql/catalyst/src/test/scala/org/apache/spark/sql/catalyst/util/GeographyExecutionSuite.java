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

import org.apache.spark.unsafe.types.GeographyVal;
import org.junit.jupiter.api.Test;

import java.nio.ByteOrder;
import java.util.HexFormat;

import static org.junit.jupiter.api.Assertions.*;


/**
 * Test suite for the Geography server-side execution class.
 */
class GeographyExecutionSuite {

  // A sample Geography byte array for testing purposes, representing a POINT(1 2) with SRID 4326.
  private final byte[] testGeographyVal = new byte[] {
    (byte)0xE6, 0x10, 0x00, 0x00,
    0x01, 0x01, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, (byte)0xF0,
    0x3F, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00,
    0x40
  };

  /** Tests for Geography factory methods and getters. */

  @Test
  void testFromBytes() {
    Geography geography = Geography.fromBytes(testGeographyVal);
    assertNotNull(geography);
    assertArrayEquals(testGeographyVal, geography.getBytes());
  }

  @Test
  void testFromValue() {
    GeographyVal value = GeographyVal.fromBytes(testGeographyVal);
    Geography geography = Geography.fromValue(value);
    assertNotNull(geography);
    assertEquals(value, geography.getValue());
  }

  @Test
  void testGetBytes() {
    Geography geography = Geography.fromBytes(testGeographyVal);
    assertArrayEquals(testGeographyVal, geography.getBytes());
  }

  @Test
  void testCopy() {
    Geography geography = Geography.fromBytes(testGeographyVal);
    Geography geographyCopy = geography.copy();
    assertNotNull(geographyCopy);
    assertArrayEquals(geography.getBytes(), geographyCopy.getBytes());
  }

  /** Tests for Geography constants. */

  @Test
  void testDefaultSrid() {
    assertEquals(4326, Geography.DEFAULT_SRID);
  }

  /** Tests for Geography WKB parsing. */

  @Test
  void testFromWkbWithSridRudimentary() {
    byte[] wkb = new byte[]{1, 2, 3};
    // Note: This is a rudimentary WKB handling test; actual WKB parsing is not yet implemented.
    // Once we implement the appropriate parsing logic, this test should be updated accordingly.
    Geography geography = Geography.fromWkb(wkb, 4326);
    assertNotNull(geography);
    assertArrayEquals(wkb, geography.toWkb());
    assertEquals(4326, geography.srid());
  }

  @Test
  void testFromWkbNoSridRudimentary() {
    byte[] wkb = new byte[]{1, 2, 3};
    // Note: This is a rudimentary WKB handling test; actual WKB parsing is not yet implemented.
    // Once we implement the appropriate parsing logic, this test should be updated accordingly.
    Geography geography = Geography.fromWkb(wkb);
    assertNotNull(geography);
    assertArrayEquals(wkb, geography.toWkb());
    assertEquals(4326, geography.srid());
  }

  /** Tests for Geography EWKB parsing. */

  @Test
  void testFromEwkbUnsupported() {
    byte[] ewkb = new byte[]{1, 2, 3};
    UnsupportedOperationException exception = assertThrows(
      UnsupportedOperationException.class,
      () -> Geography.fromEwkb(ewkb)
    );
    assertEquals("Geography EWKB parsing is not yet supported.", exception.getMessage());
  }

  /** Tests for Geography WKT parsing. */

  @Test
  void testFromWktWithSridUnsupported() {
    byte[] wkt = new byte[]{4, 5, 5};
    UnsupportedOperationException exception = assertThrows(
      UnsupportedOperationException.class,
      () -> Geography.fromWkt(wkt, 0)
    );
    assertEquals("Geography WKT parsing is not yet supported.", exception.getMessage());
  }

  @Test
  void testFromWktNoSridUnsupported() {
    byte[] wkt = new byte[]{4, 5, 5};
    UnsupportedOperationException exception = assertThrows(
      UnsupportedOperationException.class,
      () -> Geography.fromWkt(wkt)
    );
    assertEquals("Geography WKT parsing is not yet supported.", exception.getMessage());
  }

  /** Tests for Geography EWKT parsing. */

  @Test
  void testFromEwktUnsupported() {
    byte[] ewkt = new byte[]{4, 5, 5};
    UnsupportedOperationException exception = assertThrows(
      UnsupportedOperationException.class,
      () -> Geography.fromEwkt(ewkt)
    );
    assertEquals("Geography EWKT parsing is not yet supported.", exception.getMessage());
  }

  /** Tests for Geography WKB and EWKB converters. */

  @Test
  void testToWkb() {
    Geography geography = Geography.fromBytes(testGeographyVal);
    // WKB value (endianness: NDR) corresponding to WKT: POINT(1 2).
    byte[] wkb = HexFormat.of().parseHex("0101000000000000000000f03f0000000000000040");
    assertArrayEquals(wkb, geography.toWkb());
  }

  @Test
  void testToWkbEndiannessNDR() {
    Geography geography = Geography.fromBytes(testGeographyVal);
    // WKB value (endianness: NDR) corresponding to WKT: POINT(1 2).
    byte[] wkb = HexFormat.of().parseHex("0101000000000000000000f03f0000000000000040");
    assertArrayEquals(wkb, geography.toWkb(ByteOrder.LITTLE_ENDIAN));
  }

  @Test
  void testToWkbEndiannessXDR() {
    Geography geography = Geography.fromBytes(testGeographyVal);
    UnsupportedOperationException exception = assertThrows(
      UnsupportedOperationException.class,
      () -> geography.toWkb(ByteOrder.BIG_ENDIAN)
    );
    assertEquals("Geography WKB endianness is not yet supported.", exception.getMessage());
  }

  @Test
  void testToEwkbUnsupported() {
    Geography geography = Geography.fromBytes(testGeographyVal);
    UnsupportedOperationException exception = assertThrows(
      UnsupportedOperationException.class,
      geography::toEwkb
    );
    assertEquals("Geography EWKB conversion is not yet supported.", exception.getMessage());
  }

  @Test
  void testToEwkbEndiannessXDRUnsupported() {
    Geography geography = Geography.fromBytes(testGeographyVal);
    UnsupportedOperationException exception = assertThrows(
      UnsupportedOperationException.class,
      () -> geography.toEwkb(ByteOrder.BIG_ENDIAN)
    );
    assertEquals("Geography EWKB endianness is not yet supported.", exception.getMessage());
  }

  @Test
  void testToEwkbEndiannessNDRUnsupported() {
    Geography geography = Geography.fromBytes(testGeographyVal);
    UnsupportedOperationException exception = assertThrows(
      UnsupportedOperationException.class,
      () -> geography.toEwkb(ByteOrder.LITTLE_ENDIAN)
    );
    assertEquals("Geography EWKB endianness is not yet supported.", exception.getMessage());
  }

  /** Tests for Geography WKT and EWKT converters. */

  @Test
  void testToWktUnsupported() {
    Geography geography = Geography.fromBytes(testGeographyVal);
    UnsupportedOperationException exception = assertThrows(
      UnsupportedOperationException.class,
      geography::toWkt
    );
    assertEquals("Geography WKT conversion is not yet supported.", exception.getMessage());
  }

  @Test
  void testToEwktUnsupported() {
    Geography geography = Geography.fromBytes(testGeographyVal);
    UnsupportedOperationException exception = assertThrows(
      UnsupportedOperationException.class,
      geography::toEwkt
    );
    assertEquals("Geography EWKT conversion is not yet supported.", exception.getMessage());
  }

  /** Tests for other Geography methods. */

  @Test
  void testSrid() {
    Geography geography = Geography.fromBytes(testGeographyVal);
    assertEquals(4326, geography.srid());
  }
}
