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

import org.apache.spark.SparkIllegalArgumentException;
import org.apache.spark.unsafe.types.BinaryView;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
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
    BinaryView value = BinaryView.fromBytes(testGeographyVal);
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

  // Helper method to create a simple WKB for POINT(0, 1).
  private byte[] getTestWKBPoint() {
    ByteBuffer bb = ByteBuffer.allocate(1 + 4 + 8 + 8);
    bb.order(ByteOrder.LITTLE_ENDIAN);
    bb.put((byte) 1); // byte order (LE)
    bb.putInt(1); // type = 1 (Point)
    bb.putDouble(0.0); // X = 0
    bb.putDouble(1.0); // Y = 0
    return bb.array();
  }

  @Test
  void testFromWkbWithSridRudimentary() {
    byte[] wkb = getTestWKBPoint();
    Geography geography = Geography.fromWkb(wkb, 4326);
    assertNotNull(geography);
    assertArrayEquals(wkb, geography.toWkb(ByteOrder.LITTLE_ENDIAN));
    assertEquals(4326, geography.srid());
  }

  @Test
  void testFromWkbNoSridRudimentary() {
    byte[] wkb = getTestWKBPoint();
    Geography geography = Geography.fromWkb(wkb);
    assertNotNull(geography);
    assertArrayEquals(wkb, geography.toWkb(ByteOrder.LITTLE_ENDIAN));
    assertEquals(4326, geography.srid());
  }

  @Test
  void testFromWkbInvalidWkb() {
    byte[] invalidWkb = new byte[]{111};
    SparkIllegalArgumentException exception = assertThrows(
      SparkIllegalArgumentException.class,
      () -> Geometry.fromWkb(invalidWkb)
    );
    assertEquals("WKB_PARSE_ERROR", exception.getCondition());
    assertTrue(exception.getMessage().contains("Unexpected end of WKB buffer"));
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
    assertArrayEquals(wkb, geography.toWkb(ByteOrder.LITTLE_ENDIAN));
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
    // WKB value (endianness: XDR) corresponding to WKT: POINT(1 2).
    byte[] wkb = HexFormat.of().parseHex("00000000013FF00000000000004000000000000000");
    assertArrayEquals(wkb, geography.toWkb(ByteOrder.BIG_ENDIAN));
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
  void testToWkt() {
    // The test geography is POINT(1 2) with SRID 4326.
    Geography geography = Geography.fromBytes(testGeographyVal);
    assertEquals("POINT(1 2)", new String(geography.toWkt(), StandardCharsets.UTF_8));
  }

  @Test
  void testToEwkt() {
    // The test geography is POINT(1 2) with SRID 4326.
    Geography geography = Geography.fromBytes(testGeographyVal);
    assertEquals("SRID=4326;POINT(1 2)", new String(geography.toEwkt(), StandardCharsets.UTF_8));
  }

  /** Tests for other Geography methods. */

  @Test
  void testSrid() {
    Geography geography = Geography.fromBytes(testGeographyVal);
    assertEquals(4326, geography.srid());
  }

  @Test
  void testSetSridOnTightOwner() {
    // fromBytes wraps a tight on-heap array, so setSrid writes through in place.
    Geography geography = Geography.fromBytes(testGeographyVal.clone());
    geography.setSrid(4269);
    assertEquals(4269, geography.srid());
  }

  @Test
  void testSetSridThrowsWhenNotTightOwner() {
    // A sub-range view does not own a tight backing array, so getBytes() returns a copy and an
    // in-place setSrid would be silently lost. It must fail loudly instead of dropping the write.
    byte[] padded = new byte[testGeographyVal.length + 4];
    System.arraycopy(testGeographyVal, 0, padded, 4, testGeographyVal.length);
    Geography geography = Geography.fromValue(
      BinaryView.fromBytes(padded, 4, testGeographyVal.length));
    // Reads still work (they copy out), and the original SRID is intact.
    assertEquals(4326, geography.srid());
    assertThrows(IllegalStateException.class, () -> geography.setSrid(4269));
    // After copy() the value owns a tight array, so setSrid succeeds and writes through.
    Geography owned = geography.copy();
    owned.setSrid(4269);
    assertEquals(4269, owned.srid());
    // The original sub-range view is untouched.
    assertEquals(4326, geography.srid());
  }
}
