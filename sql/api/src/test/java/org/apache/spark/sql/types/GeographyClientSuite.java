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

package org.apache.spark.sql.types;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;


/**
 * Test suite for the Geography client-side class.
 */
class GeographyClientSuite {

  // A sample Geography WKB array for testing purposes, representing a POINT(1 2).
  private final byte[] testGeographyWkb = new byte[] { 0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, (byte)0xF0, 0x3F, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40 };
  private final int testGeographySrid = 4326;

  /** Tests for Geography factory methods and getters. */

  @Test
  void testFromWkb() {
    Geography geography = Geography.fromWKB(testGeographyWkb);
    assertNotNull(geography);
    assertArrayEquals(testGeographyWkb, geography.getBytes());
    assertEquals(Geography.DEFAULT_SRID, geography.getSrid());
  }

  @Test
  void testFromWkbSrid() {
    Geography geography = Geography.fromWKB(testGeographyWkb, testGeographySrid);
    assertNotNull(geography);
    assertArrayEquals(testGeographyWkb, geography.getBytes());
    assertEquals(testGeographySrid, geography.getSrid());
  }

  /** Tests for Geography constants. */

  @Test
  void testDefaultSrid() {
    assertEquals(4326, Geography.DEFAULT_SRID);
  }

  /** Tests for other Geography methods. */

  @Test
  void testEquals() {
    Geography geography1 = Geography.fromWKB(testGeographyWkb);
    Geography geography2 = Geography.fromWKB(testGeographyWkb);
    Geography geography3 = Geography.fromWKB(testGeographyWkb, 1);
    assertNotEquals(geography1, null);
    assertNotEquals(geography2, null);
    assertNotEquals(geography3, null);
    assertEquals(geography1, geography2);
    assertNotEquals(geography1, geography3);
    assertNotEquals(geography2, geography3);
  }

  @Test
  void testHashCode() {
    Geography geography1 = Geography.fromWKB(testGeographyWkb);
    Geography geography2 = Geography.fromWKB(testGeographyWkb, testGeographySrid);
    int wkbHash = Arrays.hashCode(testGeographyWkb);
    assertEquals(31 * wkbHash + Integer.hashCode(Geography.DEFAULT_SRID), geography1.hashCode());
    assertEquals(31 * wkbHash + Integer.hashCode(testGeographySrid), geography2.hashCode());
  }

}
