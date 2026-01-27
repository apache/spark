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
 * Test suite for the Geometry client-side class.
 */
class GeometryClientSuite {

  // A sample Geometry WKB array for testing purposes, representing a POINT(1 2).
  private final byte[] testGeometryWkb = new byte[] { 0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, (byte)0xF0, 0x3F, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40 };
  private final int testGeometrySrid = 4326;

  /** Tests for Geometry factory methods and getters. */

  @Test
  void testFromWkb() {
    Geometry geometry = Geometry.fromWKB(testGeometryWkb);
    assertNotNull(geometry);
    assertArrayEquals(testGeometryWkb, geometry.getBytes());
    assertEquals(Geometry.DEFAULT_SRID, geometry.getSrid());
  }

  @Test
  void testFromWkbSrid() {
    Geometry geometry = Geometry.fromWKB(testGeometryWkb, testGeometrySrid);
    assertNotNull(geometry);
    assertArrayEquals(testGeometryWkb, geometry.getBytes());
    assertEquals(testGeometrySrid, geometry.getSrid());
  }

  /** Tests for Geometry constants. */

  @Test
  void testDefaultSrid() {
    assertEquals(0, Geometry.DEFAULT_SRID);
  }

  /** Tests for other Geometry methods. */

  @Test
  void testEquals() {
    Geometry geometry1 = Geometry.fromWKB(testGeometryWkb);
    Geometry geometry2 = Geometry.fromWKB(testGeometryWkb);
    Geometry geometry3 = Geometry.fromWKB(testGeometryWkb, 1);
    assertNotEquals(geometry1, null);
    assertNotEquals(geometry2, null);
    assertNotEquals(geometry3, null);
    assertEquals(geometry1, geometry2);
    assertNotEquals(geometry1, geometry3);
    assertNotEquals(geometry2, geometry3);
  }

  @Test
  void testHashCode() {
    Geometry geometry1 = Geometry.fromWKB(testGeometryWkb);
    Geometry geometry2 = Geometry.fromWKB(testGeometryWkb, testGeometrySrid);
    int wkbHash = Arrays.hashCode(testGeometryWkb);
    assertEquals(31 * wkbHash + Integer.hashCode(Geometry.DEFAULT_SRID), geometry1.hashCode());
    assertEquals(31 * wkbHash + Integer.hashCode(testGeometrySrid), geometry2.hashCode());
  }

}
