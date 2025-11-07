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
import org.apache.spark.unsafe.types.GeographyVal;
import org.apache.spark.unsafe.types.GeometryVal;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.junit.jupiter.api.Assertions.*;


/**
 * Test suite for the ST expression utility class.
 */
class STUtilsSuite {

  /** Common test data used across multiple tests below. */

  private final byte[] testWkb = new byte[] {0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, (byte)0xF0, 0x3F, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40};

  // A sample Geography byte array for testing purposes, representing a POINT(1 2) with SRID 4326.
  private final int testGeographySrid = 4326;
  private final byte[] testGeographyBytes;

  // A sample Geometry byte array for testing purposes, representing a POINT(1 2) with SRID 0.
  private final int testGeometrySrid = 0;
  private final byte[] testGeometryBytes;

  {
    // Initialize headers.
    ByteOrder end = Geo.DEFAULT_ENDIANNESS;
    int sridLen = Geo.HEADER_SIZE;
    byte[] geogSrid = ByteBuffer.allocate(sridLen).order(end).putInt(testGeographySrid).array();
    byte[] geomSrid = ByteBuffer.allocate(sridLen).order(end).putInt(testGeometrySrid).array();
    // Initialize GEOGRAPHY.
    int wkbLen = testWkb.length;
    testGeographyBytes = new byte[sridLen + wkbLen];
    System.arraycopy(geogSrid, 0, testGeographyBytes, 0, sridLen);
    System.arraycopy(testWkb, 0, testGeographyBytes, sridLen, wkbLen);
    // Initialize GEOMETRY.
    testGeometryBytes = new byte[sridLen + wkbLen];
    System.arraycopy(geomSrid, 0, testGeometryBytes, 0, sridLen);
    System.arraycopy(testWkb, 0, testGeometryBytes, sridLen, wkbLen);
  }

  /** Geospatial type casting utility methods. */

  @Test
  void testGeographyToGeometry() {
    GeographyVal geographyVal = GeographyVal.fromBytes(testGeographyBytes);
    GeometryVal geometryVal = STUtils.geographyToGeometry(geographyVal);
    assertNotNull(geometryVal);
    assertArrayEquals(geographyVal.getBytes(), geometryVal.getBytes());
  }

  /** Tests for ST expression utility methods. */

  // ST_AsBinary
  @Test
  void testStAsBinaryGeography() {
    GeographyVal geographyVal = GeographyVal.fromBytes(testGeographyBytes);
    byte[] geographyWkb = STUtils.stAsBinary(geographyVal);
    assertNotNull(geographyWkb);
    assertArrayEquals(testWkb, geographyWkb);
  }

  @Test
  void testStAsBinaryGeometry() {
    GeometryVal geometryVal = GeometryVal.fromBytes(testGeometryBytes);
    byte[] geometryWkb = STUtils.stAsBinary(geometryVal);
    assertNotNull(geometryWkb);
    assertArrayEquals(testWkb, geometryWkb);
  }

  // ST_GeogFromWKB
  @Test
  void testStGeogFromWKB() {
    GeographyVal geographyVal = STUtils.stGeogFromWKB(testWkb);
    assertNotNull(geographyVal);
    assertArrayEquals(testGeographyBytes, geographyVal.getBytes());
  }

  // ST_GeomFromWKB
  @Test
  void testStGeomFromWKB() {
    GeometryVal geometryVal = STUtils.stGeomFromWKB(testWkb);
    assertNotNull(geometryVal);
    assertArrayEquals(testGeometryBytes, geometryVal.getBytes());
  }

  // ST_Srid
  @Test
  void testStSridGeography() {
    GeographyVal geographyVal = GeographyVal.fromBytes(testGeographyBytes);
    assertEquals(testGeographySrid, STUtils.stSrid(geographyVal));
  }

  @Test
  void testStSridGeometry() {
    GeometryVal geometryVal = GeometryVal.fromBytes(testGeometryBytes);
    assertEquals(testGeometrySrid, STUtils.stSrid(geometryVal));
  }

  // ST_SetSrid
  @Test
  void testStSetSridGeography() {
    for (int validGeographySrid : new int[]{4326}) {
      GeographyVal geographyVal = GeographyVal.fromBytes(testGeographyBytes);
      GeographyVal updatedGeographyVal = STUtils.stSetSrid(geographyVal, validGeographySrid);
      assertNotNull(updatedGeographyVal);
      Geography updatedGeography = Geography.fromBytes(updatedGeographyVal.getBytes());
      assertEquals(validGeographySrid, updatedGeography.srid());
    }
  }

  @Test
  void testStSetSridGeographyInvalidSrid() {
    for (int invalidGeographySrid : new int[]{-9999, -2, -1, 0, 1, 2, 3857, 9999}) {
      GeographyVal geographyVal = GeographyVal.fromBytes(testGeographyBytes);
      SparkIllegalArgumentException exception = assertThrows(SparkIllegalArgumentException.class,
        () -> STUtils.stSetSrid(geographyVal, invalidGeographySrid));
      assertEquals("ST_INVALID_SRID_VALUE", exception.getCondition());
      assertTrue(exception.getMessage().contains("value: " + invalidGeographySrid + "."));
    }
  }

  @Test
  void testStSetSridGeometry() {
    for (int validGeographySrid : new int[]{0, 3857, 4326}) {
      GeometryVal geometryVal = GeometryVal.fromBytes(testGeometryBytes);
      GeometryVal updatedGeometryVal = STUtils.stSetSrid(geometryVal, validGeographySrid);
      assertNotNull(updatedGeometryVal);
      Geometry updatedGeometry = Geometry.fromBytes(updatedGeometryVal.getBytes());
      assertEquals(validGeographySrid, updatedGeometry.srid());
    }
  }

  @Test
  void testStSetSridGeometryInvalidSrid() {
    for (int invalidGeometrySrid : new int[]{-9999, -2, -1, 1, 2, 9999}) {
      GeometryVal geometryVal = GeometryVal.fromBytes(testGeometryBytes);
      SparkIllegalArgumentException exception = assertThrows(SparkIllegalArgumentException.class,
        () -> STUtils.stSetSrid(geometryVal, invalidGeometrySrid));
      assertEquals("ST_INVALID_SRID_VALUE", exception.getCondition());
      assertTrue(exception.getMessage().contains("value: " + invalidGeometrySrid + "."));
    }
  }

}
