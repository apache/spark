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

  private final byte[] testWkbNdr = new byte[] {0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, (byte)0xF0, 0x3F, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40};
  private final byte[] testWkbXdr = new byte[] {0x00, 0x00, 0x00, 0x00, 0x01, 0x3f, (byte)0xf0,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00};

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
    int wkbLen = testWkbNdr.length;
    testGeographyBytes = new byte[sridLen + wkbLen];
    System.arraycopy(geogSrid, 0, testGeographyBytes, 0, sridLen);
    System.arraycopy(testWkbNdr, 0, testGeographyBytes, sridLen, wkbLen);
    // Initialize GEOMETRY.
    testGeometryBytes = new byte[sridLen + wkbLen];
    System.arraycopy(geomSrid, 0, testGeometryBytes, 0, sridLen);
    System.arraycopy(testWkbNdr, 0, testGeometryBytes, sridLen, wkbLen);
  }

  /** Tests for ST expression utility methods. */

  // ST_AsBinary
  @Test
  void testStAsBinaryGeography() {
    GeographyVal geographyVal = GeographyVal.fromBytes(testGeographyBytes);
    byte[] geographyWkb = STUtils.stAsBinary(geographyVal);
    assertNotNull(geographyWkb);
    assertArrayEquals(testWkbNdr, geographyWkb);
  }

  @Test
  void testStAsBinaryGeometry() {
    GeometryVal geometryVal = GeometryVal.fromBytes(testGeometryBytes);
    byte[] geometryWkb = STUtils.stAsBinary(geometryVal);
    assertNotNull(geometryWkb);
    assertArrayEquals(testWkbNdr, geometryWkb);
  }

  // ST_GeogFromWKB
  @Test
  void testStGeogFromWKB_NDR() {
    GeographyVal geographyVal = STUtils.stGeogFromWKB(testWkbNdr);
    assertNotNull(geographyVal);
    assertArrayEquals(testGeographyBytes, geographyVal.getBytes());
  }

  @Test
  void testStGeogFromWKB_XDR() {
    GeographyVal geographyVal = STUtils.stGeogFromWKB(testWkbXdr);
    assertNotNull(geographyVal);
    assertArrayEquals(testGeographyBytes, geographyVal.getBytes());
  }

  // ST_GeomFromWKB
  @Test
  void testStGeomFromWKB() {
    GeometryVal geometryVal = STUtils.stGeomFromWKB(testWkbNdr);
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

}
