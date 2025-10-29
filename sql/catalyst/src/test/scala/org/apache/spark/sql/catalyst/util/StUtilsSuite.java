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

import static org.junit.jupiter.api.Assertions.*;


/**
 * Test suite for the ST expression utility class.
 */
class STUtilsSuite {

  /** Common test data used across multiple tests below. */

  private final byte[] testWkb = new byte[] {0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, (byte)0xF0, 0x3F, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40};

  // A sample Geography byte array for testing purposes, representing a POINT(1 2) with SRID 4326.
  private final byte[] testGeographySrid = new byte[] {(byte)0xE6, 0x10, 0x00, 0x00};
  private final byte[] testGeographyBytes;

  // A sample Geometry byte array for testing purposes, representing a POINT(1 2) with SRID 0.
  private final byte[] testGeometrySrid = new byte[] {0x00, 0x00, 0x00, 0x00};
  private final byte[] testGeometryBytes;

  {
    int sridLen = testGeographySrid.length;
    int wkbLen = testWkb.length;
    // Initialize GEOGRAPHY.
    testGeographyBytes = new byte[sridLen + wkbLen];
    System.arraycopy(testGeographySrid, 0, testGeographyBytes, 0, sridLen);
    System.arraycopy(testWkb, 0, testGeographyBytes, sridLen, wkbLen);
    // Initialize GEOMETRY.
    testGeometryBytes = new byte[sridLen + wkbLen];
    System.arraycopy(testGeometrySrid, 0, testGeometryBytes, 0, sridLen);
    System.arraycopy(testWkb, 0, testGeometryBytes, sridLen, wkbLen);
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

}
