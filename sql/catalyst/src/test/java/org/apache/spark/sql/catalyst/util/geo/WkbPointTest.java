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

package org.apache.spark.sql.catalyst.util.geo;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Comprehensive test suite for WKB Point geometries.
 */
public class WkbPointTest extends WkbTestBase {

  @Test
  public void testEmptyPoint() {
    String wkbHexLittle = "0101000000000000000000f87f000000000000f87f";
    String wkbHexBig = "00000000017ff80000000000007ff8000000000000";

    // Test little endian
    byte[] wkbBytes = hexToBytes(wkbHexLittle);
    WkbReader reader = new WkbReader();
    GeometryModel geom = reader.read(wkbBytes);

    Assertions.assertTrue(geom.isPoint(), "Should be a Point");
    Point point = geom.asPoint();
    Assertions.assertTrue(point.isEmpty(), "Point should be empty");
    Assertions.assertTrue(Double.isNaN(point.getX()), "X should be NaN for empty point");
    Assertions.assertTrue(Double.isNaN(point.getY()), "Y should be NaN for empty point");

    // Test big endian
    byte[] wkbBytesBig = hexToBytes(wkbHexBig);
    GeometryModel geomBig = reader.read(wkbBytesBig);

    Assertions.assertTrue(geomBig.isPoint(), "Should be a Point");
    Point pointBig = geomBig.asPoint();
    Assertions.assertTrue(pointBig.isEmpty(), "Point should be empty");
  }

  @Test
  public void testSimplePoint() {
    // POINT(1 2)
    String wkbHexLittle = "0101000000000000000000f03f0000000000000040";
    String wkbHexBig = "00000000013ff00000000000004000000000000000";

    // Test little endian
    byte[] wkbBytes = hexToBytes(wkbHexLittle);
    WkbReader reader = new WkbReader();
    GeometryModel geom = reader.read(wkbBytes);

    Assertions.assertTrue(geom.isPoint(), "Should be a Point");
    Point point = geom.asPoint();
    Assertions.assertFalse(point.isEmpty(), "Point should not be empty");
    Assertions.assertEquals(1.0, point.getX(), 1e-10, "X coordinate");
    Assertions.assertEquals(2.0, point.getY(), 1e-10, "Y coordinate");

    // Test big endian
    byte[] wkbBytesBig = hexToBytes(wkbHexBig);
    GeometryModel geomBig = reader.read(wkbBytesBig);

    Point pointBig = geomBig.asPoint();
    Assertions.assertEquals(1.0, pointBig.getX(), 1e-10, "X coordinate (big endian)");
    Assertions.assertEquals(2.0, pointBig.getY(), 1e-10, "Y coordinate (big endian)");
  }

  @Test
  public void testPointAtWesternHemisphereBoundary() {
    // POINT(-180 0)
    String wkbHexLittle = "010100000000000000008066c00000000000000000";
    String wkbHexBig = "0000000001c0668000000000000000000000000000";

    byte[] wkbBytes = hexToBytes(wkbHexLittle);
    WkbReader reader = new WkbReader();
    GeometryModel geom = reader.read(wkbBytes);

    Point point = geom.asPoint();
    Assertions.assertEquals(-180.0, point.getX(), 1e-10, "X coordinate");
    Assertions.assertEquals(0.0, point.getY(), 1e-10, "Y coordinate");
  }

  @Test
  public void testPointAtSouthPole() {
    // POINT(0 -90)
    String wkbHexLittle = "0101000000000000000000000000000000008056c0";
    String wkbHexBig = "00000000010000000000000000c056800000000000";

    byte[] wkbBytes = hexToBytes(wkbHexLittle);
    WkbReader reader = new WkbReader();
    GeometryModel geom = reader.read(wkbBytes);

    Point point = geom.asPoint();
    Assertions.assertEquals(0.0, point.getX(), 1e-10, "X coordinate");
    Assertions.assertEquals(-90.0, point.getY(), 1e-10, "Y coordinate");
  }

  @Test
  public void testPointAtNorthPole() {
    // POINT(0 90)
    String wkbHexLittle = "010100000000000000000000000000000000805640";
    String wkbHexBig = "000000000100000000000000004056800000000000";

    byte[] wkbBytes = hexToBytes(wkbHexLittle);
    WkbReader reader = new WkbReader();
    GeometryModel geom = reader.read(wkbBytes);

    Point point = geom.asPoint();
    Assertions.assertEquals(0.0, point.getX(), 1e-10, "X coordinate");
    Assertions.assertEquals(90.0, point.getY(), 1e-10, "Y coordinate");
  }

  @Test
  public void testPointWithNegativeCoordinates() {
    // POINT(-1.5 -2.5)
    double[] coords = {-1.5, -2.5};
    Point point = new Point(coords, 0);

    Assertions.assertEquals(-1.5, point.getX(), 1e-10, "X coordinate");
    Assertions.assertEquals(-2.5, point.getY(), 1e-10, "Y coordinate");
    Assertions.assertFalse(point.isEmpty(), "Point should not be empty");
  }

  @Test
  public void testPointWithLargeCoordinates() {
    // POINT(1000000 2000000)
    double[] coords = {1000000.0, 2000000.0};
    Point point = new Point(coords, 0);

    Assertions.assertEquals(1000000.0, point.getX(), 1e-10, "X coordinate");
    Assertions.assertEquals(2000000.0, point.getY(), 1e-10, "Y coordinate");
  }

  @Test
  public void testPointWithSmallCoordinates() {
    // POINT(0.000001 0.000002)
    double[] coords = {0.000001, 0.000002};
    Point point = new Point(coords, 0);

    Assertions.assertEquals(0.000001, point.getX(), 1e-10, "X coordinate");
    Assertions.assertEquals(0.000002, point.getY(), 1e-10, "Y coordinate");
  }

  @Test
  public void testPointEquality() {
    Point point1 = new Point(new double[]{1.0, 2.0}, 0);
    Point point2 = new Point(new double[]{1.0, 2.0}, 0);
    Point point3 = new Point(new double[]{1.0, 2.1}, 0);

    Assertions.assertEquals(point1.getX(), point2.getX(), 1e-10,
        "Same coordinates should have same X");
    Assertions.assertEquals(point1.getY(), point2.getY(), 1e-10,
        "Same coordinates should have same Y");
    Assertions.assertNotEquals(point1.getY(), point3.getY(), "Different Y should be different");
  }

  @Test
  public void testPoint3D() {
    // Test 3D point (with Z coordinate)
    double[] coords = {1.0, 2.0, 3.0};
    Point point = new Point(coords, 0);

    Assertions.assertEquals(1.0, point.getX(), 1e-10, "X coordinate");
    Assertions.assertEquals(2.0, point.getY(), 1e-10, "Y coordinate");
    Assertions.assertEquals(3.0, point.getZ(), 1e-10, "Z coordinate");
  }

  @Test
  public void testPoint4D() {
    // Test 4D point (with Z and M coordinates)
    double[] coords = {1.0, 2.0, 3.0, 4.0};
    Point point = new Point(coords, 0);

    Assertions.assertEquals(1.0, point.getX(), 1e-10, "X coordinate");
    Assertions.assertEquals(2.0, point.getY(), 1e-10, "Y coordinate");
    Assertions.assertEquals(3.0, point.getZ(), 1e-10, "Z coordinate");
    Assertions.assertEquals(4.0, point.getM(), 1e-10, "M coordinate");
  }
}
