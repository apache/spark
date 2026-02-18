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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Test suite for WKB geography parsing with coordinate bounds validation. In geography mode,
 * X must be between -180 and 180 (inclusive), and Y must be between -90 and 90 (inclusive).
 */
public class WkbGeographyTest extends WkbTestBase {

  /** Creates a geography-mode WkbReader with default validation (level 1). */
  private WkbReader geographyReader1() {
    return new WkbReader(1, true);
  }

  /** Constructs WKB for a 2D point in little endian format. */
  private byte[] makePointWkb2D(double x, double y) {
    ByteBuffer buf = ByteBuffer.allocate(1 + 4 + 8 + 8);
    buf.order(ByteOrder.LITTLE_ENDIAN);
    buf.put((byte) 1); // Little endian.
    buf.putInt(1); // Point type (2D).
    buf.putDouble(x);
    buf.putDouble(y);
    return buf.array();
  }

  /** Constructs WKB for a 3DZ point in little endian format. */
  private byte[] makePointWkb3DZ(double x, double y, double z) {
    ByteBuffer buf = ByteBuffer.allocate(1 + 4 + 8 + 8 + 8);
    buf.order(ByteOrder.LITTLE_ENDIAN);
    buf.put((byte) 1); // Little endian.
    buf.putInt(1001); // Point type (3DZ).
    buf.putDouble(x);
    buf.putDouble(y);
    buf.putDouble(z);
    return buf.array();
  }

  /** Constructs WKB for a 3DM point in little endian format. */
  private byte[] makePointWkb3DM(double x, double y, double m) {
    ByteBuffer buf = ByteBuffer.allocate(1 + 4 + 8 + 8 + 8);
    buf.order(ByteOrder.LITTLE_ENDIAN);
    buf.put((byte) 1); // Little endian.
    buf.putInt(2001); // Point type (3DM).
    buf.putDouble(x);
    buf.putDouble(y);
    buf.putDouble(m);
    return buf.array();
  }

  /** Constructs WKB for a 4D (ZM) point in little endian format. */
  private byte[] makePointWkb4D(double x, double y, double z, double m) {
    ByteBuffer buf = ByteBuffer.allocate(1 + 4 + 8 + 8 + 8 + 8);
    buf.order(ByteOrder.LITTLE_ENDIAN);
    buf.put((byte) 1); // Little endian.
    buf.putInt(3001); // Point type (4D).
    buf.putDouble(x);
    buf.putDouble(y);
    buf.putDouble(z);
    buf.putDouble(m);
    return buf.array();
  }

  /** Constructs WKB for a 2D linestring in little endian format. */
  private byte[] makeLineStringWkb2D(double[][] points) {
    ByteBuffer buf = ByteBuffer.allocate(1 + 4 + 4 + points.length * 16);
    buf.order(ByteOrder.LITTLE_ENDIAN);
    buf.put((byte) 1); // Little endian.
    buf.putInt(2); // LineString type (2D).
    buf.putInt(points.length);
    for (double[] point : points) {
      buf.putDouble(point[0]);
      buf.putDouble(point[1]);
    }
    return buf.array();
  }

  /** Constructs WKB for a 2D polygon in little endian format. */
  private byte[] makePolygonWkb2D(double[][][] rings) {
    int size = 1 + 4 + 4;
    for (double[][] ring : rings) {
      size += 4 + ring.length * 16;
    }
    ByteBuffer buf = ByteBuffer.allocate(size);
    buf.order(ByteOrder.LITTLE_ENDIAN);
    buf.put((byte) 1); // Little endian.
    buf.putInt(3); // Polygon type (2D).
    buf.putInt(rings.length);
    for (double[][] ring : rings) {
      buf.putInt(ring.length);
      for (double[] point : ring) {
        buf.putDouble(point[0]);
        buf.putDouble(point[1]);
      }
    }
    return buf.array();
  }

  /** Constructs WKB for a 2D multipoint in little endian format. */
  private byte[] makeMultiPointWkb2D(double[][] points) {
    // Each nested point: 1 (endian) + 4 (type) + 16 (coords) = 21 bytes
    ByteBuffer buf = ByteBuffer.allocate(1 + 4 + 4 + points.length * 21);
    buf.order(ByteOrder.LITTLE_ENDIAN);
    buf.put((byte) 1); // Little endian.
    buf.putInt(4); // MultiPoint type (2D).
    buf.putInt(points.length);
    for (double[] point : points) {
      buf.put((byte) 1); // Little endian.
      buf.putInt(1); // Point type (2D).
      buf.putDouble(point[0]);
      buf.putDouble(point[1]);
    }
    return buf.array();
  }

  // ========== Valid Geography 2D Point Tests ==========

  @Test
  public void testGeographyPointOrigin() {
    // POINT(0 0) - valid geography
    byte[] wkb = makePointWkb2D(0.0, 0.0);
    GeometryModel geom = geographyReader1().read(wkb);
    Assertions.assertTrue(geom.isPoint(), "Should be a Point");
    Point point = geom.asPoint();
    Assertions.assertEquals(0.0, point.getX(), 1e-10, "X coordinate");
    Assertions.assertEquals(0.0, point.getY(), 1e-10, "Y coordinate");
  }

  @Test
  public void testGeographyPointBoundaryMinLonMinLat() {
    // POINT(-180 -90) - valid at minimum boundary
    byte[] wkb = makePointWkb2D(-180.0, -90.0);
    GeometryModel geom = geographyReader1().read(wkb);
    Point point = geom.asPoint();
    Assertions.assertEquals(-180.0, point.getX(), 1e-10, "X at min longitude");
    Assertions.assertEquals(-90.0, point.getY(), 1e-10, "Y at min latitude");
  }

  @Test
  public void testGeographyPointBoundaryMaxLonMaxLat() {
    // POINT(180 90) - valid at maximum boundary
    byte[] wkb = makePointWkb2D(180.0, 90.0);
    GeometryModel geom = geographyReader1().read(wkb);
    Point point = geom.asPoint();
    Assertions.assertEquals(180.0, point.getX(), 1e-10, "X at max longitude");
    Assertions.assertEquals(90.0, point.getY(), 1e-10, "Y at max latitude");
  }

  @Test
  public void testGeographyPointBoundaryMinLon() {
    // POINT(-180 0) - valid at min longitude
    byte[] wkb = makePointWkb2D(-180.0, 0.0);
    GeometryModel geom = geographyReader1().read(wkb);
    Assertions.assertEquals(-180.0, geom.asPoint().getX(), 1e-10, "X at min longitude");
  }

  @Test
  public void testGeographyPointBoundaryMaxLon() {
    // POINT(180 0) - valid at max longitude
    byte[] wkb = makePointWkb2D(180.0, 0.0);
    GeometryModel geom = geographyReader1().read(wkb);
    Assertions.assertEquals(180.0, geom.asPoint().getX(), 1e-10, "X at max longitude");
  }

  @Test
  public void testGeographyPointBoundaryMinLat() {
    // POINT(0 -90) - valid at min latitude
    byte[] wkb = makePointWkb2D(0.0, -90.0);
    GeometryModel geom = geographyReader1().read(wkb);
    Assertions.assertEquals(-90.0, geom.asPoint().getY(), 1e-10, "Y at min latitude");
  }

  @Test
  public void testGeographyPointBoundaryMaxLat() {
    // POINT(0 90) - valid at max latitude
    byte[] wkb = makePointWkb2D(0.0, 90.0);
    GeometryModel geom = geographyReader1().read(wkb);
    Assertions.assertEquals(90.0, geom.asPoint().getY(), 1e-10, "Y at max latitude");
  }

  @Test
  public void testGeographyPointTypicalCoordinates() {
    // POINT(-73.9857 40.7484) - New York City
    byte[] wkb = makePointWkb2D(-73.9857, 40.7484);
    GeometryModel geom = geographyReader1().read(wkb);
    Point point = geom.asPoint();
    Assertions.assertEquals(-73.9857, point.getX(), 1e-10, "X longitude");
    Assertions.assertEquals(40.7484, point.getY(), 1e-10, "Y latitude");
  }

  // ========== Empty Point in Geography Mode ==========

  @Test
  public void testGeographyEmptyPoint2D() {
    // POINT EMPTY - valid in geography mode (NaN coordinates skip bounds check)
    String wkbHex = "0101000000000000000000f87f000000000000f87f";
    byte[] wkb = hexToBytes(wkbHex);
    GeometryModel geom = geographyReader1().read(wkb);
    Assertions.assertTrue(geom.isPoint(), "Should be a Point");
    Assertions.assertTrue(geom.asPoint().isEmpty(), "Point should be empty");
  }

  @Test
  public void testGeographyEmptyPoint3DZ() {
    // POINT Z EMPTY
    String wkbHex = "01e9030000000000000000f87f000000000000f87f000000000000f87f";
    byte[] wkb = hexToBytes(wkbHex);
    GeometryModel geom = geographyReader1().read(wkb);
    Assertions.assertTrue(geom.asPoint().isEmpty(), "Point Z should be empty");
  }

  @Test
  public void testGeographyEmptyPoint4D() {
    // POINT ZM EMPTY
    String wkbHex =
        "01b90b0000000000000000f87f000000000000f87f000000000000f87f000000000000f87f";
    byte[] wkb = hexToBytes(wkbHex);
    GeometryModel geom = geographyReader1().read(wkb);
    Assertions.assertTrue(geom.asPoint().isEmpty(), "Point ZM should be empty");
  }

  // ========== Invalid Geography Point - Longitude Out of Bounds ==========

  @Test
  public void testGeographyPointLongitudeJustOverMax() {
    // POINT(180.0001 0) - longitude just over 180
    byte[] wkb = makePointWkb2D(180.0001, 0.0);
    Assertions.assertThrows(WkbParseException.class, () -> geographyReader1().read(wkb));
  }

  @Test
  public void testGeographyPointLongitudeJustUnderMin() {
    // POINT(-180.0001 0) - longitude just under -180
    byte[] wkb = makePointWkb2D(-180.0001, 0.0);
    Assertions.assertThrows(WkbParseException.class, () -> geographyReader1().read(wkb));
  }

  @Test
  public void testGeographyPointLongitudeTooHigh() {
    // POINT(181 0) - longitude > 180
    byte[] wkb = makePointWkb2D(181.0, 0.0);
    Assertions.assertThrows(WkbParseException.class, () -> geographyReader1().read(wkb));
  }

  @Test
  public void testGeographyPointLongitudeTooLow() {
    // POINT(-181 0) - longitude < -180
    byte[] wkb = makePointWkb2D(-181.0, 0.0);
    Assertions.assertThrows(WkbParseException.class, () -> geographyReader1().read(wkb));
  }

  @Test
  public void testGeographyPointLongitudeWayTooHigh() {
    // POINT(360 0) - longitude way over 180
    byte[] wkb = makePointWkb2D(360.0, 0.0);
    Assertions.assertThrows(WkbParseException.class, () -> geographyReader1().read(wkb));
  }

  @Test
  public void testGeographyPointLongitudeWayTooLow() {
    // POINT(-360 0) - longitude way under -180
    byte[] wkb = makePointWkb2D(-360.0, 0.0);
    Assertions.assertThrows(WkbParseException.class, () -> geographyReader1().read(wkb));
  }

  // ========== Invalid Geography Point - Latitude Out of Bounds ==========

  @Test
  public void testGeographyPointLatitudeJustOverMax() {
    // POINT(0 90.0001) - latitude just over 90
    byte[] wkb = makePointWkb2D(0.0, 90.0001);
    Assertions.assertThrows(WkbParseException.class, () -> geographyReader1().read(wkb));
  }

  @Test
  public void testGeographyPointLatitudeJustUnderMin() {
    // POINT(0 -90.0001) - latitude just under -90
    byte[] wkb = makePointWkb2D(0.0, -90.0001);
    Assertions.assertThrows(WkbParseException.class, () -> geographyReader1().read(wkb));
  }

  @Test
  public void testGeographyPointLatitudeTooHigh() {
    // POINT(0 91) - latitude > 90
    byte[] wkb = makePointWkb2D(0.0, 91.0);
    Assertions.assertThrows(WkbParseException.class, () -> geographyReader1().read(wkb));
  }

  @Test
  public void testGeographyPointLatitudeTooLow() {
    // POINT(0 -91) - latitude < -90
    byte[] wkb = makePointWkb2D(0.0, -91.0);
    Assertions.assertThrows(WkbParseException.class, () -> geographyReader1().read(wkb));
  }

  @Test
  public void testGeographyPointLatitudeWayTooHigh() {
    // POINT(0 180) - latitude way over 90
    byte[] wkb = makePointWkb2D(0.0, 180.0);
    Assertions.assertThrows(WkbParseException.class, () -> geographyReader1().read(wkb));
  }

  @Test
  public void testGeographyPointLatitudeWayTooLow() {
    // POINT(0 -180) - latitude way under -90
    byte[] wkb = makePointWkb2D(0.0, -180.0);
    Assertions.assertThrows(WkbParseException.class, () -> geographyReader1().read(wkb));
  }

  // ========== Invalid Geography Point - Both Out of Bounds ==========

  @Test
  public void testGeographyPointBothOutOfBounds() {
    // POINT(200 100) - both longitude and latitude out of bounds
    byte[] wkb = makePointWkb2D(200.0, 100.0);
    Assertions.assertThrows(WkbParseException.class, () -> geographyReader1().read(wkb));
  }

  // ========== Geography 3DZ Point Tests (Z has no bounds) ==========

  @Test
  public void testGeographyPoint3DZ_ValidWithLargeZ() {
    // POINT Z (0 0 999999) - Z has no bounds in geography mode
    byte[] wkb = makePointWkb3DZ(0.0, 0.0, 999999.0);
    GeometryModel geom = geographyReader1().read(wkb);
    Point point = geom.asPoint();
    Assertions.assertEquals(0.0, point.getX(), 1e-10, "X coordinate");
    Assertions.assertEquals(0.0, point.getY(), 1e-10, "Y coordinate");
    Assertions.assertEquals(999999.0, point.getZ(), 1e-10, "Z coordinate (no bounds)");
  }

  @Test
  public void testGeographyPoint3DZ_ValidWithNegativeZ() {
    // POINT Z (0 0 -999999) - negative Z has no bounds
    byte[] wkb = makePointWkb3DZ(0.0, 0.0, -999999.0);
    GeometryModel geom = geographyReader1().read(wkb);
    Assertions.assertEquals(-999999.0, geom.asPoint().getZ(), 1e-10, "Z coordinate (no bounds)");
  }

  @Test
  public void testGeographyPoint3DZ_ValidAtBoundaryWithZ() {
    // POINT Z (180 90 100000) - at boundary with large Z
    byte[] wkb = makePointWkb3DZ(180.0, 90.0, 100000.0);
    GeometryModel geom = geographyReader1().read(wkb);
    Point point = geom.asPoint();
    Assertions.assertEquals(180.0, point.getX(), 1e-10);
    Assertions.assertEquals(90.0, point.getY(), 1e-10);
    Assertions.assertEquals(100000.0, point.getZ(), 1e-10);
  }

  @Test
  public void testGeographyPoint3DZ_InvalidLongitude() {
    // POINT Z (200 0 5) - longitude out of bounds, Z irrelevant
    byte[] wkb = makePointWkb3DZ(200.0, 0.0, 5.0);
    Assertions.assertThrows(WkbParseException.class, () -> geographyReader1().read(wkb));
  }

  @Test
  public void testGeographyPoint3DZ_InvalidLatitude() {
    // POINT Z (0 100 5) - latitude out of bounds, Z irrelevant
    byte[] wkb = makePointWkb3DZ(0.0, 100.0, 5.0);
    Assertions.assertThrows(WkbParseException.class, () -> geographyReader1().read(wkb));
  }

  // ========== Geography 3DM Point Tests (M has no bounds) ==========

  @Test
  public void testGeographyPoint3DM_ValidWithLargeM() {
    // POINT M (0 0 999999) - M has no bounds in geography mode
    byte[] wkb = makePointWkb3DM(0.0, 0.0, 999999.0);
    GeometryModel geom = geographyReader1().read(wkb);
    Assertions.assertFalse(geom.asPoint().isEmpty(), "Point should not be empty");
  }

  @Test
  public void testGeographyPoint3DM_ValidWithNegativeM() {
    // POINT M (0 0 -999999) - negative M has no bounds
    byte[] wkb = makePointWkb3DM(0.0, 0.0, -999999.0);
    GeometryModel geom = geographyReader1().read(wkb);
    Assertions.assertFalse(geom.asPoint().isEmpty(), "Point should not be empty");
  }

  @Test
  public void testGeographyPoint3DM_InvalidLongitude() {
    // POINT M (200 0 5) - longitude out of bounds
    byte[] wkb = makePointWkb3DM(200.0, 0.0, 5.0);
    Assertions.assertThrows(WkbParseException.class, () -> geographyReader1().read(wkb));
  }

  @Test
  public void testGeographyPoint3DM_InvalidLatitude() {
    // POINT M (0 100 5) - latitude out of bounds
    byte[] wkb = makePointWkb3DM(0.0, 100.0, 5.0);
    Assertions.assertThrows(WkbParseException.class, () -> geographyReader1().read(wkb));
  }

  // ========== Geography 4D Point Tests (Z and M have no bounds) ==========

  @Test
  public void testGeographyPoint4D_ValidWithLargeZM() {
    // POINT ZM (0 0 999999 -999999) - Z and M have no bounds
    byte[] wkb = makePointWkb4D(0.0, 0.0, 999999.0, -999999.0);
    GeometryModel geom = geographyReader1().read(wkb);
    Point point = geom.asPoint();
    Assertions.assertEquals(999999.0, point.getZ(), 1e-10, "Z (no bounds)");
    Assertions.assertEquals(-999999.0, point.getM(), 1e-10, "M (no bounds)");
  }

  @Test
  public void testGeographyPoint4D_ValidAtBoundaryWithZM() {
    // POINT ZM (-180 -90 -100000 100000) - at boundary with arbitrary Z and M
    byte[] wkb = makePointWkb4D(-180.0, -90.0, -100000.0, 100000.0);
    GeometryModel geom = geographyReader1().read(wkb);
    Point point = geom.asPoint();
    Assertions.assertEquals(-180.0, point.getX(), 1e-10);
    Assertions.assertEquals(-90.0, point.getY(), 1e-10);
    Assertions.assertEquals(-100000.0, point.getZ(), 1e-10);
    Assertions.assertEquals(100000.0, point.getM(), 1e-10);
  }

  @Test
  public void testGeographyPoint4D_InvalidLongitude() {
    // POINT ZM (200 0 5 10) - longitude out of bounds
    byte[] wkb = makePointWkb4D(200.0, 0.0, 5.0, 10.0);
    Assertions.assertThrows(WkbParseException.class, () -> geographyReader1().read(wkb));
  }

  @Test
  public void testGeographyPoint4D_InvalidLatitude() {
    // POINT ZM (0 100 5 10) - latitude out of bounds
    byte[] wkb = makePointWkb4D(0.0, 100.0, 5.0, 10.0);
    Assertions.assertThrows(WkbParseException.class, () -> geographyReader1().read(wkb));
  }

  // ========== Geography LineString Tests ==========

  @Test
  public void testGeographyLineStringValid() {
    // LINESTRING(0 0, 10 10) - valid geography coordinates
    byte[] wkb = makeLineStringWkb2D(new double[][]{{0.0, 0.0}, {10.0, 10.0}});
    GeometryModel geom = geographyReader1().read(wkb);
    Assertions.assertTrue(geom instanceof LineString, "Should be a LineString");
    Assertions.assertFalse(geom.isEmpty(), "Should not be empty");
  }

  @Test
  public void testGeographyLineStringValidAtBoundary() {
    // LINESTRING(-180 -90, 180 90) - at boundary
    byte[] wkb = makeLineStringWkb2D(new double[][]{{-180.0, -90.0}, {180.0, 90.0}});
    GeometryModel geom = geographyReader1().read(wkb);
    Assertions.assertTrue(geom instanceof LineString);
  }

  @Test
  public void testGeographyLineStringEmpty() {
    // LINESTRING EMPTY - valid in geography mode
    String wkbHex = "010200000000000000";
    byte[] wkb = hexToBytes(wkbHex);
    GeometryModel geom = geographyReader1().read(wkb);
    Assertions.assertTrue(geom instanceof LineString);
    Assertions.assertTrue(geom.isEmpty(), "Should be empty");
  }

  @Test
  public void testGeographyLineStringInvalidSecondPoint() {
    // LINESTRING(0 0, 200 0) - second point longitude out of bounds
    byte[] wkb = makeLineStringWkb2D(new double[][]{{0.0, 0.0}, {200.0, 0.0}});
    Assertions.assertThrows(WkbParseException.class, () -> geographyReader1().read(wkb));
  }

  @Test
  public void testGeographyLineStringInvalidFirstPoint() {
    // LINESTRING(-200 0, 0 0) - first point longitude out of bounds
    byte[] wkb = makeLineStringWkb2D(new double[][]{{-200.0, 0.0}, {0.0, 0.0}});
    Assertions.assertThrows(WkbParseException.class, () -> geographyReader1().read(wkb));
  }

  @Test
  public void testGeographyLineStringInvalidLatitude() {
    // LINESTRING(0 0, 0 100) - second point latitude out of bounds
    byte[] wkb = makeLineStringWkb2D(new double[][]{{0.0, 0.0}, {0.0, 100.0}});
    Assertions.assertThrows(WkbParseException.class, () -> geographyReader1().read(wkb));
  }

  // ========== Geography Polygon Tests ==========

  @Test
  public void testGeographyPolygonValid() {
    // POLYGON((0 0, 10 0, 0 10, 0 0)) - valid geography coordinates
    byte[] wkb = makePolygonWkb2D(new double[][][]{
      {{0.0, 0.0}, {10.0, 0.0}, {0.0, 10.0}, {0.0, 0.0}}
    });
    GeometryModel geom = geographyReader1().read(wkb);
    Assertions.assertTrue(geom instanceof Polygon, "Should be a Polygon");
  }

  @Test
  public void testGeographyPolygonEmpty() {
    // POLYGON EMPTY - valid in geography mode
    String wkbHex = "010300000000000000";
    byte[] wkb = hexToBytes(wkbHex);
    GeometryModel geom = geographyReader1().read(wkb);
    Assertions.assertTrue(geom instanceof Polygon);
    Assertions.assertTrue(geom.isEmpty(), "Should be empty");
  }

  @Test
  public void testGeographyPolygonInvalidVertex() {
    // POLYGON with a vertex at (200, 0) - longitude out of bounds
    byte[] wkb = makePolygonWkb2D(new double[][][]{
      {{0.0, 0.0}, {200.0, 0.0}, {0.0, 10.0}, {0.0, 0.0}}
    });
    Assertions.assertThrows(WkbParseException.class, () -> geographyReader1().read(wkb));
  }

  @Test
  public void testGeographyPolygonInvalidLatitude() {
    // POLYGON with a vertex at (0, 100) - latitude out of bounds
    byte[] wkb = makePolygonWkb2D(new double[][][]{
      {{0.0, 0.0}, {10.0, 0.0}, {0.0, 100.0}, {0.0, 0.0}}
    });
    Assertions.assertThrows(WkbParseException.class, () -> geographyReader1().read(wkb));
  }

  // ========== Geography MultiPoint Tests ==========

  @Test
  public void testGeographyMultiPointValid() {
    // MULTIPOINT((0 0), (10 20)) - valid geography
    byte[] wkb = makeMultiPointWkb2D(new double[][]{{0.0, 0.0}, {10.0, 20.0}});
    GeometryModel geom = geographyReader1().read(wkb);
    Assertions.assertTrue(geom instanceof MultiPoint, "Should be a MultiPoint");
  }

  @Test
  public void testGeographyMultiPointInvalidSecondPoint() {
    // MULTIPOINT((0 0), (200 0)) - second point longitude out of bounds
    byte[] wkb = makeMultiPointWkb2D(new double[][]{{0.0, 0.0}, {200.0, 0.0}});
    Assertions.assertThrows(WkbParseException.class, () -> geographyReader1().read(wkb));
  }

  @Test
  public void testGeographyMultiPointEmpty() {
    // MULTIPOINT EMPTY - valid
    String wkbHex = "010400000000000000";
    byte[] wkb = hexToBytes(wkbHex);
    GeometryModel geom = geographyReader1().read(wkb);
    Assertions.assertTrue(geom instanceof MultiPoint);
    Assertions.assertTrue(geom.isEmpty(), "Should be empty");
  }

  // ========== Validation Level 0 Bypasses Geography Bounds Check ==========

  /** Creates a geography-mode WkbReader with no validation (level 0). */
  private WkbReader geographyReader0() {
    return new WkbReader(0, true);
  }

  @Test
  public void testGeographyNoValidation_OutOfBoundsAccepted() {
    // With validation level 0, out-of-bounds coordinates should be accepted
    byte[] wkb = makePointWkb2D(200.0, 100.0);
    GeometryModel geom = geographyReader0().read(wkb);
    Assertions.assertTrue(geom.isPoint(), "Should be a Point");
    Point point = geom.asPoint();
    Assertions.assertEquals(200.0, point.getX(), 1e-10, "X coordinate preserved");
    Assertions.assertEquals(100.0, point.getY(), 1e-10, "Y coordinate preserved");
  }

  @Test
  public void testGeographyNoValidation_LineStringOutOfBoundsAccepted() {
    // With validation level 0, out-of-bounds linestring accepted
    byte[] wkb = makeLineStringWkb2D(new double[][]{{0.0, 0.0}, {200.0, 0.0}});
    GeometryModel geom = geographyReader0().read(wkb);
    Assertions.assertTrue(geom instanceof LineString);
  }

  // ========== Geometry Mode Does NOT Apply Geography Bounds ==========

  @Test
  public void testGeometryModeAcceptsOutOfBoundsCoordinates() {
    // In geometry mode (non-geography), coordinates outside geography bounds are valid
    WkbReader geometryReader = new WkbReader(1, false);
    byte[] wkb = makePointWkb2D(200.0, 100.0);
    GeometryModel geom = geometryReader.read(wkb);
    Assertions.assertTrue(geom.isPoint(), "Should be a Point");
    Point point = geom.asPoint();
    Assertions.assertEquals(200.0, point.getX(), 1e-10, "X coordinate");
    Assertions.assertEquals(100.0, point.getY(), 1e-10, "Y coordinate");
  }

  @Test
  public void testGeometryModeLargeCoordinatesAccepted() {
    // In geometry mode, large coordinates are valid
    WkbReader geometryReader = new WkbReader();
    byte[] wkb = makePointWkb2D(1000000.0, 2000000.0);
    GeometryModel geom = geometryReader.read(wkb);
    Assertions.assertTrue(geom.isPoint(), "Should be a Point");
    Assertions.assertEquals(1000000.0, geom.asPoint().getX(), 1e-10);
    Assertions.assertEquals(2000000.0, geom.asPoint().getY(), 1e-10);
  }

  // ========== Error Message Tests ==========

  @Test
  public void testGeographyErrorMessageContainsBoundsInfo() {
    // Verify error message mentions longitude/latitude bounds
    byte[] wkb = makePointWkb2D(200.0, 0.0);
    WkbParseException ex = Assertions.assertThrows(
        WkbParseException.class, () -> geographyReader1().read(wkb));
    String msg = ex.getMessage();
    Assertions.assertTrue(msg.contains("Invalid coordinate value"));
  }
}
