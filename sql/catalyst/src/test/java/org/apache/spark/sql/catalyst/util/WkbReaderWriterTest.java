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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteOrder;

/**
 * Test suite for WKB (Well-Known Binary) reader and writer functionality.
 */
public class WkbReaderWriterTest {

  /**
   * Helper method to convert hex string to byte array
   */
  private byte[] hexToBytes(String hex) {
    int len = hex.length();
    byte[] data = new byte[len / 2];
    for (int i = 0; i < len; i += 2) {
      data[i / 2] = (byte) ((Character.digit(hex.charAt(i), 16) << 4)
          + Character.digit(hex.charAt(i + 1), 16));
    }
    return data;
  }

  /**
   * Helper method to convert byte array to hex string
   */
  private String bytesToHex(byte[] bytes) {
    StringBuilder sb = new StringBuilder();
    for (byte b : bytes) {
      sb.append(String.format("%02x", b));
    }
    return sb.toString();
  }

  /**
   * Test helper to verify WKB round-trip (write and read)
   * @return the parsed geometry for further assertions
   */
  private Geometry checkRoundTrip(Geometry geometry, String expectedWkbHexLittle,
      String expectedWkbHexBig) {
    // Test with little endian
    WkbWriter writerLittle = new WkbWriter(false);
    byte[] wkbLittle = writerLittle.write(geometry, ByteOrder.LITTLE_ENDIAN);
    String actualHexLittle = bytesToHex(wkbLittle);

    Assertions.assertEquals(expectedWkbHexLittle, actualHexLittle, "WKB little endian mismatch");

    // Test with big endian
    WkbWriter writerBig = new WkbWriter(false);
    byte[] wkbBig = writerBig.write(geometry, ByteOrder.BIG_ENDIAN);
    String actualHexBig = bytesToHex(wkbBig);

    Assertions.assertEquals(expectedWkbHexBig, actualHexBig, "WKB big endian mismatch");

    // Test reading back the WKB
    WkbReader reader = new WkbReader();
    Geometry parsedFromLittle = reader.read(wkbLittle, geometry.srid());
    Geometry parsedFromBig = reader.read(wkbBig, geometry.srid());

    // Verify the geometries match
    Assertions.assertEquals(geometry.getTypeId(), parsedFromLittle.getTypeId(),
        "Geometry type mismatch after parsing little endian");
    Assertions.assertEquals(geometry.getTypeId(), parsedFromBig.getTypeId(),
        "Geometry type mismatch after parsing big endian");

    return parsedFromLittle;
  }

  /**
   * Test empty point
   */
  @Test
  public void testEmptyPoint() {
    // POINT EMPTY
    double[] coords = {Double.NaN, Double.NaN};
    Point emptyPoint = new Point(coords, 0);

    Assertions.assertTrue(emptyPoint.isEmpty(), "Empty point should be empty");

    checkRoundTrip(emptyPoint,
        "0101000000000000000000f87f000000000000f87f",
        "00000000017ff80000000000007ff8000000000000");
  }

  /**
   * Test simple 2D point
   */
  @Test
  public void testSimplePoint() {
    // POINT(1 2)
    double[] coords = {1.0, 2.0};
    Point point = new Point(coords, 0);

    Assertions.assertFalse(point.isEmpty(), "Point should not be empty");
    Assertions.assertEquals(1.0, point.getX(), 0.0001, "X coordinate mismatch");
    Assertions.assertEquals(2.0, point.getY(), 0.0001, "Y coordinate mismatch");

    checkRoundTrip(point,
        "0101000000000000000000f03f0000000000000040",
        "00000000013ff00000000000004000000000000000");
  }

  /**
   * Test point with negative coordinates
   */
  @Test
  public void testPointNegativeCoordinates() {
    // POINT(-180 0)
    double[] coords = {-180.0, 0.0};
    Point point = new Point(coords, 0);

    Assertions.assertEquals(-180.0, point.getX(), 0.0001, "X coordinate mismatch");
    Assertions.assertEquals(0.0, point.getY(), 0.0001, "Y coordinate mismatch");

    checkRoundTrip(point,
        "010100000000000000008066c00000000000000000",
        "0000000001c0668000000000000000000000000000");
  }

  /**
   * Test point at south pole
   */
  @Test
  public void testPointSouthPole() {
    // POINT(0 -90)
    double[] coords = {0.0, -90.0};
    Point point = new Point(coords, 0);

    Assertions.assertEquals(0.0, point.getX(), 0.0001, "X coordinate mismatch");
    Assertions.assertEquals(-90.0, point.getY(), 0.0001, "Y coordinate mismatch");

    checkRoundTrip(point,
        "0101000000000000000000000000000000008056c0",
        "00000000010000000000000000c056800000000000");
  }

  /**
   * Test point at north pole
   */
  @Test
  public void testPointNorthPole() {
    // POINT(0 90)
    double[] coords = {0.0, 90.0};
    Point point = new Point(coords, 0);

    Assertions.assertEquals(0.0, point.getX(), 0.0001, "X coordinate mismatch");
    Assertions.assertEquals(90.0, point.getY(), 0.0001, "Y coordinate mismatch");

    checkRoundTrip(point,
        "010100000000000000000000000000000000805640",
        "000000000100000000000000004056800000000000");
  }

  /**
   * Test parsing WKB point from hex string
   */
  @Test
  public void testParseWkbPoint() {
    // Parse little endian WKB: POINT(1 2)
    String wkbHex = "0101000000000000000000f03f0000000000000040";
    byte[] wkbBytes = hexToBytes(wkbHex);

    Geometry geom = Geometry.fromWkb(wkbBytes);

    Assertions.assertTrue(geom.isPoint(), "Should be a Point");
    Point point = geom.asPoint();
    Assertions.assertEquals(1.0, point.getX(), 0.0001, "X coordinate mismatch");
    Assertions.assertEquals(2.0, point.getY(), 0.0001, "Y coordinate mismatch");
  }

  /**
   * Test parsing WKB point with big endian
   */
  @Test
  public void testParseWkbPointBigEndian() {
    // Parse big endian WKB: POINT(1 2)
    String wkbHex = "00000000013ff00000000000004000000000000000";
    byte[] wkbBytes = hexToBytes(wkbHex);

    Geometry geom = Geometry.fromWkb(wkbBytes);

    Assertions.assertTrue(geom.isPoint(), "Should be a Point");
    Point point = geom.asPoint();
    Assertions.assertEquals(1.0, point.getX(), 0.0001, "X coordinate mismatch");
    Assertions.assertEquals(2.0, point.getY(), 0.0001, "Y coordinate mismatch");
  }

  /**
   * Test parsing empty point from WKB
   */
  @Test
  public void testParseEmptyPoint() {
    // Parse little endian WKB: POINT EMPTY
    String wkbHex = "0101000000000000000000f87f000000000000f87f";
    byte[] wkbBytes = hexToBytes(wkbHex);

    Geometry geom = Geometry.fromWkb(wkbBytes);

    Assertions.assertTrue(geom.isPoint(), "Should be a Point");
    Point point = geom.asPoint();
    Assertions.assertTrue(point.isEmpty(), "Point should be empty");
  }

  /**
   * Test invalid WKB (too short)
   */
  @Test
  public void testInvalidWkbTooShort() {
    byte[] invalidWkb = {0x01};
    Assertions.assertThrows(WkbParseException.class, () -> Geometry.fromWkb(invalidWkb));
  }

  /**
   * Test invalid byte order
   */
  @Test
  public void testInvalidByteOrder() {
    byte[] invalidWkb = {0x02, 0x01, 0x00, 0x00, 0x00};
    Assertions.assertThrows(WkbParseException.class, () -> Geometry.fromWkb(invalidWkb));
  }

  /**
   * Test unsupported geometry type
   */
  @Test
  public void testUnsupportedGeometryType() {
    // Invalid type 99
    byte[] invalidWkb = hexToBytes("0163000000000000000000f03f0000000000000040");
    Assertions.assertThrows(WkbParseException.class, () -> Geometry.fromWkb(invalidWkb));
  }

  /**
   * Test simple linestring
   */
  @Test
  public void testSimpleLineString() {
    // LINESTRING(0 0, 1 1, 2 2)
    java.util.List<Point> points = java.util.Arrays.asList(
        new Point(new double[]{0.0, 0.0}, 0),
        new Point(new double[]{1.0, 1.0}, 0),
        new Point(new double[]{2.0, 2.0}, 0)
    );
    LineString lineString = new LineString(points, 0);

    Assertions.assertFalse(lineString.isEmpty(), "LineString should not be empty");
    Assertions.assertEquals(3, lineString.getNumPoints(), "Number of points mismatch");

    // checkstyle.off: LineLength
    Geometry parsed = checkRoundTrip(lineString,
        "01020000000300000000000000000000000000000000000000000000000000f03f000000000000f03f00000000000000400000000000000040000000000000000000000000000000",
        "000000000200000003000000000000000000000000000000003ff00000000000003ff000000000000040000000000000004000000000000000000000000000000000000000000000");
    // checkstyle.on: LineLength

    Assertions.assertInstanceOf(LineString.class, parsed, "Parsed geometry should be LineString");
    LineString parsedLS = (LineString) parsed;
    Assertions.assertEquals(3, parsedLS.getNumPoints(), "Number of points should match");
  }

  /**
   * Test simple polygon
   */
  @Test
  public void testSimplePolygon() {
    // POLYGON((0 0, 4 0, 4 4, 0 4, 0 0))
    java.util.List<Point> ringPoints = java.util.Arrays.asList(
        new Point(new double[]{0.0, 0.0}, 0),
        new Point(new double[]{4.0, 0.0}, 0),
        new Point(new double[]{4.0, 4.0}, 0),
        new Point(new double[]{0.0, 4.0}, 0),
        new Point(new double[]{0.0, 0.0}, 0)
    );
    Ring ring = new Ring(ringPoints);
    java.util.List<Ring> rings = java.util.Arrays.asList(ring);
    Polygon polygon = new Polygon(rings, 0);

    Assertions.assertFalse(polygon.isEmpty(), "Polygon should not be empty");
    Assertions.assertEquals(1, polygon.getRings().size(), "Should have one ring");
    Assertions.assertTrue(ring.isClosed(), "Ring should be closed");

    // checkstyle.off: LineLength
    Geometry parsed = checkRoundTrip(polygon,
        "010300000001000000050000000000000000000000000000000000000000000000000010400000000000000000000000000000104000000000000010400000000000000000000000000000104000000000000000000000000000000000",
        "000000000300000001000000050000000000000000000000000000000040100000000000000000000000000000401000000000000040100000000000000000000000000000401000000000000000000000000000000000000000000000");
    // checkstyle.on: LineLength

    Assertions.assertInstanceOf(Polygon.class, parsed, "Parsed geometry should be Polygon");
    Polygon parsedPoly = (Polygon) parsed;
    Assertions.assertEquals(1, parsedPoly.getRings().size(), "Number of rings should match");
  }

  /**
   * Test multipoint
   */
  @Test
  public void testMultiPoint() {
    // MULTIPOINT((0 0), (1 1))
    java.util.List<Point> points = java.util.Arrays.asList(
        new Point(new double[]{0.0, 0.0}, 0),
        new Point(new double[]{1.0, 1.0}, 0)
    );
    MultiPoint multiPoint = new MultiPoint(points, 0);

    Assertions.assertFalse(multiPoint.isEmpty(), "MultiPoint should not be empty");
    Assertions.assertEquals(2, multiPoint.getNumGeometries(), "Number of points mismatch");

    // checkstyle.off: LineLength
    Geometry parsed = checkRoundTrip(multiPoint,
        "0104000000020000000101000000000000000000000000000000000000000101000000000000000000f03f000000000000f03f",
        "00000000040000000200000000010000000000000000000000000000000000000000013ff00000000000003ff0000000000000");
    // checkstyle.on: LineLength

    Assertions.assertInstanceOf(MultiPoint.class, parsed, "Parsed geometry should be MultiPoint");
    MultiPoint parsedMP = (MultiPoint) parsed;
    Assertions.assertEquals(2, parsedMP.getNumGeometries(), "Number of points should match");
  }

  /**
   * Test geometry collection
   */
  @Test
  public void testGeometryCollection() {
    // GEOMETRYCOLLECTION(POINT(1 2), LINESTRING(0 0, 1 1))
    Point point = new Point(new double[]{1.0, 2.0}, 0);

    java.util.List<Point> lsPoints = java.util.Arrays.asList(
        new Point(new double[]{0.0, 0.0}, 0),
        new Point(new double[]{1.0, 1.0}, 0)
    );
    LineString lineString = new LineString(lsPoints, 0);

    java.util.List<Geometry> geometries = java.util.Arrays.asList(point, lineString);
    GeometryCollection collection = new GeometryCollection(geometries, 0);

    Assertions.assertFalse(collection.isEmpty(), "GeometryCollection should not be empty");
    Assertions.assertEquals(2, collection.getNumGeometries(), "Number of geometries mismatch");

    // checkstyle.off: LineLength
    Geometry parsed = checkRoundTrip(collection,
        "0107000000020000000101000000000000000000f03f000000000000004001020000000200000000000000000000000000000000000000000000000000f03f000000000000f03f00000000000000000000",
        "00000000070000000200000000013ff00000000000004000000000000000000000000200000002000000000000000000000000000000003ff00000000000003ff000000000000000000000000000000000");
    // checkstyle.on: LineLength

    Assertions.assertInstanceOf(GeometryCollection.class, parsed, "Parsed geometry should be GeometryCollection");
    GeometryCollection parsedGC = (GeometryCollection) parsed;
    Assertions.assertEquals(2, parsedGC.getNumGeometries(), "Number of geometries should match");
  }

  /**
   * Test SRID preservation
   */
  @Test
  public void testSridPreservation() {
    double[] coords = {1.0, 2.0};
    Point point = new Point(coords, 4326);

    Assertions.assertEquals(4326, point.srid(), "SRID mismatch");

    // SRID is not written in standard WKB, only in EWKB
    WkbWriter writer = new WkbWriter(false);
    byte[] wkb = writer.write(point);

    // When reading back without SRID info, we need to provide it
    WkbReader reader = new WkbReader();
    Geometry parsed = reader.read(wkb, 4326);

    Assertions.assertEquals(4326, parsed.srid(), "SRID should be preserved");
  }

  /**
   * Test EWKB with SRID
   */
  @Test
  public void testEwkbWithSrid() {
    double[] coords = {1.0, 2.0};
    Point point = new Point(coords, 4326);

    // Write EWKB
    WkbWriter writer = new WkbWriter(true);
    byte[] ewkb = writer.write(point);

    // Read EWKB
    WkbReader reader = new WkbReader(true, 1);
    Geometry parsed = reader.read(ewkb);

    Assertions.assertEquals(4326, parsed.srid(), "SRID should be embedded in EWKB");
    Assertions.assertTrue(parsed instanceof Point, "Parsed geometry should be Point");
  }
}

