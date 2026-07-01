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

import java.nio.ByteOrder;
import java.util.List;

/**
 * Test suite for WKB (Well-Known Binary) reader and writer functionality.
 */
public class WkbReaderWriterGeometryModelTest extends WkbTestBase {

  /**
   * Test helper to verify WKB round-trip (write and read)
   * @return the parsed geometry for further assertions
   */
  private GeometryModel checkRoundTrip(GeometryModel geometry, String expectedWkbHexLittle,
      String expectedWkbHexBig) {
    // Test with little endian
    WkbWriter writerLittle = new WkbWriter();
    byte[] wkbLittle = writerLittle.write(geometry, ByteOrder.LITTLE_ENDIAN);
    String actualHexLittle = bytesToHex(wkbLittle);

    Assertions.assertEquals(expectedWkbHexLittle, actualHexLittle, "WKB little endian mismatch");

    // Test with big endian
    WkbWriter writerBig = new WkbWriter();
    byte[] wkbBig = writerBig.write(geometry, ByteOrder.BIG_ENDIAN);
    String actualHexBig = bytesToHex(wkbBig);

    Assertions.assertEquals(expectedWkbHexBig, actualHexBig, "WKB big endian mismatch");

    // Test reading back the WKB
    WkbReader reader = new WkbReader();
    GeometryModel parsedFromLittle = reader.read(wkbLittle, geometry.srid());
    GeometryModel parsedFromBig = reader.read(wkbBig, geometry.srid());

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

    WkbReader reader = new WkbReader();
    GeometryModel geom = reader.read(wkbBytes);

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

    WkbReader reader = new WkbReader();
    GeometryModel geom = reader.read(wkbBytes);

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

    WkbReader reader = new WkbReader();
    GeometryModel geom = reader.read(wkbBytes);

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
    WkbReader reader = new WkbReader();
    Assertions.assertThrows(WkbParseException.class, () -> reader.read(invalidWkb));
  }

  /**
   * Test invalid byte order
   */
  @Test
  public void testInvalidByteOrder() {
    byte[] invalidWkb = {0x02, 0x01, 0x00, 0x00, 0x00};
    WkbReader reader = new WkbReader();
    Assertions.assertThrows(WkbParseException.class, () -> reader.read(invalidWkb));
  }

  /**
   * Test unsupported geometry type
   */
  @Test
  public void testUnsupportedGeometryType() {
    // Invalid type 99
    byte[] invalidWkb = hexToBytes("0163000000000000000000f03f0000000000000040");
    WkbReader reader = new WkbReader();
    Assertions.assertThrows(WkbParseException.class, () -> reader.read(invalidWkb));
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
    LineString lineString = new LineString(points, 0, false, false);

    Assertions.assertFalse(lineString.isEmpty(), "LineString should not be empty");
    Assertions.assertEquals(3, lineString.getNumPoints(), "Number of points mismatch");

    GeometryModel parsed = checkRoundTrip(lineString,
        "01020000000300000000000000000000000000000000000000000000000000f03f000000000000f03f00000000000000400000000000000040", // checkstyle.off: LineLength
        "000000000200000003000000000000000000000000000000003ff00000000000003ff000000000000040000000000000004000000000000000"); // checkstyle.off: LineLength

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
    Polygon polygon = new Polygon(rings, 0, false, false);

    Assertions.assertFalse(polygon.isEmpty(), "Polygon should not be empty");
    Assertions.assertEquals(1, polygon.getRings().size(), "Should have one ring");
    Assertions.assertTrue(ring.isClosed(), "Ring should be closed");

    GeometryModel parsed = checkRoundTrip(polygon,
        "010300000001000000050000000000000000000000000000000000000000000000000010400000000000000000000000000000104000000000000010400000000000000000000000000000104000000000000000000000000000000000", // checkstyle.off: LineLength
        "000000000300000001000000050000000000000000000000000000000040100000000000000000000000000000401000000000000040100000000000000000000000000000401000000000000000000000000000000000000000000000"); // checkstyle.off: LineLength

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
    MultiPoint multiPoint = new MultiPoint(points, 0, false, false);

    Assertions.assertFalse(multiPoint.isEmpty(), "MultiPoint should not be empty");
    Assertions.assertEquals(2, multiPoint.getNumGeometries(), "Number of points mismatch");

    GeometryModel parsed = checkRoundTrip(multiPoint,
      "0104000000020000000101000000000000000000000000000000000000000101000000000000000000f03f000000000000f03f", // checkstyle.off: LineLength
      "00000000040000000200000000010000000000000000000000000000000000000000013ff00000000000003ff0000000000000"); // checkstyle.off: LineLength

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
    LineString lineString = new LineString(lsPoints, 0, false, false);

    java.util.List<GeometryModel> geometries = java.util.Arrays.asList(point, lineString);
    GeometryCollection collection = new GeometryCollection(geometries, 0, false, false);

    Assertions.assertFalse(collection.isEmpty(), "GeometryCollection should not be empty");
    Assertions.assertEquals(2, collection.getNumGeometries(), "Number of geometries mismatch");

    GeometryModel parsed = checkRoundTrip(collection,
        "0107000000020000000101000000000000000000f03f000000000000004001020000000200000000000000000000000000000000000000000000000000f03f000000000000f03f", // checkstyle.off: LineLength
        "00000000070000000200000000013ff00000000000004000000000000000000000000200000002000000000000000000000000000000003ff00000000000003ff0000000000000"); // checkstyle.off: LineLength

    Assertions.assertInstanceOf(GeometryCollection.class, parsed,
      "Parsed geometry should be GeometryCollection");
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

    WkbWriter writer = new WkbWriter();
    byte[] wkb = writer.write(point);

    // When reading back without SRID info, we need to provide it
    WkbReader reader = new WkbReader();
    GeometryModel parsed = reader.read(wkb, 4326);

    Assertions.assertEquals(4326, parsed.srid(), "SRID should be preserved");
  }

  @Test
  public void testGeometryCollectionDifferentEndianness() {
    // GEOMETRYCOLLECTION(
    //  POINT(1 2),   -- big endian
    //  POINT(3 4)    -- little endian
    //)

    String wkbLe = "01070000000200000000000000013ff00000000000004000000000000000010100000000000000000008400000000000001040"; // checkstyle.off: LineLength
    String wkbBe = "00000000070000000200000000013ff00000000000004000000000000000010100000000000000000008400000000000001040"; // checkstyle.off: LineLength
    WkbReader reader = new WkbReader();
    GeometryModel parsedLe = reader.read(hexToBytes(wkbLe));

    Assertions.assertInstanceOf(GeometryCollection.class, parsedLe, "Should be GeometryCollection");
    List<GeometryModel> childLe = parsedLe.asGeometryCollection().getGeometries();
    for (GeometryModel geom : childLe) {
      Assertions.assertTrue(geom.isPoint(), "Child should be Point");
    }
    Assertions.assertEquals(1.0, childLe.get(0).asPoint().getX(), 0.0001, "First point X mismatch");
    Assertions.assertEquals(2.0, childLe.get(0).asPoint().getY(), 0.0001, "First point Y mismatch");
    Assertions.assertEquals(3.0, childLe.get(1).asPoint().getX(), 0.0001,
      "Second point X mismatch");
    Assertions.assertEquals(4.0, childLe.get(1).asPoint().getY(), 0.0001,
      "Second point Y mismatch");

    GeometryModel parsedBe = reader.read(hexToBytes(wkbBe));
    Assertions.assertInstanceOf(GeometryCollection.class, parsedBe, "Should be GeometryCollection");
    List<GeometryModel> childBe = parsedBe.asGeometryCollection().getGeometries();
    for (GeometryModel geom : childBe) {
      Assertions.assertTrue(geom.isPoint(), "Child should be Point");
    }
    Assertions.assertEquals(1.0, childBe.get(0).asPoint().getX(), 0.0001, "First point X mismatch");
    Assertions.assertEquals(2.0, childBe.get(0).asPoint().getY(), 0.0001, "First point Y mismatch");
    Assertions.assertEquals(3.0, childBe.get(1).asPoint().getX(), 0.0001,
      "Second point X mismatch");
    Assertions.assertEquals(4.0, childBe.get(1).asPoint().getY(), 0.0001,
      "Second point Y mismatch");
  }
}
