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

import java.util.Arrays;
import java.util.List;

/**
 * Test suite for WKT (Well-Known Text) writing functionality.
 */
public class WktWriterTest extends WkbTestBase {

  /** Tests for: POINT */

  @Test
  public void testEmptyPointToString() {
    Point point = new Point(new double[]{Double.NaN, Double.NaN}, 0);
    String wkt = point.toString();
    Assertions.assertEquals("POINT EMPTY", wkt);
  }

  @Test
  public void testPointWithZeroCoordinate() {
    // 0.0 should be normalized to "0".
    Point point = new Point(new double[]{0.0, 0.0}, 0);
    Assertions.assertEquals("POINT(0 0)", point.toString());
  }

  @Test
  public void testPointWithNegativeZeroCoordinate() {
    // -0.0 should be normalized to "0" (not "-0" or "-0.0").
    Point point = new Point(new double[]{-0.0, -0.0}, 0);
    Assertions.assertEquals("POINT(0 0)", point.toString());
  }

  @Test
  public void testPointWithLargeIntegerCoordinateBeyondLongRange() {
    // For very large values WKT should use scientific notation.
    Point point = new Point(new double[]{1.2e19, -3.4e19}, 0);
    String wkt = point.toString();
    Assertions.assertEquals("POINT(1.2E19 -3.4E19)", wkt);
  }

  @Test
  public void testPointWithDoubleMaxValue() {
    Point point = new Point(new double[]{Double.MAX_VALUE, -Double.MAX_VALUE}, 0);
    String wkt = point.toString();
    Assertions.assertEquals("POINT(1.7976931348623157E308 -1.7976931348623157E308)", wkt);
  }

  @Test
  public void testPointToString() {
    Point point = new Point(new double[]{1.0, 2.0}, 0);
    String wkt = point.toString();
    Assertions.assertEquals("POINT(1 2)", wkt);
  }

  @Test
  public void testPointZToString() {
    Point point = new Point(new double[]{1.0, 2.0, 3.0}, 0, true, false);
    String wkt = point.toString();
    Assertions.assertEquals("POINT Z (1 2 3)", wkt);
  }

  @Test
  public void testPointMToString() {
    Point point = new Point(new double[]{1.0, 2.0, 4.0}, 0, false, true);
    String wkt = point.toString();
    Assertions.assertEquals("POINT M (1 2 4)", wkt);
  }

  @Test
  public void testPointZMToString() {
    Point point = new Point(new double[]{1.0, 2.0, 3.0, 4.0}, 0, true, true);
    String wkt = point.toString();
    Assertions.assertEquals("POINT ZM (1 2 3 4)", wkt);
  }

  @Test
  public void testPointWithNegativeCoordinatesToString() {
    Point point = new Point(new double[]{-180.0, -90.0}, 0);
    String wkt = point.toString();
    Assertions.assertEquals("POINT(-180 -90)", wkt);
  }

  @Test
  public void testPointWithNaN() {
    Point point = new Point(new double[]{Double.NaN, Double.NaN}, 0);
    StringBuilder sb = new StringBuilder();
    IllegalArgumentException e = Assertions.assertThrows(
        IllegalArgumentException.class, () -> point.appendWktContent(sb));
    Assertions.assertEquals("Coordinate value must not be NaN.", e.getMessage());
  }

  /** Tests for: LINESTRING */

  @Test
  public void testEmptyLineStringToString() {
    LineString lineString = new LineString(Arrays.asList(), 0, false, false);
    String wkt = lineString.toString();
    Assertions.assertEquals("LINESTRING EMPTY", wkt);
  }

  @Test
  public void testLineStringToString() {
    List<Point> points = Arrays.asList(
        new Point(new double[]{0.0, 0.0}, 0),
        new Point(new double[]{1.0, 1.0}, 0),
        new Point(new double[]{2.0, 2.0}, 0)
    );
    LineString lineString = new LineString(points, 0, false, false);
    String wkt = lineString.toString();
    Assertions.assertEquals("LINESTRING(0 0,1 1,2 2)", wkt);
  }

  @Test
  public void testLineStringZToString() {
    List<Point> points = Arrays.asList(
        new Point(new double[]{0.0, 0.0, 0.0}, 0, true, false),
        new Point(new double[]{1.0, 1.0, 1.0}, 0, true, false)
    );
    LineString lineString = new LineString(points, 0, true, false);
    String wkt = lineString.toString();
    Assertions.assertEquals("LINESTRING Z (0 0 0,1 1 1)", wkt);
  }

  @Test
  public void testLineStringMToString() {
    List<Point> points = Arrays.asList(
        new Point(new double[]{0.0, 0.0, 10.0}, 0, false, true),
        new Point(new double[]{1.0, 1.0, 20.0}, 0, false, true)
    );
    LineString lineString = new LineString(points, 0, false, true);
    String wkt = lineString.toString();
    Assertions.assertEquals("LINESTRING M (0 0 10,1 1 20)", wkt);
  }

  @Test
  public void testLineStringZMToString() {
    List<Point> points = Arrays.asList(
        new Point(new double[]{0.0, 0.0, 0.0, 10.0}, 0, true, true),
        new Point(new double[]{1.0, 1.0, 1.0, 20.0}, 0, true, true)
    );
    LineString lineString = new LineString(points, 0, true, true);
    String wkt = lineString.toString();
    Assertions.assertEquals("LINESTRING ZM (0 0 0 10,1 1 1 20)", wkt);
  }

  @Test
  public void testLineStringWithNaN() {
    List<Point> points = Arrays.asList(
        new Point(new double[]{0.0, 0.0}, 0),
        new Point(new double[]{Double.NaN, Double.NaN}, 0),
        new Point(new double[]{2.0, 2.0}, 0)
    );
    LineString lineString = new LineString(points, 0, false, false);
    IllegalArgumentException e = Assertions.assertThrows(
        IllegalArgumentException.class, lineString::toString);
    Assertions.assertEquals("Coordinate value must not be NaN.", e.getMessage());
  }

  /** Tests for: POLYGON */

  @Test
  public void testEmptyPolygonToString() {
    Polygon polygon = new Polygon(Arrays.asList(), 0, false, false);
    String wkt = polygon.toString();
    Assertions.assertEquals("POLYGON EMPTY", wkt);
  }

  @Test
  public void testPolygonToString() {
    List<Point> ringPoints = Arrays.asList(
        new Point(new double[]{0.0, 0.0}, 0),
        new Point(new double[]{4.0, 0.0}, 0),
        new Point(new double[]{4.0, 4.0}, 0),
        new Point(new double[]{0.0, 4.0}, 0),
        new Point(new double[]{0.0, 0.0}, 0)
    );
    Ring ring = new Ring(ringPoints);
    Polygon polygon = new Polygon(Arrays.asList(ring), 0, false, false);
    String wkt = polygon.toString();
    Assertions.assertEquals("POLYGON((0 0,4 0,4 4,0 4,0 0))", wkt);
  }

  @Test
  public void testPolygonWithHoleToString() {
    // Exterior ring
    List<Point> exteriorPoints = Arrays.asList(
        new Point(new double[]{0.0, 0.0}, 0),
        new Point(new double[]{10.0, 0.0}, 0),
        new Point(new double[]{10.0, 10.0}, 0),
        new Point(new double[]{0.0, 10.0}, 0),
        new Point(new double[]{0.0, 0.0}, 0)
    );
    Ring exteriorRing = new Ring(exteriorPoints);

    // Interior ring (hole)
    List<Point> interiorPoints = Arrays.asList(
        new Point(new double[]{2.0, 2.0}, 0),
        new Point(new double[]{8.0, 2.0}, 0),
        new Point(new double[]{8.0, 8.0}, 0),
        new Point(new double[]{2.0, 8.0}, 0),
        new Point(new double[]{2.0, 2.0}, 0)
    );
    Ring interiorRing = new Ring(interiorPoints);

    Polygon polygon = new Polygon(Arrays.asList(exteriorRing, interiorRing), 0, false, false);
    String wkt = polygon.toString();
    Assertions.assertEquals(
        "POLYGON((0 0,10 0,10 10,0 10,0 0),(2 2,8 2,8 8,2 8,2 2))", wkt);
  }

  @Test
  public void testPolygonZToString() {
    List<Point> ringPoints = Arrays.asList(
        new Point(new double[]{0.0, 0.0, 0.0}, 0, true, false),
        new Point(new double[]{4.0, 0.0, 0.0}, 0, true, false),
        new Point(new double[]{4.0, 4.0, 0.0}, 0, true, false),
        new Point(new double[]{0.0, 4.0, 0.0}, 0, true, false),
        new Point(new double[]{0.0, 0.0, 0.0}, 0, true, false)
    );
    Ring ring = new Ring(ringPoints);
    Polygon polygon = new Polygon(Arrays.asList(ring), 0, true, false);
    String wkt = polygon.toString();
    Assertions.assertEquals("POLYGON Z ((0 0 0,4 0 0,4 4 0,0 4 0,0 0 0))", wkt);
  }

  @Test
  public void testPolygonWithNaN() {
    List<Point> ringPoints = Arrays.asList(
        new Point(new double[]{0.0, 0.0}, 0),
        new Point(new double[]{Double.NaN, Double.NaN}, 0),
        new Point(new double[]{4.0, 4.0}, 0),
        new Point(new double[]{0.0, 0.0}, 0)
    );
    Ring ring = new Ring(ringPoints);
    Polygon polygon = new Polygon(List.of(ring), 0, false, false);
    IllegalArgumentException e = Assertions.assertThrows(
        IllegalArgumentException.class, polygon::toString);
    Assertions.assertEquals("Coordinate value must not be NaN.", e.getMessage());
  }

  /** Tests for: MULTIPOINT */

  @Test
  public void testEmptyMultiPointToString() {
    MultiPoint multiPoint = new MultiPoint(Arrays.asList(), 0, false, false);
    String wkt = multiPoint.toString();
    Assertions.assertEquals("MULTIPOINT EMPTY", wkt);
  }

  @Test
  public void testMultiPointToString() {
    List<Point> points = Arrays.asList(
        new Point(new double[]{0.0, 0.0}, 0),
        new Point(new double[]{1.0, 1.0}, 0),
        new Point(new double[]{2.0, 2.0}, 0)
    );
    MultiPoint multiPoint = new MultiPoint(points, 0, false, false);
    String wkt = multiPoint.toString();
    Assertions.assertEquals("MULTIPOINT((0 0),(1 1),(2 2))", wkt);
  }

  @Test
  public void testMultiPointZToString() {
    List<Point> points = Arrays.asList(
        new Point(new double[]{0.0, 0.0, 0.0}, 0, true, false),
        new Point(new double[]{1.0, 1.0, 1.0}, 0, true, false)
    );
    MultiPoint multiPoint = new MultiPoint(points, 0, true, false);
    String wkt = multiPoint.toString();
    Assertions.assertEquals("MULTIPOINT Z ((0 0 0),(1 1 1))", wkt);
  }

  /** Tests for: MULTILINESTRING */

  @Test
  public void testEmptyMultiLineStringToString() {
    MultiLineString multiLineString = new MultiLineString(Arrays.asList(), 0, false, false);
    String wkt = multiLineString.toString();
    Assertions.assertEquals("MULTILINESTRING EMPTY", wkt);
  }

  @Test
  public void testMultiLineStringToString() {
    List<Point> ls1Points = Arrays.asList(
        new Point(new double[]{0.0, 0.0}, 0),
        new Point(new double[]{1.0, 1.0}, 0)
    );
    LineString ls1 = new LineString(ls1Points, 0, false, false);

    List<Point> ls2Points = Arrays.asList(
        new Point(new double[]{2.0, 2.0}, 0),
        new Point(new double[]{3.0, 3.0}, 0)
    );
    LineString ls2 = new LineString(ls2Points, 0, false, false);

    MultiLineString multiLineString = new MultiLineString(Arrays.asList(ls1, ls2), 0, false, false);
    String wkt = multiLineString.toString();
    Assertions.assertEquals("MULTILINESTRING((0 0,1 1),(2 2,3 3))", wkt);
  }

  @Test
  public void testMultiLineStringZToString() {
    List<Point> ls1Points = Arrays.asList(
        new Point(new double[]{0.0, 0.0, 0.0}, 0, true, false),
        new Point(new double[]{1.0, 1.0, 1.0}, 0, true, false)
    );
    LineString ls1 = new LineString(ls1Points, 0, true, false);

    List<Point> ls2Points = Arrays.asList(
        new Point(new double[]{2.0, 2.0, 2.0}, 0, true, false),
        new Point(new double[]{3.0, 3.0, 3.0}, 0, true, false)
    );
    LineString ls2 = new LineString(ls2Points, 0, true, false);

    MultiLineString multiLineString = new MultiLineString(Arrays.asList(ls1, ls2), 0, true, false);
    String wkt = multiLineString.toString();
    Assertions.assertEquals("MULTILINESTRING Z ((0 0 0,1 1 1),(2 2 2,3 3 3))", wkt);
  }

  /** Tests for: MULTIPOLYGON */

  @Test
  public void testEmptyMultiPolygonToString() {
    MultiPolygon multiPolygon = new MultiPolygon(Arrays.asList(), 0, false, false);
    String wkt = multiPolygon.toString();
    Assertions.assertEquals("MULTIPOLYGON EMPTY", wkt);
  }

  @Test
  public void testMultiPolygonToString() {
    // First polygon
    List<Point> poly1Points = Arrays.asList(
        new Point(new double[]{0.0, 0.0}, 0),
        new Point(new double[]{1.0, 0.0}, 0),
        new Point(new double[]{1.0, 1.0}, 0),
        new Point(new double[]{0.0, 1.0}, 0),
        new Point(new double[]{0.0, 0.0}, 0)
    );
    Polygon poly1 = new Polygon(Arrays.asList(new Ring(poly1Points)), 0, false, false);

    // Second polygon
    List<Point> poly2Points = Arrays.asList(
        new Point(new double[]{2.0, 2.0}, 0),
        new Point(new double[]{3.0, 2.0}, 0),
        new Point(new double[]{3.0, 3.0}, 0),
        new Point(new double[]{2.0, 3.0}, 0),
        new Point(new double[]{2.0, 2.0}, 0)
    );
    Polygon poly2 = new Polygon(Arrays.asList(new Ring(poly2Points)), 0, false, false);

    MultiPolygon multiPolygon = new MultiPolygon(Arrays.asList(poly1, poly2), 0, false, false);
    String wkt = multiPolygon.toString();
    Assertions.assertEquals(
        "MULTIPOLYGON(((0 0,1 0,1 1,0 1,0 0)),((2 2,3 2,3 3,2 3,2 2)))", wkt);
  }

  @Test
  public void testMultiPolygonZToString() {
    List<Point> poly1Points = Arrays.asList(
        new Point(new double[]{0.0, 0.0, 0.0}, 0, true, false),
        new Point(new double[]{1.0, 0.0, 0.0}, 0, true, false),
        new Point(new double[]{1.0, 1.0, 0.0}, 0, true, false),
        new Point(new double[]{0.0, 1.0, 0.0}, 0, true, false),
        new Point(new double[]{0.0, 0.0, 0.0}, 0, true, false)
    );
    Polygon poly1 = new Polygon(Arrays.asList(new Ring(poly1Points)), 0, true, false);

    MultiPolygon multiPolygon = new MultiPolygon(Arrays.asList(poly1), 0, true, false);
    String wkt = multiPolygon.toString();
    Assertions.assertEquals("MULTIPOLYGON Z (((0 0 0,1 0 0,1 1 0,0 1 0,0 0 0)))", wkt);
  }

  /** Tests for: GEOMETRYCOLLECTION */

  @Test
  public void testEmptyGeometryCollectionToString() {
    GeometryCollection collection = new GeometryCollection(Arrays.asList(), 0, false, false);
    String wkt = collection.toString();
    Assertions.assertEquals("GEOMETRYCOLLECTION EMPTY", wkt);
  }

  @Test
  public void testGeometryCollectionToString() {
    Point point = new Point(new double[]{1.0, 2.0}, 0);

    List<Point> lsPoints = Arrays.asList(
        new Point(new double[]{0.0, 0.0}, 0),
        new Point(new double[]{1.0, 1.0}, 0)
    );
    LineString lineString = new LineString(lsPoints, 0, false, false);

    GeometryCollection collection = new GeometryCollection(
        Arrays.asList(point, lineString), 0, false, false);
    String wkt = collection.toString();
    Assertions.assertEquals("GEOMETRYCOLLECTION(POINT(1 2),LINESTRING(0 0,1 1))", wkt);
  }

  @Test
  public void testGeometryCollectionZToString() {
    Point point = new Point(new double[]{1.0, 2.0, 3.0}, 0, true, false);

    List<Point> lsPoints = Arrays.asList(
        new Point(new double[]{0.0, 0.0, 0.0}, 0, true, false),
        new Point(new double[]{1.0, 1.0, 1.0}, 0, true, false)
    );
    LineString lineString = new LineString(lsPoints, 0, true, false);

    GeometryCollection collection = new GeometryCollection(
        Arrays.asList(point, lineString), 0, true, false);
    String wkt = collection.toString();
    Assertions.assertEquals(
        "GEOMETRYCOLLECTION Z (POINT Z (1 2 3),LINESTRING Z (0 0 0,1 1 1))", wkt);
  }

  @Test
  public void testNestedGeometryCollectionToString() {
    Point point = new Point(new double[]{1.0, 2.0}, 0);

    List<Point> lsPoints = Arrays.asList(
        new Point(new double[]{0.0, 0.0}, 0),
        new Point(new double[]{1.0, 1.0}, 0)
    );
    LineString lineString = new LineString(lsPoints, 0, false, false);

    GeometryCollection innerCollection = new GeometryCollection(
        Arrays.asList(point), 0, false, false);

    GeometryCollection outerCollection = new GeometryCollection(
        Arrays.asList(innerCollection, lineString), 0, false, false);
    String wkt = outerCollection.toString();
    Assertions.assertEquals(
        "GEOMETRYCOLLECTION(GEOMETRYCOLLECTION(POINT(1 2)),LINESTRING(0 0,1 1))", wkt);
  }

  /** End-to-end Tests: WKB -> Geometry -> WKT */

  @Test
  public void testWkbToWktRoundTripPoint() {
    List<String[]> testCases = Arrays.asList(
      new String[]{"POINT EMPTY", "0101000000000000000000F87F000000000000F87F"}, // checkstyle.off: LineLength
      new String[]{"POINT(0 0)", "010100000000000000000000000000000000000000"}, // checkstyle.off: LineLength
      new String[]{"POINT(1 2)", "0101000000000000000000F03F0000000000000040"}, // checkstyle.off: LineLength
      new String[]{"POINT(-1 -2)", "0101000000000000000000F0BF00000000000000C0"}, // checkstyle.off: LineLength
      new String[]{"POINT(1.5 2.5)", "0101000000000000000000F83F0000000000000440"}, // checkstyle.off: LineLength
      new String[]{"POINT(-1.5 -2.5)", "0101000000000000000000F8BF00000000000004C0"}, // checkstyle.off: LineLength
      new String[]{"POINT Z EMPTY", "01E9030000000000000000F87F000000000000F87F000000000000F87F"}, // checkstyle.off: LineLength
      new String[]{"POINT Z (0 0 0)", "01E9030000000000000000000000000000000000000000000000000000"}, // checkstyle.off: LineLength
      new String[]{"POINT Z (1 2 3)", "01E9030000000000000000F03F00000000000000400000000000000840"}, // checkstyle.off: LineLength
      new String[]{"POINT Z (-1 -2 -3)", "01E9030000000000000000F0BF00000000000000C000000000000008C0"}, // checkstyle.off: LineLength
      new String[]{"POINT Z (1.5 2.5 3.5)", "01E9030000000000000000F83F00000000000004400000000000000C40"}, // checkstyle.off: LineLength
      new String[]{"POINT Z (-1.5 -2.5 -3.5)", "01E9030000000000000000F8BF00000000000004C00000000000000CC0"}, // checkstyle.off: LineLength
      new String[]{"POINT M EMPTY", "01D1070000000000000000F87F000000000000F87F000000000000F87F"}, // checkstyle.off: LineLength
      new String[]{"POINT M (0 0 0)", "01D1070000000000000000000000000000000000000000000000000000"}, // checkstyle.off: LineLength
      new String[]{"POINT M (1 2 3)", "01D1070000000000000000F03F00000000000000400000000000000840"}, // checkstyle.off: LineLength
      new String[]{"POINT M (-1 -2 3)", "01D1070000000000000000F0BF00000000000000C00000000000000840"}, // checkstyle.off: LineLength
      new String[]{"POINT M (1.5 2.5 3.5)", "01D1070000000000000000F83F00000000000004400000000000000C40"}, // checkstyle.off: LineLength
      new String[]{"POINT M (-1.5 -2.5 3.5)", "01D1070000000000000000F8BF00000000000004C00000000000000C40"}, // checkstyle.off: LineLength
      new String[]{"POINT ZM EMPTY", "01B90B0000000000000000F87F000000000000F87F000000000000F87F000000000000F87F"}, // checkstyle.off: LineLength
      new String[]{"POINT ZM (0 0 0 0)", "01B90B00000000000000000000000000000000000000000000000000000000000000000000"}, // checkstyle.off: LineLength
      new String[]{"POINT ZM (1 2 3 4)", "01B90B0000000000000000F03F000000000000004000000000000008400000000000001040"}, // checkstyle.off: LineLength
      new String[]{"POINT ZM (-1 -2 -3 4)", "01B90B0000000000000000F0BF00000000000000C000000000000008C00000000000001040"}, // checkstyle.off: LineLength
      new String[]{"POINT ZM (1.5 2.5 3.5 4.5)", "01B90B0000000000000000F83F00000000000004400000000000000C400000000000001240"}, // checkstyle.off: LineLength
      new String[]{"POINT ZM (-1.5 -2.5 -3.5 4.5)", "01B90B0000000000000000F8BF00000000000004C00000000000000CC00000000000001240"}, // checkstyle.off: LineLength
      new String[]{"LINESTRING EMPTY", "010200000000000000"}, // checkstyle.off: LineLength
      new String[]{"LINESTRING(0 0,0 0)", "0102000000020000000000000000000000000000000000000000000000000000000000000000000000"}, // checkstyle.off: LineLength
      new String[]{"LINESTRING(1 2,3 4)", "010200000002000000000000000000F03F000000000000004000000000000008400000000000001040"}, // checkstyle.off: LineLength
      new String[]{"LINESTRING(-1 -2,-3 -4)", "010200000002000000000000000000F0BF00000000000000C000000000000008C000000000000010C0"}, // checkstyle.off: LineLength
      new String[]{"LINESTRING(1.5 2.5,3.5 4.5)", "010200000002000000000000000000F83F00000000000004400000000000000C400000000000001240"}, // checkstyle.off: LineLength
      new String[]{"LINESTRING(-1.5 -2.5,-3.5 -4.5)", "010200000002000000000000000000F8BF00000000000004C00000000000000CC000000000000012C0"}, // checkstyle.off: LineLength
      new String[]{"LINESTRING Z EMPTY", "01EA03000000000000"}, // checkstyle.off: LineLength
      new String[]{"LINESTRING Z (0 0 0,0 0 0)", "01EA03000002000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"}, // checkstyle.off: LineLength
      new String[]{"LINESTRING Z (1 2 11,3 4 22)", "01EA03000002000000000000000000F03F00000000000000400000000000002640000000000000084000000000000010400000000000003640"}, // checkstyle.off: LineLength
      new String[]{"LINESTRING Z (-1 -2 -11,-3 -4 -22)", "01EA03000002000000000000000000F0BF00000000000000C000000000000026C000000000000008C000000000000010C000000000000036C0"}, // checkstyle.off: LineLength
      new String[]{"LINESTRING Z (1.5 2.5 11.5,3.5 4.5 22.5)", "01EA03000002000000000000000000F83F000000000000044000000000000027400000000000000C4000000000000012400000000000803640"}, // checkstyle.off: LineLength
      new String[]{"LINESTRING Z (-1.5 -2.5 -11.5,-3.5 -4.5 -22.5)", "01EA03000002000000000000000000F8BF00000000000004C000000000000027C00000000000000CC000000000000012C000000000008036C0"}, // checkstyle.off: LineLength
      new String[]{"LINESTRING M EMPTY", "01D207000000000000"}, // checkstyle.off: LineLength
      new String[]{"LINESTRING M (0 0 0,0 0 0)", "01D207000002000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"}, // checkstyle.off: LineLength
      new String[]{"LINESTRING M (1 2 111,3 4 222)", "01D207000002000000000000000000F03F00000000000000400000000000C05B40000000000000084000000000000010400000000000C06B40"}, // checkstyle.off: LineLength
      new String[]{"LINESTRING M (-1 -2 111,-3 -4 222)", "01D207000002000000000000000000F0BF00000000000000C00000000000C05B4000000000000008C000000000000010C00000000000C06B40"}, // checkstyle.off: LineLength
      new String[]{"LINESTRING M (1.5 2.5 111.5,3.5 4.5 222.5)", "01D207000002000000000000000000F83F00000000000004400000000000E05B400000000000000C4000000000000012400000000000D06B40"}, // checkstyle.off: LineLength
      new String[]{"LINESTRING M (-1.5 -2.5 111.5,-3.5 -4.5 222.5)", "01D207000002000000000000000000F8BF00000000000004C00000000000E05B400000000000000CC000000000000012C00000000000D06B40"}, // checkstyle.off: LineLength
      new String[]{"LINESTRING ZM EMPTY", "01BA0B000000000000"}, // checkstyle.off: LineLength
      new String[]{"LINESTRING ZM (0 0 0 0,0 0 0 0)", "01BA0B00000200000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"}, // checkstyle.off: LineLength
      new String[]{"LINESTRING ZM (1 2 11 111,3 4 22 222)", "01BA0B000002000000000000000000F03F000000000000004000000000000026400000000000C05B400000000000000840000000000000104000000000000036400000000000C06B40"}, // checkstyle.off: LineLength
      new String[]{"LINESTRING ZM (-1 -2 -11 -111,-3 -4 -22 -222)", "01BA0B000002000000000000000000F0BF00000000000000C000000000000026C00000000000C05BC000000000000008C000000000000010C000000000000036C00000000000C06BC0"}, // checkstyle.off: LineLength
      new String[]{"LINESTRING ZM (1.5 2.5 11.5 111.5,3.5 4.5 22.5 222.5)", "01BA0B000002000000000000000000F83F000000000000044000000000000027400000000000E05B400000000000000C40000000000000124000000000008036400000000000D06B40"}, // checkstyle.off: LineLength
      new String[]{"LINESTRING ZM (-1.5 -2.5 -11.5 -111.5,-3.5 -4.5 -22.5 -222.5)", "01BA0B000002000000000000000000F8BF00000000000004C000000000000027C00000000000E05BC00000000000000CC000000000000012C000000000008036C00000000000D06BC0"}, // checkstyle.off: LineLength
      new String[]{"POLYGON EMPTY", "010300000000000000"}, // checkstyle.off: LineLength
      new String[]{"POLYGON((0 0,10 0,5 10,0 0))", "0103000000010000000400000000000000000000000000000000000000000000000000244000000000000000000000000000001440000000000000244000000000000000000000000000000000"}, // checkstyle.off: LineLength
      new String[]{"POLYGON((1 2,11 2,6 12,1 2))", "01030000000100000004000000000000000000F03F00000000000000400000000000002640000000000000004000000000000018400000000000002840000000000000F03F0000000000000040"}, // checkstyle.off: LineLength
      new String[]{"POLYGON((-1 -2,-11 -2,-6 -12,-1 -2))", "01030000000100000004000000000000000000F0BF00000000000000C000000000000026C000000000000000C000000000000018C000000000000028C0000000000000F0BF00000000000000C0"}, // checkstyle.off: LineLength
      new String[]{"POLYGON((1.5 2.5,11.5 2.5,6.5 12.5,1.5 2.5))", "01030000000100000004000000000000000000F83F0000000000000440000000000000274000000000000004400000000000001A400000000000002940000000000000F83F0000000000000440"}, // checkstyle.off: LineLength
      new String[]{"POLYGON((-1.5 -2.5,-11.5 -2.5,-6.5 -12.5,-1.5 -2.5))", "01030000000100000004000000000000000000F8BF00000000000004C000000000000027C000000000000004C00000000000001AC000000000000029C0000000000000F8BF00000000000004C0"}, // checkstyle.off: LineLength
      new String[]{"POLYGON((0 0,10 0,10 10,0 10,0 0))", "010300000001000000050000000000000000000000000000000000000000000000000024400000000000000000000000000000244000000000000024400000000000000000000000000000244000000000000000000000000000000000"}, // checkstyle.off: LineLength
      new String[]{"POLYGON((1 2,11 2,11 12,1 12,1 2))", "01030000000100000005000000000000000000F03F00000000000000400000000000002640000000000000004000000000000026400000000000002840000000000000F03F0000000000002840000000000000F03F0000000000000040"}, // checkstyle.off: LineLength
      new String[]{"POLYGON((-1 -2,-11 -2,-11 -12,-1 -12,-1 -2))", "01030000000100000005000000000000000000F0BF00000000000000C000000000000026C000000000000000C000000000000026C000000000000028C0000000000000F0BF00000000000028C0000000000000F0BF00000000000000C0"}, // checkstyle.off: LineLength
      new String[]{"POLYGON((1.5 2.5,11.5 2.5,11.5 12.5,1.5 12.5,1.5 2.5))", "01030000000100000005000000000000000000F83F00000000000004400000000000002740000000000000044000000000000027400000000000002940000000000000F83F0000000000002940000000000000F83F0000000000000440"}, // checkstyle.off: LineLength
      new String[]{"POLYGON((-1.5 -2.5,-11.5 -2.5,-11.5 -12.5,-1.5 -12.5,-1.5 -2.5))", "01030000000100000005000000000000000000F8BF00000000000004C000000000000027C000000000000004C000000000000027C000000000000029C0000000000000F8BF00000000000029C0000000000000F8BF00000000000004C0"}, // checkstyle.off: LineLength
      new String[]{"POLYGON((0 0,20 0,20 20,0 20,0 0),(5 5,15 5,15 15,5 15,5 5))", "01030000000200000005000000000000000000000000000000000000000000000000003440000000000000000000000000000034400000000000003440000000000000000000000000000034400000000000000000000000000000000005000000000000000000144000000000000014400000000000002E4000000000000014400000000000002E400000000000002E4000000000000014400000000000002E4000000000000014400000000000001440"}, // checkstyle.off: LineLength
      new String[]{"POLYGON((1 2,21 2,21 22,1 22,1 2),(6 7,16 7,16 17,6 17,6 7))", "01030000000200000005000000000000000000F03F00000000000000400000000000003540000000000000004000000000000035400000000000003640000000000000F03F0000000000003640000000000000F03F00000000000000400500000000000000000018400000000000001C4000000000000030400000000000001C40000000000000304000000000000031400000000000001840000000000000314000000000000018400000000000001C40"}, // checkstyle.off: LineLength
      new String[]{"POLYGON((-1 -2,-21 -2,-21 -22,-1 -22,-1 -2),(-6 -7,-16 -7,-16 -17,-6 -17,-6 -7))", "01030000000200000005000000000000000000F0BF00000000000000C000000000000035C000000000000000C000000000000035C000000000000036C0000000000000F0BF00000000000036C0000000000000F0BF00000000000000C00500000000000000000018C00000000000001CC000000000000030C00000000000001CC000000000000030C000000000000031C000000000000018C000000000000031C000000000000018C00000000000001CC0"}, // checkstyle.off: LineLength
      new String[]{"POLYGON((1.5 2.5,21.5 2.5,21.5 22.5,1.5 22.5,1.5 2.5),(6.5 7.5,16.5 7.5,16.5 17.5,6.5 17.5,6.5 7.5))", "01030000000200000005000000000000000000F83F00000000000004400000000000803540000000000000044000000000008035400000000000803640000000000000F83F0000000000803640000000000000F83F0000000000000440050000000000000000001A400000000000001E4000000000008030400000000000001E40000000000080304000000000008031400000000000001A4000000000008031400000000000001A400000000000001E40"}, // checkstyle.off: LineLength
      new String[]{"POLYGON((-1.5 -2.5,-21.5 -2.5,-21.5 -22.5,-1.5 -22.5,-1.5 -2.5),(-6.5 -7.5,-16.5 -7.5,-16.5 -17.5,-6.5 -17.5,-6.5 -7.5))", "01030000000200000005000000000000000000F8BF00000000000004C000000000008035C000000000000004C000000000008035C000000000008036C0000000000000F8BF00000000008036C0000000000000F8BF00000000000004C0050000000000000000001AC00000000000001EC000000000008030C00000000000001EC000000000008030C000000000008031C00000000000001AC000000000008031C00000000000001AC00000000000001EC0"}, // checkstyle.off: LineLength
      new String[]{"POLYGON((0 0,30 0,30 30,0 30,0 0),(2 2,8 2,8 8,2 8,2 2),(12 12,18 12,18 18,12 18,12 12))", "01030000000300000005000000000000000000000000000000000000000000000000003E4000000000000000000000000000003E400000000000003E4000000000000000000000000000003E4000000000000000000000000000000000050000000000000000000040000000000000004000000000000020400000000000000040000000000000204000000000000020400000000000000040000000000000204000000000000000400000000000000040050000000000000000002840000000000000284000000000000032400000000000002840000000000000324000000000000032400000000000002840000000000000324000000000000028400000000000002840"}, // checkstyle.off: LineLength
      new String[]{"POLYGON((1 2,31 2,31 32,1 32,1 2),(3 4,9 4,9 10,3 10,3 4),(13 14,19 14,19 20,13 20,13 14))", "01030000000300000005000000000000000000F03F00000000000000400000000000003F4000000000000000400000000000003F400000000000004040000000000000F03F0000000000004040000000000000F03F0000000000000040050000000000000000000840000000000000104000000000000022400000000000001040000000000000224000000000000024400000000000000840000000000000244000000000000008400000000000001040050000000000000000002A400000000000002C4000000000000033400000000000002C40000000000000334000000000000034400000000000002A4000000000000034400000000000002A400000000000002C40"}, // checkstyle.off: LineLength
      new String[]{"POLYGON((-1 -2,-31 -2,-31 -32,-1 -32,-1 -2),(-3 -4,-9 -4,-9 -10,-3 -10,-3 -4),(-13 -14,-19 -14,-19 -20,-13 -20,-13 -14))", "01030000000300000005000000000000000000F0BF00000000000000C00000000000003FC000000000000000C00000000000003FC000000000000040C0000000000000F0BF00000000000040C0000000000000F0BF00000000000000C00500000000000000000008C000000000000010C000000000000022C000000000000010C000000000000022C000000000000024C000000000000008C000000000000024C000000000000008C000000000000010C0050000000000000000002AC00000000000002CC000000000000033C00000000000002CC000000000000033C000000000000034C00000000000002AC000000000000034C00000000000002AC00000000000002CC0"}, // checkstyle.off: LineLength
      new String[]{"POLYGON((1.5 2.5,31.5 2.5,31.5 32.5,1.5 32.5,1.5 2.5),(3.5 4.5,9.5 4.5,9.5 10.5,3.5 10.5,3.5 4.5),(13.5 14.5,19.5 14.5,19.5 20.5,13.5 20.5,13.5 14.5))", "01030000000300000005000000000000000000F83F00000000000004400000000000803F4000000000000004400000000000803F400000000000404040000000000000F83F0000000000404040000000000000F83F0000000000000440050000000000000000000C40000000000000124000000000000023400000000000001240000000000000234000000000000025400000000000000C4000000000000025400000000000000C400000000000001240050000000000000000002B400000000000002D4000000000008033400000000000002D40000000000080334000000000008034400000000000002B4000000000008034400000000000002B400000000000002D40"}, // checkstyle.off: LineLength
      new String[]{"POLYGON((-1.5 -2.5,-31.5 -2.5,-31.5 -32.5,-1.5 -32.5,-1.5 -2.5),(-3.5 -4.5,-9.5 -4.5,-9.5 -10.5,-3.5 -10.5,-3.5 -4.5),(-13.5 -14.5,-19.5 -14.5,-19.5 -20.5,-13.5 -20.5,-13.5 -14.5))", "01030000000300000005000000000000000000F8BF00000000000004C00000000000803FC000000000000004C00000000000803FC000000000004040C0000000000000F8BF00000000004040C0000000000000F8BF00000000000004C0050000000000000000000CC000000000000012C000000000000023C000000000000012C000000000000023C000000000000025C00000000000000CC000000000000025C00000000000000CC000000000000012C0050000000000000000002BC00000000000002DC000000000008033C00000000000002DC000000000008033C000000000008034C00000000000002BC000000000008034C00000000000002BC00000000000002DC0"}, // checkstyle.off: LineLength
      new String[]{"POLYGON((0 0,40 0,40 40,0 40,0 0),(2 2,8 2,8 8,2 8,2 2),(12 2,18 2,18 8,12 8,12 2),(2 12,8 12,8 18,2 18,2 12))", "010300000004000000050000000000000000000000000000000000000000000000000044400000000000000000000000000000444000000000000044400000000000000000000000000000444000000000000000000000000000000000050000000000000000000040000000000000004000000000000020400000000000000040000000000000204000000000000020400000000000000040000000000000204000000000000000400000000000000040050000000000000000002840000000000000004000000000000032400000000000000040000000000000324000000000000020400000000000002840000000000000204000000000000028400000000000000040050000000000000000000040000000000000284000000000000020400000000000002840000000000000204000000000000032400000000000000040000000000000324000000000000000400000000000002840"}, // checkstyle.off: LineLength
      new String[]{"POLYGON Z EMPTY", "01EB03000000000000"}, // checkstyle.off: LineLength
      new String[]{"POLYGON Z ((0 0 0,10 0 0,5 10 0,0 0 0))", "01EB0300000100000004000000000000000000000000000000000000000000000000000000000000000000244000000000000000000000000000000000000000000000144000000000000024400000000000000000000000000000000000000000000000000000000000000000"}, // checkstyle.off: LineLength
      new String[]{"POLYGON Z ((1 2 11,11 2 22,6 12 33,1 2 11))", "01EB0300000100000004000000000000000000F03F00000000000000400000000000002640000000000000264000000000000000400000000000003640000000000000184000000000000028400000000000804040000000000000F03F00000000000000400000000000002640"}, // checkstyle.off: LineLength
      new String[]{"POLYGON Z ((-1 -2 -11,-11 -2 -22,-6 -12 -33,-1 -2 -11))", "01EB0300000100000004000000000000000000F0BF00000000000000C000000000000026C000000000000026C000000000000000C000000000000036C000000000000018C000000000000028C000000000008040C0000000000000F0BF00000000000000C000000000000026C0"}, // checkstyle.off: LineLength
      new String[]{"POLYGON Z ((1.5 2.5 11.5,11.5 2.5 22.5,6.5 12.5 33.5,1.5 2.5 11.5))", "01EB0300000100000004000000000000000000F83F000000000000044000000000000027400000000000002740000000000000044000000000008036400000000000001A4000000000000029400000000000C04040000000000000F83F00000000000004400000000000002740"}, // checkstyle.off: LineLength
      new String[]{"POLYGON Z ((-1.5 -2.5 -11.5,-11.5 -2.5 -22.5,-6.5 -12.5 -33.5,-1.5 -2.5 -11.5))", "01EB0300000100000004000000000000000000F8BF00000000000004C000000000000027C000000000000027C000000000000004C000000000008036C00000000000001AC000000000000029C00000000000C040C0000000000000F8BF00000000000004C000000000000027C0"}, // checkstyle.off: LineLength
      new String[]{"POLYGON Z ((0 0 0,20 0 0,20 20 0,0 20 0,0 0 0),(5 5 0,15 5 0,15 15 0,5 15 0,5 5 0))", "01EB0300000200000005000000000000000000000000000000000000000000000000000000000000000000344000000000000000000000000000000000000000000000344000000000000034400000000000000000000000000000000000000000000034400000000000000000000000000000000000000000000000000000000000000000050000000000000000001440000000000000144000000000000000000000000000002E40000000000000144000000000000000000000000000002E400000000000002E40000000000000000000000000000014400000000000002E400000000000000000000000000000144000000000000014400000000000000000"}, // checkstyle.off: LineLength
      new String[]{"POLYGON Z ((1 2 11,21 2 11,21 22 11,1 22 11,1 2 11),(6 7 22,16 7 22,16 17 22,6 17 22,6 7 22))", "01EB0300000200000005000000000000000000F03F00000000000000400000000000002640000000000000354000000000000000400000000000002640000000000000354000000000000036400000000000002640000000000000F03F00000000000036400000000000002640000000000000F03F000000000000004000000000000026400500000000000000000018400000000000001C40000000000000364000000000000030400000000000001C40000000000000364000000000000030400000000000003140000000000000364000000000000018400000000000003140000000000000364000000000000018400000000000001C400000000000003640"}, // checkstyle.off: LineLength
      new String[]{"POLYGON Z ((0 0 0,30 0 0,30 30 0,0 30 0,0 0 0),(2 2 0,8 2 0,8 8 0,2 8 0,2 2 0),(12 12 0,18 12 0,18 18 0,12 18 0,12 12 0))", "01EB03000003000000050000000000000000000000000000000000000000000000000000000000000000003E40000000000000000000000000000000000000000000003E400000000000003E40000000000000000000000000000000000000000000003E4000000000000000000000000000000000000000000000000000000000000000000500000000000000000000400000000000000040000000000000000000000000000020400000000000000040000000000000000000000000000020400000000000002040000000000000000000000000000000400000000000002040000000000000000000000000000000400000000000000040000000000000000005000000000000000000284000000000000028400000000000000000000000000000324000000000000028400000000000000000000000000000324000000000000032400000000000000000000000000000284000000000000032400000000000000000000000000000284000000000000028400000000000000000"}, // checkstyle.off: LineLength
      new String[]{"POLYGON M EMPTY", "01D307000000000000"}, // checkstyle.off: LineLength
      new String[]{"POLYGON M ((0 0 0,10 0 0,5 10 0,0 0 0))", "01D30700000100000004000000000000000000000000000000000000000000000000000000000000000000244000000000000000000000000000000000000000000000144000000000000024400000000000000000000000000000000000000000000000000000000000000000"}, // checkstyle.off: LineLength
      new String[]{"POLYGON M ((1 2 111,11 2 222,6 12 333,1 2 111))", "01D30700000100000004000000000000000000F03F00000000000000400000000000C05B40000000000000264000000000000000400000000000C06B40000000000000184000000000000028400000000000D07440000000000000F03F00000000000000400000000000C05B40"}, // checkstyle.off: LineLength
      new String[]{"POLYGON M ((-1 -2 111,-11 -2 222,-6 -12 333,-1 -2 111))", "01D30700000100000004000000000000000000F0BF00000000000000C00000000000C05B4000000000000026C000000000000000C00000000000C06B4000000000000018C000000000000028C00000000000D07440000000000000F0BF00000000000000C00000000000C05B40"}, // checkstyle.off: LineLength
      new String[]{"POLYGON M ((1.5 2.5 111.5,11.5 2.5 222.5,6.5 12.5 333.5,1.5 2.5 111.5))", "01D30700000100000004000000000000000000F83F00000000000004400000000000E05B40000000000000274000000000000004400000000000D06B400000000000001A4000000000000029400000000000D87440000000000000F83F00000000000004400000000000E05B40"}, // checkstyle.off: LineLength
      new String[]{"POLYGON M ((-1.5 -2.5 111.5,-11.5 -2.5 222.5,-6.5 -12.5 333.5,-1.5 -2.5 111.5))", "01D30700000100000004000000000000000000F8BF00000000000004C00000000000E05B4000000000000027C000000000000004C00000000000D06B400000000000001AC000000000000029C00000000000D87440000000000000F8BF00000000000004C00000000000E05B40"}, // checkstyle.off: LineLength
      new String[]{"POLYGON M ((0 0 0,20 0 0,20 20 0,0 20 0,0 0 0),(5 5 0,15 5 0,15 15 0,5 15 0,5 5 0))", "01D30700000200000005000000000000000000000000000000000000000000000000000000000000000000344000000000000000000000000000000000000000000000344000000000000034400000000000000000000000000000000000000000000034400000000000000000000000000000000000000000000000000000000000000000050000000000000000001440000000000000144000000000000000000000000000002E40000000000000144000000000000000000000000000002E400000000000002E40000000000000000000000000000014400000000000002E400000000000000000000000000000144000000000000014400000000000000000"}, // checkstyle.off: LineLength
      new String[]{"POLYGON M ((1 2 111,21 2 111,21 22 111,1 22 111,1 2 111),(6 7 222,16 7 222,16 17 222,6 17 222,6 7 222))", "01D30700000200000005000000000000000000F03F00000000000000400000000000C05B40000000000000354000000000000000400000000000C05B40000000000000354000000000000036400000000000C05B40000000000000F03F00000000000036400000000000C05B40000000000000F03F00000000000000400000000000C05B400500000000000000000018400000000000001C400000000000C06B4000000000000030400000000000001C400000000000C06B40000000000000304000000000000031400000000000C06B40000000000000184000000000000031400000000000C06B4000000000000018400000000000001C400000000000C06B40"}, // checkstyle.off: LineLength
      new String[]{"POLYGON M ((0 0 0,30 0 0,30 30 0,0 30 0,0 0 0),(2 2 0,8 2 0,8 8 0,2 8 0,2 2 0),(12 12 0,18 12 0,18 18 0,12 18 0,12 12 0))", "01D307000003000000050000000000000000000000000000000000000000000000000000000000000000003E40000000000000000000000000000000000000000000003E400000000000003E40000000000000000000000000000000000000000000003E4000000000000000000000000000000000000000000000000000000000000000000500000000000000000000400000000000000040000000000000000000000000000020400000000000000040000000000000000000000000000020400000000000002040000000000000000000000000000000400000000000002040000000000000000000000000000000400000000000000040000000000000000005000000000000000000284000000000000028400000000000000000000000000000324000000000000028400000000000000000000000000000324000000000000032400000000000000000000000000000284000000000000032400000000000000000000000000000284000000000000028400000000000000000"}, // checkstyle.off: LineLength
      new String[]{"POLYGON ZM EMPTY", "01BB0B000000000000"}, // checkstyle.off: LineLength
      new String[]{"POLYGON ZM ((0 0 0 0,10 0 0 0,5 10 0 0,0 0 0 0))", "01BB0B000001000000040000000000000000000000000000000000000000000000000000000000000000000000000000000000244000000000000000000000000000000000000000000000000000000000000014400000000000002440000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"}, // checkstyle.off: LineLength
      new String[]{"POLYGON ZM ((1 2 11 111,11 2 22 222,6 12 33 333,1 2 11 111))", "01BB0B00000100000004000000000000000000F03F000000000000004000000000000026400000000000C05B400000000000002640000000000000004000000000000036400000000000C06B400000000000001840000000000000284000000000008040400000000000D07440000000000000F03F000000000000004000000000000026400000000000C05B40"}, // checkstyle.off: LineLength
      new String[]{"POLYGON ZM ((-1 -2 -11 111,-11 -2 -22 222,-6 -12 -33 333,-1 -2 -11 111))", "01BB0B00000100000004000000000000000000F0BF00000000000000C000000000000026C00000000000C05B4000000000000026C000000000000000C000000000000036C00000000000C06B4000000000000018C000000000000028C000000000008040C00000000000D07440000000000000F0BF00000000000000C000000000000026C00000000000C05B40"}, // checkstyle.off: LineLength
      new String[]{"POLYGON ZM ((1.5 2.5 11.5 111.5,11.5 2.5 22.5 222.5,6.5 12.5 33.5 333.5,1.5 2.5 11.5 111.5))", "01BB0B00000100000004000000000000000000F83F000000000000044000000000000027400000000000E05B400000000000002740000000000000044000000000008036400000000000D06B400000000000001A4000000000000029400000000000C040400000000000D87440000000000000F83F000000000000044000000000000027400000000000E05B40"}, // checkstyle.off: LineLength
      new String[]{"POLYGON ZM ((-1.5 -2.5 -11.5 111.5,-11.5 -2.5 -22.5 222.5,-6.5 -12.5 -33.5 333.5,-1.5 -2.5 -11.5 111.5))", "01BB0B00000100000004000000000000000000F8BF00000000000004C000000000000027C00000000000E05B4000000000000027C000000000000004C000000000008036C00000000000D06B400000000000001AC000000000000029C00000000000C040C00000000000D87440000000000000F8BF00000000000004C000000000000027C00000000000E05B40"}, // checkstyle.off: LineLength
      new String[]{"POLYGON ZM ((0 0 0 0,20 0 0 0,20 20 0 0,0 20 0 0,0 0 0 0),(5 5 0 0,15 5 0 0,15 15 0 0,5 15 0 0,5 5 0 0))", "01BB0B00000200000005000000000000000000000000000000000000000000000000000000000000000000000000000000000034400000000000000000000000000000000000000000000000000000000000003440000000000000344000000000000000000000000000000000000000000000000000000000000034400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000500000000000000000014400000000000001440000000000000000000000000000000000000000000002E400000000000001440000000000000000000000000000000000000000000002E400000000000002E400000000000000000000000000000000000000000000014400000000000002E40000000000000000000000000000000000000000000001440000000000000144000000000000000000000000000000000"}, // checkstyle.off: LineLength
      new String[]{"POLYGON ZM ((1 2 11 111,21 2 11 111,21 22 11 111,1 22 11 111,1 2 11 111),(6 7 22 222,16 7 22 222,16 17 22 222,6 17 22 222,6 7 22 222))", "01BB0B00000200000005000000000000000000F03F000000000000004000000000000026400000000000C05B400000000000003540000000000000004000000000000026400000000000C05B400000000000003540000000000000364000000000000026400000000000C05B40000000000000F03F000000000000364000000000000026400000000000C05B40000000000000F03F000000000000004000000000000026400000000000C05B400500000000000000000018400000000000001C4000000000000036400000000000C06B4000000000000030400000000000001C4000000000000036400000000000C06B400000000000003040000000000000314000000000000036400000000000C06B400000000000001840000000000000314000000000000036400000000000C06B4000000000000018400000000000001C4000000000000036400000000000C06B40"}, // checkstyle.off: LineLength
      new String[]{"POLYGON ZM ((0 0 0 0,30 0 0 0,30 30 0 0,0 30 0 0,0 0 0 0),(2 2 0 0,8 2 0 0,8 8 0 0,2 8 0 0,2 2 0 0),(12 12 0 0,18 12 0 0,18 18 0 0,12 18 0 0,12 12 0 0))", "01BB0B0000030000000500000000000000000000000000000000000000000000000000000000000000000000000000000000003E400000000000000000000000000000000000000000000000000000000000003E400000000000003E400000000000000000000000000000000000000000000000000000000000003E4000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000005000000000000000000004000000000000000400000000000000000000000000000000000000000000020400000000000000040000000000000000000000000000000000000000000002040000000000000204000000000000000000000000000000000000000000000004000000000000020400000000000000000000000000000000000000000000000400000000000000040000000000000000000000000000000000500000000000000000028400000000000002840000000000000000000000000000000000000000000003240000000000000284000000000000000000000000000000000000000000000324000000000000032400000000000000000000000000000000000000000000028400000000000003240000000000000000000000000000000000000000000002840000000000000284000000000000000000000000000000000"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOINT EMPTY", "010400000000000000"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOINT((0 0))", "010400000001000000010100000000000000000000000000000000000000"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOINT((1 2))", "0104000000010000000101000000000000000000F03F0000000000000040"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOINT((-1 -2))", "0104000000010000000101000000000000000000F0BF00000000000000C0"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOINT((1.5 2.5))", "0104000000010000000101000000000000000000F83F0000000000000440"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOINT((-1.5 -2.5))", "0104000000010000000101000000000000000000F8BF00000000000004C0"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOINT((0 0),(0 0))", "010400000002000000010100000000000000000000000000000000000000010100000000000000000000000000000000000000"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOINT((1 2),(3 4))", "0104000000020000000101000000000000000000F03F0000000000000040010100000000000000000008400000000000001040"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOINT((-1 -2),(-3 -4))", "0104000000020000000101000000000000000000F0BF00000000000000C0010100000000000000000008C000000000000010C0"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOINT((1.5 2.5),(3.5 4.5))", "0104000000020000000101000000000000000000F83F000000000000044001010000000000000000000C400000000000001240"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOINT((-1.5 -2.5),(-3.5 -4.5))", "0104000000020000000101000000000000000000F8BF00000000000004C001010000000000000000000CC000000000000012C0"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOINT((0 0),(0 0),(0 0))", "010400000003000000010100000000000000000000000000000000000000010100000000000000000000000000000000000000010100000000000000000000000000000000000000"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOINT((1 2),(3 4),(5 6))", "0104000000030000000101000000000000000000F03F0000000000000040010100000000000000000008400000000000001040010100000000000000000014400000000000001840"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOINT((-1 -2),(-3 -4),(-5 -6))", "0104000000030000000101000000000000000000F0BF00000000000000C0010100000000000000000008C000000000000010C0010100000000000000000014C000000000000018C0"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOINT((1.5 2.5),(3.5 4.5),(5.5 6.5))", "0104000000030000000101000000000000000000F83F000000000000044001010000000000000000000C400000000000001240010100000000000000000016400000000000001A40"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOINT((-1.5 -2.5),(-3.5 -4.5),(-5.5 -6.5))", "0104000000030000000101000000000000000000F8BF00000000000004C001010000000000000000000CC000000000000012C0010100000000000000000016C00000000000001AC0"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOINT Z EMPTY", "01EC03000000000000"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOINT Z ((0 0 0))", "01EC0300000100000001E9030000000000000000000000000000000000000000000000000000"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOINT Z ((1 2 11))", "01EC0300000100000001E9030000000000000000F03F00000000000000400000000000002640"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOINT Z ((-1 -2 -11))", "01EC0300000100000001E9030000000000000000F0BF00000000000000C000000000000026C0"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOINT Z ((1.5 2.5 11.5))", "01EC0300000100000001E9030000000000000000F83F00000000000004400000000000002740"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOINT Z ((-1.5 -2.5 -11.5))", "01EC0300000100000001E9030000000000000000F8BF00000000000004C000000000000027C0"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOINT Z ((0 0 0),(0 0 0))", "01EC0300000200000001E903000000000000000000000000000000000000000000000000000001E9030000000000000000000000000000000000000000000000000000"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOINT Z ((1 2 11),(3 4 22))", "01EC0300000200000001E9030000000000000000F03F0000000000000040000000000000264001E9030000000000000000084000000000000010400000000000003640"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOINT Z ((-1 -2 -11),(-3 -4 -22))", "01EC0300000200000001E9030000000000000000F0BF00000000000000C000000000000026C001E903000000000000000008C000000000000010C000000000000036C0"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOINT Z ((1.5 2.5 11.5),(3.5 4.5 22.5))", "01EC0300000200000001E9030000000000000000F83F0000000000000440000000000000274001E90300000000000000000C4000000000000012400000000000803640"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOINT Z ((-1.5 -2.5 -11.5),(-3.5 -4.5 -22.5))", "01EC0300000200000001E9030000000000000000F8BF00000000000004C000000000000027C001E90300000000000000000CC000000000000012C000000000008036C0"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOINT Z ((0 0 0),(0 0 0),(0 0 0))", "01EC0300000300000001E903000000000000000000000000000000000000000000000000000001E903000000000000000000000000000000000000000000000000000001E9030000000000000000000000000000000000000000000000000000"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOINT Z ((1 2 11),(3 4 22),(5 6 33))", "01EC0300000300000001E9030000000000000000F03F0000000000000040000000000000264001E903000000000000000008400000000000001040000000000000364001E9030000000000000000144000000000000018400000000000804040"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOINT Z ((-1 -2 -11),(-3 -4 -22),(-5 -6 -33))", "01EC0300000300000001E9030000000000000000F0BF00000000000000C000000000000026C001E903000000000000000008C000000000000010C000000000000036C001E903000000000000000014C000000000000018C000000000008040C0"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOINT Z ((1.5 2.5 11.5),(3.5 4.5 22.5),(5.5 6.5 33.5))", "01EC0300000300000001E9030000000000000000F83F0000000000000440000000000000274001E90300000000000000000C400000000000001240000000000080364001E903000000000000000016400000000000001A400000000000C04040"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOINT Z ((-1.5 -2.5 -11.5),(-3.5 -4.5 -22.5),(-5.5 -6.5 -33.5))", "01EC0300000300000001E9030000000000000000F8BF00000000000004C000000000000027C001E90300000000000000000CC000000000000012C000000000008036C001E903000000000000000016C00000000000001AC00000000000C040C0"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOINT M EMPTY", "01D407000000000000"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOINT M ((0 0 0))", "01D40700000100000001D1070000000000000000000000000000000000000000000000000000"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOINT M ((1 2 111))", "01D40700000100000001D1070000000000000000F03F00000000000000400000000000C05B40"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOINT M ((-1 -2 111))", "01D40700000100000001D1070000000000000000F0BF00000000000000C00000000000C05B40"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOINT M ((1.5 2.5 111.5))", "01D40700000100000001D1070000000000000000F83F00000000000004400000000000E05B40"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOINT M ((-1.5 -2.5 111.5))", "01D40700000100000001D1070000000000000000F8BF00000000000004C00000000000E05B40"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOINT M ((0 0 0),(0 0 0))", "01D40700000200000001D107000000000000000000000000000000000000000000000000000001D1070000000000000000000000000000000000000000000000000000"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOINT M ((1 2 111),(3 4 222))", "01D40700000200000001D1070000000000000000F03F00000000000000400000000000C05B4001D1070000000000000000084000000000000010400000000000C06B40"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOINT M ((-1 -2 111),(-3 -4 222))", "01D40700000200000001D1070000000000000000F0BF00000000000000C00000000000C05B4001D107000000000000000008C000000000000010C00000000000C06B40"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOINT M ((1.5 2.5 111.5),(3.5 4.5 222.5))", "01D40700000200000001D1070000000000000000F83F00000000000004400000000000E05B4001D10700000000000000000C4000000000000012400000000000D06B40"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOINT M ((-1.5 -2.5 111.5),(-3.5 -4.5 222.5))", "01D40700000200000001D1070000000000000000F8BF00000000000004C00000000000E05B4001D10700000000000000000CC000000000000012C00000000000D06B40"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOINT M ((0 0 0),(0 0 0),(0 0 0))", "01D40700000300000001D107000000000000000000000000000000000000000000000000000001D107000000000000000000000000000000000000000000000000000001D1070000000000000000000000000000000000000000000000000000"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOINT M ((1 2 111),(3 4 222),(5 6 333))", "01D40700000300000001D1070000000000000000F03F00000000000000400000000000C05B4001D1070000000000000000084000000000000010400000000000C06B4001D1070000000000000000144000000000000018400000000000D07440"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOINT M ((-1 -2 111),(-3 -4 222),(-5 -6 333))", "01D40700000300000001D1070000000000000000F0BF00000000000000C00000000000C05B4001D107000000000000000008C000000000000010C00000000000C06B4001D107000000000000000014C000000000000018C00000000000D07440"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOINT M ((1.5 2.5 111.5),(3.5 4.5 222.5),(5.5 6.5 333.5))", "01D40700000300000001D1070000000000000000F83F00000000000004400000000000E05B4001D10700000000000000000C4000000000000012400000000000D06B4001D107000000000000000016400000000000001A400000000000D87440"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOINT M ((-1.5 -2.5 111.5),(-3.5 -4.5 222.5),(-5.5 -6.5 333.5))", "01D40700000300000001D1070000000000000000F8BF00000000000004C00000000000E05B4001D10700000000000000000CC000000000000012C00000000000D06B4001D107000000000000000016C00000000000001AC00000000000D87440"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOINT ZM EMPTY", "01BC0B000000000000"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOINT ZM ((0 0 0 0))", "01BC0B00000100000001B90B00000000000000000000000000000000000000000000000000000000000000000000"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOINT ZM ((1 2 11 111))", "01BC0B00000100000001B90B0000000000000000F03F000000000000004000000000000026400000000000C05B40"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOINT ZM ((-1 -2 -11 111))", "01BC0B00000100000001B90B0000000000000000F0BF00000000000000C000000000000026C00000000000C05B40"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOINT ZM ((1.5 2.5 11.5 111.5))", "01BC0B00000100000001B90B0000000000000000F83F000000000000044000000000000027400000000000E05B40"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOINT ZM ((-1.5 -2.5 -11.5 111.5))", "01BC0B00000100000001B90B0000000000000000F8BF00000000000004C000000000000027C00000000000E05B40"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOINT ZM ((0 0 0 0),(0 0 0 0))", "01BC0B00000200000001B90B0000000000000000000000000000000000000000000000000000000000000000000001B90B00000000000000000000000000000000000000000000000000000000000000000000"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOINT ZM ((1 2 11 111),(3 4 22 222))", "01BC0B00000200000001B90B0000000000000000F03F000000000000004000000000000026400000000000C05B4001B90B00000000000000000840000000000000104000000000000036400000000000C06B40"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOINT ZM ((-1 -2 -11 111),(-3 -4 -22 222))", "01BC0B00000200000001B90B0000000000000000F0BF00000000000000C000000000000026C00000000000C05B4001B90B000000000000000008C000000000000010C000000000000036C00000000000C06B40"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOINT ZM ((1.5 2.5 11.5 111.5),(3.5 4.5 22.5 222.5))", "01BC0B00000200000001B90B0000000000000000F83F000000000000044000000000000027400000000000E05B4001B90B00000000000000000C40000000000000124000000000008036400000000000D06B40"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOINT ZM ((-1.5 -2.5 -11.5 111.5),(-3.5 -4.5 -22.5 222.5))", "01BC0B00000200000001B90B0000000000000000F8BF00000000000004C000000000000027C00000000000E05B4001B90B00000000000000000CC000000000000012C000000000008036C00000000000D06B40"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOINT ZM ((0 0 0 0),(0 0 0 0),(0 0 0 0))", "01BC0B00000300000001B90B0000000000000000000000000000000000000000000000000000000000000000000001B90B0000000000000000000000000000000000000000000000000000000000000000000001B90B00000000000000000000000000000000000000000000000000000000000000000000"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOINT ZM ((1 2 11 111),(3 4 22 222),(5 6 33 333))", "01BC0B00000300000001B90B0000000000000000F03F000000000000004000000000000026400000000000C05B4001B90B00000000000000000840000000000000104000000000000036400000000000C06B4001B90B00000000000000001440000000000000184000000000008040400000000000D07440"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOINT ZM ((-1 -2 -11 111),(-3 -4 -22 222),(-5 -6 -33 333))", "01BC0B00000300000001B90B0000000000000000F0BF00000000000000C000000000000026C00000000000C05B4001B90B000000000000000008C000000000000010C000000000000036C00000000000C06B4001B90B000000000000000014C000000000000018C000000000008040C00000000000D07440"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOINT ZM ((1.5 2.5 11.5 111.5),(3.5 4.5 22.5 222.5),(5.5 6.5 33.5 333.5))", "01BC0B00000300000001B90B0000000000000000F83F000000000000044000000000000027400000000000E05B4001B90B00000000000000000C40000000000000124000000000008036400000000000D06B4001B90B000000000000000016400000000000001A400000000000C040400000000000D87440"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOINT ZM ((-1.5 -2.5 -11.5 111.5),(-3.5 -4.5 -22.5 222.5),(-5.5 -6.5 -33.5 333.5))", "01BC0B00000300000001B90B0000000000000000F8BF00000000000004C000000000000027C00000000000E05B4001B90B00000000000000000CC000000000000012C000000000008036C00000000000D06B4001B90B000000000000000016C00000000000001AC00000000000C040C00000000000D87440"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOINT(EMPTY)", "0104000000010000000101000000000000000000F87F000000000000F87F"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOINT(EMPTY,EMPTY)", "0104000000020000000101000000000000000000F87F000000000000F87F0101000000000000000000F87F000000000000F87F"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOINT(EMPTY,(1 2))", "0104000000020000000101000000000000000000F87F000000000000F87F0101000000000000000000F03F0000000000000040"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOINT((1 2),EMPTY)", "0104000000020000000101000000000000000000F03F00000000000000400101000000000000000000F87F000000000000F87F"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOINT(EMPTY,(1 2),EMPTY)", "0104000000030000000101000000000000000000F87F000000000000F87F0101000000000000000000F03F00000000000000400101000000000000000000F87F000000000000F87F"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOINT((1 2),EMPTY,(3 4))", "0104000000030000000101000000000000000000F03F00000000000000400101000000000000000000F87F000000000000F87F010100000000000000000008400000000000001040"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOINT(EMPTY,EMPTY,(1 2))", "0104000000030000000101000000000000000000F87F000000000000F87F0101000000000000000000F87F000000000000F87F0101000000000000000000F03F0000000000000040"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOINT((1 2),EMPTY,EMPTY)", "0104000000030000000101000000000000000000F03F00000000000000400101000000000000000000F87F000000000000F87F0101000000000000000000F87F000000000000F87F"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOINT(EMPTY,(1 2),EMPTY,(3 4),EMPTY)", "0104000000050000000101000000000000000000F87F000000000000F87F0101000000000000000000F03F00000000000000400101000000000000000000F87F000000000000F87F0101000000000000000000084000000000000010400101000000000000000000F87F000000000000F87F"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOINT((1 2),EMPTY,(3 4),EMPTY,(5 6))", "0104000000050000000101000000000000000000F03F00000000000000400101000000000000000000F87F000000000000F87F0101000000000000000000084000000000000010400101000000000000000000F87F000000000000F87F010100000000000000000014400000000000001840"}, // checkstyle.off: LineLength
      new String[]{"MULTILINESTRING EMPTY", "010500000000000000"}, // checkstyle.off: LineLength
      new String[]{"MULTILINESTRING((0 0,0 0))", "0105000000010000000102000000020000000000000000000000000000000000000000000000000000000000000000000000"}, // checkstyle.off: LineLength
      new String[]{"MULTILINESTRING((1 2,3 4))", "010500000001000000010200000002000000000000000000F03F000000000000004000000000000008400000000000001040"}, // checkstyle.off: LineLength
      new String[]{"MULTILINESTRING((-1 -2,-3 -4))", "010500000001000000010200000002000000000000000000F0BF00000000000000C000000000000008C000000000000010C0"}, // checkstyle.off: LineLength
      new String[]{"MULTILINESTRING((1.5 2.5,3.5 4.5))", "010500000001000000010200000002000000000000000000F83F00000000000004400000000000000C400000000000001240"}, // checkstyle.off: LineLength
      new String[]{"MULTILINESTRING((-1.5 -2.5,-3.5 -4.5))", "010500000001000000010200000002000000000000000000F8BF00000000000004C00000000000000CC000000000000012C0"}, // checkstyle.off: LineLength
      new String[]{"MULTILINESTRING((0 0,0 0),(0 0,0 0))", "01050000000200000001020000000200000000000000000000000000000000000000000000000000000000000000000000000102000000020000000000000000000000000000000000000000000000000000000000000000000000"}, // checkstyle.off: LineLength
      new String[]{"MULTILINESTRING((1 2,3 4),(5 6,7 8))", "010500000002000000010200000002000000000000000000F03F000000000000004000000000000008400000000000001040010200000002000000000000000000144000000000000018400000000000001C400000000000002040"}, // checkstyle.off: LineLength
      new String[]{"MULTILINESTRING((-1 -2,-3 -4),(-5 -6,-7 -8))", "010500000002000000010200000002000000000000000000F0BF00000000000000C000000000000008C000000000000010C001020000000200000000000000000014C000000000000018C00000000000001CC000000000000020C0"}, // checkstyle.off: LineLength
      new String[]{"MULTILINESTRING((1.5 2.5,3.5 4.5),(5.5 6.5,7.5 8.5))", "010500000002000000010200000002000000000000000000F83F00000000000004400000000000000C40000000000000124001020000000200000000000000000016400000000000001A400000000000001E400000000000002140"}, // checkstyle.off: LineLength
      new String[]{"MULTILINESTRING((-1.5 -2.5,-3.5 -4.5),(-5.5 -6.5,-7.5 -8.5))", "010500000002000000010200000002000000000000000000F8BF00000000000004C00000000000000CC000000000000012C001020000000200000000000000000016C00000000000001AC00000000000001EC000000000000021C0"}, // checkstyle.off: LineLength
      new String[]{"MULTILINESTRING((0 0,0 0),(0 0,0 0),(0 0,0 0))", "010500000003000000010200000002000000000000000000000000000000000000000000000000000000000000000000000001020000000200000000000000000000000000000000000000000000000000000000000000000000000102000000020000000000000000000000000000000000000000000000000000000000000000000000"}, // checkstyle.off: LineLength
      new String[]{"MULTILINESTRING((1 2,3 4),(5 6,7 8),(9 10,11 12))", "010500000003000000010200000002000000000000000000F03F000000000000004000000000000008400000000000001040010200000002000000000000000000144000000000000018400000000000001C4000000000000020400102000000020000000000000000002240000000000000244000000000000026400000000000002840"}, // checkstyle.off: LineLength
      new String[]{"MULTILINESTRING((-1 -2,-3 -4),(-5 -6,-7 -8),(-9 -10,-11 -12))", "010500000003000000010200000002000000000000000000F0BF00000000000000C000000000000008C000000000000010C001020000000200000000000000000014C000000000000018C00000000000001CC000000000000020C001020000000200000000000000000022C000000000000024C000000000000026C000000000000028C0"}, // checkstyle.off: LineLength
      new String[]{"MULTILINESTRING((1.5 2.5,3.5 4.5),(5.5 6.5,7.5 8.5),(9.5 10.5,11.5 12.5))", "010500000003000000010200000002000000000000000000F83F00000000000004400000000000000C40000000000000124001020000000200000000000000000016400000000000001A400000000000001E4000000000000021400102000000020000000000000000002340000000000000254000000000000027400000000000002940"}, // checkstyle.off: LineLength
      new String[]{"MULTILINESTRING((-1.5 -2.5,-3.5 -4.5),(-5.5 -6.5,-7.5 -8.5),(-9.5 -10.5,-11.5 -12.5))", "010500000003000000010200000002000000000000000000F8BF00000000000004C00000000000000CC000000000000012C001020000000200000000000000000016C00000000000001AC00000000000001EC000000000000021C001020000000200000000000000000023C000000000000025C000000000000027C000000000000029C0"}, // checkstyle.off: LineLength
      new String[]{"MULTILINESTRING Z EMPTY", "01ED03000000000000"}, // checkstyle.off: LineLength
      new String[]{"MULTILINESTRING Z ((0 0 0,0 0 0))", "01ED0300000100000001EA03000002000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"}, // checkstyle.off: LineLength
      new String[]{"MULTILINESTRING Z ((1 2 11,3 4 22))", "01ED0300000100000001EA03000002000000000000000000F03F00000000000000400000000000002640000000000000084000000000000010400000000000003640"}, // checkstyle.off: LineLength
      new String[]{"MULTILINESTRING Z ((-1 -2 -11,-3 -4 -22))", "01ED0300000100000001EA03000002000000000000000000F0BF00000000000000C000000000000026C000000000000008C000000000000010C000000000000036C0"}, // checkstyle.off: LineLength
      new String[]{"MULTILINESTRING Z ((1.5 2.5 11.5,3.5 4.5 22.5))", "01ED0300000100000001EA03000002000000000000000000F83F000000000000044000000000000027400000000000000C4000000000000012400000000000803640"}, // checkstyle.off: LineLength
      new String[]{"MULTILINESTRING Z ((-1.5 -2.5 -11.5,-3.5 -4.5 -22.5))", "01ED0300000100000001EA03000002000000000000000000F8BF00000000000004C000000000000027C00000000000000CC000000000000012C000000000008036C0"}, // checkstyle.off: LineLength
      new String[]{"MULTILINESTRING Z ((0 0 0,0 0 0),(0 0 0,0 0 0))", "01ED0300000200000001EA0300000200000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001EA03000002000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"}, // checkstyle.off: LineLength
      new String[]{"MULTILINESTRING Z ((1 2 11,3 4 22),(5 6 33,7 8 44))", "01ED0300000200000001EA03000002000000000000000000F03F0000000000000040000000000000264000000000000008400000000000001040000000000000364001EA030000020000000000000000001440000000000000184000000000008040400000000000001C4000000000000020400000000000004640"}, // checkstyle.off: LineLength
      new String[]{"MULTILINESTRING Z ((-1 -2 -11,-3 -4 -22),(-5 -6 -33,-7 -8 -44))", "01ED0300000200000001EA03000002000000000000000000F0BF00000000000000C000000000000026C000000000000008C000000000000010C000000000000036C001EA0300000200000000000000000014C000000000000018C000000000008040C00000000000001CC000000000000020C000000000000046C0"}, // checkstyle.off: LineLength
      new String[]{"MULTILINESTRING Z ((1.5 2.5 11.5,3.5 4.5 22.5),(5.5 6.5 33.5,7.5 8.5 44.5))", "01ED0300000200000001EA03000002000000000000000000F83F000000000000044000000000000027400000000000000C400000000000001240000000000080364001EA0300000200000000000000000016400000000000001A400000000000C040400000000000001E4000000000000021400000000000404640"}, // checkstyle.off: LineLength
      new String[]{"MULTILINESTRING Z ((-1.5 -2.5 -11.5,-3.5 -4.5 -22.5),(-5.5 -6.5 -33.5,-7.5 -8.5 -44.5))", "01ED0300000200000001EA03000002000000000000000000F8BF00000000000004C000000000000027C00000000000000CC000000000000012C000000000008036C001EA0300000200000000000000000016C00000000000001AC00000000000C040C00000000000001EC000000000000021C000000000004046C0"}, // checkstyle.off: LineLength
      new String[]{"MULTILINESTRING Z ((0 0 0,0 0 0),(0 0 0,0 0 0),(0 0 0,0 0 0))", "01ED0300000300000001EA0300000200000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001EA0300000200000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001EA03000002000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"}, // checkstyle.off: LineLength
      new String[]{"MULTILINESTRING Z ((1 2 11,3 4 22),(5 6 33,7 8 44),(9 10 55,11 12 66))", "01ED0300000300000001EA03000002000000000000000000F03F0000000000000040000000000000264000000000000008400000000000001040000000000000364001EA030000020000000000000000001440000000000000184000000000008040400000000000001C400000000000002040000000000000464001EA03000002000000000000000000224000000000000024400000000000804B40000000000000264000000000000028400000000000805040"}, // checkstyle.off: LineLength
      new String[]{"MULTILINESTRING Z ((-1 -2 -11,-3 -4 -22),(-5 -6 -33,-7 -8 -44),(-9 -10 -55,-11 -12 -66))", "01ED0300000300000001EA03000002000000000000000000F0BF00000000000000C000000000000026C000000000000008C000000000000010C000000000000036C001EA0300000200000000000000000014C000000000000018C000000000008040C00000000000001CC000000000000020C000000000000046C001EA0300000200000000000000000022C000000000000024C00000000000804BC000000000000026C000000000000028C000000000008050C0"}, // checkstyle.off: LineLength
      new String[]{"MULTILINESTRING Z ((1.5 2.5 11.5,3.5 4.5 22.5),(5.5 6.5 33.5,7.5 8.5 44.5),(9.5 10.5 55.5,11.5 12.5 66.5))", "01ED0300000300000001EA03000002000000000000000000F83F000000000000044000000000000027400000000000000C400000000000001240000000000080364001EA0300000200000000000000000016400000000000001A400000000000C040400000000000001E400000000000002140000000000040464001EA03000002000000000000000000234000000000000025400000000000C04B40000000000000274000000000000029400000000000A05040"}, // checkstyle.off: LineLength
      new String[]{"MULTILINESTRING Z ((-1.5 -2.5 -11.5,-3.5 -4.5 -22.5),(-5.5 -6.5 -33.5,-7.5 -8.5 -44.5),(-9.5 -10.5 -55.5,-11.5 -12.5 -66.5))", "01ED0300000300000001EA03000002000000000000000000F8BF00000000000004C000000000000027C00000000000000CC000000000000012C000000000008036C001EA0300000200000000000000000016C00000000000001AC00000000000C040C00000000000001EC000000000000021C000000000004046C001EA0300000200000000000000000023C000000000000025C00000000000C04BC000000000000027C000000000000029C00000000000A050C0"}, // checkstyle.off: LineLength
      new String[]{"MULTILINESTRING M EMPTY", "01D507000000000000"}, // checkstyle.off: LineLength
      new String[]{"MULTILINESTRING M ((0 0 0,0 0 0))", "01D50700000100000001D207000002000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"}, // checkstyle.off: LineLength
      new String[]{"MULTILINESTRING M ((1 2 111,3 4 222))", "01D50700000100000001D207000002000000000000000000F03F00000000000000400000000000C05B40000000000000084000000000000010400000000000C06B40"}, // checkstyle.off: LineLength
      new String[]{"MULTILINESTRING M ((-1 -2 111,-3 -4 222))", "01D50700000100000001D207000002000000000000000000F0BF00000000000000C00000000000C05B4000000000000008C000000000000010C00000000000C06B40"}, // checkstyle.off: LineLength
      new String[]{"MULTILINESTRING M ((1.5 2.5 111.5,3.5 4.5 222.5))", "01D50700000100000001D207000002000000000000000000F83F00000000000004400000000000E05B400000000000000C4000000000000012400000000000D06B40"}, // checkstyle.off: LineLength
      new String[]{"MULTILINESTRING M ((-1.5 -2.5 111.5,-3.5 -4.5 222.5))", "01D50700000100000001D207000002000000000000000000F8BF00000000000004C00000000000E05B400000000000000CC000000000000012C00000000000D06B40"}, // checkstyle.off: LineLength
      new String[]{"MULTILINESTRING M ((0 0 0,0 0 0),(0 0 0,0 0 0))", "01D50700000200000001D20700000200000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001D207000002000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"}, // checkstyle.off: LineLength
      new String[]{"MULTILINESTRING M ((1 2 111,3 4 222),(5 6 333,7 8 444))", "01D50700000200000001D207000002000000000000000000F03F00000000000000400000000000C05B40000000000000084000000000000010400000000000C06B4001D207000002000000000000000000144000000000000018400000000000D074400000000000001C4000000000000020400000000000C07B40"}, // checkstyle.off: LineLength
      new String[]{"MULTILINESTRING M ((-1 -2 111,-3 -4 222),(-5 -6 333,-7 -8 444))", "01D50700000200000001D207000002000000000000000000F0BF00000000000000C00000000000C05B4000000000000008C000000000000010C00000000000C06B4001D20700000200000000000000000014C000000000000018C00000000000D074400000000000001CC000000000000020C00000000000C07B40"}, // checkstyle.off: LineLength
      new String[]{"MULTILINESTRING M ((1.5 2.5 111.5,3.5 4.5 222.5),(5.5 6.5 333.5,7.5 8.5 444.5))", "01D50700000200000001D207000002000000000000000000F83F00000000000004400000000000E05B400000000000000C4000000000000012400000000000D06B4001D20700000200000000000000000016400000000000001A400000000000D874400000000000001E4000000000000021400000000000C87B40"}, // checkstyle.off: LineLength
      new String[]{"MULTILINESTRING M ((-1.5 -2.5 111.5,-3.5 -4.5 222.5),(-5.5 -6.5 333.5,-7.5 -8.5 444.5))", "01D50700000200000001D207000002000000000000000000F8BF00000000000004C00000000000E05B400000000000000CC000000000000012C00000000000D06B4001D20700000200000000000000000016C00000000000001AC00000000000D874400000000000001EC000000000000021C00000000000C87B40"}, // checkstyle.off: LineLength
      new String[]{"MULTILINESTRING M ((0 0 0,0 0 0),(0 0 0,0 0 0),(0 0 0,0 0 0))", "01D50700000300000001D20700000200000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001D20700000200000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001D207000002000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"}, // checkstyle.off: LineLength
      new String[]{"MULTILINESTRING M ((1 2 111,3 4 222),(5 6 333,7 8 444),(9 10 555,11 12 666))", "01D50700000300000001D207000002000000000000000000F03F00000000000000400000000000C05B40000000000000084000000000000010400000000000C06B4001D207000002000000000000000000144000000000000018400000000000D074400000000000001C4000000000000020400000000000C07B4001D207000002000000000000000000224000000000000024400000000000588140000000000000264000000000000028400000000000D08440"}, // checkstyle.off: LineLength
      new String[]{"MULTILINESTRING M ((-1 -2 111,-3 -4 222),(-5 -6 333,-7 -8 444),(-9 -10 555,-11 -12 666))", "01D50700000300000001D207000002000000000000000000F0BF00000000000000C00000000000C05B4000000000000008C000000000000010C00000000000C06B4001D20700000200000000000000000014C000000000000018C00000000000D074400000000000001CC000000000000020C00000000000C07B4001D20700000200000000000000000022C000000000000024C0000000000058814000000000000026C000000000000028C00000000000D08440"}, // checkstyle.off: LineLength
      new String[]{"MULTILINESTRING M ((1.5 2.5 111.5,3.5 4.5 222.5),(5.5 6.5 333.5,7.5 8.5 444.5),(9.5 10.5 555.5,11.5 12.5 666.5))", "01D50700000300000001D207000002000000000000000000F83F00000000000004400000000000E05B400000000000000C4000000000000012400000000000D06B4001D20700000200000000000000000016400000000000001A400000000000D874400000000000001E4000000000000021400000000000C87B4001D2070000020000000000000000002340000000000000254000000000005C8140000000000000274000000000000029400000000000D48440"}, // checkstyle.off: LineLength
      new String[]{"MULTILINESTRING M ((-1.5 -2.5 111.5,-3.5 -4.5 222.5),(-5.5 -6.5 333.5,-7.5 -8.5 444.5),(-9.5 -10.5 555.5,-11.5 -12.5 666.5))", "01D50700000300000001D207000002000000000000000000F8BF00000000000004C00000000000E05B400000000000000CC000000000000012C00000000000D06B4001D20700000200000000000000000016C00000000000001AC00000000000D874400000000000001EC000000000000021C00000000000C87B4001D20700000200000000000000000023C000000000000025C000000000005C814000000000000027C000000000000029C00000000000D48440"}, // checkstyle.off: LineLength
      new String[]{"MULTILINESTRING ZM EMPTY", "01BD0B000000000000"}, // checkstyle.off: LineLength
      new String[]{"MULTILINESTRING ZM ((0 0 0 0,0 0 0 0))", "01BD0B00000100000001BA0B00000200000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"}, // checkstyle.off: LineLength
      new String[]{"MULTILINESTRING ZM ((1 2 11 111,3 4 22 222))", "01BD0B00000100000001BA0B000002000000000000000000F03F000000000000004000000000000026400000000000C05B400000000000000840000000000000104000000000000036400000000000C06B40"}, // checkstyle.off: LineLength
      new String[]{"MULTILINESTRING ZM ((-1 -2 -11 111,-3 -4 -22 222))", "01BD0B00000100000001BA0B000002000000000000000000F0BF00000000000000C000000000000026C00000000000C05B4000000000000008C000000000000010C000000000000036C00000000000C06B40"}, // checkstyle.off: LineLength
      new String[]{"MULTILINESTRING ZM ((1.5 2.5 11.5 111.5,3.5 4.5 22.5 222.5))", "01BD0B00000100000001BA0B000002000000000000000000F83F000000000000044000000000000027400000000000E05B400000000000000C40000000000000124000000000008036400000000000D06B40"}, // checkstyle.off: LineLength
      new String[]{"MULTILINESTRING ZM ((-1.5 -2.5 -11.5 111.5,-3.5 -4.5 -22.5 222.5))", "01BD0B00000100000001BA0B000002000000000000000000F8BF00000000000004C000000000000027C00000000000E05B400000000000000CC000000000000012C000000000008036C00000000000D06B40"}, // checkstyle.off: LineLength
      new String[]{"MULTILINESTRING ZM ((0 0 0 0,0 0 0 0),(0 0 0 0,0 0 0 0))", "01BD0B00000200000001BA0B0000020000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001BA0B00000200000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"}, // checkstyle.off: LineLength
      new String[]{"MULTILINESTRING ZM ((1 2 11 111,3 4 22 222),(5 6 33 333,7 8 44 444))", "01BD0B00000200000001BA0B000002000000000000000000F03F000000000000004000000000000026400000000000C05B400000000000000840000000000000104000000000000036400000000000C06B4001BA0B0000020000000000000000001440000000000000184000000000008040400000000000D074400000000000001C40000000000000204000000000000046400000000000C07B40"}, // checkstyle.off: LineLength
      new String[]{"MULTILINESTRING ZM ((-1 -2 -11 111,-3 -4 -22 222),(-5 -6 -33 333,-7 -8 -44 444))", "01BD0B00000200000001BA0B000002000000000000000000F0BF00000000000000C000000000000026C00000000000C05B4000000000000008C000000000000010C000000000000036C00000000000C06B4001BA0B00000200000000000000000014C000000000000018C000000000008040C00000000000D074400000000000001CC000000000000020C000000000000046C00000000000C07B40"}, // checkstyle.off: LineLength
      new String[]{"MULTILINESTRING ZM ((1.5 2.5 11.5 111.5,3.5 4.5 22.5 222.5),(5.5 6.5 33.5 333.5,7.5 8.5 44.5 444.5))", "01BD0B00000200000001BA0B000002000000000000000000F83F000000000000044000000000000027400000000000E05B400000000000000C40000000000000124000000000008036400000000000D06B4001BA0B00000200000000000000000016400000000000001A400000000000C040400000000000D874400000000000001E40000000000000214000000000004046400000000000C87B40"}, // checkstyle.off: LineLength
      new String[]{"MULTILINESTRING ZM ((-1.5 -2.5 -11.5 111.5,-3.5 -4.5 -22.5 222.5),(-5.5 -6.5 -33.5 333.5,-7.5 -8.5 -44.5 444.5))", "01BD0B00000200000001BA0B000002000000000000000000F8BF00000000000004C000000000000027C00000000000E05B400000000000000CC000000000000012C000000000008036C00000000000D06B4001BA0B00000200000000000000000016C00000000000001AC00000000000C040C00000000000D874400000000000001EC000000000000021C000000000004046C00000000000C87B40"}, // checkstyle.off: LineLength
      new String[]{"MULTILINESTRING ZM ((0 0 0 0,0 0 0 0),(0 0 0 0,0 0 0 0),(0 0 0 0,0 0 0 0))", "01BD0B00000300000001BA0B0000020000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001BA0B0000020000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001BA0B00000200000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"}, // checkstyle.off: LineLength
      new String[]{"MULTILINESTRING ZM ((1 2 11 111,3 4 22 222),(5 6 33 333,7 8 44 444),(9 10 55 555,11 12 66 666))", "01BD0B00000300000001BA0B000002000000000000000000F03F000000000000004000000000000026400000000000C05B400000000000000840000000000000104000000000000036400000000000C06B4001BA0B0000020000000000000000001440000000000000184000000000008040400000000000D074400000000000001C40000000000000204000000000000046400000000000C07B4001BA0B000002000000000000000000224000000000000024400000000000804B4000000000005881400000000000002640000000000000284000000000008050400000000000D08440"}, // checkstyle.off: LineLength
      new String[]{"MULTILINESTRING ZM ((-1 -2 -11 111,-3 -4 -22 222),(-5 -6 -33 333,-7 -8 -44 444),(-9 -10 -55 555,-11 -12 -66 666))", "01BD0B00000300000001BA0B000002000000000000000000F0BF00000000000000C000000000000026C00000000000C05B4000000000000008C000000000000010C000000000000036C00000000000C06B4001BA0B00000200000000000000000014C000000000000018C000000000008040C00000000000D074400000000000001CC000000000000020C000000000000046C00000000000C07B4001BA0B00000200000000000000000022C000000000000024C00000000000804BC0000000000058814000000000000026C000000000000028C000000000008050C00000000000D08440"}, // checkstyle.off: LineLength
      new String[]{"MULTILINESTRING ZM ((1.5 2.5 11.5 111.5,3.5 4.5 22.5 222.5),(5.5 6.5 33.5 333.5,7.5 8.5 44.5 444.5),(9.5 10.5 55.5 555.5,11.5 12.5 66.5 666.5))", "01BD0B00000300000001BA0B000002000000000000000000F83F000000000000044000000000000027400000000000E05B400000000000000C40000000000000124000000000008036400000000000D06B4001BA0B00000200000000000000000016400000000000001A400000000000C040400000000000D874400000000000001E40000000000000214000000000004046400000000000C87B4001BA0B000002000000000000000000234000000000000025400000000000C04B4000000000005C8140000000000000274000000000000029400000000000A050400000000000D48440"}, // checkstyle.off: LineLength
      new String[]{"MULTILINESTRING ZM ((-1.5 -2.5 -11.5 111.5,-3.5 -4.5 -22.5 222.5),(-5.5 -6.5 -33.5 333.5,-7.5 -8.5 -44.5 444.5),(-9.5 -10.5 -55.5 555.5,-11.5 -12.5 -66.5 666.5))", "01BD0B00000300000001BA0B000002000000000000000000F8BF00000000000004C000000000000027C00000000000E05B400000000000000CC000000000000012C000000000008036C00000000000D06B4001BA0B00000200000000000000000016C00000000000001AC00000000000C040C00000000000D874400000000000001EC000000000000021C000000000004046C00000000000C87B4001BA0B00000200000000000000000023C000000000000025C00000000000C04BC000000000005C814000000000000027C000000000000029C00000000000A050C00000000000D48440"}, // checkstyle.off: LineLength
      new String[]{"MULTILINESTRING(EMPTY)", "010500000001000000010200000000000000"}, // checkstyle.off: LineLength
      new String[]{"MULTILINESTRING(EMPTY,EMPTY)", "010500000002000000010200000000000000010200000000000000"}, // checkstyle.off: LineLength
      new String[]{"MULTILINESTRING(EMPTY,(1 2,3 4))", "010500000002000000010200000000000000010200000002000000000000000000F03F000000000000004000000000000008400000000000001040"}, // checkstyle.off: LineLength
      new String[]{"MULTILINESTRING((1 2,3 4),EMPTY)", "010500000002000000010200000002000000000000000000F03F000000000000004000000000000008400000000000001040010200000000000000"}, // checkstyle.off: LineLength
      new String[]{"MULTILINESTRING(EMPTY,(1 2,3 4),EMPTY)", "010500000003000000010200000000000000010200000002000000000000000000F03F000000000000004000000000000008400000000000001040010200000000000000"}, // checkstyle.off: LineLength
      new String[]{"MULTILINESTRING((1 2,3 4),EMPTY,(5 6,7 8))", "010500000003000000010200000002000000000000000000F03F000000000000004000000000000008400000000000001040010200000000000000010200000002000000000000000000144000000000000018400000000000001C400000000000002040"}, // checkstyle.off: LineLength
      new String[]{"MULTILINESTRING(EMPTY,EMPTY,(1 2,3 4))", "010500000003000000010200000000000000010200000000000000010200000002000000000000000000F03F000000000000004000000000000008400000000000001040"}, // checkstyle.off: LineLength
      new String[]{"MULTILINESTRING((1 2,3 4),EMPTY,EMPTY)", "010500000003000000010200000002000000000000000000F03F000000000000004000000000000008400000000000001040010200000000000000010200000000000000"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOLYGON EMPTY", "010600000000000000"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOLYGON(((0 0,10 0,5 10,0 0)))", "0106000000010000000103000000010000000400000000000000000000000000000000000000000000000000244000000000000000000000000000001440000000000000244000000000000000000000000000000000"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOLYGON(((1 2,11 2,6 12,1 2)))", "01060000000100000001030000000100000004000000000000000000F03F00000000000000400000000000002640000000000000004000000000000018400000000000002840000000000000F03F0000000000000040"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOLYGON(((-1 -2,-11 -2,-6 -12,-1 -2)))", "01060000000100000001030000000100000004000000000000000000F0BF00000000000000C000000000000026C000000000000000C000000000000018C000000000000028C0000000000000F0BF00000000000000C0"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOLYGON(((1.5 2.5,11.5 2.5,6.5 12.5,1.5 2.5)))", "01060000000100000001030000000100000004000000000000000000F83F0000000000000440000000000000274000000000000004400000000000001A400000000000002940000000000000F83F0000000000000440"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOLYGON(((-1.5 -2.5,-11.5 -2.5,-6.5 -12.5,-1.5 -2.5)))", "01060000000100000001030000000100000004000000000000000000F8BF00000000000004C000000000000027C000000000000004C00000000000001AC000000000000029C0000000000000F8BF00000000000004C0"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOLYGON(((0 0,10 0,5 10,0 0)),((20 0,30 0,25 10,20 0)))", "010600000002000000010300000001000000040000000000000000000000000000000000000000000000000024400000000000000000000000000000144000000000000024400000000000000000000000000000000001030000000100000004000000000000000000344000000000000000000000000000003E4000000000000000000000000000003940000000000000244000000000000034400000000000000000"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOLYGON(((1 2,11 2,6 12,1 2)),((21 2,31 2,26 12,21 2)))", "01060000000200000001030000000100000004000000000000000000F03F00000000000000400000000000002640000000000000004000000000000018400000000000002840000000000000F03F000000000000004001030000000100000004000000000000000000354000000000000000400000000000003F4000000000000000400000000000003A40000000000000284000000000000035400000000000000040"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOLYGON(((-1 -2,-11 -2,-6 -12,-1 -2)),((-21 -2,-31 -2,-26 -12,-21 -2)))", "01060000000200000001030000000100000004000000000000000000F0BF00000000000000C000000000000026C000000000000000C000000000000018C000000000000028C0000000000000F0BF00000000000000C00103000000010000000400000000000000000035C000000000000000C00000000000003FC000000000000000C00000000000003AC000000000000028C000000000000035C000000000000000C0"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOLYGON(((1.5 2.5,11.5 2.5,6.5 12.5,1.5 2.5)),((21.5 2.5,31.5 2.5,26.5 12.5,21.5 2.5)))", "01060000000200000001030000000100000004000000000000000000F83F0000000000000440000000000000274000000000000004400000000000001A400000000000002940000000000000F83F000000000000044001030000000100000004000000000000000080354000000000000004400000000000803F4000000000000004400000000000803A40000000000000294000000000008035400000000000000440"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOLYGON(((-1.5 -2.5,-11.5 -2.5,-6.5 -12.5,-1.5 -2.5)),((-21.5 -2.5,-31.5 -2.5,-26.5 -12.5,-21.5 -2.5)))", "01060000000200000001030000000100000004000000000000000000F8BF00000000000004C000000000000027C000000000000004C00000000000001AC000000000000029C0000000000000F8BF00000000000004C00103000000010000000400000000000000008035C000000000000004C00000000000803FC000000000000004C00000000000803AC000000000000029C000000000008035C000000000000004C0"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOLYGON(((0 0,10 0,5 10,0 0)),((20 0,30 0,25 10,20 0)),((40 0,50 0,45 10,40 0)))", "010600000003000000010300000001000000040000000000000000000000000000000000000000000000000024400000000000000000000000000000144000000000000024400000000000000000000000000000000001030000000100000004000000000000000000344000000000000000000000000000003E40000000000000000000000000000039400000000000002440000000000000344000000000000000000103000000010000000400000000000000000044400000000000000000000000000000494000000000000000000000000000804640000000000000244000000000000044400000000000000000"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOLYGON(((1 2,11 2,6 12,1 2)),((21 2,31 2,26 12,21 2)),((41 2,51 2,46 12,41 2)))", "01060000000300000001030000000100000004000000000000000000F03F00000000000000400000000000002640000000000000004000000000000018400000000000002840000000000000F03F000000000000004001030000000100000004000000000000000000354000000000000000400000000000003F4000000000000000400000000000003A400000000000002840000000000000354000000000000000400103000000010000000400000000000000008044400000000000000040000000000080494000000000000000400000000000004740000000000000284000000000008044400000000000000040"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOLYGON(((-1 -2,-11 -2,-6 -12,-1 -2)),((-21 -2,-31 -2,-26 -12,-21 -2)),((-41 -2,-51 -2,-46 -12,-41 -2)))", "01060000000300000001030000000100000004000000000000000000F0BF00000000000000C000000000000026C000000000000000C000000000000018C000000000000028C0000000000000F0BF00000000000000C00103000000010000000400000000000000000035C000000000000000C00000000000003FC000000000000000C00000000000003AC000000000000028C000000000000035C000000000000000C00103000000010000000400000000000000008044C000000000000000C000000000008049C000000000000000C000000000000047C000000000000028C000000000008044C000000000000000C0"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOLYGON(((1.5 2.5,11.5 2.5,6.5 12.5,1.5 2.5)),((21.5 2.5,31.5 2.5,26.5 12.5,21.5 2.5)),((41.5 2.5,51.5 2.5,46.5 12.5,41.5 2.5)))", "01060000000300000001030000000100000004000000000000000000F83F0000000000000440000000000000274000000000000004400000000000001A400000000000002940000000000000F83F000000000000044001030000000100000004000000000000000080354000000000000004400000000000803F4000000000000004400000000000803A40000000000000294000000000008035400000000000000440010300000001000000040000000000000000C0444000000000000004400000000000C049400000000000000440000000000040474000000000000029400000000000C044400000000000000440"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOLYGON(((-1.5 -2.5,-11.5 -2.5,-6.5 -12.5,-1.5 -2.5)),((-21.5 -2.5,-31.5 -2.5,-26.5 -12.5,-21.5 -2.5)),((-41.5 -2.5,-51.5 -2.5,-46.5 -12.5,-41.5 -2.5)))", "01060000000300000001030000000100000004000000000000000000F8BF00000000000004C000000000000027C000000000000004C00000000000001AC000000000000029C0000000000000F8BF00000000000004C00103000000010000000400000000000000008035C000000000000004C00000000000803FC000000000000004C00000000000803AC000000000000029C000000000008035C000000000000004C0010300000001000000040000000000000000C044C000000000000004C00000000000C049C000000000000004C000000000004047C000000000000029C00000000000C044C000000000000004C0"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOLYGON Z EMPTY", "01EE03000000000000"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOLYGON Z (((0 0 0,10 0 0,5 10 0,0 0 0)))", "01EE0300000100000001EB0300000100000004000000000000000000000000000000000000000000000000000000000000000000244000000000000000000000000000000000000000000000144000000000000024400000000000000000000000000000000000000000000000000000000000000000"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOLYGON Z (((1 2 11,11 2 22,6 12 33,1 2 11)))", "01EE0300000100000001EB0300000100000004000000000000000000F03F00000000000000400000000000002640000000000000264000000000000000400000000000003640000000000000184000000000000028400000000000804040000000000000F03F00000000000000400000000000002640"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOLYGON Z (((-1 -2 -11,-11 -2 -22,-6 -12 -33,-1 -2 -11)))", "01EE0300000100000001EB0300000100000004000000000000000000F0BF00000000000000C000000000000026C000000000000026C000000000000000C000000000000036C000000000000018C000000000000028C000000000008040C0000000000000F0BF00000000000000C000000000000026C0"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOLYGON Z (((1.5 2.5 11.5,11.5 2.5 22.5,6.5 12.5 33.5,1.5 2.5 11.5)))", "01EE0300000100000001EB0300000100000004000000000000000000F83F000000000000044000000000000027400000000000002740000000000000044000000000008036400000000000001A4000000000000029400000000000C04040000000000000F83F00000000000004400000000000002740"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOLYGON Z (((-1.5 -2.5 -11.5,-11.5 -2.5 -22.5,-6.5 -12.5 -33.5,-1.5 -2.5 -11.5)))", "01EE0300000100000001EB0300000100000004000000000000000000F8BF00000000000004C000000000000027C000000000000027C000000000000004C000000000008036C00000000000001AC000000000000029C00000000000C040C0000000000000F8BF00000000000004C000000000000027C0"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOLYGON Z (((0 0 0,10 0 0,5 10 0,0 0 0)),((20 0 0,30 0 0,25 10 0,20 0 0)))", "01EE0300000200000001EB030000010000000400000000000000000000000000000000000000000000000000000000000000000024400000000000000000000000000000000000000000000014400000000000002440000000000000000000000000000000000000000000000000000000000000000001EB03000001000000040000000000000000003440000000000000000000000000000000000000000000003E4000000000000000000000000000000000000000000000394000000000000024400000000000000000000000000000344000000000000000000000000000000000"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOLYGON Z (((1 2 11,11 2 22,6 12 33,1 2 11)),((21 2 44,31 2 55,26 12 66,21 2 44)))", "01EE0300000200000001EB0300000100000004000000000000000000F03F00000000000000400000000000002640000000000000264000000000000000400000000000003640000000000000184000000000000028400000000000804040000000000000F03F0000000000000040000000000000264001EB03000001000000040000000000000000003540000000000000004000000000000046400000000000003F4000000000000000400000000000804B400000000000003A4000000000000028400000000000805040000000000000354000000000000000400000000000004640"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOLYGON Z (((-1 -2 -11,-11 -2 -22,-6 -12 -33,-1 -2 -11)),((-21 -2 -44,-31 -2 -55,-26 -12 -66,-21 -2 -44)))", "01EE0300000200000001EB0300000100000004000000000000000000F0BF00000000000000C000000000000026C000000000000026C000000000000000C000000000000036C000000000000018C000000000000028C000000000008040C0000000000000F0BF00000000000000C000000000000026C001EB030000010000000400000000000000000035C000000000000000C000000000000046C00000000000003FC000000000000000C00000000000804BC00000000000003AC000000000000028C000000000008050C000000000000035C000000000000000C000000000000046C0"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOLYGON Z (((1.5 2.5 11.5,11.5 2.5 22.5,6.5 12.5 33.5,1.5 2.5 11.5)),((21.5 2.5 44.5,31.5 2.5 55.5,26.5 12.5 66.5,21.5 2.5 44.5)))", "01EE0300000200000001EB0300000100000004000000000000000000F83F000000000000044000000000000027400000000000002740000000000000044000000000008036400000000000001A4000000000000029400000000000C04040000000000000F83F0000000000000440000000000000274001EB03000001000000040000000000000000803540000000000000044000000000004046400000000000803F4000000000000004400000000000C04B400000000000803A4000000000000029400000000000A05040000000000080354000000000000004400000000000404640"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOLYGON Z (((-1.5 -2.5 -11.5,-11.5 -2.5 -22.5,-6.5 -12.5 -33.5,-1.5 -2.5 -11.5)),((-21.5 -2.5 -44.5,-31.5 -2.5 -55.5,-26.5 -12.5 -66.5,-21.5 -2.5 -44.5)))", "01EE0300000200000001EB0300000100000004000000000000000000F8BF00000000000004C000000000000027C000000000000027C000000000000004C000000000008036C00000000000001AC000000000000029C00000000000C040C0000000000000F8BF00000000000004C000000000000027C001EB030000010000000400000000000000008035C000000000000004C000000000004046C00000000000803FC000000000000004C00000000000C04BC00000000000803AC000000000000029C00000000000A050C000000000008035C000000000000004C000000000004046C0"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOLYGON Z (((0 0 0,10 0 0,5 10 0,0 0 0)),((20 0 0,30 0 0,25 10 0,20 0 0)),((40 0 0,50 0 0,45 10 0,40 0 0)))", "01EE0300000300000001EB030000010000000400000000000000000000000000000000000000000000000000000000000000000024400000000000000000000000000000000000000000000014400000000000002440000000000000000000000000000000000000000000000000000000000000000001EB03000001000000040000000000000000003440000000000000000000000000000000000000000000003E400000000000000000000000000000000000000000000039400000000000002440000000000000000000000000000034400000000000000000000000000000000001EB0300000100000004000000000000000000444000000000000000000000000000000000000000000000494000000000000000000000000000000000000000000080464000000000000024400000000000000000000000000000444000000000000000000000000000000000"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOLYGON Z (((1 2 11,11 2 22,6 12 33,1 2 11)),((21 2 44,31 2 55,26 12 66,21 2 44)),((41 2 77,51 2 88,46 12 99,41 2 77)))", "01EE0300000300000001EB0300000100000004000000000000000000F03F00000000000000400000000000002640000000000000264000000000000000400000000000003640000000000000184000000000000028400000000000804040000000000000F03F0000000000000040000000000000264001EB03000001000000040000000000000000003540000000000000004000000000000046400000000000003F4000000000000000400000000000804B400000000000003A400000000000002840000000000080504000000000000035400000000000000040000000000000464001EB0300000100000004000000000000000080444000000000000000400000000000405340000000000080494000000000000000400000000000005640000000000000474000000000000028400000000000C05840000000000080444000000000000000400000000000405340"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOLYGON Z (((-1 -2 -11,-11 -2 -22,-6 -12 -33,-1 -2 -11)),((-21 -2 -44,-31 -2 -55,-26 -12 -66,-21 -2 -44)),((-41 -2 -77,-51 -2 -88,-46 -12 -99,-41 -2 -77)))", "01EE0300000300000001EB0300000100000004000000000000000000F0BF00000000000000C000000000000026C000000000000026C000000000000000C000000000000036C000000000000018C000000000000028C000000000008040C0000000000000F0BF00000000000000C000000000000026C001EB030000010000000400000000000000000035C000000000000000C000000000000046C00000000000003FC000000000000000C00000000000804BC00000000000003AC000000000000028C000000000008050C000000000000035C000000000000000C000000000000046C001EB030000010000000400000000000000008044C000000000000000C000000000004053C000000000008049C000000000000000C000000000000056C000000000000047C000000000000028C00000000000C058C000000000008044C000000000000000C000000000004053C0"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOLYGON Z (((1.5 2.5 11.5,11.5 2.5 22.5,6.5 12.5 33.5,1.5 2.5 11.5)),((21.5 2.5 44.5,31.5 2.5 55.5,26.5 12.5 66.5,21.5 2.5 44.5)),((41.5 2.5 77.5,51.5 2.5 88.5,46.5 12.5 99.5,41.5 2.5 77.5)))", "01EE0300000300000001EB0300000100000004000000000000000000F83F000000000000044000000000000027400000000000002740000000000000044000000000008036400000000000001A4000000000000029400000000000C04040000000000000F83F0000000000000440000000000000274001EB03000001000000040000000000000000803540000000000000044000000000004046400000000000803F4000000000000004400000000000C04B400000000000803A4000000000000029400000000000A0504000000000008035400000000000000440000000000040464001EB03000001000000040000000000000000C04440000000000000044000000000006053400000000000C0494000000000000004400000000000205640000000000040474000000000000029400000000000E058400000000000C0444000000000000004400000000000605340"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOLYGON Z (((-1.5 -2.5 -11.5,-11.5 -2.5 -22.5,-6.5 -12.5 -33.5,-1.5 -2.5 -11.5)),((-21.5 -2.5 -44.5,-31.5 -2.5 -55.5,-26.5 -12.5 -66.5,-21.5 -2.5 -44.5)),((-41.5 -2.5 -77.5,-51.5 -2.5 -88.5,-46.5 -12.5 -99.5,-41.5 -2.5 -77.5)))", "01EE0300000300000001EB0300000100000004000000000000000000F8BF00000000000004C000000000000027C000000000000027C000000000000004C000000000008036C00000000000001AC000000000000029C00000000000C040C0000000000000F8BF00000000000004C000000000000027C001EB030000010000000400000000000000008035C000000000000004C000000000004046C00000000000803FC000000000000004C00000000000C04BC00000000000803AC000000000000029C00000000000A050C000000000008035C000000000000004C000000000004046C001EB03000001000000040000000000000000C044C000000000000004C000000000006053C00000000000C049C000000000000004C000000000002056C000000000004047C000000000000029C00000000000E058C00000000000C044C000000000000004C000000000006053C0"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOLYGON M EMPTY", "01D607000000000000"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOLYGON M (((0 0 0,10 0 0,5 10 0,0 0 0)))", "01D60700000100000001D30700000100000004000000000000000000000000000000000000000000000000000000000000000000244000000000000000000000000000000000000000000000144000000000000024400000000000000000000000000000000000000000000000000000000000000000"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOLYGON M (((1 2 111,11 2 222,6 12 333,1 2 111)))", "01D60700000100000001D30700000100000004000000000000000000F03F00000000000000400000000000C05B40000000000000264000000000000000400000000000C06B40000000000000184000000000000028400000000000D07440000000000000F03F00000000000000400000000000C05B40"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOLYGON M (((-1 -2 111,-11 -2 222,-6 -12 333,-1 -2 111)))", "01D60700000100000001D30700000100000004000000000000000000F0BF00000000000000C00000000000C05B4000000000000026C000000000000000C00000000000C06B4000000000000018C000000000000028C00000000000D07440000000000000F0BF00000000000000C00000000000C05B40"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOLYGON M (((1.5 2.5 111.5,11.5 2.5 222.5,6.5 12.5 333.5,1.5 2.5 111.5)))", "01D60700000100000001D30700000100000004000000000000000000F83F00000000000004400000000000E05B40000000000000274000000000000004400000000000D06B400000000000001A4000000000000029400000000000D87440000000000000F83F00000000000004400000000000E05B40"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOLYGON M (((-1.5 -2.5 111.5,-11.5 -2.5 222.5,-6.5 -12.5 333.5,-1.5 -2.5 111.5)))", "01D60700000100000001D30700000100000004000000000000000000F8BF00000000000004C00000000000E05B4000000000000027C000000000000004C00000000000D06B400000000000001AC000000000000029C00000000000D87440000000000000F8BF00000000000004C00000000000E05B40"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOLYGON M (((0 0 0,10 0 0,5 10 0,0 0 0)),((20 0 0,30 0 0,25 10 0,20 0 0)))", "01D60700000200000001D3070000010000000400000000000000000000000000000000000000000000000000000000000000000024400000000000000000000000000000000000000000000014400000000000002440000000000000000000000000000000000000000000000000000000000000000001D307000001000000040000000000000000003440000000000000000000000000000000000000000000003E4000000000000000000000000000000000000000000000394000000000000024400000000000000000000000000000344000000000000000000000000000000000"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOLYGON M (((1 2 111,11 2 222,6 12 333,1 2 111)),((21 2 444,31 2 555,26 12 666,21 2 444)))", "01D60700000200000001D30700000100000004000000000000000000F03F00000000000000400000000000C05B40000000000000264000000000000000400000000000C06B40000000000000184000000000000028400000000000D07440000000000000F03F00000000000000400000000000C05B4001D30700000100000004000000000000000000354000000000000000400000000000C07B400000000000003F40000000000000004000000000005881400000000000003A4000000000000028400000000000D08440000000000000354000000000000000400000000000C07B40"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOLYGON M (((-1 -2 111,-11 -2 222,-6 -12 333,-1 -2 111)),((-21 -2 444,-31 -2 555,-26 -12 666,-21 -2 444)))", "01D60700000200000001D30700000100000004000000000000000000F0BF00000000000000C00000000000C05B4000000000000026C000000000000000C00000000000C06B4000000000000018C000000000000028C00000000000D07440000000000000F0BF00000000000000C00000000000C05B4001D3070000010000000400000000000000000035C000000000000000C00000000000C07B400000000000003FC000000000000000C000000000005881400000000000003AC000000000000028C00000000000D0844000000000000035C000000000000000C00000000000C07B40"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOLYGON M (((1.5 2.5 111.5,11.5 2.5 222.5,6.5 12.5 333.5,1.5 2.5 111.5)),((21.5 2.5 444.5,31.5 2.5 555.5,26.5 12.5 666.5,21.5 2.5 444.5)))", "01D60700000200000001D30700000100000004000000000000000000F83F00000000000004400000000000E05B40000000000000274000000000000004400000000000D06B400000000000001A4000000000000029400000000000D87440000000000000F83F00000000000004400000000000E05B4001D30700000100000004000000000000000080354000000000000004400000000000C87B400000000000803F40000000000000044000000000005C81400000000000803A4000000000000029400000000000D48440000000000080354000000000000004400000000000C87B40"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOLYGON M (((-1.5 -2.5 111.5,-11.5 -2.5 222.5,-6.5 -12.5 333.5,-1.5 -2.5 111.5)),((-21.5 -2.5 444.5,-31.5 -2.5 555.5,-26.5 -12.5 666.5,-21.5 -2.5 444.5)))", "01D60700000200000001D30700000100000004000000000000000000F8BF00000000000004C00000000000E05B4000000000000027C000000000000004C00000000000D06B400000000000001AC000000000000029C00000000000D87440000000000000F8BF00000000000004C00000000000E05B4001D3070000010000000400000000000000008035C000000000000004C00000000000C87B400000000000803FC000000000000004C000000000005C81400000000000803AC000000000000029C00000000000D4844000000000008035C000000000000004C00000000000C87B40"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOLYGON M (((0 0 0,10 0 0,5 10 0,0 0 0)),((20 0 0,30 0 0,25 10 0,20 0 0)),((40 0 0,50 0 0,45 10 0,40 0 0)))", "01D60700000300000001D3070000010000000400000000000000000000000000000000000000000000000000000000000000000024400000000000000000000000000000000000000000000014400000000000002440000000000000000000000000000000000000000000000000000000000000000001D307000001000000040000000000000000003440000000000000000000000000000000000000000000003E400000000000000000000000000000000000000000000039400000000000002440000000000000000000000000000034400000000000000000000000000000000001D30700000100000004000000000000000000444000000000000000000000000000000000000000000000494000000000000000000000000000000000000000000080464000000000000024400000000000000000000000000000444000000000000000000000000000000000"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOLYGON M (((1 2 111,11 2 222,6 12 333,1 2 111)),((21 2 444,31 2 555,26 12 666,21 2 444)),((41 2 777,51 2 888,46 12 999,41 2 777)))", "01D60700000300000001D30700000100000004000000000000000000F03F00000000000000400000000000C05B40000000000000264000000000000000400000000000C06B40000000000000184000000000000028400000000000D07440000000000000F03F00000000000000400000000000C05B4001D30700000100000004000000000000000000354000000000000000400000000000C07B400000000000003F40000000000000004000000000005881400000000000003A4000000000000028400000000000D08440000000000000354000000000000000400000000000C07B4001D30700000100000004000000000000000080444000000000000000400000000000488840000000000080494000000000000000400000000000C08B40000000000000474000000000000028400000000000388F40000000000080444000000000000000400000000000488840"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOLYGON M (((-1 -2 111,-11 -2 222,-6 -12 333,-1 -2 111)),((-21 -2 444,-31 -2 555,-26 -12 666,-21 -2 444)),((-41 -2 777,-51 -2 888,-46 -12 999,-41 -2 777)))", "01D60700000300000001D30700000100000004000000000000000000F0BF00000000000000C00000000000C05B4000000000000026C000000000000000C00000000000C06B4000000000000018C000000000000028C00000000000D07440000000000000F0BF00000000000000C00000000000C05B4001D3070000010000000400000000000000000035C000000000000000C00000000000C07B400000000000003FC000000000000000C000000000005881400000000000003AC000000000000028C00000000000D0844000000000000035C000000000000000C00000000000C07B4001D3070000010000000400000000000000008044C000000000000000C0000000000048884000000000008049C000000000000000C00000000000C08B4000000000000047C000000000000028C00000000000388F4000000000008044C000000000000000C00000000000488840"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOLYGON M (((1.5 2.5 111.5,11.5 2.5 222.5,6.5 12.5 333.5,1.5 2.5 111.5)),((21.5 2.5 444.5,31.5 2.5 555.5,26.5 12.5 666.5,21.5 2.5 444.5)),((41.5 2.5 777.5,51.5 2.5 888.5,46.5 12.5 999.5,41.5 2.5 777.5)))", "01D60700000300000001D30700000100000004000000000000000000F83F00000000000004400000000000E05B40000000000000274000000000000004400000000000D06B400000000000001A4000000000000029400000000000D87440000000000000F83F00000000000004400000000000E05B4001D30700000100000004000000000000000080354000000000000004400000000000C87B400000000000803F40000000000000044000000000005C81400000000000803A4000000000000029400000000000D48440000000000080354000000000000004400000000000C87B4001D307000001000000040000000000000000C04440000000000000044000000000004C88400000000000C0494000000000000004400000000000C48B400000000000404740000000000000294000000000003C8F400000000000C04440000000000000044000000000004C8840"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOLYGON M (((-1.5 -2.5 111.5,-11.5 -2.5 222.5,-6.5 -12.5 333.5,-1.5 -2.5 111.5)),((-21.5 -2.5 444.5,-31.5 -2.5 555.5,-26.5 -12.5 666.5,-21.5 -2.5 444.5)),((-41.5 -2.5 777.5,-51.5 -2.5 888.5,-46.5 -12.5 999.5,-41.5 -2.5 777.5)))", "01D60700000300000001D30700000100000004000000000000000000F8BF00000000000004C00000000000E05B4000000000000027C000000000000004C00000000000D06B400000000000001AC000000000000029C00000000000D87440000000000000F8BF00000000000004C00000000000E05B4001D3070000010000000400000000000000008035C000000000000004C00000000000C87B400000000000803FC000000000000004C000000000005C81400000000000803AC000000000000029C00000000000D4844000000000008035C000000000000004C00000000000C87B4001D307000001000000040000000000000000C044C000000000000004C000000000004C88400000000000C049C000000000000004C00000000000C48B4000000000004047C000000000000029C000000000003C8F400000000000C044C000000000000004C000000000004C8840"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOLYGON ZM EMPTY", "01BE0B000000000000"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOLYGON ZM (((0 0 0 0,10 0 0 0,5 10 0 0,0 0 0 0)))", "01BE0B00000100000001BB0B000001000000040000000000000000000000000000000000000000000000000000000000000000000000000000000000244000000000000000000000000000000000000000000000000000000000000014400000000000002440000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOLYGON ZM (((1 2 11 111,11 2 22 222,6 12 33 333,1 2 11 111)))", "01BE0B00000100000001BB0B00000100000004000000000000000000F03F000000000000004000000000000026400000000000C05B400000000000002640000000000000004000000000000036400000000000C06B400000000000001840000000000000284000000000008040400000000000D07440000000000000F03F000000000000004000000000000026400000000000C05B40"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOLYGON ZM (((-1 -2 -11 111,-11 -2 -22 222,-6 -12 -33 333,-1 -2 -11 111)))", "01BE0B00000100000001BB0B00000100000004000000000000000000F0BF00000000000000C000000000000026C00000000000C05B4000000000000026C000000000000000C000000000000036C00000000000C06B4000000000000018C000000000000028C000000000008040C00000000000D07440000000000000F0BF00000000000000C000000000000026C00000000000C05B40"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOLYGON ZM (((1.5 2.5 11.5 111.5,11.5 2.5 22.5 222.5,6.5 12.5 33.5 333.5,1.5 2.5 11.5 111.5)))", "01BE0B00000100000001BB0B00000100000004000000000000000000F83F000000000000044000000000000027400000000000E05B400000000000002740000000000000044000000000008036400000000000D06B400000000000001A4000000000000029400000000000C040400000000000D87440000000000000F83F000000000000044000000000000027400000000000E05B40"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOLYGON ZM (((-1.5 -2.5 -11.5 111.5,-11.5 -2.5 -22.5 222.5,-6.5 -12.5 -33.5 333.5,-1.5 -2.5 -11.5 111.5)))", "01BE0B00000100000001BB0B00000100000004000000000000000000F8BF00000000000004C000000000000027C00000000000E05B4000000000000027C000000000000004C000000000008036C00000000000D06B400000000000001AC000000000000029C00000000000C040C00000000000D87440000000000000F8BF00000000000004C000000000000027C00000000000E05B40"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOLYGON ZM (((0 0 0 0,10 0 0 0,5 10 0 0,0 0 0 0)),((20 0 0 0,30 0 0 0,25 10 0 0,20 0 0 0)))", "01BE0B00000200000001BB0B00000100000004000000000000000000000000000000000000000000000000000000000000000000000000000000000024400000000000000000000000000000000000000000000000000000000000001440000000000000244000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001BB0B0000010000000400000000000000000034400000000000000000000000000000000000000000000000000000000000003E4000000000000000000000000000000000000000000000000000000000000039400000000000002440000000000000000000000000000000000000000000003440000000000000000000000000000000000000000000000000"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOLYGON ZM (((1 2 11 111,11 2 22 222,6 12 33 333,1 2 11 111)),((21 2 44 444,31 2 55 555,26 12 66 666,21 2 44 444)))", "01BE0B00000200000001BB0B00000100000004000000000000000000F03F000000000000004000000000000026400000000000C05B400000000000002640000000000000004000000000000036400000000000C06B400000000000001840000000000000284000000000008040400000000000D07440000000000000F03F000000000000004000000000000026400000000000C05B4001BB0B000001000000040000000000000000003540000000000000004000000000000046400000000000C07B400000000000003F4000000000000000400000000000804B4000000000005881400000000000003A40000000000000284000000000008050400000000000D084400000000000003540000000000000004000000000000046400000000000C07B40"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOLYGON ZM (((-1 -2 -11 111,-11 -2 -22 222,-6 -12 -33 333,-1 -2 -11 111)),((-21 -2 -44 444,-31 -2 -55 555,-26 -12 -66 666,-21 -2 -44 444)))", "01BE0B00000200000001BB0B00000100000004000000000000000000F0BF00000000000000C000000000000026C00000000000C05B4000000000000026C000000000000000C000000000000036C00000000000C06B4000000000000018C000000000000028C000000000008040C00000000000D07440000000000000F0BF00000000000000C000000000000026C00000000000C05B4001BB0B0000010000000400000000000000000035C000000000000000C000000000000046C00000000000C07B400000000000003FC000000000000000C00000000000804BC000000000005881400000000000003AC000000000000028C000000000008050C00000000000D0844000000000000035C000000000000000C000000000000046C00000000000C07B40"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOLYGON ZM (((1.5 2.5 11.5 111.5,11.5 2.5 22.5 222.5,6.5 12.5 33.5 333.5,1.5 2.5 11.5 111.5)),((21.5 2.5 44.5 444.5,31.5 2.5 55.5 555.5,26.5 12.5 66.5 666.5,21.5 2.5 44.5 444.5)))", "01BE0B00000200000001BB0B00000100000004000000000000000000F83F000000000000044000000000000027400000000000E05B400000000000002740000000000000044000000000008036400000000000D06B400000000000001A4000000000000029400000000000C040400000000000D87440000000000000F83F000000000000044000000000000027400000000000E05B4001BB0B000001000000040000000000000000803540000000000000044000000000004046400000000000C87B400000000000803F4000000000000004400000000000C04B4000000000005C81400000000000803A4000000000000029400000000000A050400000000000D484400000000000803540000000000000044000000000004046400000000000C87B40"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOLYGON ZM (((-1.5 -2.5 -11.5 111.5,-11.5 -2.5 -22.5 222.5,-6.5 -12.5 -33.5 333.5,-1.5 -2.5 -11.5 111.5)),((-21.5 -2.5 -44.5 444.5,-31.5 -2.5 -55.5 555.5,-26.5 -12.5 -66.5 666.5,-21.5 -2.5 -44.5 444.5)))", "01BE0B00000200000001BB0B00000100000004000000000000000000F8BF00000000000004C000000000000027C00000000000E05B4000000000000027C000000000000004C000000000008036C00000000000D06B400000000000001AC000000000000029C00000000000C040C00000000000D87440000000000000F8BF00000000000004C000000000000027C00000000000E05B4001BB0B0000010000000400000000000000008035C000000000000004C000000000004046C00000000000C87B400000000000803FC000000000000004C00000000000C04BC000000000005C81400000000000803AC000000000000029C00000000000A050C00000000000D4844000000000008035C000000000000004C000000000004046C00000000000C87B40"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOLYGON ZM (((0 0 0 0,10 0 0 0,5 10 0 0,0 0 0 0)),((20 0 0 0,30 0 0 0,25 10 0 0,20 0 0 0)),((40 0 0 0,50 0 0 0,45 10 0 0,40 0 0 0)))", "01BE0B00000300000001BB0B00000100000004000000000000000000000000000000000000000000000000000000000000000000000000000000000024400000000000000000000000000000000000000000000000000000000000001440000000000000244000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001BB0B0000010000000400000000000000000034400000000000000000000000000000000000000000000000000000000000003E400000000000000000000000000000000000000000000000000000000000003940000000000000244000000000000000000000000000000000000000000000344000000000000000000000000000000000000000000000000001BB0B000001000000040000000000000000004440000000000000000000000000000000000000000000000000000000000000494000000000000000000000000000000000000000000000000000000000008046400000000000002440000000000000000000000000000000000000000000004440000000000000000000000000000000000000000000000000"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOLYGON ZM (((1 2 11 111,11 2 22 222,6 12 33 333,1 2 11 111)),((21 2 44 444,31 2 55 555,26 12 66 666,21 2 44 444)),((41 2 77 777,51 2 88 888,46 12 99 999,41 2 77 777)))", "01BE0B00000300000001BB0B00000100000004000000000000000000F03F000000000000004000000000000026400000000000C05B400000000000002640000000000000004000000000000036400000000000C06B400000000000001840000000000000284000000000008040400000000000D07440000000000000F03F000000000000004000000000000026400000000000C05B4001BB0B000001000000040000000000000000003540000000000000004000000000000046400000000000C07B400000000000003F4000000000000000400000000000804B4000000000005881400000000000003A40000000000000284000000000008050400000000000D084400000000000003540000000000000004000000000000046400000000000C07B4001BB0B0000010000000400000000000000008044400000000000000040000000000040534000000000004888400000000000804940000000000000004000000000000056400000000000C08B40000000000000474000000000000028400000000000C058400000000000388F400000000000804440000000000000004000000000004053400000000000488840"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOLYGON ZM (((-1 -2 -11 111,-11 -2 -22 222,-6 -12 -33 333,-1 -2 -11 111)),((-21 -2 -44 444,-31 -2 -55 555,-26 -12 -66 666,-21 -2 -44 444)),((-41 -2 -77 777,-51 -2 -88 888,-46 -12 -99 999,-41 -2 -77 777)))", "01BE0B00000300000001BB0B00000100000004000000000000000000F0BF00000000000000C000000000000026C00000000000C05B4000000000000026C000000000000000C000000000000036C00000000000C06B4000000000000018C000000000000028C000000000008040C00000000000D07440000000000000F0BF00000000000000C000000000000026C00000000000C05B4001BB0B0000010000000400000000000000000035C000000000000000C000000000000046C00000000000C07B400000000000003FC000000000000000C00000000000804BC000000000005881400000000000003AC000000000000028C000000000008050C00000000000D0844000000000000035C000000000000000C000000000000046C00000000000C07B4001BB0B0000010000000400000000000000008044C000000000000000C000000000004053C0000000000048884000000000008049C000000000000000C000000000000056C00000000000C08B4000000000000047C000000000000028C00000000000C058C00000000000388F4000000000008044C000000000000000C000000000004053C00000000000488840"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOLYGON ZM (((1.5 2.5 11.5 111.5,11.5 2.5 22.5 222.5,6.5 12.5 33.5 333.5,1.5 2.5 11.5 111.5)),((21.5 2.5 44.5 444.5,31.5 2.5 55.5 555.5,26.5 12.5 66.5 666.5,21.5 2.5 44.5 444.5)),((41.5 2.5 77.5 777.5,51.5 2.5 88.5 888.5,46.5 12.5 99.5 999.5,41.5 2.5 77.5 777.5)))", "01BE0B00000300000001BB0B00000100000004000000000000000000F83F000000000000044000000000000027400000000000E05B400000000000002740000000000000044000000000008036400000000000D06B400000000000001A4000000000000029400000000000C040400000000000D87440000000000000F83F000000000000044000000000000027400000000000E05B4001BB0B000001000000040000000000000000803540000000000000044000000000004046400000000000C87B400000000000803F4000000000000004400000000000C04B4000000000005C81400000000000803A4000000000000029400000000000A050400000000000D484400000000000803540000000000000044000000000004046400000000000C87B4001BB0B000001000000040000000000000000C044400000000000000440000000000060534000000000004C88400000000000C04940000000000000044000000000002056400000000000C48B40000000000040474000000000000029400000000000E0584000000000003C8F400000000000C044400000000000000440000000000060534000000000004C8840"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOLYGON ZM (((-1.5 -2.5 -11.5 111.5,-11.5 -2.5 -22.5 222.5,-6.5 -12.5 -33.5 333.5,-1.5 -2.5 -11.5 111.5)),((-21.5 -2.5 -44.5 444.5,-31.5 -2.5 -55.5 555.5,-26.5 -12.5 -66.5 666.5,-21.5 -2.5 -44.5 444.5)),((-41.5 -2.5 -77.5 777.5,-51.5 -2.5 -88.5 888.5,-46.5 -12.5 -99.5 999.5,-41.5 -2.5 -77.5 777.5)))", "01BE0B00000300000001BB0B00000100000004000000000000000000F8BF00000000000004C000000000000027C00000000000E05B4000000000000027C000000000000004C000000000008036C00000000000D06B400000000000001AC000000000000029C00000000000C040C00000000000D87440000000000000F8BF00000000000004C000000000000027C00000000000E05B4001BB0B0000010000000400000000000000008035C000000000000004C000000000004046C00000000000C87B400000000000803FC000000000000004C00000000000C04BC000000000005C81400000000000803AC000000000000029C00000000000A050C00000000000D4844000000000008035C000000000000004C000000000004046C00000000000C87B4001BB0B000001000000040000000000000000C044C000000000000004C000000000006053C000000000004C88400000000000C049C000000000000004C000000000002056C00000000000C48B4000000000004047C000000000000029C00000000000E058C000000000003C8F400000000000C044C000000000000004C000000000006053C000000000004C8840"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOLYGON(EMPTY)", "010600000001000000010300000000000000"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOLYGON(EMPTY,EMPTY)", "010600000002000000010300000000000000010300000000000000"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOLYGON(EMPTY,((1 2,11 2,6 12,1 2)))", "01060000000200000001030000000000000001030000000100000004000000000000000000F03F00000000000000400000000000002640000000000000004000000000000018400000000000002840000000000000F03F0000000000000040"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOLYGON(((1 2,11 2,6 12,1 2)),EMPTY)", "01060000000200000001030000000100000004000000000000000000F03F00000000000000400000000000002640000000000000004000000000000018400000000000002840000000000000F03F0000000000000040010300000000000000"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOLYGON(EMPTY,((1 2,11 2,6 12,1 2)),EMPTY)", "01060000000300000001030000000000000001030000000100000004000000000000000000F03F00000000000000400000000000002640000000000000004000000000000018400000000000002840000000000000F03F0000000000000040010300000000000000"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOLYGON(((1 2,11 2,6 12,1 2)),EMPTY,((21 2,31 2,26 12,21 2)))", "01060000000300000001030000000100000004000000000000000000F03F00000000000000400000000000002640000000000000004000000000000018400000000000002840000000000000F03F000000000000004001030000000000000001030000000100000004000000000000000000354000000000000000400000000000003F4000000000000000400000000000003A40000000000000284000000000000035400000000000000040"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOLYGON(EMPTY,EMPTY,((1 2,11 2,6 12,1 2)))", "01060000000300000001030000000000000001030000000000000001030000000100000004000000000000000000F03F00000000000000400000000000002640000000000000004000000000000018400000000000002840000000000000F03F0000000000000040"}, // checkstyle.off: LineLength
      new String[]{"MULTIPOLYGON(((1 2,11 2,6 12,1 2)),EMPTY,EMPTY)", "01060000000300000001030000000100000004000000000000000000F03F00000000000000400000000000002640000000000000004000000000000018400000000000002840000000000000F03F0000000000000040010300000000000000010300000000000000"}, // checkstyle.off: LineLength
      new String[]{"GEOMETRYCOLLECTION EMPTY", "010700000000000000"}, // checkstyle.off: LineLength
      new String[]{"GEOMETRYCOLLECTION(POINT(0 0))", "010700000001000000010100000000000000000000000000000000000000"}, // checkstyle.off: LineLength
      new String[]{"GEOMETRYCOLLECTION(POINT(1 2))", "0107000000010000000101000000000000000000F03F0000000000000040"}, // checkstyle.off: LineLength
      new String[]{"GEOMETRYCOLLECTION(POINT(-1 -2))", "0107000000010000000101000000000000000000F0BF00000000000000C0"}, // checkstyle.off: LineLength
      new String[]{"GEOMETRYCOLLECTION(POINT(1.5 2.5))", "0107000000010000000101000000000000000000F83F0000000000000440"}, // checkstyle.off: LineLength
      new String[]{"GEOMETRYCOLLECTION(POINT(-1.5 -2.5))", "0107000000010000000101000000000000000000F8BF00000000000004C0"}, // checkstyle.off: LineLength
      new String[]{"GEOMETRYCOLLECTION(POINT(0 0),LINESTRING(0 0,0 0))", "0107000000020000000101000000000000000000000000000000000000000102000000020000000000000000000000000000000000000000000000000000000000000000000000"}, // checkstyle.off: LineLength
      new String[]{"GEOMETRYCOLLECTION(POINT(1 2),LINESTRING(1 2,3 4))", "0107000000020000000101000000000000000000F03F0000000000000040010200000002000000000000000000F03F000000000000004000000000000008400000000000001040"}, // checkstyle.off: LineLength
      new String[]{"GEOMETRYCOLLECTION(POINT(-1 -2),LINESTRING(-1 -2,-3 -4))", "0107000000020000000101000000000000000000F0BF00000000000000C0010200000002000000000000000000F0BF00000000000000C000000000000008C000000000000010C0"}, // checkstyle.off: LineLength
      new String[]{"GEOMETRYCOLLECTION(POINT(1.5 2.5),LINESTRING(1.5 2.5,3.5 4.5))", "0107000000020000000101000000000000000000F83F0000000000000440010200000002000000000000000000F83F00000000000004400000000000000C400000000000001240"}, // checkstyle.off: LineLength
      new String[]{"GEOMETRYCOLLECTION(POINT(-1.5 -2.5),LINESTRING(-1.5 -2.5,-3.5 -4.5))", "0107000000020000000101000000000000000000F8BF00000000000004C0010200000002000000000000000000F8BF00000000000004C00000000000000CC000000000000012C0"}, // checkstyle.off: LineLength
      new String[]{"GEOMETRYCOLLECTION(POINT(0 0),LINESTRING(0 0,0 0),POLYGON((0 0,10 0,5 10,0 0)))", "01070000000300000001010000000000000000000000000000000000000001020000000200000000000000000000000000000000000000000000000000000000000000000000000103000000010000000400000000000000000000000000000000000000000000000000244000000000000000000000000000001440000000000000244000000000000000000000000000000000"}, // checkstyle.off: LineLength
      new String[]{"GEOMETRYCOLLECTION(POINT(1 2),LINESTRING(1 2,3 4),POLYGON((1 2,11 2,6 12,1 2)))", "0107000000030000000101000000000000000000F03F0000000000000040010200000002000000000000000000F03F00000000000000400000000000000840000000000000104001030000000100000004000000000000000000F03F00000000000000400000000000002640000000000000004000000000000018400000000000002840000000000000F03F0000000000000040"}, // checkstyle.off: LineLength
      new String[]{"GEOMETRYCOLLECTION(POINT(-1 -2),LINESTRING(-1 -2,-3 -4),POLYGON((-1 -2,-11 -2,-6 -12,-1 -2)))", "0107000000030000000101000000000000000000F0BF00000000000000C0010200000002000000000000000000F0BF00000000000000C000000000000008C000000000000010C001030000000100000004000000000000000000F0BF00000000000000C000000000000026C000000000000000C000000000000018C000000000000028C0000000000000F0BF00000000000000C0"}, // checkstyle.off: LineLength
      new String[]{"GEOMETRYCOLLECTION(POINT(1.5 2.5),LINESTRING(1.5 2.5,3.5 4.5),POLYGON((1.5 2.5,11.5 2.5,6.5 12.5,1.5 2.5)))", "0107000000030000000101000000000000000000F83F0000000000000440010200000002000000000000000000F83F00000000000004400000000000000C40000000000000124001030000000100000004000000000000000000F83F0000000000000440000000000000274000000000000004400000000000001A400000000000002940000000000000F83F0000000000000440"}, // checkstyle.off: LineLength
      new String[]{"GEOMETRYCOLLECTION(POINT(-1.5 -2.5),LINESTRING(-1.5 -2.5,-3.5 -4.5),POLYGON((-1.5 -2.5,-11.5 -2.5,-6.5 -12.5,-1.5 -2.5)))", "0107000000030000000101000000000000000000F8BF00000000000004C0010200000002000000000000000000F8BF00000000000004C00000000000000CC000000000000012C001030000000100000004000000000000000000F8BF00000000000004C000000000000027C000000000000004C00000000000001AC000000000000029C0000000000000F8BF00000000000004C0"}, // checkstyle.off: LineLength
      new String[]{"GEOMETRYCOLLECTION Z EMPTY", "01EF03000000000000"}, // checkstyle.off: LineLength
      new String[]{"GEOMETRYCOLLECTION Z (POINT Z (0 0 0))", "01EF0300000100000001E9030000000000000000000000000000000000000000000000000000"}, // checkstyle.off: LineLength
      new String[]{"GEOMETRYCOLLECTION Z (POINT Z (1 2 11))", "01EF0300000100000001E9030000000000000000F03F00000000000000400000000000002640"}, // checkstyle.off: LineLength
      new String[]{"GEOMETRYCOLLECTION Z (POINT Z (-1 -2 -11))", "01EF0300000100000001E9030000000000000000F0BF00000000000000C000000000000026C0"}, // checkstyle.off: LineLength
      new String[]{"GEOMETRYCOLLECTION Z (POINT Z (1.5 2.5 11.5))", "01EF0300000100000001E9030000000000000000F83F00000000000004400000000000002740"}, // checkstyle.off: LineLength
      new String[]{"GEOMETRYCOLLECTION Z (POINT Z (-1.5 -2.5 -11.5))", "01EF0300000100000001E9030000000000000000F8BF00000000000004C000000000000027C0"}, // checkstyle.off: LineLength
      new String[]{"GEOMETRYCOLLECTION Z (POINT Z (0 0 0),LINESTRING Z (0 0 0,0 0 0))", "01EF0300000200000001E903000000000000000000000000000000000000000000000000000001EA03000002000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"}, // checkstyle.off: LineLength
      new String[]{"GEOMETRYCOLLECTION Z (POINT Z (1 2 11),LINESTRING Z (1 2 11,3 4 22))", "01EF0300000200000001E9030000000000000000F03F0000000000000040000000000000264001EA03000002000000000000000000F03F00000000000000400000000000002640000000000000084000000000000010400000000000003640"}, // checkstyle.off: LineLength
      new String[]{"GEOMETRYCOLLECTION Z (POINT Z (-1 -2 -11),LINESTRING Z (-1 -2 -11,-3 -4 -22))", "01EF0300000200000001E9030000000000000000F0BF00000000000000C000000000000026C001EA03000002000000000000000000F0BF00000000000000C000000000000026C000000000000008C000000000000010C000000000000036C0"}, // checkstyle.off: LineLength
      new String[]{"GEOMETRYCOLLECTION Z (POINT Z (1.5 2.5 11.5),LINESTRING Z (1.5 2.5 11.5,3.5 4.5 22.5))", "01EF0300000200000001E9030000000000000000F83F0000000000000440000000000000274001EA03000002000000000000000000F83F000000000000044000000000000027400000000000000C4000000000000012400000000000803640"}, // checkstyle.off: LineLength
      new String[]{"GEOMETRYCOLLECTION Z (POINT Z (-1.5 -2.5 -11.5),LINESTRING Z (-1.5 -2.5 -11.5,-3.5 -4.5 -22.5))", "01EF0300000200000001E9030000000000000000F8BF00000000000004C000000000000027C001EA03000002000000000000000000F8BF00000000000004C000000000000027C00000000000000CC000000000000012C000000000008036C0"}, // checkstyle.off: LineLength
      new String[]{"GEOMETRYCOLLECTION Z (POINT Z (0 0 0),LINESTRING Z (0 0 0,0 0 0),POLYGON Z ((0 0 0,10 0 0,5 10 0,0 0 0)))", "01EF0300000300000001E903000000000000000000000000000000000000000000000000000001EA0300000200000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001EB0300000100000004000000000000000000000000000000000000000000000000000000000000000000244000000000000000000000000000000000000000000000144000000000000024400000000000000000000000000000000000000000000000000000000000000000"}, // checkstyle.off: LineLength
      new String[]{"GEOMETRYCOLLECTION Z (POINT Z (1 2 11),LINESTRING Z (1 2 11,3 4 22),POLYGON Z ((1 2 11,11 2 22,6 12 33,1 2 11)))", "01EF0300000300000001E9030000000000000000F03F0000000000000040000000000000264001EA03000002000000000000000000F03F0000000000000040000000000000264000000000000008400000000000001040000000000000364001EB0300000100000004000000000000000000F03F00000000000000400000000000002640000000000000264000000000000000400000000000003640000000000000184000000000000028400000000000804040000000000000F03F00000000000000400000000000002640"}, // checkstyle.off: LineLength
      new String[]{"GEOMETRYCOLLECTION Z (POINT Z (-1 -2 -11),LINESTRING Z (-1 -2 -11,-3 -4 -22),POLYGON Z ((-1 -2 -11,-11 -2 -22,-6 -12 -33,-1 -2 -11)))", "01EF0300000300000001E9030000000000000000F0BF00000000000000C000000000000026C001EA03000002000000000000000000F0BF00000000000000C000000000000026C000000000000008C000000000000010C000000000000036C001EB0300000100000004000000000000000000F0BF00000000000000C000000000000026C000000000000026C000000000000000C000000000000036C000000000000018C000000000000028C000000000008040C0000000000000F0BF00000000000000C000000000000026C0"}, // checkstyle.off: LineLength
      new String[]{"GEOMETRYCOLLECTION Z (POINT Z (1.5 2.5 11.5),LINESTRING Z (1.5 2.5 11.5,3.5 4.5 22.5),POLYGON Z ((1.5 2.5 11.5,11.5 2.5 22.5,6.5 12.5 33.5,1.5 2.5 11.5)))", "01EF0300000300000001E9030000000000000000F83F0000000000000440000000000000274001EA03000002000000000000000000F83F000000000000044000000000000027400000000000000C400000000000001240000000000080364001EB0300000100000004000000000000000000F83F000000000000044000000000000027400000000000002740000000000000044000000000008036400000000000001A4000000000000029400000000000C04040000000000000F83F00000000000004400000000000002740"}, // checkstyle.off: LineLength
      new String[]{"GEOMETRYCOLLECTION Z (POINT Z (-1.5 -2.5 -11.5),LINESTRING Z (-1.5 -2.5 -11.5,-3.5 -4.5 -22.5),POLYGON Z ((-1.5 -2.5 -11.5,-11.5 -2.5 -22.5,-6.5 -12.5 -33.5,-1.5 -2.5 -11.5)))", "01EF0300000300000001E9030000000000000000F8BF00000000000004C000000000000027C001EA03000002000000000000000000F8BF00000000000004C000000000000027C00000000000000CC000000000000012C000000000008036C001EB0300000100000004000000000000000000F8BF00000000000004C000000000000027C000000000000027C000000000000004C000000000008036C00000000000001AC000000000000029C00000000000C040C0000000000000F8BF00000000000004C000000000000027C0"}, // checkstyle.off: LineLength
      new String[]{"GEOMETRYCOLLECTION M EMPTY", "01D707000000000000"}, // checkstyle.off: LineLength
      new String[]{"GEOMETRYCOLLECTION M (POINT M (0 0 0))", "01D70700000100000001D1070000000000000000000000000000000000000000000000000000"}, // checkstyle.off: LineLength
      new String[]{"GEOMETRYCOLLECTION M (POINT M (1 2 111))", "01D70700000100000001D1070000000000000000F03F00000000000000400000000000C05B40"}, // checkstyle.off: LineLength
      new String[]{"GEOMETRYCOLLECTION M (POINT M (-1 -2 111))", "01D70700000100000001D1070000000000000000F0BF00000000000000C00000000000C05B40"}, // checkstyle.off: LineLength
      new String[]{"GEOMETRYCOLLECTION M (POINT M (1.5 2.5 111.5))", "01D70700000100000001D1070000000000000000F83F00000000000004400000000000E05B40"}, // checkstyle.off: LineLength
      new String[]{"GEOMETRYCOLLECTION M (POINT M (-1.5 -2.5 111.5))", "01D70700000100000001D1070000000000000000F8BF00000000000004C00000000000E05B40"}, // checkstyle.off: LineLength
      new String[]{"GEOMETRYCOLLECTION M (POINT M (0 0 0),LINESTRING M (0 0 0,0 0 0))", "01D70700000200000001D107000000000000000000000000000000000000000000000000000001D207000002000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"}, // checkstyle.off: LineLength
      new String[]{"GEOMETRYCOLLECTION M (POINT M (1 2 111),LINESTRING M (1 2 111,3 4 222))", "01D70700000200000001D1070000000000000000F03F00000000000000400000000000C05B4001D207000002000000000000000000F03F00000000000000400000000000C05B40000000000000084000000000000010400000000000C06B40"}, // checkstyle.off: LineLength
      new String[]{"GEOMETRYCOLLECTION M (POINT M (-1 -2 111),LINESTRING M (-1 -2 111,-3 -4 222))", "01D70700000200000001D1070000000000000000F0BF00000000000000C00000000000C05B4001D207000002000000000000000000F0BF00000000000000C00000000000C05B4000000000000008C000000000000010C00000000000C06B40"}, // checkstyle.off: LineLength
      new String[]{"GEOMETRYCOLLECTION M (POINT M (1.5 2.5 111.5),LINESTRING M (1.5 2.5 111.5,3.5 4.5 222.5))", "01D70700000200000001D1070000000000000000F83F00000000000004400000000000E05B4001D207000002000000000000000000F83F00000000000004400000000000E05B400000000000000C4000000000000012400000000000D06B40"}, // checkstyle.off: LineLength
      new String[]{"GEOMETRYCOLLECTION M (POINT M (-1.5 -2.5 111.5),LINESTRING M (-1.5 -2.5 111.5,-3.5 -4.5 222.5))", "01D70700000200000001D1070000000000000000F8BF00000000000004C00000000000E05B4001D207000002000000000000000000F8BF00000000000004C00000000000E05B400000000000000CC000000000000012C00000000000D06B40"}, // checkstyle.off: LineLength
      new String[]{"GEOMETRYCOLLECTION M (POINT M (0 0 0),LINESTRING M (0 0 0,0 0 0),POLYGON M ((0 0 0,10 0 0,5 10 0,0 0 0)))", "01D70700000300000001D107000000000000000000000000000000000000000000000000000001D20700000200000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001D30700000100000004000000000000000000000000000000000000000000000000000000000000000000244000000000000000000000000000000000000000000000144000000000000024400000000000000000000000000000000000000000000000000000000000000000"}, // checkstyle.off: LineLength
      new String[]{"GEOMETRYCOLLECTION M (POINT M (1 2 111),LINESTRING M (1 2 111,3 4 222),POLYGON M ((1 2 111,11 2 222,6 12 333,1 2 111)))", "01D70700000300000001D1070000000000000000F03F00000000000000400000000000C05B4001D207000002000000000000000000F03F00000000000000400000000000C05B40000000000000084000000000000010400000000000C06B4001D30700000100000004000000000000000000F03F00000000000000400000000000C05B40000000000000264000000000000000400000000000C06B40000000000000184000000000000028400000000000D07440000000000000F03F00000000000000400000000000C05B40"}, // checkstyle.off: LineLength
      new String[]{"GEOMETRYCOLLECTION M (POINT M (-1 -2 111),LINESTRING M (-1 -2 111,-3 -4 222),POLYGON M ((-1 -2 111,-11 -2 222,-6 -12 333,-1 -2 111)))", "01D70700000300000001D1070000000000000000F0BF00000000000000C00000000000C05B4001D207000002000000000000000000F0BF00000000000000C00000000000C05B4000000000000008C000000000000010C00000000000C06B4001D30700000100000004000000000000000000F0BF00000000000000C00000000000C05B4000000000000026C000000000000000C00000000000C06B4000000000000018C000000000000028C00000000000D07440000000000000F0BF00000000000000C00000000000C05B40"}, // checkstyle.off: LineLength
      new String[]{"GEOMETRYCOLLECTION M (POINT M (1.5 2.5 111.5),LINESTRING M (1.5 2.5 111.5,3.5 4.5 222.5),POLYGON M ((1.5 2.5 111.5,11.5 2.5 222.5,6.5 12.5 333.5,1.5 2.5 111.5)))", "01D70700000300000001D1070000000000000000F83F00000000000004400000000000E05B4001D207000002000000000000000000F83F00000000000004400000000000E05B400000000000000C4000000000000012400000000000D06B4001D30700000100000004000000000000000000F83F00000000000004400000000000E05B40000000000000274000000000000004400000000000D06B400000000000001A4000000000000029400000000000D87440000000000000F83F00000000000004400000000000E05B40"}, // checkstyle.off: LineLength
      new String[]{"GEOMETRYCOLLECTION M (POINT M (-1.5 -2.5 111.5),LINESTRING M (-1.5 -2.5 111.5,-3.5 -4.5 222.5),POLYGON M ((-1.5 -2.5 111.5,-11.5 -2.5 222.5,-6.5 -12.5 333.5,-1.5 -2.5 111.5)))", "01D70700000300000001D1070000000000000000F8BF00000000000004C00000000000E05B4001D207000002000000000000000000F8BF00000000000004C00000000000E05B400000000000000CC000000000000012C00000000000D06B4001D30700000100000004000000000000000000F8BF00000000000004C00000000000E05B4000000000000027C000000000000004C00000000000D06B400000000000001AC000000000000029C00000000000D87440000000000000F8BF00000000000004C00000000000E05B40"}, // checkstyle.off: LineLength
      new String[]{"GEOMETRYCOLLECTION ZM EMPTY", "01BF0B000000000000"}, // checkstyle.off: LineLength
      new String[]{"GEOMETRYCOLLECTION ZM (POINT ZM (0 0 0 0))", "01BF0B00000100000001B90B00000000000000000000000000000000000000000000000000000000000000000000"}, // checkstyle.off: LineLength
      new String[]{"GEOMETRYCOLLECTION ZM (POINT ZM (1 2 11 111))", "01BF0B00000100000001B90B0000000000000000F03F000000000000004000000000000026400000000000C05B40"}, // checkstyle.off: LineLength
      new String[]{"GEOMETRYCOLLECTION ZM (POINT ZM (-1 -2 -11 111))", "01BF0B00000100000001B90B0000000000000000F0BF00000000000000C000000000000026C00000000000C05B40"}, // checkstyle.off: LineLength
      new String[]{"GEOMETRYCOLLECTION ZM (POINT ZM (1.5 2.5 11.5 111.5))", "01BF0B00000100000001B90B0000000000000000F83F000000000000044000000000000027400000000000E05B40"}, // checkstyle.off: LineLength
      new String[]{"GEOMETRYCOLLECTION ZM (POINT ZM (-1.5 -2.5 -11.5 111.5))", "01BF0B00000100000001B90B0000000000000000F8BF00000000000004C000000000000027C00000000000E05B40"}, // checkstyle.off: LineLength
      new String[]{"GEOMETRYCOLLECTION ZM (POINT ZM (0 0 0 0),LINESTRING ZM (0 0 0 0,0 0 0 0))", "01BF0B00000200000001B90B0000000000000000000000000000000000000000000000000000000000000000000001BA0B00000200000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"}, // checkstyle.off: LineLength
      new String[]{"GEOMETRYCOLLECTION ZM (POINT ZM (1 2 11 111),LINESTRING ZM (1 2 11 111,3 4 22 222))", "01BF0B00000200000001B90B0000000000000000F03F000000000000004000000000000026400000000000C05B4001BA0B000002000000000000000000F03F000000000000004000000000000026400000000000C05B400000000000000840000000000000104000000000000036400000000000C06B40"}, // checkstyle.off: LineLength
      new String[]{"GEOMETRYCOLLECTION ZM (POINT ZM (-1 -2 -11 111),LINESTRING ZM (-1 -2 -11 111,-3 -4 -22 222))", "01BF0B00000200000001B90B0000000000000000F0BF00000000000000C000000000000026C00000000000C05B4001BA0B000002000000000000000000F0BF00000000000000C000000000000026C00000000000C05B4000000000000008C000000000000010C000000000000036C00000000000C06B40"}, // checkstyle.off: LineLength
      new String[]{"GEOMETRYCOLLECTION ZM (POINT ZM (1.5 2.5 11.5 111.5),LINESTRING ZM (1.5 2.5 11.5 111.5,3.5 4.5 22.5 222.5))", "01BF0B00000200000001B90B0000000000000000F83F000000000000044000000000000027400000000000E05B4001BA0B000002000000000000000000F83F000000000000044000000000000027400000000000E05B400000000000000C40000000000000124000000000008036400000000000D06B40"}, // checkstyle.off: LineLength
      new String[]{"GEOMETRYCOLLECTION ZM (POINT ZM (-1.5 -2.5 -11.5 111.5),LINESTRING ZM (-1.5 -2.5 -11.5 111.5,-3.5 -4.5 -22.5 222.5))", "01BF0B00000200000001B90B0000000000000000F8BF00000000000004C000000000000027C00000000000E05B4001BA0B000002000000000000000000F8BF00000000000004C000000000000027C00000000000E05B400000000000000CC000000000000012C000000000008036C00000000000D06B40"}, // checkstyle.off: LineLength
      new String[]{"GEOMETRYCOLLECTION ZM (POINT ZM (0 0 0 0),LINESTRING ZM (0 0 0 0,0 0 0 0),POLYGON ZM ((0 0 0 0,10 0 0 0,5 10 0 0,0 0 0 0)))", "01BF0B00000300000001B90B0000000000000000000000000000000000000000000000000000000000000000000001BA0B0000020000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001BB0B000001000000040000000000000000000000000000000000000000000000000000000000000000000000000000000000244000000000000000000000000000000000000000000000000000000000000014400000000000002440000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"}, // checkstyle.off: LineLength
      new String[]{"GEOMETRYCOLLECTION ZM (POINT ZM (1 2 11 111),LINESTRING ZM (1 2 11 111,3 4 22 222),POLYGON ZM ((1 2 11 111,11 2 22 222,6 12 33 333,1 2 11 111)))", "01BF0B00000300000001B90B0000000000000000F03F000000000000004000000000000026400000000000C05B4001BA0B000002000000000000000000F03F000000000000004000000000000026400000000000C05B400000000000000840000000000000104000000000000036400000000000C06B4001BB0B00000100000004000000000000000000F03F000000000000004000000000000026400000000000C05B400000000000002640000000000000004000000000000036400000000000C06B400000000000001840000000000000284000000000008040400000000000D07440000000000000F03F000000000000004000000000000026400000000000C05B40"}, // checkstyle.off: LineLength
      new String[]{"GEOMETRYCOLLECTION ZM (POINT ZM (-1 -2 -11 111),LINESTRING ZM (-1 -2 -11 111,-3 -4 -22 222),POLYGON ZM ((-1 -2 -11 111,-11 -2 -22 222,-6 -12 -33 333,-1 -2 -11 111)))", "01BF0B00000300000001B90B0000000000000000F0BF00000000000000C000000000000026C00000000000C05B4001BA0B000002000000000000000000F0BF00000000000000C000000000000026C00000000000C05B4000000000000008C000000000000010C000000000000036C00000000000C06B4001BB0B00000100000004000000000000000000F0BF00000000000000C000000000000026C00000000000C05B4000000000000026C000000000000000C000000000000036C00000000000C06B4000000000000018C000000000000028C000000000008040C00000000000D07440000000000000F0BF00000000000000C000000000000026C00000000000C05B40"}, // checkstyle.off: LineLength
      new String[]{"GEOMETRYCOLLECTION ZM (POINT ZM (1.5 2.5 11.5 111.5),LINESTRING ZM (1.5 2.5 11.5 111.5,3.5 4.5 22.5 222.5),POLYGON ZM ((1.5 2.5 11.5 111.5,11.5 2.5 22.5 222.5,6.5 12.5 33.5 333.5,1.5 2.5 11.5 111.5)))", "01BF0B00000300000001B90B0000000000000000F83F000000000000044000000000000027400000000000E05B4001BA0B000002000000000000000000F83F000000000000044000000000000027400000000000E05B400000000000000C40000000000000124000000000008036400000000000D06B4001BB0B00000100000004000000000000000000F83F000000000000044000000000000027400000000000E05B400000000000002740000000000000044000000000008036400000000000D06B400000000000001A4000000000000029400000000000C040400000000000D87440000000000000F83F000000000000044000000000000027400000000000E05B40"}, // checkstyle.off: LineLength
      new String[]{"GEOMETRYCOLLECTION ZM (POINT ZM (-1.5 -2.5 -11.5 111.5),LINESTRING ZM (-1.5 -2.5 -11.5 111.5,-3.5 -4.5 -22.5 222.5),POLYGON ZM ((-1.5 -2.5 -11.5 111.5,-11.5 -2.5 -22.5 222.5,-6.5 -12.5 -33.5 333.5,-1.5 -2.5 -11.5 111.5)))", "01BF0B00000300000001B90B0000000000000000F8BF00000000000004C000000000000027C00000000000E05B4001BA0B000002000000000000000000F8BF00000000000004C000000000000027C00000000000E05B400000000000000CC000000000000012C000000000008036C00000000000D06B4001BB0B00000100000004000000000000000000F8BF00000000000004C000000000000027C00000000000E05B4000000000000027C000000000000004C000000000008036C00000000000D06B400000000000001AC000000000000029C00000000000C040C00000000000D87440000000000000F8BF00000000000004C000000000000027C00000000000E05B40"}, // checkstyle.off: LineLength
      new String[]{"GEOMETRYCOLLECTION(POINT EMPTY)", "0107000000010000000101000000000000000000F87F000000000000F87F"}, // checkstyle.off: LineLength
      new String[]{"GEOMETRYCOLLECTION(LINESTRING EMPTY)", "010700000001000000010200000000000000"}, // checkstyle.off: LineLength
      new String[]{"GEOMETRYCOLLECTION(POLYGON EMPTY)", "010700000001000000010300000000000000"}, // checkstyle.off: LineLength
      new String[]{"GEOMETRYCOLLECTION(POINT EMPTY,POINT EMPTY)", "0107000000020000000101000000000000000000F87F000000000000F87F0101000000000000000000F87F000000000000F87F"}, // checkstyle.off: LineLength
      new String[]{"GEOMETRYCOLLECTION(POINT EMPTY,POINT(1 2))", "0107000000020000000101000000000000000000F87F000000000000F87F0101000000000000000000F03F0000000000000040"}, // checkstyle.off: LineLength
      new String[]{"GEOMETRYCOLLECTION(POINT(1 2),POINT EMPTY)", "0107000000020000000101000000000000000000F03F00000000000000400101000000000000000000F87F000000000000F87F"}, // checkstyle.off: LineLength
      new String[]{"GEOMETRYCOLLECTION(POINT EMPTY,POINT(1 2),POINT EMPTY)", "0107000000030000000101000000000000000000F87F000000000000F87F0101000000000000000000F03F00000000000000400101000000000000000000F87F000000000000F87F"}, // checkstyle.off: LineLength
      new String[]{"GEOMETRYCOLLECTION(LINESTRING EMPTY,LINESTRING(1 2,3 4))", "010700000002000000010200000000000000010200000002000000000000000000F03F000000000000004000000000000008400000000000001040"}, // checkstyle.off: LineLength
      new String[]{"GEOMETRYCOLLECTION(LINESTRING(1 2,3 4),LINESTRING EMPTY)", "010700000002000000010200000002000000000000000000F03F000000000000004000000000000008400000000000001040010200000000000000"}, // checkstyle.off: LineLength
      new String[]{"GEOMETRYCOLLECTION(POLYGON EMPTY,POLYGON((1 2,11 2,6 12,1 2)))", "01070000000200000001030000000000000001030000000100000004000000000000000000F03F00000000000000400000000000002640000000000000004000000000000018400000000000002840000000000000F03F0000000000000040"}, // checkstyle.off: LineLength
      new String[]{"GEOMETRYCOLLECTION(POLYGON((1 2,11 2,6 12,1 2)),POLYGON EMPTY)", "01070000000200000001030000000100000004000000000000000000F03F00000000000000400000000000002640000000000000004000000000000018400000000000002840000000000000F03F0000000000000040010300000000000000"}, // checkstyle.off: LineLength
      new String[]{"GEOMETRYCOLLECTION(POINT EMPTY,LINESTRING EMPTY,POLYGON EMPTY)", "0107000000030000000101000000000000000000F87F000000000000F87F010200000000000000010300000000000000"}, // checkstyle.off: LineLength
      new String[]{"GEOMETRYCOLLECTION(POINT(1 2),LINESTRING EMPTY,POLYGON((1 2,11 2,6 12,1 2)))", "0107000000030000000101000000000000000000F03F000000000000004001020000000000000001030000000100000004000000000000000000F03F00000000000000400000000000002640000000000000004000000000000018400000000000002840000000000000F03F0000000000000040"}, // checkstyle.off: LineLength
      new String[]{"GEOMETRYCOLLECTION(POINT EMPTY,LINESTRING(1 2,3 4),POLYGON EMPTY)", "0107000000030000000101000000000000000000F87F000000000000F87F010200000002000000000000000000F03F000000000000004000000000000008400000000000001040010300000000000000"}, // checkstyle.off: LineLength
      new String[]{"GEOMETRYCOLLECTION(MULTIPOINT EMPTY)", "010700000001000000010400000000000000"}, // checkstyle.off: LineLength
      new String[]{"GEOMETRYCOLLECTION(MULTILINESTRING EMPTY)", "010700000001000000010500000000000000"}, // checkstyle.off: LineLength
      new String[]{"GEOMETRYCOLLECTION(MULTIPOLYGON EMPTY)", "010700000001000000010600000000000000"}, // checkstyle.off: LineLength
      new String[]{"GEOMETRYCOLLECTION(GEOMETRYCOLLECTION EMPTY)", "010700000001000000010700000000000000"}, // checkstyle.off: LineLength
      new String[]{"GEOMETRYCOLLECTION(POINT(1 2),GEOMETRYCOLLECTION EMPTY,LINESTRING(1 2,3 4))", "0107000000030000000101000000000000000000F03F0000000000000040010700000000000000010200000002000000000000000000F03F000000000000004000000000000008400000000000001040"} // checkstyle.off: LineLength
    );
    WkbReader reader = new WkbReader();
    for (String[] testCase : testCases) {
      String expectedWkt = testCase[0];
      String wkbHex = testCase[1];
      byte[] wkbBytes = hexToBytes(wkbHex);
      GeometryModel geom = reader.read(wkbBytes);
      Assertions.assertEquals(expectedWkt, geom.toString());
    }
  }
}
