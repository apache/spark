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

import java.util.Arrays;
import java.util.List;

/**
 * Test suite for WKB multi-geometry types (MultiPoint, MultiLineString, MultiPolygon).
 */
public class WkbMultiGeometryTest {

  @Test
  public void testSimpleMultiPoint() {
    // MULTIPOINT((0 0), (1 1), (2 2))
    List<Point> points = Arrays.asList(
        new Point(new double[]{0.0, 0.0}, 0),
        new Point(new double[]{1.0, 1.0}, 0),
        new Point(new double[]{2.0, 2.0}, 0)
    );
    MultiPoint multiPoint = new MultiPoint(points, 0);

    Assertions.assertFalse(multiPoint.isEmpty(), "MultiPoint should not be empty");
    Assertions.assertEquals(3, multiPoint.getNumGeometries(), "Number of geometries");
    Assertions.assertEquals(0.0, multiPoint.getPoints().get(0).getX(), 1e-10, "First point X");
    Assertions.assertEquals(1.0, multiPoint.getPoints().get(1).getX(), 1e-10, "Second point X");
    Assertions.assertEquals(2.0, multiPoint.getPoints().get(2).getX(), 1e-10, "Third point X");
  }

  @Test
  public void testEmptyMultiPoint() {
    // MULTIPOINT EMPTY
    MultiPoint multiPoint = new MultiPoint(Arrays.asList(), 0);

    Assertions.assertTrue(multiPoint.isEmpty(), "MultiPoint should be empty");
    Assertions.assertEquals(0, multiPoint.getNumGeometries(), "Number of geometries should be 0");
  }

  @Test
  public void testMultiPointRoundTrip() {
    List<Point> points = Arrays.asList(
        new Point(new double[]{1.0, 2.0}, 0),
        new Point(new double[]{3.0, 4.0}, 0)
    );
    MultiPoint original = new MultiPoint(points, 0);

    WkbWriter writer = new WkbWriter();
    byte[] wkb = writer.write(original);

    WkbReader reader = new WkbReader();
    Geometry parsed = reader.read(wkb, 0);

    Assertions.assertTrue(parsed instanceof MultiPoint, "Parsed geometry should be MultiPoint");
    MultiPoint parsedMP = (MultiPoint) parsed;
    Assertions.assertEquals(original.getNumGeometries(), parsedMP.getNumGeometries(),
        "Number of points should match");
  }

  @Test
  public void testSimpleMultiLineString() {
    // MULTILINESTRING((0 0, 1 1), (2 2, 3 3))
    List<Point> ls1Points = Arrays.asList(
        new Point(new double[]{0.0, 0.0}, 0),
        new Point(new double[]{1.0, 1.0}, 0)
    );
    LineString ls1 = new LineString(ls1Points, 0);

    List<Point> ls2Points = Arrays.asList(
        new Point(new double[]{2.0, 2.0}, 0),
        new Point(new double[]{3.0, 3.0}, 0)
    );
    LineString ls2 = new LineString(ls2Points, 0);

    MultiLineString multiLineString = new MultiLineString(Arrays.asList(ls1, ls2), 0);

    Assertions.assertFalse(multiLineString.isEmpty(), "MultiLineString should not be empty");
    Assertions.assertEquals(2, multiLineString.getNumGeometries(), "Number of geometries");
  }

  @Test
  public void testEmptyMultiLineString() {
    // MULTILINESTRING EMPTY
    MultiLineString multiLineString = new MultiLineString(Arrays.asList(), 0);

    Assertions.assertTrue(multiLineString.isEmpty(), "MultiLineString should be empty");
    Assertions.assertEquals(0, multiLineString.getNumGeometries(),
        "Number of geometries should be 0");
  }

  @Test
  public void testMultiLineStringRoundTrip() {
    List<Point> ls1Points = Arrays.asList(
        new Point(new double[]{0.0, 0.0}, 0),
        new Point(new double[]{1.0, 1.0}, 0)
    );
    List<Point> ls2Points = Arrays.asList(
        new Point(new double[]{2.0, 2.0}, 0),
        new Point(new double[]{3.0, 3.0}, 0)
    );
    MultiLineString original = new MultiLineString(Arrays.asList(
        new LineString(ls1Points, 0),
        new LineString(ls2Points, 0)
    ), 0);

    WkbWriter writer = new WkbWriter();
    byte[] wkb = writer.write(original);
    System.out.println("WKB hex: " + bytesToHex(wkb));

    WkbReader reader = new WkbReader();
    Geometry parsed = reader.read(wkb, 0);

    Assertions.assertTrue(parsed instanceof MultiLineString,
        "Parsed geometry should be MultiLineString");
    MultiLineString parsedMLS = (MultiLineString) parsed;
    Assertions.assertEquals(original.getNumGeometries(), parsedMLS.getNumGeometries(),
        "Number of linestrings should match");
  }

  @Test
  public void testSimpleMultiPolygon() {
    // MULTIPOLYGON(((0 0, 1 0, 1 1, 0 1, 0 0)), ((2 2, 3 2, 3 3, 2 3, 2 2)))

    // First polygon
    List<Point> poly1Points = Arrays.asList(
        new Point(new double[]{0.0, 0.0}, 0),
        new Point(new double[]{1.0, 0.0}, 0),
        new Point(new double[]{1.0, 1.0}, 0),
        new Point(new double[]{0.0, 1.0}, 0),
        new Point(new double[]{0.0, 0.0}, 0)
    );
    Polygon poly1 = new Polygon(Arrays.asList(new Ring(poly1Points)), 0);

    // Second polygon
    List<Point> poly2Points = Arrays.asList(
        new Point(new double[]{2.0, 2.0}, 0),
        new Point(new double[]{3.0, 2.0}, 0),
        new Point(new double[]{3.0, 3.0}, 0),
        new Point(new double[]{2.0, 3.0}, 0),
        new Point(new double[]{2.0, 2.0}, 0)
    );
    Polygon poly2 = new Polygon(Arrays.asList(new Ring(poly2Points)), 0);

    MultiPolygon multiPolygon = new MultiPolygon(Arrays.asList(poly1, poly2), 0);

    Assertions.assertFalse(multiPolygon.isEmpty(), "MultiPolygon should not be empty");
    Assertions.assertEquals(2, multiPolygon.getNumGeometries(), "Number of geometries");
  }

  @Test
  public void testEmptyMultiPolygon() {
    // MULTIPOLYGON EMPTY
    MultiPolygon multiPolygon = new MultiPolygon(Arrays.asList(), 0);

    Assertions.assertTrue(multiPolygon.isEmpty(), "MultiPolygon should be empty");
    Assertions.assertEquals(0, multiPolygon.getNumGeometries(), "Number of geometries should be 0");
  }

  @Test
  public void testMultiPolygonRoundTrip() {
    List<Point> poly1Points = Arrays.asList(
        new Point(new double[]{0.0, 0.0}, 0),
        new Point(new double[]{1.0, 0.0}, 0),
        new Point(new double[]{1.0, 1.0}, 0),
        new Point(new double[]{0.0, 1.0}, 0),
        new Point(new double[]{0.0, 0.0}, 0)
    );

    MultiPolygon original = new MultiPolygon(Arrays.asList(
        new Polygon(Arrays.asList(new Ring(poly1Points)), 0)
    ), 0);

    WkbWriter writer = new WkbWriter();
    byte[] wkb = writer.write(original);

    WkbReader reader = new WkbReader();
    Geometry parsed = reader.read(wkb, 0);

    Assertions.assertTrue(parsed instanceof MultiPolygon, "Parsed geometry should be MultiPolygon");
    MultiPolygon parsedMP = (MultiPolygon) parsed;
    Assertions.assertEquals(original.getNumGeometries(), parsedMP.getNumGeometries(),
        "Number of polygons should match");
  }

  @Test
  public void testToStringMethods() {
    // MultiPoint
    MultiPoint mp = new MultiPoint(Arrays.asList(
        new Point(new double[]{0.0, 0.0}, 0)
    ), 0);
    Assertions.assertTrue(mp.toString().contains("MULTIPOINT"),
        "MultiPoint toString should contain MULTIPOINT");

    // MultiLineString
    MultiLineString mls = new MultiLineString(Arrays.asList(
        new LineString(Arrays.asList(
            new Point(new double[]{0.0, 0.0}, 0),
            new Point(new double[]{1.0, 1.0}, 0)
        ), 0)
    ), 0);
    Assertions.assertTrue(mls.toString().contains("MULTILINESTRING"),
        "MultiLineString toString should contain MULTILINESTRING");

    // MultiPolygon
    MultiPolygon mpoly = new MultiPolygon(Arrays.asList(
        new Polygon(Arrays.asList(new Ring(Arrays.asList(
            new Point(new double[]{0.0, 0.0}, 0),
            new Point(new double[]{1.0, 0.0}, 0),
            new Point(new double[]{1.0, 1.0}, 0),
            new Point(new double[]{0.0, 1.0}, 0),
            new Point(new double[]{0.0, 0.0}, 0)
        ))), 0)
    ), 0);
    Assertions.assertTrue(mpoly.toString().contains("MULTIPOLYGON"),
        "MultiPolygon toString should contain MULTIPOLYGON");
  }

  private static String bytesToHex(byte[] bytes) {
    StringBuilder hex = new StringBuilder(bytes.length * 2);
    for (byte b : bytes) {
      hex.append(String.format("%02x", b));
    }
    return hex.toString();
  }
}

