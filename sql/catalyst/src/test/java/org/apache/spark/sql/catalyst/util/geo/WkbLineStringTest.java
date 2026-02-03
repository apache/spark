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
 * Test suite for WKB LineString geometries.
 */
public class WkbLineStringTest extends WkbTestBase {

  @Test
  public void testSimpleLineString() {
    // LINESTRING(0 0, 1 1, 2 2)
    List<Point> points = Arrays.asList(
        new Point(new double[]{0.0, 0.0}, 0),
        new Point(new double[]{1.0, 1.0}, 0),
        new Point(new double[]{2.0, 2.0}, 0)
    );
    LineString lineString = new LineString(points, 0, false, false);

    Assertions.assertFalse(lineString.isEmpty(), "LineString should not be empty");
    Assertions.assertEquals(3, lineString.getNumPoints(), "Number of points");

    // Verify each point
    List<Point> retrievedPoints = lineString.getPoints();
    Assertions.assertEquals(0.0, retrievedPoints.get(0).getX(), 1e-10, "First point X");
    Assertions.assertEquals(0.0, retrievedPoints.get(0).getY(), 1e-10, "First point Y");
    Assertions.assertEquals(1.0, retrievedPoints.get(1).getX(), 1e-10, "Second point X");
    Assertions.assertEquals(1.0, retrievedPoints.get(1).getY(), 1e-10, "Second point Y");
    Assertions.assertEquals(2.0, retrievedPoints.get(2).getX(), 1e-10, "Third point X");
    Assertions.assertEquals(2.0, retrievedPoints.get(2).getY(), 1e-10, "Third point Y");
  }

  @Test
  public void testEmptyLineString() {
    // LINESTRING EMPTY
    List<Point> points = Arrays.asList();
    LineString lineString = new LineString(points, 0, false, false);

    Assertions.assertTrue(lineString.isEmpty(), "LineString should be empty");
    Assertions.assertEquals(0, lineString.getNumPoints(), "Number of points should be 0");
  }

  @Test
  public void testLineStringWithTwoPoints() {
    // LINESTRING(0 0, 10 10)
    List<Point> points = Arrays.asList(
        new Point(new double[]{0.0, 0.0}, 0),
        new Point(new double[]{10.0, 10.0}, 0)
    );
    LineString lineString = new LineString(points, 0, false, false);

    Assertions.assertFalse(lineString.isEmpty(), "LineString should not be empty");
    Assertions.assertEquals(2, lineString.getNumPoints(), "Number of points");
  }

  @Test
  public void testLineStringRoundTrip() {
    // Create a LineString
    List<Point> points = Arrays.asList(
        new Point(new double[]{1.0, 2.0}, 0),
        new Point(new double[]{3.0, 4.0}, 0),
        new Point(new double[]{5.0, 6.0}, 0)
    );
    LineString original = new LineString(points, 0, false, false);

    // Write to WKB
    WkbWriter writer = new WkbWriter();
    byte[] wkb = writer.write(original);

    // Read back
    WkbReader reader = new WkbReader();
    GeometryModel parsed = reader.read(wkb, 0);

    Assertions.assertTrue(parsed instanceof LineString, "Parsed geometry should be LineString");
    LineString parsedLS = (LineString) parsed;

    Assertions.assertEquals(original.getNumPoints(), parsedLS.getNumPoints(),
        "Number of points should match");

    // Verify coordinates match
    for (int i = 0; i < original.getNumPoints(); i++) {
      Assertions.assertEquals(original.getPoints().get(i).getX(),
          parsedLS.getPoints().get(i).getX(), 1e-10, "Point " + i + " X coordinate");
      Assertions.assertEquals(original.getPoints().get(i).getY(),
          parsedLS.getPoints().get(i).getY(), 1e-10, "Point " + i + " Y coordinate");
    }
  }

  @Test
  public void testLineStringWithNegativeCoordinates() {
    // LINESTRING(-10 -20, -5 -15, 0 0)
    List<Point> points = Arrays.asList(
        new Point(new double[]{-10.0, -20.0}, 0),
        new Point(new double[]{-5.0, -15.0}, 0),
        new Point(new double[]{0.0, 0.0}, 0)
    );
    LineString lineString = new LineString(points, 0, false, false);

    Assertions.assertEquals(-10.0, lineString.getPoints().get(0).getX(), 1e-10, "First point X");
    Assertions.assertEquals(-20.0, lineString.getPoints().get(0).getY(), 1e-10, "First point Y");
  }

  @Test
  public void testLineStringToString() {
    List<Point> points = Arrays.asList(
        new Point(new double[]{0.0, 0.0}, 0),
        new Point(new double[]{1.0, 1.0}, 0)
    );
    LineString lineString = new LineString(points, 0, false, false);

    String str = lineString.toString();
    Assertions.assertTrue(str.contains("LINESTRING"), "toString should contain LINESTRING");
    Assertions.assertFalse(str.contains("EMPTY"),
        "toString should not contain EMPTY for non-empty linestring");

    LineString emptyLS = new LineString(Arrays.asList(), 0, false, false);
    String emptyStr = emptyLS.toString();
    Assertions.assertTrue(emptyStr.contains("EMPTY"),
        "toString should contain EMPTY for empty linestring");
  }
}
