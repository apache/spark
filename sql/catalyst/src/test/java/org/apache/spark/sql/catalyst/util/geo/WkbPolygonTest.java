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
 * Test suite for WKB Polygon geometries.
 */
public class WkbPolygonTest {

  @Test
  public void testSimplePolygon() {
    // POLYGON((0 0, 4 0, 4 4, 0 4, 0 0))
    List<Point> ringPoints = Arrays.asList(
        new Point(new double[]{0.0, 0.0}, 0),
        new Point(new double[]{4.0, 0.0}, 0),
        new Point(new double[]{4.0, 4.0}, 0),
        new Point(new double[]{0.0, 4.0}, 0),
        new Point(new double[]{0.0, 0.0}, 0)
    );
    Ring ring = new Ring(ringPoints);
    Polygon polygon = new Polygon(Arrays.asList(ring), 0, false, false);

    Assertions.assertFalse(polygon.isEmpty(), "Polygon should not be empty");
    Assertions.assertEquals(1, polygon.getRings().size(), "Should have one ring");
    Assertions.assertTrue(ring.isClosed(), "Ring should be closed");
    Assertions.assertEquals(5, ring.getNumPoints(), "Ring should have 5 points");
  }

  @Test
  public void testEmptyPolygon() {
    // POLYGON EMPTY
    Polygon polygon = new Polygon(Arrays.asList(), 0, false, false);

    Assertions.assertTrue(polygon.isEmpty(), "Polygon should be empty");
    Assertions.assertEquals(0, polygon.getRings().size(), "Should have no rings");
  }

  @Test
  public void testPolygonWithHole() {
    // POLYGON((0 0, 10 0, 10 10, 0 10, 0 0), (2 2, 8 2, 8 8, 2 8, 2 2))

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

    Assertions.assertFalse(polygon.isEmpty(), "Polygon should not be empty");
    Assertions.assertEquals(2, polygon.getRings().size(), "Should have two rings");
    Assertions.assertEquals(1, polygon.getNumInteriorRings(), "Should have one interior ring");
    Assertions.assertSame(exteriorRing, polygon.getExteriorRing(), "Exterior ring should match");
    Assertions.assertSame(interiorRing, polygon.getInteriorRingN(0), "Interior ring should match");
  }

  @Test
  public void testPolygonRoundTrip() {
    // Create a polygon
    List<Point> ringPoints = Arrays.asList(
        new Point(new double[]{0.0, 0.0}, 0),
        new Point(new double[]{5.0, 0.0}, 0),
        new Point(new double[]{5.0, 5.0}, 0),
        new Point(new double[]{0.0, 5.0}, 0),
        new Point(new double[]{0.0, 0.0}, 0)
    );
    Ring ring = new Ring(ringPoints);
    Polygon original = new Polygon(Arrays.asList(ring), 0, false, false);

    // Write to WKB
    WkbWriter writer = new WkbWriter();
    byte[] wkb = writer.write(original);

    // Read back
    WkbReader reader = new WkbReader();
    GeometryModel parsed = reader.read(wkb, 0);

    Assertions.assertTrue(parsed instanceof Polygon, "Parsed geometry should be Polygon");
    Polygon parsedPoly = (Polygon) parsed;

    Assertions.assertEquals(original.getRings().size(), parsedPoly.getRings().size(),
        "Number of rings should match");
    Assertions.assertEquals(original.getExteriorRing().getNumPoints(),
        parsedPoly.getExteriorRing().getNumPoints(),
        "Number of points in ring should match");
  }

  @Test
  public void testRingIsClosed() {
    // Closed ring
    List<Point> closedPoints = Arrays.asList(
        new Point(new double[]{0.0, 0.0}, 0),
        new Point(new double[]{1.0, 0.0}, 0),
        new Point(new double[]{1.0, 1.0}, 0),
        new Point(new double[]{0.0, 1.0}, 0),
        new Point(new double[]{0.0, 0.0}, 0)
    );
    Ring closedRing = new Ring(closedPoints);
    Assertions.assertTrue(closedRing.isClosed(), "Ring should be closed");

    // Non-closed ring (fewer than 4 points)
    List<Point> openPoints = Arrays.asList(
        new Point(new double[]{0.0, 0.0}, 0),
        new Point(new double[]{1.0, 0.0}, 0),
        new Point(new double[]{1.0, 1.0}, 0)
    );
    Ring openRing = new Ring(openPoints);
    Assertions.assertFalse(openRing.isClosed(),
        "Ring with fewer than 4 points should not be closed");

    // Ring where first and last points don't match
    List<Point> notClosedPoints = Arrays.asList(
        new Point(new double[]{0.0, 0.0}, 0),
        new Point(new double[]{1.0, 0.0}, 0),
        new Point(new double[]{1.0, 1.0}, 0),
        new Point(new double[]{0.0, 1.0}, 0),
        new Point(new double[]{0.1, 0.1}, 0)  // Different from first point
    );
    Ring notClosedRing = new Ring(notClosedPoints);
    Assertions.assertFalse(notClosedRing.isClosed(),
        "Ring where first and last points differ should not be closed");
  }

  @Test
  public void testTrianglePolygon() {
    // POLYGON((0 0, 1 0, 0.5 0.866, 0 0))
    List<Point> trianglePoints = Arrays.asList(
        new Point(new double[]{0.0, 0.0}, 0),
        new Point(new double[]{1.0, 0.0}, 0),
        new Point(new double[]{0.5, 0.866}, 0),
        new Point(new double[]{0.0, 0.0}, 0)
    );
    Ring ring = new Ring(trianglePoints);
    Polygon triangle = new Polygon(Arrays.asList(ring), 0, false, false);

    Assertions.assertEquals(4, triangle.getExteriorRing().getNumPoints(),
        "Triangle should have 4 points (including closing point)");
    Assertions.assertTrue(ring.isClosed(), "Triangle ring should be closed");
  }
}
