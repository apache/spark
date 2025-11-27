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

/**
 * Test suite for WKB error handling and edge cases.
 */
public class WkbErrorHandlingTest {

  private byte[] hexToBytes(String hex) {
    int len = hex.length();
    byte[] data = new byte[len / 2];
    for (int i = 0; i < len; i += 2) {
      data[i / 2] = (byte) ((Character.digit(hex.charAt(i), 16) << 4)
          + Character.digit(hex.charAt(i + 1), 16));
    }
    return data;
  }

  @Test
  public void testEmptyWkb() {
    byte[] emptyWkb = new byte[0];
    Assertions.assertThrows(WkbParseException.class, () -> Geometry.fromWkb(emptyWkb));
  }

  @Test
  public void testTooShortWkb() {
    // Only endianness byte
    byte[] tooShort = {0x01};
    Assertions.assertThrows(WkbParseException.class, () -> Geometry.fromWkb(tooShort));
  }

  @Test
  public void testInvalidByteOrder() {
    // Invalid byte order value (should be 0 or 1)
    byte[] invalidByteOrder = {0x02, 0x01, 0x00, 0x00, 0x00};
    Assertions.assertThrows(WkbParseException.class, () -> Geometry.fromWkb(invalidByteOrder));
  }

  @Test
  public void testInvalidGeometryType() {
    // Type = 99 (invalid)
    byte[] invalidType = hexToBytes("0163000000000000000000f03f0000000000000040");
    Assertions.assertThrows(WkbParseException.class, () -> Geometry.fromWkb(invalidType));
  }

  @Test
  public void testInvalidGeometryTypeZero() {
    // Type = 0 (invalid, should be 1-7)
    byte[] invalidType = hexToBytes("0100000000000000000000f03f0000000000000040");
    Assertions.assertThrows(WkbParseException.class, () -> Geometry.fromWkb(invalidType));
  }

  @Test
  public void testTruncatedPointCoordinates() {
    // Point WKB with truncated coordinates
    byte[] truncated = hexToBytes("0101000000000000000000f03f");  // Missing Y coordinate
    Assertions.assertThrows(WkbParseException.class, () -> Geometry.fromWkb(truncated));
  }

  @Test
  public void testTruncatedLineString() {
    // LineString with declared 2 points but only 1 provided
    byte[] truncated = hexToBytes(
        "010200000002000000" +  // LineString with 2 points
        "0000000000000000" +     // X of first point
        "0000000000000000"       // Y of first point (missing second point)
    );
    Assertions.assertThrows(WkbParseException.class, () -> Geometry.fromWkb(truncated));
  }

  @Test
  public void testValidationLevels() {
    // With validation level 0, invalid geometries might be accepted
    // With validation level 1 (default), they should be rejected

    // Create a ring with too few points (less than 4)
    java.util.List<Point> tooFewPoints = Arrays.asList(
        new Point(new double[]{0.0, 0.0}, 0),
        new Point(new double[]{1.0, 0.0}, 0),
        new Point(new double[]{0.0, 0.0}, 0)  // Only 3 points
    );
    Ring invalidRing = new Ring(tooFewPoints);

    // This ring should not pass isClosed() check since it has fewer than 4 points
    Assertions.assertFalse(invalidRing.isClosed(),
        "Ring with fewer than 4 points should not be closed");
  }

  @Test
  public void testRingWithTooFewPoints() {
    // Try to parse a polygon with a ring that has fewer than 4 points
    // This should fail with validation level > 0
    WkbReader reader = new WkbReader(false, 1);  // validation level = 1

    // Manually construct WKB for polygon with invalid ring (3 points)
    byte[] invalidPolygon = hexToBytes(
        "01" +           // Little endian
        "03000000" +     // Polygon type
        "01000000" +     // 1 ring
        "03000000" +     // 3 points (invalid, should be >= 4)
        "0000000000000000" + "0000000000000000" +  // Point 1
        "000000000000f03f" + "0000000000000000" +  // Point 2
        "0000000000000000" + "0000000000000000"    // Point 3
    );

    Assertions.assertThrows(WkbParseException.class, () -> reader.read(invalidPolygon));
  }

  @Test
  public void testNonClosedRing() {
    // Try to parse a polygon with a non-closed ring with validation
    WkbReader reader = new WkbReader(false, 1);

    // Polygon with ring where first and last points don't match
    byte[] nonClosedRing = hexToBytes(
        "01" +           // Little endian
        "03000000" +     // Polygon type
        "01000000" +     // 1 ring
        "04000000" +     // 4 points
        "0000000000000000" + "0000000000000000" +  // (0, 0)
        "000000000000f03f" + "0000000000000000" +  // (1, 0)
        "000000000000f03f" + "000000000000f03f" +  // (1, 1)
        "0000000000000040" + "0000000000000040"    // (2, 2) - doesn't match first point!
    );

    Assertions.assertThrows(WkbParseException.class, () -> reader.read(nonClosedRing));
  }

  @Test
  public void testNullByteArray() {
    Assertions.assertThrows(WkbParseException.class, () -> Geometry.fromWkb(null),
        "Should throw WKBParseException for null byte array");
  }

  @Test
  public void testGeometryEquality() {
    Point p1 = new Point(new double[]{1.0, 2.0}, 0);
    Point p2 = new Point(new double[]{1.0, 2.0}, 0);
    Point p3 = new Point(new double[]{1.0, 2.0}, 4326);

    // Points with same coordinates should have same coordinates
    Assertions.assertEquals(p1.getX(), p2.getX(), 1e-10, "X coordinates should match");
    Assertions.assertEquals(p1.getY(), p2.getY(), 1e-10, "Y coordinates should match");

    // SRID should be preserved
    Assertions.assertNotEquals(p1.srid(), p3.srid(), "SRIDs should differ");
  }

  @Test
  public void testGeometryWkbRoundTrip() {
    // Create geometry, convert to WKB, parse back, compare
    Point original = new Point(new double[]{123.456, 78.9}, 0);

    WkbWriter writer = new WkbWriter();
    byte[] wkb = writer.write(original);

    Geometry parsed = Geometry.fromWkb(wkb);
    Point parsedPoint = parsed.asPoint();

    Assertions.assertEquals(original.getX(), parsedPoint.getX(), 1e-10,
        "X should match after round trip");
    Assertions.assertEquals(original.getY(), parsedPoint.getY(), 1e-10,
        "Y should match after round trip");
  }
}

