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

/**
 * Test suite for WKB error handling and edge cases.
 */
public class WkbErrorHandlingTest extends WkbTestBase {

  /**
   * Helper method to assert that parsing a WKB hex string throws WkbParseException
   * containing the expected error message and the original WKB hex.
   */
  private void assertParseError(String hex, String expectedMessagePart) {
    assertParseError(hex, expectedMessagePart, 1);
  }

  /**
   * Helper method to assert that parsing a WKB hex string throws WkbParseException
   * containing the expected error message and the original WKB hex, with specified
   * validation level.
   */
  private void assertParseError(String hex, String expectedMessagePart, int validationLevel) {
    byte[] wkb = hexToBytes(hex);
    WkbReader reader = new WkbReader(validationLevel);
    WkbParseException ex = Assertions.assertThrows(
        WkbParseException.class, () -> reader.read(wkb),
        "Should throw WkbParseException for WKB: " + hex);
    Assertions.assertTrue(ex.getMessage().toUpperCase().contains(hex.toUpperCase()),
        "Exception message should contain the WKB hex: " + hex + ", actual: " + ex.getMessage());
    if (expectedMessagePart != null && !expectedMessagePart.isEmpty()) {
      Assertions.assertTrue(
          ex.getMessage().toLowerCase().contains(expectedMessagePart.toLowerCase()),
          "Exception message should contain '" + expectedMessagePart + "', actual: " +
              ex.getMessage());
    }
  }

  @Test
  public void testEmptyWkb() {
    byte[] emptyWkb = new byte[0];
    WkbReader reader = new WkbReader();
    WkbParseException ex = Assertions.assertThrows(
      WkbParseException.class, () -> reader.read(emptyWkb));
    // Empty WKB produces empty hex string, so just verify exception was thrown
    Assertions.assertNotNull(ex.getMessage());
  }

  @Test
  public void testTooShortWkb() {
    // Only endianness byte
    String hex = "01";
    byte[] tooShort = hexToBytes(hex);
    WkbReader reader = new WkbReader();
    WkbParseException ex = Assertions.assertThrows(
      WkbParseException.class, () -> reader.read(tooShort));
    Assertions.assertTrue(ex.getMessage().toUpperCase().contains(hex.toUpperCase()),
      "Exception message should contain the WKB hex: " + hex);
  }

  @Test
  public void testInvalidGeometryTypeZero() {
    // Type = 0 (invalid, should be 1-7)
    String hex = "0100000000000000000000F03F0000000000000040";
    byte[] invalidType = hexToBytes(hex);
    WkbReader reader = new WkbReader();
    WkbParseException ex = Assertions.assertThrows(
      WkbParseException.class, () -> reader.read(invalidType));
    Assertions.assertTrue(ex.getMessage().toUpperCase().contains(hex.toUpperCase()),
      "Exception message should contain the WKB hex: " + hex);
  }

  @Test
  public void testTruncatedPointCoordinates() {
    // Point WKB with truncated coordinates (missing Y coordinate)
    String hex = "0101000000000000000000F03F";
    byte[] truncated = hexToBytes(hex);
    WkbReader reader = new WkbReader();
    WkbParseException ex = Assertions.assertThrows(
      WkbParseException.class, () -> reader.read(truncated));
    Assertions.assertTrue(ex.getMessage().toUpperCase().contains(hex.toUpperCase()),
      "Exception message should contain the WKB hex: " + hex);
  }

  @Test
  public void testTruncatedByte() {
    // Only one byte (FF) of the 4-byte INT field.
    String hex = "0102000000ff";
    byte[] truncated = hexToBytes(hex);
    WkbReader reader = new WkbReader();
    WkbParseException ex = Assertions.assertThrows(
      WkbParseException.class, () -> reader.read(truncated));
    Assertions.assertTrue(ex.getMessage().toUpperCase().contains(hex.toUpperCase()),
      "Exception message should contain the WKB hex: " + hex);
  }

  @Test
  public void testTruncatedLineString() {
    // LineString with declared 2 points but only 1 provided
    String hex = "010200000002000000" +  // LineString with 2 points
      "0000000000000000" +              // X of first point
      "0000000000000000";               // Y of first point (missing second point)
    byte[] truncated = hexToBytes(hex);
    WkbReader reader = new WkbReader();
    WkbParseException ex = Assertions.assertThrows(
      WkbParseException.class, () -> reader.read(truncated));
    Assertions.assertTrue(ex.getMessage().toUpperCase().contains(hex.toUpperCase()),
      "Exception message should contain the WKB hex: " + hex);
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
    WkbReader reader = new WkbReader(1);  // validation level = 1

    // Manually construct WKB for polygon with invalid ring (3 points)
    String hex = "01" +                                      // Little endian
      "03000000" +                                         // Polygon type
      "01000000" +                                         // 1 ring
      "03000000" +                                         // 3 points (invalid, should be >= 4)
      "0000000000000000" + "0000000000000000" +            // Point 1
      "000000000000F03F" + "0000000000000000" +            // Point 2
      "0000000000000000" + "0000000000000000";             // Point 3
    byte[] invalidPolygon = hexToBytes(hex);

    WkbParseException ex = Assertions.assertThrows(
      WkbParseException.class, () -> reader.read(invalidPolygon));
    Assertions.assertTrue(ex.getMessage().toUpperCase().contains(hex.toUpperCase()),
      "Exception message should contain the WKB hex: " + hex);
  }

  @Test
  public void testNonClosedRing() {
    // Try to parse a polygon with a non-closed ring with validation
    WkbReader reader = new WkbReader(1);

    // Polygon with ring where first and last points don't match
    String hex = "01" +                                      // Little endian
      "03000000" +                                         // Polygon type
      "01000000" +                                         // 1 ring
      "04000000" +                                         // 4 points
      "0000000000000000" + "0000000000000000" +            // (0, 0)
      "000000000000F03F" + "0000000000000000" +            // (1, 0)
      "000000000000F03F" + "000000000000F03F" +            // (1, 1)
      "0000000000000040" + "0000000000000040";             // (2, 2) - doesn't match first point!
    byte[] nonClosedRing = hexToBytes(hex);

    WkbParseException ex = Assertions.assertThrows(
      WkbParseException.class, () -> reader.read(nonClosedRing));
    Assertions.assertTrue(ex.getMessage().toUpperCase().contains(hex.toUpperCase()),
      "Exception message should contain the WKB hex: " + hex);
  }

  @Test
  public void testNullByteArray() {
    WkbReader reader = new WkbReader();
    WkbParseException ex = Assertions.assertThrows(
      WkbParseException.class, () -> reader.read(null),
      "Should throw WKBParseException for null byte array");
    // Null WKB cannot produce hex string, just verify exception was thrown
    Assertions.assertNotNull(ex.getMessage());
  }

  // ========== Invalid Byte Order Tests ==========

  @Test
  public void testInvalidByteOrder() {
    // Invalid byte order 18 (0x12)
    assertParseError("120200000000000000", "Invalid byte order"); // checkstyle.off: LineLength
  }

  @Test
  public void testInvalidByteOrderInNestedElement() {
    // MultiPoint with invalid byte order 170 (0xAA) for nested element
    assertParseError("010400000001000000AA0100000000000000000000000000000000000000", "Invalid byte order"); // checkstyle.off: LineLength
  }

  // ========== Invalid Geometry Type Tests ==========

  @Test
  public void testInvalidGeometryType() {
    // Type 15 is not a valid geometry type
    assertParseError("010f00000000000000", "Invalid or unsupported type 15"); // checkstyle.off: LineLength
  }

  @Test
  public void testInvalidTypeInMultiPoint() {
    // MultiPoint containing element with invalid type 255
    assertParseError("01040000000100000001FF00000000000000000000000000000000000000", "Invalid or unsupported type 255"); // checkstyle.off: LineLength
    assertParseError("01040000000100000000000000ff00000000000000000000000000000000", "Invalid or unsupported type 255"); // checkstyle.off: LineLength
    assertParseError("00000000040000000100000000ff00000000000000000000000000000000", "Invalid or unsupported type 255"); // checkstyle.off: LineLength
    assertParseError("00000000040000000101ff00000000000000000000000000000000000000", "Invalid or unsupported type 255"); // checkstyle.off: LineLength
  }

  @Test
  public void testInvalidTypeInMultiLineString() {
    // MultiLineString containing element with invalid type 255
    assertParseError("01050000000100000001FF00000000000000", "Invalid or unsupported type 255"); // checkstyle.off: LineLength
    assertParseError("01050000000100000000000000FF00000000", "Invalid or unsupported type 255"); // checkstyle.off: LineLength
    assertParseError("00000000050000000101FF00000000000000", "Invalid or unsupported type 255"); // checkstyle.off: LineLength
    assertParseError("00000000050000000100000000FF00000000", "Invalid or unsupported type 255"); // checkstyle.off: LineLength
  }

  @Test
  public void testInvalidTypeInMultiPolygon() {
    // MultiPolygon containing element with invalid type 255
    assertParseError("01060000000100000001FF00000000000000", "Invalid or unsupported type 255"); // checkstyle.off: LineLength
    assertParseError("01060000000100000000000000FF00000000", "Invalid or unsupported type 255"); // checkstyle.off: LineLength
    assertParseError("00000000060000000101FF00000000000000", "Invalid or unsupported type 255"); // checkstyle.off: LineLength
    assertParseError("00000000060000000100000000FF00000000", "Invalid or unsupported type 255"); // checkstyle.off: LineLength
  }

  @Test
  public void testInvalidTypeInGeometryCollection() {
    // GeometryCollection containing element with invalid type 255
    assertParseError("01070000000100000001FF00000000000000", "Invalid or unsupported type 255"); // checkstyle.off: LineLength
  }

  // ========== Dimension Mismatch Tests - MultiPoint ==========

  @Test
  public void testMultiPointDimensionMismatch() {
    // 2D MultiPoint containing Z/M/ZM elements
    assertParseError("01040000000100000001e903000000000000000000000000000000000000", "Invalid or unsupported type 1001"); // checkstyle.off: LineLength
    assertParseError("01040000000100000001d107000000000000000000000000000000000000", "Invalid or unsupported type 2001"); // checkstyle.off: LineLength
    assertParseError("01040000000100000001b90b000000000000000000000000000000000000", "Invalid or unsupported type 3001"); // checkstyle.off: LineLength
  }

  @Test
  public void testMultiPointZDimensionMismatch() {
    // Z MultiPoint containing 2D/M/ZM elements
    assertParseError("01ec03000001000000010100000000000000000000000000000000000000", "Invalid or unsupported type 1"); // checkstyle.off: LineLength
    assertParseError("01ec0300000100000001d107000000000000000000000000000000000000", "Invalid or unsupported type 2001"); // checkstyle.off: LineLength
    assertParseError("01ec0300000100000001b90b000000000000000000000000000000000000", "Invalid or unsupported type 3001"); // checkstyle.off: LineLength
  }

  @Test
  public void testMultiPointMDimensionMismatch() {
    // M MultiPoint containing 2D/Z/ZM elements
    assertParseError("01d407000001000000010100000000000000000000000000000000000000", "Invalid or unsupported type 1"); // checkstyle.off: LineLength
    assertParseError("01d40700000100000001e903000000000000000000000000000000000000", "Invalid or unsupported type 1001"); // checkstyle.off: LineLength
    assertParseError("01d40700000100000001b90b000000000000000000000000000000000000", "Invalid or unsupported type 3001"); // checkstyle.off: LineLength
  }

  @Test
  public void testMultiPointZMDimensionMismatch() {
    // ZM MultiPoint containing 2D/Z/M elements
    assertParseError("01bc0b000001000000010100000000000000000000000000000000000000", "Invalid or unsupported type 1"); // checkstyle.off: LineLength
    assertParseError("01bc0b00000100000001e903000000000000000000000000000000000000", "Invalid or unsupported type 1001"); // checkstyle.off: LineLength
    assertParseError("01bc0b00000100000001d107000000000000000000000000000000000000", "Invalid or unsupported type 2001"); // checkstyle.off: LineLength
  }

  // ========== Dimension Mismatch Tests - MultiLineString ==========

  @Test
  public void testMultiLineStringDimensionMismatch() {
    // 2D MultiLineString containing Z/M/ZM elements
    assertParseError("01050000000100000001ea03000000000000", "Invalid or unsupported type 1002"); // checkstyle.off: LineLength
    assertParseError("01050000000100000001d207000000000000", "Invalid or unsupported type 2002"); // checkstyle.off: LineLength
    assertParseError("01050000000100000001ba0b000000000000", "Invalid or unsupported type 3002"); // checkstyle.off: LineLength
  }

  @Test
  public void testMultiLineStringZDimensionMismatch() {
    // Z MultiLineString containing 2D/M/ZM elements
    assertParseError("01ed03000001000000010200000000000000", "Invalid or unsupported type 2"); // checkstyle.off: LineLength
    assertParseError("01ed0300000100000001d207000000000000", "Invalid or unsupported type 2002"); // checkstyle.off: LineLength
    assertParseError("01ed0300000100000001ba0b000000000000", "Invalid or unsupported type 3002"); // checkstyle.off: LineLength
  }

  @Test
  public void testMultiLineStringMDimensionMismatch() {
    // M MultiLineString containing 2D/Z/ZM elements
    assertParseError("01d507000001000000010200000000000000", "Invalid or unsupported type 2"); // checkstyle.off: LineLength
    assertParseError("01d50700000100000001ea03000000000000", "Invalid or unsupported type 1002"); // checkstyle.off: LineLength
    assertParseError("01d50700000100000001ba0b000000000000", "Invalid or unsupported type 3002"); // checkstyle.off: LineLength
  }

  @Test
  public void testMultiLineStringZMDimensionMismatch() {
    // ZM MultiLineString containing 2D/Z/M elements
    assertParseError("01bd0b000001000000010200000000000000", "Invalid or unsupported type 2"); // checkstyle.off: LineLength
    assertParseError("01bd0b00000100000001ea03000000000000", "Invalid or unsupported type 1002"); // checkstyle.off: LineLength
    assertParseError("01bd0b00000100000001d207000000000000", "Invalid or unsupported type 2002"); // checkstyle.off: LineLength
  }

  // ========== Dimension Mismatch Tests - MultiPolygon ==========

  @Test
  public void testMultiPolygonDimensionMismatch() {
    // 2D MultiPolygon containing Z/M/ZM elements
    assertParseError("01060000000100000001eb03000000000000", "Invalid or unsupported type 1003"); // checkstyle.off: LineLength
    assertParseError("01060000000100000001d307000000000000", "Invalid or unsupported type 2003"); // checkstyle.off: LineLength
    assertParseError("01060000000100000001bb0b000000000000", "Invalid or unsupported type 3003"); // checkstyle.off: LineLength
  }

  @Test
  public void testMultiPolygonZDimensionMismatch() {
    // Z MultiPolygon containing 2D/M/ZM elements
    assertParseError("01ee03000001000000010300000000000000", "Invalid or unsupported type 3"); // checkstyle.off: LineLength
    assertParseError("01ee0300000100000001d307000000000000", "Invalid or unsupported type 2003"); // checkstyle.off: LineLength
    assertParseError("01ee0300000100000001bb0b000000000000", "Invalid or unsupported type 3003"); // checkstyle.off: LineLength
  }

  @Test
  public void testMultiPolygonMDimensionMismatch() {
    // M MultiPolygon containing 2D/Z/ZM elements
    assertParseError("01d607000001000000010300000000000000", "Invalid or unsupported type 3"); // checkstyle.off: LineLength
    assertParseError("01d60700000100000001eb03000000000000", "Invalid or unsupported type 1003"); // checkstyle.off: LineLength
    assertParseError("01d60700000100000001bb0b000000000000", "Invalid or unsupported type 3003"); // checkstyle.off: LineLength
  }

  @Test
  public void testMultiPolygonZMDimensionMismatch() {
    // ZM MultiPolygon containing 2D/Z/M elements
    assertParseError("01be0b000001000000010300000000000000", "Invalid or unsupported type 3"); // checkstyle.off: LineLength
    assertParseError("01be0b00000100000001eb03000000000000", "Invalid or unsupported type 1003"); // checkstyle.off: LineLength
    assertParseError("01be0b00000100000001d307000000000000", "Invalid or unsupported type 2003"); // checkstyle.off: LineLength
  }

  // ========== Dimension Mismatch Tests - GeometryCollection ==========

  @Test
  public void testGeometryCollectionDimensionMismatch() {
    // 2D GeometryCollection containing Z/M/ZM elements
    assertParseError("01070000000100000001ef03000000000000", "Invalid or unsupported type 1007"); // checkstyle.off: LineLength
    assertParseError("01070000000100000001d707000000000000", "Invalid or unsupported type 2007"); // checkstyle.off: LineLength
    assertParseError("01070000000100000001bf0b000000000000", "Invalid or unsupported type 3007"); // checkstyle.off: LineLength
  }

  @Test
  public void testGeometryCollectionZDimensionMismatch() {
    // Z GeometryCollection containing 2D/M/ZM elements
    assertParseError("01ef03000001000000010700000000000000", "Invalid or unsupported type 7"); // checkstyle.off: LineLength
    assertParseError("01ef0300000100000001d707000000000000", "Invalid or unsupported type 2007"); // checkstyle.off: LineLength
    assertParseError("01ef0300000100000001bf0b000000000000", "Invalid or unsupported type 3007"); // checkstyle.off: LineLength
  }

  @Test
  public void testGeometryCollectionMDimensionMismatch() {
    // M GeometryCollection containing 2D/Z/ZM elements
    assertParseError("01d707000001000000010700000000000000", "Invalid or unsupported type 7"); // checkstyle.off: LineLength
    assertParseError("01d70700000100000001ef03000000000000", "Invalid or unsupported type 1007"); // checkstyle.off: LineLength
    assertParseError("01d70700000100000001bf0b000000000000", "Invalid or unsupported type 3007"); // checkstyle.off: LineLength
  }

  @Test
  public void testGeometryCollectionZMDimensionMismatch() {
    // ZM GeometryCollection containing 2D/Z/M elements
    assertParseError("01bf0b000001000000010700000000000000", "Invalid or unsupported type 7"); // checkstyle.off: LineLength
    assertParseError("01bf0b00000100000001ef03000000000000", "Invalid or unsupported type 1007"); // checkstyle.off: LineLength
    assertParseError("01bf0b00000100000001d707000000000000", "Invalid or unsupported type 2007"); // checkstyle.off: LineLength
  }

  @Test
  public void testEwkbRejected() {
    // EWKB format (with SRID flag in type) should be rejected
    assertParseError("0101000080000000000000f03f00000000000010400000000000002240", "Invalid or unsupported type -2147483647"); // checkstyle.off: LineLength
  }
}
