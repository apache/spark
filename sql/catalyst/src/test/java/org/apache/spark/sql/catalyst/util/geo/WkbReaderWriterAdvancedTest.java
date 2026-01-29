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

import org.apache.spark.sql.catalyst.util.Geometry;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteOrder;
import java.util.List;

/**
 * Test suite for WKB (Well-Known Binary) reader and writer functionality.
 *
 * These tests verify WKB round-trip (read/write) for various geometry types
 * in different dimensions (2D, 3DZ, 3DM, 4D).
 */
public class WkbReaderWriterAdvancedTest extends WkbTestBase {

  /**
   * Test helper to verify WKB parsing from hex string in both byte orders
   */
  private void checkWkbParsing(String wkbHexLittle, String wkbHexBig, GeoTypeId expectedType,
      boolean expectedEmpty) {
    checkWkbParsing(wkbHexLittle, wkbHexBig, expectedType, expectedEmpty, false);
  }

  /**
   * Test helper to verify WKB parsing from hex string in both byte orders
   */
  private void checkWkbParsing(String wkbHexLittle, String wkbHexBig, GeoTypeId expectedType,
      boolean expectedEmpty, boolean expectedValidateError) {
    // Parse little endian
    byte[] wkbLittle = hexToBytes(wkbHexLittle);
    WkbReader noValidateReader = new WkbReader(0);
    WkbReader validateReader = new WkbReader();

    GeometryModel geomLittle;
    if (expectedValidateError) {
      WkbParseException ex = Assertions.assertThrows(
        WkbParseException.class, () -> validateReader.read(wkbLittle, 0));
      Assertions.assertTrue(ex.getMessage().toUpperCase().contains(wkbHexLittle.toUpperCase()),
        "Exception message should contain the WKB hex: " + wkbHexLittle);
      geomLittle = noValidateReader.read(wkbLittle, 0);
    } else {
      geomLittle = validateReader.read(wkbLittle, 0);
    }

    Assertions.assertEquals(expectedType, geomLittle.getTypeId(),
        "Geometry type mismatch (little endian)");
    Assertions.assertEquals(expectedEmpty, geomLittle.isEmpty(),
        "Empty status mismatch (little endian)");

    // Parse big endian
    byte[] wkbBig = hexToBytes(wkbHexBig);
    GeometryModel geomBig;
    if (expectedValidateError) {
      WkbParseException ex = Assertions.assertThrows(
        WkbParseException.class, () -> validateReader.read(wkbBig, 0));
      Assertions.assertTrue(ex.getMessage().toUpperCase().contains(wkbHexBig.toUpperCase()),
        "Exception message should contain the WKB hex: " + wkbHexBig);
      geomBig = noValidateReader.read(wkbBig, 0);
    } else {
      geomBig = validateReader.read(wkbBig, 0);
    }
    Assertions.assertEquals(expectedType, geomBig.getTypeId(),
        "Geometry type mismatch (big endian)");
    Assertions.assertEquals(expectedEmpty, geomBig.isEmpty(),
        "Empty status mismatch (big endian)");
  }

  /**
   * Test helper to verify WKB round-trip (write and read back)
   */
  private void checkWkbRoundTrip(String wkbHexLittle, String wkbHexBig) {
    byte[] wkbLittle = hexToBytes(wkbHexLittle);
    byte[] wkbBig = hexToBytes(wkbHexBig);

    // Parse the WKB (little)
    WkbReader reader = new WkbReader();
    GeometryModel model = reader.read(wkbLittle, 0);
    WkbWriter writer = new WkbWriter();
    byte[] writtenLittleFromModelLittle = writer.write(model, ByteOrder.LITTLE_ENDIAN);
    byte[] writtenBigFromModelLittle = writer.write(model, ByteOrder.BIG_ENDIAN);
    Assertions.assertEquals(wkbHexLittle, bytesToHex(writtenLittleFromModelLittle),
        "WKB little endian round-trip failed");
    Assertions.assertEquals(wkbHexBig, bytesToHex(writtenBigFromModelLittle),
        "WKB big endian round-trip failed");

    // Parse the WKB (big)
    GeometryModel geomFromBig = reader.read(wkbBig, 0);
    byte[] writtenLittleFromModelBig = writer.write(geomFromBig, ByteOrder.LITTLE_ENDIAN);
    byte[] writtenBigFromModelBig = writer.write(geomFromBig, ByteOrder.BIG_ENDIAN);
    Assertions.assertEquals(wkbHexLittle, bytesToHex(writtenLittleFromModelBig),
      "WKB little endian round-trip from big endian failed");
    Assertions.assertEquals(wkbHexBig, bytesToHex(writtenBigFromModelBig),
      "WKB big endian round-trip from big endian failed");

    // Use Geometry.fromWkb (little)
    Geometry geometryFromLittle = Geometry.fromWkb(wkbLittle, 0);
    byte[] wkbLittleFromGeometryLittle = geometryFromLittle.toWkb(ByteOrder.LITTLE_ENDIAN);
    Assertions.assertEquals(wkbHexLittle, bytesToHex(wkbLittleFromGeometryLittle),
        "Geometry.fromWKB little endian round-trip failed");
    byte[] wkbBigFromGeometryLittle = geometryFromLittle.toWkb(ByteOrder.BIG_ENDIAN);
    Assertions.assertEquals(wkbHexBig, bytesToHex(wkbBigFromGeometryLittle),
        "Geometry.fromWKB big endian round-trip failed");

    // Use Geometry.fromWkb (big)
    Geometry geometryFromBig = Geometry.fromWkb(writtenBigFromModelLittle, 0);
    byte[] wkbLittleFromGeometryBig = geometryFromBig.toWkb(ByteOrder.LITTLE_ENDIAN);
    Assertions.assertEquals(wkbHexLittle, bytesToHex(wkbLittleFromGeometryBig),
        "Geometry.fromWKB little endian round-trip from big endian failed");
    byte[] wkbBigFromGeometryBig = geometryFromBig.toWkb(ByteOrder.BIG_ENDIAN);
    Assertions.assertEquals(wkbHexBig, bytesToHex(wkbBigFromGeometryBig),
        "Geometry.fromWKB big endian round-trip from big endian failed");

  }

  // ========== Point Tests (2D) ==========

  @Test
  public void testPoint2DEmpty() {
    // WKT: POINT EMPTY
    String wkbLe = "0101000000000000000000f87f000000000000f87f";
    String wkbBe = "00000000017ff80000000000007ff8000000000000";
    checkWkbParsing(wkbLe, wkbBe, GeoTypeId.POINT, true);
    checkWkbRoundTrip(wkbLe, wkbBe);
  }

  @Test
  public void testPoint2D_1_2() {
    // WKT: POINT(1 2)
    String wkbLe = "0101000000000000000000f03f0000000000000040";
    String wkbBe = "00000000013ff00000000000004000000000000000";
    checkWkbParsing(wkbLe, wkbBe, GeoTypeId.POINT, false);
    checkWkbRoundTrip(wkbLe, wkbBe);

    // Verify coordinates
    byte[] wkb = hexToBytes(wkbLe);
    WkbReader reader = new WkbReader();
    Point point = (Point) reader.read(wkb, 0);
    Assertions.assertEquals(1.0, point.getX(), 0.0001);
    Assertions.assertEquals(2.0, point.getY(), 0.0001);
  }

  @Test
  public void testPoint2D_Negative180_0() {
    // WKT: POINT(-180 0)
    String wkbLe = "010100000000000000008066c00000000000000000";
    String wkbBe = "0000000001c0668000000000000000000000000000";
    checkWkbParsing(wkbLe, wkbBe, GeoTypeId.POINT, false);
    checkWkbRoundTrip(wkbLe, wkbBe);

    byte[] wkb = hexToBytes(wkbLe);
    WkbReader reader = new WkbReader();
    Point point = (Point) reader.read(wkb, 0);
    Assertions.assertEquals(-180.0, point.getX(), 0.0001);
    Assertions.assertEquals(0.0, point.getY(), 0.0001);
  }

  @Test
  public void testPoint2D_0_Negative90() {
    // WKT: POINT(0 -90)
    String wkbLe = "0101000000000000000000000000000000008056c0";
    String wkbBe = "00000000010000000000000000c056800000000000";
    checkWkbParsing(wkbLe, wkbBe, GeoTypeId.POINT, false);
    checkWkbRoundTrip(wkbLe, wkbBe);
  }

  @Test
  public void testPoint2D_0_90() {
    // WKT: POINT(0 90)
    String wkbLe = "010100000000000000000000000000000000805640";
    String wkbBe = "000000000100000000000000004056800000000000";
    checkWkbParsing(wkbLe, wkbBe, GeoTypeId.POINT, false);
    checkWkbRoundTrip(wkbLe, wkbBe);
  }

  @Test
  public void testPoint2D_Pi_E() {
    // WKT: POINT(3.141592653589793 2.718281828459045)
    String wkbLe = "0101000000182d4454fb2109406957148b0abf0540";
    String wkbBe = "0000000001400921fb54442d184005bf0a8b145769";
    checkWkbParsing(wkbLe, wkbBe, GeoTypeId.POINT, false);
    checkWkbRoundTrip(wkbLe, wkbBe);

    byte[] wkb = hexToBytes(wkbLe);
    WkbReader reader = new WkbReader();
    Point point = (Point) reader.read(wkb, 0);
    Assertions.assertEquals(Math.PI, point.getX(), 0.0000001);
    Assertions.assertEquals(Math.E, point.getY(), 0.0000001);
  }

  // ========== Point Tests (3DZ) ==========

  @Test
  public void testPoint3DZEmpty() {
    // WKT: POINT Z EMPTY
    String wkbLe = "01e9030000000000000000f87f000000000000f87f000000000000f87f";
    String wkbBe = "00000003e97ff80000000000007ff80000000000007ff8000000000000";
    checkWkbParsing(wkbLe, wkbBe, GeoTypeId.POINT, true);
    checkWkbRoundTrip(wkbLe, wkbBe);
  }

  @Test
  public void testPoint3DZ_1_2_3() {
    // WKT: POINT Z (1 2 3)
    String wkbLe = "01e9030000000000000000f03f00000000000000400000000000000840";
    String wkbBe = "00000003e93ff000000000000040000000000000004008000000000000";
    checkWkbParsing(wkbLe, wkbBe, GeoTypeId.POINT, false);
    checkWkbRoundTrip(wkbLe, wkbBe);

    byte[] wkb = hexToBytes(wkbLe);
    WkbReader reader = new WkbReader();
    Point point = (Point) reader.read(wkb, 0);
    double[] coords = point.getCoordinates();
    Assertions.assertEquals(3, coords.length);
    Assertions.assertEquals(1.0, coords[0], 0.0001);
    Assertions.assertEquals(2.0, coords[1], 0.0001);
    Assertions.assertEquals(3.0, coords[2], 0.0001);
  }

  // ========== Point Tests (3DM) ==========

  @Test
  public void testPoint3DMEmpty() {
    // WKT: POINT M EMPTY
    String wkbLe = "01d1070000000000000000f87f000000000000f87f000000000000f87f";
    String wkbBe = "00000007d17ff80000000000007ff80000000000007ff8000000000000";
    checkWkbParsing(wkbLe, wkbBe, GeoTypeId.POINT, true);
    checkWkbRoundTrip(wkbLe, wkbBe);
  }

  @Test
  public void testPoint3DM_1_2_3() {
    // WKT: POINT M (1 2 3)
    String wkbLe = "01d1070000000000000000f03f00000000000000400000000000000840";
    String wkbBe = "00000007d13ff000000000000040000000000000004008000000000000";
    checkWkbParsing(wkbLe, wkbBe, GeoTypeId.POINT, false);
    checkWkbRoundTrip(wkbLe, wkbBe);
  }

  // ========== Point Tests (4D) ==========

  @Test
  public void testPoint4DEmpty() {
    // WKT: POINT ZM EMPTY
    String wkbLe = "01b90b0000000000000000f87f000000000000f87f000000000000f87f000000000000f87f";
    String wkbBe = "0000000bb97ff80000000000007ff80000000000007ff80000000000007ff8000000000000";
    checkWkbParsing(wkbLe, wkbBe, GeoTypeId.POINT, true);
    checkWkbRoundTrip(wkbLe, wkbBe);
  }

  @Test
  public void testPoint4D_1_2_3_4() {
    // WKT: POINT ZM (1 2 3 4)
    String wkbLe = "01b90b0000000000000000f03f000000000000004000000000000008400000000000001040";
    String wkbBe = "0000000bb93ff0000000000000400000000000000040080000000000004010000000000000";
    checkWkbParsing(wkbLe, wkbBe, GeoTypeId.POINT, false);
    checkWkbRoundTrip(wkbLe, wkbBe);

    byte[] wkb = hexToBytes(wkbLe);
    WkbReader reader = new WkbReader();
    Point point = (Point) reader.read(wkb, 0);
    double[] coords = point.getCoordinates();
    Assertions.assertEquals(4, coords.length);
    Assertions.assertEquals(1.0, coords[0], 0.0001);
    Assertions.assertEquals(2.0, coords[1], 0.0001);
    Assertions.assertEquals(3.0, coords[2], 0.0001);
    Assertions.assertEquals(4.0, coords[3], 0.0001);
  }

  // ========== LineString Tests (2D) ==========

  @Test
  public void testLineString2DEmpty() {
    // WKT: LINESTRING EMPTY
    String wkbLe = "010200000000000000";
    String wkbBe = "000000000200000000";
    checkWkbParsing(wkbLe, wkbBe, GeoTypeId.LINESTRING, true);
    checkWkbRoundTrip(wkbLe, wkbBe);
  }

  @Test
  public void testLineString2D_OnePoint() {
    // WKT: LINESTRING(1 2)
    String wkbLe = "010200000001000000000000000000f03f0000000000000040";
    String wkbBe = "0000000002000000013ff00000000000004000000000000000";
    checkWkbParsing(wkbLe, wkbBe, GeoTypeId.LINESTRING, false, true);
    // Invalid with too few points in linestring
  }

  @Test
  public void testLineString2D_TwoPoints() {
    // WKT: LINESTRING(1 2,3 4)
    String wkbLe = "010200000002000000000000000000f03f000000000000004000000000000008400000000000001040"; // checkstyle.off: LineLength
    String wkbBe = "0000000002000000023ff0000000000000400000000000000040080000000000004010000000000000"; // checkstyle.off: LineLength
    checkWkbParsing(wkbLe, wkbBe, GeoTypeId.LINESTRING, false);
    checkWkbRoundTrip(wkbLe, wkbBe);

    byte[] wkb = hexToBytes(wkbLe);
    WkbReader reader = new WkbReader();
    LineString lineString = (LineString) reader.read(wkb, 0);
    Assertions.assertEquals(2, lineString.getNumPoints());
  }

  @Test
  public void testLineString2D_FivePoints() {
    // WKT: LINESTRING(0 1,1 0,2 -1,-1 -2,0 1)
    String wkbLe = "0102000000050000000000000000000000000000000000f03f000000000000f03f00000000000000000000000000000040000000000000f0bf000000000000f0bf00000000000000c00000000000000000000000000000f03f"; // checkstyle.off: LineLength
    String wkbBe = "00000000020000000500000000000000003ff00000000000003ff000000000000000000000000000004000000000000000bff0000000000000bff0000000000000c00000000000000000000000000000003ff0000000000000"; // checkstyle.off: LineLength
    checkWkbParsing(wkbLe, wkbBe, GeoTypeId.LINESTRING, false);
    checkWkbRoundTrip(wkbLe, wkbBe);
  }

  // ========== LineString Tests (3DZ) ==========

  @Test
  public void testLineString3DZEmpty() {
    // WKT: LINESTRING Z EMPTY
    String wkbLe = "01ea03000000000000";
    String wkbBe = "00000003ea00000000";
    checkWkbParsing(wkbLe, wkbBe, GeoTypeId.LINESTRING, true);
    checkWkbRoundTrip(wkbLe, wkbBe);
  }

  @Test
  public void testLineString3DZ_OnePoint() {
    // WKT: LINESTRING Z (1 2 3)
    String wkbLe = "01ea03000001000000000000000000f03f00000000000000400000000000000840";
    String wkbBe = "00000003ea000000013ff000000000000040000000000000004008000000000000";
    checkWkbParsing(wkbLe, wkbBe, GeoTypeId.LINESTRING, false, true);
    // Invalid with too few points in linestring
  }

  @Test
  public void testLineString3DZ_TwoPoints() {
    // WKT: LINESTRING Z (1 2 3,4 5 6)
    String wkbLe = "01ea03000002000000000000000000f03f00000000000000400000000000000840000000000000104000000000000014400000000000001840"; // checkstyle.off: LineLength
    String wkbBe = "00000003ea000000023ff000000000000040000000000000004008000000000000401000000000000040140000000000004018000000000000"; // checkstyle.off: LineLength
    checkWkbParsing(wkbLe, wkbBe, GeoTypeId.LINESTRING, false);
    checkWkbRoundTrip(wkbLe, wkbBe);
  }

  @Test
  public void testLineString3DZ_FivePoints() {
    // WKT: LINESTRING Z (0 1 -1,1 0 -2,2 -1 17,-1 -2 58,0 1 45)
    String wkbLe = "01ea030000050000000000000000000000000000000000f03f000000000000f0bf000000000000f03f000000000000000000000000000000c00000000000000040000000000000f0bf0000000000003140000000000000f0bf00000000000000c00000000000004d400000000000000000000000000000f03f0000000000804640"; // checkstyle.off: LineLength
    String wkbBe = "00000003ea0000000500000000000000003ff0000000000000bff00000000000003ff00000000000000000000000000000c0000000000000004000000000000000bff00000000000004031000000000000bff0000000000000c000000000000000404d00000000000000000000000000003ff00000000000004046800000000000"; // checkstyle.off: LineLength
    checkWkbParsing(wkbLe, wkbBe, GeoTypeId.LINESTRING, false);
    checkWkbRoundTrip(wkbLe, wkbBe);
  }

  // ========== LineString Tests (3DM) ==========

  @Test
  public void testLineString3DMEmpty() {
    // WKT: LINESTRING M EMPTY
    String wkbLe = "01d207000000000000";
    String wkbBe = "00000007d200000000";
    checkWkbParsing(wkbLe, wkbBe, GeoTypeId.LINESTRING, true);
    checkWkbRoundTrip(wkbLe, wkbBe);
  }

  @Test
  public void testLineString3DM_OnePoint() {
    // WKT: LINESTRING M (1 2 3)
    String wkbLe = "01d207000001000000000000000000f03f00000000000000400000000000000840";
    String wkbBe = "00000007d2000000013ff000000000000040000000000000004008000000000000";
    checkWkbParsing(wkbLe, wkbBe, GeoTypeId.LINESTRING, false, true);
    // Invalid with too few points in linestring
  }

  @Test
  public void testLineString3DM_TwoPoints() {
    // WKT: LINESTRING M (1 2 3,4 5 6)
    String wkbLe = "01d207000002000000000000000000f03f00000000000000400000000000000840000000000000104000000000000014400000000000001840"; // checkstyle.off: LineLength
    String wkbBe = "00000007d2000000023ff000000000000040000000000000004008000000000000401000000000000040140000000000004018000000000000"; // checkstyle.off: LineLength
    checkWkbParsing(wkbLe, wkbBe, GeoTypeId.LINESTRING, false);
    checkWkbRoundTrip(wkbLe, wkbBe);
  }

  @Test
  public void testLineString3DM_FivePoints() {
    // WKT: LINESTRING M (0 1 -1,1 0 -2,2 -1 17,-1 -2 58,0 1 45)
    String wkbLe = "01d2070000050000000000000000000000000000000000f03f000000000000f0bf000000000000f03f000000000000000000000000000000c00000000000000040000000000000f0bf0000000000003140000000000000f0bf00000000000000c00000000000004d400000000000000000000000000000f03f0000000000804640"; // checkstyle.off: LineLength
    String wkbBe = "00000007d20000000500000000000000003ff0000000000000bff00000000000003ff00000000000000000000000000000c0000000000000004000000000000000bff00000000000004031000000000000bff0000000000000c000000000000000404d00000000000000000000000000003ff00000000000004046800000000000"; // checkstyle.off: LineLength
    checkWkbParsing(wkbLe, wkbBe, GeoTypeId.LINESTRING, false);
    checkWkbRoundTrip(wkbLe, wkbBe);
  }

  // ========== LineString Tests (4D) ==========

  @Test
  public void testLineString4DEmpty() {
    // WKT: LINESTRING ZM EMPTY
    String wkbLe = "01ba0b000000000000";
    String wkbBe = "0000000bba00000000";
    checkWkbParsing(wkbLe, wkbBe, GeoTypeId.LINESTRING, true);
    checkWkbRoundTrip(wkbLe, wkbBe);
  }

  @Test
  public void testLineString4D_OnePoint() {
    // WKT: LINESTRING ZM (1 2 3 4)
    String wkbLe = "01ba0b000001000000000000000000f03f000000000000004000000000000008400000000000001040"; // checkstyle.off: LineLength
    String wkbBe = "0000000bba000000013ff0000000000000400000000000000040080000000000004010000000000000"; // checkstyle.off: LineLength
    checkWkbParsing(wkbLe, wkbBe, GeoTypeId.LINESTRING, false, true);
    // Invalid with too few points in linestring
  }

  @Test
  public void testLineString4D_TwoPoints() {
    // WKT: LINESTRING ZM (1 2 3 4,5 6 7 8)
    String wkbLe = "01ba0b000002000000000000000000f03f000000000000004000000000000008400000000000001040000000000000144000000000000018400000000000001c400000000000002040"; // checkstyle.off: LineLength
    String wkbBe = "0000000bba000000023ff000000000000040000000000000004008000000000000401000000000000040140000000000004018000000000000401c0000000000004020000000000000"; // checkstyle.off: LineLength
    checkWkbParsing(wkbLe, wkbBe, GeoTypeId.LINESTRING, false);
    checkWkbRoundTrip(wkbLe, wkbBe);
  }

  @Test
  public void testLineString4D_FivePoints() {
    // WKT: LINESTRING ZM (0 1 -1 100,1 0 -2 200,2 -1 17 500,-1 -2 58 400,0 1 45 300)
    String wkbLe = "01ba0b0000050000000000000000000000000000000000f03f000000000000f0bf0000000000005940000000000000f03f000000000000000000000000000000c000000000000069400000000000000040000000000000f0bf00000000000031400000000000407f40000000000000f0bf00000000000000c00000000000004d4000000000000079400000000000000000000000000000f03f00000000008046400000000000c07240"; // checkstyle.off: LineLength
    String wkbBe = "0000000bba0000000500000000000000003ff0000000000000bff000000000000040590000000000003ff00000000000000000000000000000c00000000000000040690000000000004000000000000000bff00000000000004031000000000000407f400000000000bff0000000000000c000000000000000404d000000000000407900000000000000000000000000003ff000000000000040468000000000004072c00000000000"; // checkstyle.off: LineLength
    checkWkbParsing(wkbLe, wkbBe, GeoTypeId.LINESTRING, false);
    checkWkbRoundTrip(wkbLe, wkbBe);
  }

  // ========== Polygon Tests (2D) ==========

  @Test
  public void testPolygon2DEmpty() {
    // WKT: POLYGON EMPTY
    String wkbLe = "010300000000000000";
    String wkbBe = "000000000300000000";
    checkWkbParsing(wkbLe, wkbBe, GeoTypeId.POLYGON, true);
    checkWkbRoundTrip(wkbLe, wkbBe);
  }

  @Test
  public void testPolygon2D_EmptyRings() {
    // WKT: POLYGON((),(),())
    String wkbLe = "010300000003000000000000000000000000000000";
    String wkbBe = "000000000300000003000000000000000000000000";
    checkWkbParsing(wkbLe, wkbBe, GeoTypeId.POLYGON, true, true);
    // Invalid with too few points in ring
  }

  @Test
  public void testPolygon2D_WithHole() {
    // WKT: POLYGON((0 0,10 0,0 10,0 0),(1 1,1 2,2 1,1 1))
    String wkbLe = "010300000002000000040000000000000000000000000000000000000000000000000024400000000000000000000000000000000000000000000024400000000000000000000000000000000004000000000000000000f03f000000000000f03f000000000000f03f00000000000000400000000000000040000000000000f03f000000000000f03f000000000000f03f"; // checkstyle.off: LineLength
    String wkbBe = "0000000003000000020000000400000000000000000000000000000000402400000000000000000000000000000000000000000000402400000000000000000000000000000000000000000000000000043ff00000000000003ff00000000000003ff0000000000000400000000000000040000000000000003ff00000000000003ff00000000000003ff0000000000000"; // checkstyle.off: LineLength
    checkWkbParsing(wkbLe, wkbBe, GeoTypeId.POLYGON, false);
    checkWkbRoundTrip(wkbLe, wkbBe);

    byte[] wkb = hexToBytes(wkbLe);
    WkbReader reader = new WkbReader();
    Polygon polygon = (Polygon) reader.read(wkb, 0);
    Assertions.assertEquals(2, polygon.getRings().size());
  }

  // ========== Polygon Tests (3DZ) ==========

  @Test
  public void testPolygon3DZEmpty() {
    // WKT: POLYGON Z EMPTY
    String wkbLe = "01eb03000000000000";
    String wkbBe = "00000003eb00000000";
    checkWkbParsing(wkbLe, wkbBe, GeoTypeId.POLYGON, true);
    checkWkbRoundTrip(wkbLe, wkbBe);
  }

  @Test
  public void testPolygon3DZ_EmptyRings() {
    // WKT: POLYGON Z ((),(),())
    String wkbLe = "01eb03000003000000000000000000000000000000";
    String wkbBe = "00000003eb00000003000000000000000000000000";
    checkWkbParsing(wkbLe, wkbBe, GeoTypeId.POLYGON, true, true);
    // Invalid with too few points in ring
  }

  @Test
  public void testPolygon3DZ_WithHole() {
    // WKT: POLYGON Z ((0 0 -1,10 0 -1,0 10 -1,0 0 -1),(1 1 -1,1 2 -1,2 1 -1,1 1 -1))
    String wkbLe = "01eb030000020000000400000000000000000000000000000000000000000000000000f0bf00000000000024400000000000000000000000000000f0bf00000000000000000000000000002440000000000000f0bf00000000000000000000000000000000000000000000f0bf04000000000000000000f03f000000000000f03f000000000000f0bf000000000000f03f0000000000000040000000000000f0bf0000000000000040000000000000f03f000000000000f0bf000000000000f03f000000000000f03f000000000000f0bf"; // checkstyle.off: LineLength
    String wkbBe = "00000003eb000000020000000400000000000000000000000000000000bff000000000000040240000000000000000000000000000bff000000000000000000000000000004024000000000000bff000000000000000000000000000000000000000000000bff0000000000000000000043ff00000000000003ff0000000000000bff00000000000003ff00000000000004000000000000000bff000000000000040000000000000003ff0000000000000bff00000000000003ff00000000000003ff0000000000000bff0000000000000"; // checkstyle.off: LineLength
    checkWkbParsing(wkbLe, wkbBe, GeoTypeId.POLYGON, false);
    checkWkbRoundTrip(wkbLe, wkbBe);
  }

  // ========== Polygon Tests (3DM) ==========

  @Test
  public void testPolygon3DMEmpty() {
    // WKT: POLYGON M EMPTY
    String wkbLe = "01d307000000000000";
    String wkbBe = "00000007d300000000";
    checkWkbParsing(wkbLe, wkbBe, GeoTypeId.POLYGON, true);
    checkWkbRoundTrip(wkbLe, wkbBe);
  }

  @Test
  public void testPolygon3DM_EmptyRings() {
    // WKT: POLYGON M ((),(),())
    String wkbLe = "01d307000003000000000000000000000000000000";
    String wkbBe = "00000007d300000003000000000000000000000000";
    checkWkbParsing(wkbLe, wkbBe, GeoTypeId.POLYGON, true, true);
    // Invalid with too few points in ring
  }

  @Test
  public void testPolygon3DM_WithHole() {
    // WKT: POLYGON M ((0 0 -1,10 0 -1,0 10 -1,0 0 -1),(1 1 -1,1 2 -1,2 1 -1,1 1 -1))
    String wkbLe = "01d3070000020000000400000000000000000000000000000000000000000000000000f0bf00000000000024400000000000000000000000000000f0bf00000000000000000000000000002440000000000000f0bf00000000000000000000000000000000000000000000f0bf04000000000000000000f03f000000000000f03f000000000000f0bf000000000000f03f0000000000000040000000000000f0bf0000000000000040000000000000f03f000000000000f0bf000000000000f03f000000000000f03f000000000000f0bf"; // checkstyle.off: LineLength
    String wkbBe = "00000007d3000000020000000400000000000000000000000000000000bff000000000000040240000000000000000000000000000bff000000000000000000000000000004024000000000000bff000000000000000000000000000000000000000000000bff0000000000000000000043ff00000000000003ff0000000000000bff00000000000003ff00000000000004000000000000000bff000000000000040000000000000003ff0000000000000bff00000000000003ff00000000000003ff0000000000000bff0000000000000"; // checkstyle.off: LineLength
    checkWkbParsing(wkbLe, wkbBe, GeoTypeId.POLYGON, false);
    checkWkbRoundTrip(wkbLe, wkbBe);
  }

  // ========== Polygon Tests (4D) ==========

  @Test
  public void testPolygon4DEmpty() {
    // WKT: POLYGON ZM EMPTY
    String wkbLe = "01bb0b000000000000";
    String wkbBe = "0000000bbb00000000";
    checkWkbParsing(wkbLe, wkbBe, GeoTypeId.POLYGON, true);
    checkWkbRoundTrip(wkbLe, wkbBe);
  }

  @Test
  public void testPolygon4D_EmptyRings() {
    // WKT: POLYGON ZM ((),(),())
    String wkbLe = "01bb0b000003000000000000000000000000000000";
    String wkbBe = "0000000bbb00000003000000000000000000000000";
    checkWkbParsing(wkbLe, wkbBe, GeoTypeId.POLYGON, true, true);
    // Invalid with too few points in ring
  }

  @Test
  public void testPolygon4D_WithHole() {
    // WKT: POLYGON ZM ((0 0 -1 -2,10 0 -1 -2,0 10 -1 -2,0 0 -1 -2),
    //                  (1 1 -1 -2,1 2 -1 -2,2 1 -1 -2,1 1 -1 -2))
    String wkbLe = "01bb0b0000020000000400000000000000000000000000000000000000000000000000f0bf00000000000000c000000000000024400000000000000000000000000000f0bf00000000000000c000000000000000000000000000002440000000000000f0bf00000000000000c000000000000000000000000000000000000000000000f0bf00000000000000c004000000000000000000f03f000000000000f03f000000000000f0bf00000000000000c0000000000000f03f0000000000000040000000000000f0bf00000000000000c00000000000000040000000000000f03f000000000000f0bf00000000000000c0000000000000f03f000000000000f03f000000000000f0bf00000000000000c0"; // checkstyle.off: LineLength
    String wkbBe = "0000000bbb000000020000000400000000000000000000000000000000bff0000000000000c00000000000000040240000000000000000000000000000bff0000000000000c00000000000000000000000000000004024000000000000bff0000000000000c00000000000000000000000000000000000000000000000bff0000000000000c000000000000000000000043ff00000000000003ff0000000000000bff0000000000000c0000000000000003ff00000000000004000000000000000bff0000000000000c00000000000000040000000000000003ff0000000000000bff0000000000000c0000000000000003ff00000000000003ff0000000000000bff0000000000000c000000000000000"; // checkstyle.off: LineLength
    checkWkbParsing(wkbLe, wkbBe, GeoTypeId.POLYGON, false);
    checkWkbRoundTrip(wkbLe, wkbBe);
  }

  // ========== MultiPoint Tests (2D) ==========

  @Test
  public void testMultiPoint2DEmpty() {
    // WKT: MULTIPOINT EMPTY
    String wkbLe = "010400000000000000";
    String wkbBe = "000000000400000000";
    checkWkbParsing(wkbLe, wkbBe, GeoTypeId.MULTI_POINT, true);
    checkWkbRoundTrip(wkbLe, wkbBe);
  }

  @Test
  public void testMultiPoint2D_TwoEmptyPoints() {
    // WKT: MULTIPOINT(EMPTY,EMPTY)
    String wkbLe = "0104000000020000000101000000000000000000f87f000000000000f87f0101000000000000000000f87f000000000000f87f"; // checkstyle.off: LineLength
    String wkbBe = "00000000040000000200000000017ff80000000000007ff800000000000000000000017ff80000000000007ff8000000000000"; // checkstyle.off: LineLength
    checkWkbParsing(wkbLe, wkbBe, GeoTypeId.MULTI_POINT, true);
    checkWkbRoundTrip(wkbLe, wkbBe);
  }

  @Test
  public void testMultiPoint2D_OnePoint() {
    // WKT: MULTIPOINT((1 2))
    String wkbLe = "0104000000010000000101000000000000000000f03f0000000000000040";
    String wkbBe = "00000000040000000100000000013ff00000000000004000000000000000";
    checkWkbParsing(wkbLe, wkbBe, GeoTypeId.MULTI_POINT, false);
    checkWkbRoundTrip(wkbLe, wkbBe);
  }

  @Test
  public void testMultiPoint2D_TwoPoints() {
    // WKT: MULTIPOINT((1 2),(3 4))
    String wkbLe = "0104000000020000000101000000000000000000f03f0000000000000040010100000000000000000008400000000000001040"; // checkstyle.off: LineLength
    String wkbBe = "00000000040000000200000000013ff00000000000004000000000000000000000000140080000000000004010000000000000"; // checkstyle.off: LineLength
    checkWkbParsing(wkbLe, wkbBe, GeoTypeId.MULTI_POINT, false);
    checkWkbRoundTrip(wkbLe, wkbBe);
  }

  @Test
  public void testMultiPoint2D_MixedWithEmpty() {
    // WKT: MULTIPOINT((1 2),EMPTY,EMPTY,(3 4))
    String wkbLe = "0104000000040000000101000000000000000000f03f00000000000000400101000000000000000000f87f000000000000f87f0101000000000000000000f87f000000000000f87f010100000000000000000008400000000000001040"; // checkstyle.off: LineLength
    String wkbBe = "00000000040000000400000000013ff0000000000000400000000000000000000000017ff80000000000007ff800000000000000000000017ff80000000000007ff8000000000000000000000140080000000000004010000000000000"; // checkstyle.off: LineLength
    checkWkbParsing(wkbLe, wkbBe, GeoTypeId.MULTI_POINT, false);
    checkWkbRoundTrip(wkbLe, wkbBe);
  }

  // ========== MultiPoint Tests (3DZ) ==========

  @Test
  public void testMultiPoint3DZEmpty() {
    // WKT: MULTIPOINT Z EMPTY
    String wkbLe = "01ec03000000000000";
    String wkbBe = "00000003ec00000000";
    checkWkbParsing(wkbLe, wkbBe, GeoTypeId.MULTI_POINT, true);
    checkWkbRoundTrip(wkbLe, wkbBe);
  }

  @Test
  public void testMultiPoint3DZ_TwoEmptyPoints() {
    // WKT: MULTIPOINT Z (EMPTY,EMPTY)
    String wkbLe = "01ec0300000200000001e9030000000000000000f87f000000000000f87f000000000000f87f01e9030000000000000000f87f000000000000f87f000000000000f87f"; // checkstyle.off: LineLength
    String wkbBe = "00000003ec0000000200000003e97ff80000000000007ff80000000000007ff800000000000000000003e97ff80000000000007ff80000000000007ff8000000000000"; // checkstyle.off: LineLength
    checkWkbParsing(wkbLe, wkbBe, GeoTypeId.MULTI_POINT, true);
    checkWkbRoundTrip(wkbLe, wkbBe);
  }

  @Test
  public void testMultiPoint3DZ_OnePoint() {
    // WKT: MULTIPOINT Z ((1 2 3))
    String wkbLe = "01ec0300000100000001e9030000000000000000f03f00000000000000400000000000000840";
    String wkbBe = "00000003ec0000000100000003e93ff000000000000040000000000000004008000000000000";
    checkWkbParsing(wkbLe, wkbBe, GeoTypeId.MULTI_POINT, false);
    checkWkbRoundTrip(wkbLe, wkbBe);
  }

  @Test
  public void testMultiPoint3DZ_TwoPoints() {
    // WKT: MULTIPOINT Z ((1 2 3),(4 5 6))
    String wkbLe = "01ec0300000200000001e9030000000000000000f03f0000000000000040000000000000084001e9030000000000000000104000000000000014400000000000001840"; // checkstyle.off: LineLength
    String wkbBe = "00000003ec0000000200000003e93ff00000000000004000000000000000400800000000000000000003e9401000000000000040140000000000004018000000000000"; // checkstyle.off: LineLength
    checkWkbParsing(wkbLe, wkbBe, GeoTypeId.MULTI_POINT, false);
    checkWkbRoundTrip(wkbLe, wkbBe);
  }

  @Test
  public void testMultiPoint3DZ_MixedWithEmpty() {
    // WKT: MULTIPOINT Z ((1 2 3),EMPTY,EMPTY,(4 5 6))
    String wkbLe = "01ec0300000400000001e9030000000000000000f03f0000000000000040000000000000084001e9030000000000000000f87f000000000000f87f000000000000f87f01e9030000000000000000f87f000000000000f87f000000000000f87f01e9030000000000000000104000000000000014400000000000001840"; // checkstyle.off: LineLength
    String wkbBe = "00000003ec0000000400000003e93ff00000000000004000000000000000400800000000000000000003e97ff80000000000007ff80000000000007ff800000000000000000003e97ff80000000000007ff80000000000007ff800000000000000000003e9401000000000000040140000000000004018000000000000"; // checkstyle.off: LineLength
    checkWkbParsing(wkbLe, wkbBe, GeoTypeId.MULTI_POINT, false);
    checkWkbRoundTrip(wkbLe, wkbBe);
  }

  // ========== MultiPoint Tests (3DM) ==========

  @Test
  public void testMultiPoint3DMEmpty() {
    // WKT: MULTIPOINT M EMPTY
    String wkbLe = "01d407000000000000";
    String wkbBe = "00000007d400000000";
    checkWkbParsing(wkbLe, wkbBe, GeoTypeId.MULTI_POINT, true);
    checkWkbRoundTrip(wkbLe, wkbBe);
  }

  @Test
  public void testMultiPoint3DM_TwoEmptyPoints() {
    // WKT: MULTIPOINT M (EMPTY,EMPTY)
    String wkbLe = "01d40700000200000001d1070000000000000000f87f000000000000f87f000000000000f87f01d1070000000000000000f87f000000000000f87f000000000000f87f"; // checkstyle.off: LineLength
    String wkbBe = "00000007d40000000200000007d17ff80000000000007ff80000000000007ff800000000000000000007d17ff80000000000007ff80000000000007ff8000000000000"; // checkstyle.off: LineLength
    checkWkbParsing(wkbLe, wkbBe, GeoTypeId.MULTI_POINT, true);
    checkWkbRoundTrip(wkbLe, wkbBe);
  }

  @Test
  public void testMultiPoint3DM_TwoPoints() {
    // WKT: MULTIPOINT M ((1 2 3),(4 5 6))
    String wkbLe = "01d40700000200000001d1070000000000000000f03f0000000000000040000000000000084001d1070000000000000000104000000000000014400000000000001840"; // checkstyle.off: LineLength
    String wkbBe = "00000007d40000000200000007d13ff00000000000004000000000000000400800000000000000000007d1401000000000000040140000000000004018000000000000"; // checkstyle.off: LineLength
    checkWkbParsing(wkbLe, wkbBe, GeoTypeId.MULTI_POINT, false);
    checkWkbRoundTrip(wkbLe, wkbBe);
  }

  // ========== MultiPoint Tests (4D) ==========

  @Test
  public void testMultiPoint4DEmpty() {
    // WKT: MULTIPOINT ZM EMPTY
    String wkbLe = "01bc0b000000000000";
    String wkbBe = "0000000bbc00000000";
    checkWkbParsing(wkbLe, wkbBe, GeoTypeId.MULTI_POINT, true);
    checkWkbRoundTrip(wkbLe, wkbBe);
  }

  @Test
  public void testMultiPoint4D_TwoEmptyPoints() {
    // WKT: MULTIPOINT ZM (EMPTY,EMPTY)
    String wkbLe = "01bc0b00000200000001b90b0000000000000000f87f000000000000f87f000000000000f87f000000000000f87f01b90b0000000000000000f87f000000000000f87f000000000000f87f000000000000f87f"; // checkstyle.off: LineLength
    String wkbBe = "0000000bbc000000020000000bb97ff80000000000007ff80000000000007ff80000000000007ff80000000000000000000bb97ff80000000000007ff80000000000007ff80000000000007ff8000000000000"; // checkstyle.off: LineLength
    checkWkbParsing(wkbLe, wkbBe, GeoTypeId.MULTI_POINT, true);
    checkWkbRoundTrip(wkbLe, wkbBe);
  }

  @Test
  public void testMultiPoint4D_OnePoint() {
    // WKT: MULTIPOINT ZM ((1 2 3 4))
    String wkbLe = "01bc0b00000100000001b90b0000000000000000f03f000000000000004000000000000008400000000000001040"; // checkstyle.off: LineLength
    String wkbBe = "0000000bbc000000010000000bb93ff0000000000000400000000000000040080000000000004010000000000000"; // checkstyle.off: LineLength
    checkWkbParsing(wkbLe, wkbBe, GeoTypeId.MULTI_POINT, false);
    checkWkbRoundTrip(wkbLe, wkbBe);
  }

  @Test
  public void testMultiPoint4D_TwoPoints() {
    // WKT: MULTIPOINT ZM ((1 2 3 4),(5 6 7 8))
    String wkbLe = "01bc0b00000200000001b90b0000000000000000f03f00000000000000400000000000000840000000000000104001b90b0000000000000000144000000000000018400000000000001c400000000000002040"; // checkstyle.off: LineLength
    String wkbBe = "0000000bbc000000020000000bb93ff00000000000004000000000000000400800000000000040100000000000000000000bb940140000000000004018000000000000401c0000000000004020000000000000"; // checkstyle.off: LineLength
    checkWkbParsing(wkbLe, wkbBe, GeoTypeId.MULTI_POINT, false);
    checkWkbRoundTrip(wkbLe, wkbBe);
  }

  @Test
  public void testMultiPoint4D_MixedWithEmpty() {
    // WKT: MULTIPOINT ZM ((1 2 3 4),EMPTY,EMPTY,(5 6 7 8))
    String wkbLe = "01bc0b00000400000001b90b0000000000000000f03f00000000000000400000000000000840000000000000104001b90b0000000000000000f87f000000000000f87f000000000000f87f000000000000f87f01b90b0000000000000000f87f000000000000f87f000000000000f87f000000000000f87f01b90b0000000000000000144000000000000018400000000000001c400000000000002040"; // checkstyle.off: LineLength
    String wkbBe = "0000000bbc000000040000000bb93ff00000000000004000000000000000400800000000000040100000000000000000000bb97ff80000000000007ff80000000000007ff80000000000007ff80000000000000000000bb97ff80000000000007ff80000000000007ff80000000000007ff80000000000000000000bb940140000000000004018000000000000401c0000000000004020000000000000"; // checkstyle.off: LineLength
    checkWkbParsing(wkbLe, wkbBe, GeoTypeId.MULTI_POINT, false);
    checkWkbRoundTrip(wkbLe, wkbBe);
  }

  // ========== MultiLineString Tests (2D) ==========

  @Test
  public void testMultiLineString2DEmpty() {
    // WKT: MULTILINESTRING EMPTY
    String wkbLe = "010500000000000000";
    String wkbBe = "000000000500000000";
    checkWkbParsing(wkbLe, wkbBe, GeoTypeId.MULTI_LINESTRING, true);
    checkWkbRoundTrip(wkbLe, wkbBe);
  }

  @Test
  public void testMultiLineString2D_ThreeEmptyLineStrings() {
    // WKT: MULTILINESTRING(EMPTY,EMPTY,EMPTY)
    String wkbLe = "010500000003000000010200000000000000010200000000000000010200000000000000";
    String wkbBe = "000000000500000003000000000200000000000000000200000000000000000200000000";
    checkWkbParsing(wkbLe, wkbBe, GeoTypeId.MULTI_LINESTRING, true);
    checkWkbRoundTrip(wkbLe, wkbBe);
  }

  @Test
  public void testMultiLineString2D_MixedWithEmptyOnePoint() {
    // WKT: MULTILINESTRING(EMPTY,(1 2),EMPTY)
    String wkbLe = "010500000003000000010200000000000000010200000001000000000000000000f03f0000000000000040010200000000000000"; // checkstyle.off: LineLength
    String wkbBe = "0000000005000000030000000002000000000000000002000000013ff00000000000004000000000000000000000000200000000"; // checkstyle.off: LineLength
    checkWkbParsing(wkbLe, wkbBe, GeoTypeId.MULTI_LINESTRING, false, true);
    // Invalid with too few points in line string
  }

  @Test
  public void testMultiLineString2D_MixedWithEmpty() {
    // WKT: MULTILINESTRING(EMPTY,(1 2,3 4),EMPTY)
    String wkbLe = "010500000003000000010200000000000000010200000002000000000000000000f03f00000000000000400000000000000840000000000000104001020000" + "0000000000"; // checkstyle.off: LineLength
    String wkbBe = "0000000005000000030000000002000000000000000002000000023ff000000000000040000000000000004008000000000000401000000000000000000000020" + "0000000"; // checkstyle.off: LineLength
    checkWkbParsing(wkbLe, wkbBe, GeoTypeId.MULTI_LINESTRING, false);
    checkWkbRoundTrip(wkbLe, wkbBe);
  }

  @Test
  public void testMultiLineString2D_TwoLineStrings() {
    // WKT: MULTILINESTRING((1 2,3 4),(5 6,7 8))
    String wkbLe = "010500000002000000010200000002000000000000000000f03f000000000000004000000000000008400000000000001040010200000002000000000000000000144000000000000018400000000000001c400000000000002040"; // checkstyle.off: LineLength
    String wkbBe = "0000000005000000020000000002000000023ff000000000000040000000000000004008000000000000401000000000000000000000020000000240140000000000004018000000000000401c0000000000004020000000000000"; // checkstyle.off: LineLength
    checkWkbParsing(wkbLe, wkbBe, GeoTypeId.MULTI_LINESTRING, false);
    checkWkbRoundTrip(wkbLe, wkbBe);
  }

  // ========== MultiLineString Tests (3DZ) ==========

  @Test
  public void testMultiLineString3DZEmpty() {
    // WKT: MULTILINESTRING Z EMPTY
    String wkbLe = "01ed03000000000000";
    String wkbBe = "00000003ed00000000";
    checkWkbParsing(wkbLe, wkbBe, GeoTypeId.MULTI_LINESTRING, true);
    checkWkbRoundTrip(wkbLe, wkbBe);
  }

  @Test
  public void testMultiLineString3DZ_ThreeEmptyLineStrings() {
    // WKT: MULTILINESTRING Z (EMPTY,EMPTY,EMPTY)
    String wkbLe = "01ed0300000300000001ea0300000000000001ea0300000000000001ea03000000000000";
    String wkbBe = "00000003ed0000000300000003ea0000000000000003ea0000000000000003ea00000000";
    checkWkbParsing(wkbLe, wkbBe, GeoTypeId.MULTI_LINESTRING, true);
    checkWkbRoundTrip(wkbLe, wkbBe);
  }

  @Test
  public void testMultiLineString3DZ_MixedWithEmpty() {
    // WKT: MULTILINESTRING Z (EMPTY,(1 2 3,4 5 6),EMPTY)
    String wkbLe = "01ed0300000300000001ea0300000000000001ea03000002000000000000000000f03f00000000000000400000000000000840000000000000104000000000000014400000000000001840" + "01ea03000000000000"; // checkstyle.off: LineLength
    String wkbBe = "00000003ed0000000300000003ea0000000000000003ea000000023ff00000000000004000000000000000400800000000000040100000000000004014000000000000401800000000000000000003ea00000000"; // checkstyle.off: LineLength
    checkWkbParsing(wkbLe, wkbBe, GeoTypeId.MULTI_LINESTRING, false);
    checkWkbRoundTrip(wkbLe, wkbBe);
  }

  // ========== MultiLineString Tests (3DM) ==========

  @Test
  public void testMultiLineString3DMEmpty() {
    // WKT: MULTILINESTRING M EMPTY
    String wkbLe = "01d507000000000000";
    String wkbBe = "00000007d500000000";
    checkWkbParsing(wkbLe, wkbBe, GeoTypeId.MULTI_LINESTRING, true);
    checkWkbRoundTrip(wkbLe, wkbBe);
  }

  @Test
  public void testMultiLineString3DM_ThreeEmptyLineStrings() {
    // WKT: MULTILINESTRING M (EMPTY,EMPTY,EMPTY)
    String wkbLe = "01d50700000300000001d20700000000000001d20700000000000001d207000000000000";
    String wkbBe = "00000007d50000000300000007d20000000000000007d20000000000000007d200000000";
    checkWkbParsing(wkbLe, wkbBe, GeoTypeId.MULTI_LINESTRING, true);
    checkWkbRoundTrip(wkbLe, wkbBe);
  }

  @Test
  public void testMultiLineString3DM_MixedWithEmpty() {
    // WKT: MULTILINESTRING M (EMPTY,(1 2 3,4 5 6),EMPTY)
    String wkbLe = "01d50700000300000001d20700000000000001d207000002000000000000000000f03f00000000000000400000000000000840000000000000104000000000000014400000000000001840" + "01d207000000000000"; // checkstyle.off: LineLength
    String wkbBe = "00000007d50000000300000007d20000000000000007d2000000023ff00000000000004000000000000000400800000000000040100000000000004014000000000000401800000000000000000007d200000000"; // checkstyle.off: LineLength
    checkWkbParsing(wkbLe, wkbBe, GeoTypeId.MULTI_LINESTRING, false);
    checkWkbRoundTrip(wkbLe, wkbBe);
  }

  // ========== MultiLineString Tests (4D) ==========

  @Test
  public void testMultiLineString4DEmpty() {
    // WKT: MULTILINESTRING ZM EMPTY
    String wkbLe = "01bd0b000000000000";
    String wkbBe = "0000000bbd00000000";
    checkWkbParsing(wkbLe, wkbBe, GeoTypeId.MULTI_LINESTRING, true);
    checkWkbRoundTrip(wkbLe, wkbBe);
  }

  @Test
  public void testMultiLineString4D_ThreeEmptyLineStrings() {
    // WKT: MULTILINESTRING ZM (EMPTY,EMPTY,EMPTY)
    String wkbLe = "01bd0b00000300000001ba0b00000000000001ba0b00000000000001ba0b000000000000";
    String wkbBe = "0000000bbd000000030000000bba000000000000000bba000000000000000bba00000000";
    checkWkbParsing(wkbLe, wkbBe, GeoTypeId.MULTI_LINESTRING, true);
    checkWkbRoundTrip(wkbLe, wkbBe);
  }

  @Test
  public void testMultiLineString4D_MixedWithEmpty() {
    // WKT: MULTILINESTRING ZM (EMPTY,(1 2 3 4,5 6 7 8),EMPTY)
    String wkbLe = "01bd0b00000300000001ba0b00000000000001ba0b000002000000000000000000f03f000000000000004000000000000008400000000000001040000000000000144000000000000018400000000000001c40000000000000204001ba0b000000000000"; // checkstyle.off: LineLength
    String wkbBe = "0000000bbd000000030000000bba000000000000000bba000000023ff000000000000040000000000000004008000000000000401000000000000040140000000000004018000000000000401c00000000000040200000000000000000000bba00000000"; // checkstyle.off: LineLength
    checkWkbParsing(wkbLe, wkbBe, GeoTypeId.MULTI_LINESTRING, false);
    checkWkbRoundTrip(wkbLe, wkbBe);
  }

  // ========== MultiPolygon Tests (2D) ==========

  @Test
  public void testMultiPolygon2DEmpty() {
    // WKT: MULTIPOLYGON EMPTY
    String wkbLe = "010600000000000000";
    String wkbBe = "000000000600000000";
    checkWkbParsing(wkbLe, wkbBe, GeoTypeId.MULTI_POLYGON, true);
    checkWkbRoundTrip(wkbLe, wkbBe);
  }

  @Test
  public void testMultiPolygon2D_EmptyAndEmptyRings() {
    // WKT: MULTIPOLYGON(EMPTY,((),()))
    String wkbLe = "0106000000020000000103000000000000000103000000020000000000000000000000";
    String wkbBe = "0000000006000000020000000003000000000000000003000000020000000000000000";
    checkWkbParsing(wkbLe, wkbBe, GeoTypeId.MULTI_POLYGON, true, true);
    // Invalid with too few points in ring
  }

  @Test
  public void testMultiPolygon2D_WithHole() {
    // WKT: MULTIPOLYGON(EMPTY,((0 0,10 0,0 10,0 0),(1 1,1 2,2 1,1 1)))
    String wkbLe = "010600000002000000010300000000000000010300000002000000040000000000000000000000000000000000000000000000000024400000000000000000000000000000000000000000000024400000000000000000000000000000000004000000000000000000f03f000000000000f03f000000000000f03f00000000000000400000000000000040000000000000f03f000000000000f03f000000000000f03f"; // checkstyle.off: LineLength
    String wkbBe = "0000000006000000020000000003000000000000000003000000020000000400000000000000000000000000000000402400000000000000000000000000000000000000000000402400000000000000000000000000000000000000000000000000043ff00000000000003ff00000000000003ff0000000000000400000000000000040000000000000003ff00000000000003ff00000000000003ff0000000000000"; // checkstyle.off: LineLength
    checkWkbParsing(wkbLe, wkbBe, GeoTypeId.MULTI_POLYGON, false);
    checkWkbRoundTrip(wkbLe, wkbBe);
  }

  // ========== MultiPolygon Tests (3DZ) ==========

  @Test
  public void testMultiPolygon3DZEmpty() {
    // WKT: MULTIPOLYGON Z EMPTY
    String wkbLe = "01ee03000000000000";
    String wkbBe = "00000003ee00000000";
    checkWkbParsing(wkbLe, wkbBe, GeoTypeId.MULTI_POLYGON, true);
    checkWkbRoundTrip(wkbLe, wkbBe);
  }

  @Test
  public void testMultiPolygon3DZ_EmptyAndEmptyRings() {
    // WKT: MULTIPOLYGON Z (EMPTY,((),()))
    String wkbLe = "01ee0300000200000001eb0300000000000001eb030000020000000000000000000000";
    String wkbBe = "00000003ee0000000200000003eb0000000000000003eb000000020000000000000000";
    checkWkbParsing(wkbLe, wkbBe, GeoTypeId.MULTI_POLYGON, true, true);
    // Invalid with too few points in ring
  }

  @Test
  public void testMultiPolygon3DZ_WithHole() {
    // WKT: MULTIPOLYGON Z (EMPTY,((0 0 -1,10 0 -1,0 10 -1,0 0 -1),(1 1 -1,1 2 -1,2 1 -1,1 1 -1)))
    String wkbLe = "01ee0300000200000001eb0300000000000001eb030000020000000400000000000000000000000000000000000000000000000000f0bf00000000000024400000000000000000000000000000f0bf00000000000000000000000000002440000000000000f0bf00000000000000000000000000000000000000000000f0bf04000000000000000000f03f000000000000f03f000000000000f0bf000000000000f03f0000000000000040000000000000f0bf0000000000000040000000000000f03f000000000000f0bf000000000000f03f000000000000f03f000000000000f0bf"; // checkstyle.off: LineLength
    String wkbBe = "00000003ee0000000200000003eb0000000000000003eb000000020000000400000000000000000000000000000000bff000000000000040240000000000000000000000000000bff000000000000000000000000000004024000000000000bff000000000000000000000000000000000000000000000bff0000000000000000000043ff00000000000003ff0000000000000bff00000000000003ff00000000000004000000000000000bff000000000000040000000000000003ff0000000000000bff00000000000003ff00000000000003ff0000000000000bff0000000000000"; // checkstyle.off: LineLength
    checkWkbParsing(wkbLe, wkbBe, GeoTypeId.MULTI_POLYGON, false);
    checkWkbRoundTrip(wkbLe, wkbBe);
  }

  // ========== MultiPolygon Tests (3DM) ==========

  @Test
  public void testMultiPolygon3DMEmpty() {
    // WKT: MULTIPOLYGON M EMPTY
    String wkbLe = "01d607000000000000";
    String wkbBe = "00000007d600000000";
    checkWkbParsing(wkbLe, wkbBe, GeoTypeId.MULTI_POLYGON, true);
    checkWkbRoundTrip(wkbLe, wkbBe);
  }

  @Test
  public void testMultiPolygon3DM_EmptyAndEmptyRings() {
    // WKT: MULTIPOLYGON M (EMPTY,((),()))
    String wkbLe = "01d60700000200000001d30700000000000001d3070000020000000000000000000000";
    String wkbBe = "00000007d60000000200000007d30000000000000007d3000000020000000000000000";
    checkWkbParsing(wkbLe, wkbBe, GeoTypeId.MULTI_POLYGON, true, true);
    // Invalid with too few points in ring
  }

  @Test
  public void testMultiPolygon3DM_WithHole() {
    // WKT: MULTIPOLYGON M (EMPTY,((0 0 -1,10 0 -1,0 10 -1,0 0 -1),(1 1 -1,1 2 -1,2 1 -1,1 1 -1)))
    String wkbLe = "01d60700000200000001d30700000000000001d3070000020000000400000000000000000000000000000000000000000000000000f0bf00000000000024400000000000000000000000000000f0bf00000000000000000000000000002440000000000000f0bf00000000000000000000000000000000000000000000f0bf04000000000000000000f03f000000000000f03f000000000000f0bf000000000000f03f0000000000000040000000000000f0bf0000000000000040000000000000f03f000000000000f0bf000000000000f03f000000000000f03f000000000000f0bf"; // checkstyle.off: LineLength
    String wkbBe = "00000007d60000000200000007d30000000000000007d3000000020000000400000000000000000000000000000000bff000000000000040240000000000000000000000000000bff000000000000000000000000000004024000000000000bff000000000000000000000000000000000000000000000bff0000000000000000000043ff00000000000003ff0000000000000bff00000000000003ff00000000000004000000000000000bff000000000000040000000000000003ff0000000000000bff00000000000003ff00000000000003ff0000000000000bff0000000000000"; // checkstyle.off: LineLength
    checkWkbParsing(wkbLe, wkbBe, GeoTypeId.MULTI_POLYGON, false);
    checkWkbRoundTrip(wkbLe, wkbBe);
  }

  // ========== MultiPolygon Tests (4D) ==========

  @Test
  public void testMultiPolygon4DEmpty() {
    // WKT: MULTIPOLYGON ZM EMPTY
    String wkbLe = "01be0b000000000000";
    String wkbBe = "0000000bbe00000000";
    checkWkbParsing(wkbLe, wkbBe, GeoTypeId.MULTI_POLYGON, true);
    checkWkbRoundTrip(wkbLe, wkbBe);
  }

  @Test
  public void testMultiPolygon4D_EmptyAndEmptyRings() {
    // WKT: MULTIPOLYGON ZM (EMPTY,((),()))
    String wkbLe = "01be0b00000200000001bb0b00000000000001bb0b0000020000000000000000000000";
    String wkbBe = "0000000bbe000000020000000bbb000000000000000bbb000000020000000000000000";
    checkWkbParsing(wkbLe, wkbBe, GeoTypeId.MULTI_POLYGON, true, true);
    // Invalid with too few points in ring
  }

  @Test
  public void testMultiPolygon4D_WithHole() {
    // WKT: MULTIPOLYGON ZM (EMPTY,((0 0 -1 -2,10 0 -1 -2,0 10 -1 -2,0 0 -1 -2),
    //                             (1 1 -1 -2,1 2 -1 -2,2 1 -1 -2,1 1 -1 -2)))
    String wkbLe = "01be0b00000200000001bb0b00000000000001bb0b0000020000000400000000000000000000000000000000000000000000000000f0bf00000000000000c000000000000024400000000000000000000000000000f0bf00000000000000c000000000000000000000000000002440000000000000f0bf00000000000000c000000000000000000000000000000000000000000000f0bf00000000000000c004000000000000000000f03f000000000000f03f000000000000f0bf00000000000000c0000000000000f03f0000000000000040000000000000f0bf00000000000000c00000000000000040000000000000f03f000000000000f0bf00000000000000c0000000000000f03f000000000000f03f000000000000f0bf00000000000000c0"; // checkstyle.off: LineLength
    String wkbBe = "0000000bbe000000020000000bbb000000000000000bbb000000020000000400000000000000000000000000000000bff0000000000000c00000000000000040240000000000000000000000000000bff0000000000000c00000000000000000000000000000004024000000000000bff0000000000000c00000000000000000000000000000000000000000000000bff0000000000000c000000000000000000000043ff00000000000003ff0000000000000bff0000000000000c0000000000000003ff00000000000004000000000000000bff0000000000000c00000000000000040000000000000003ff0000000000000bff0000000000000c0000000000000003ff00000000000003ff0000000000000bff0000000000000c000000000000000"; // checkstyle.off: LineLength
    checkWkbParsing(wkbLe, wkbBe, GeoTypeId.MULTI_POLYGON, false);
    checkWkbRoundTrip(wkbLe, wkbBe);
  }

  // ========== GeometryCollection Tests (2D) ==========

  @Test
  public void testGeometryCollection2DEmpty() {
    // WKT: GEOMETRYCOLLECTION EMPTY
    String wkbLe = "010700000000000000";
    String wkbBe = "000000000700000000";
    checkWkbParsing(wkbLe, wkbBe, GeoTypeId.GEOMETRY_COLLECTION, true);
    checkWkbRoundTrip(wkbLe, wkbBe);
  }

  @Test
  public void testGeometryCollection2D_NestedEmpty() {
    // WKT: GEOMETRYCOLLECTION(POINT EMPTY,LINESTRING EMPTY,GEOMETRYCOLLECTION EMPTY,
    //      GEOMETRYCOLLECTION(GEOMETRYCOLLECTION EMPTY,GEOMETRYCOLLECTION EMPTY),
    //      GEOMETRYCOLLECTION(MULTIPOINT EMPTY,MULTIPOINT(EMPTY),
    //      GEOMETRYCOLLECTION(MULTILINESTRING EMPTY,MULTIPOLYGON EMPTY,GEOMETRYCOLLECTION EMPTY)))
    String wkbLe = "0107000000050000000101000000000000000000f87f000000000000f87f0102000000000000000107000000000000000107000000020000000107000000000000000107000000000000000107000000030000000104000000000000000104000000010000000101000000000000000000f87f000000000000f87f010700000003000000010500000000000000010600000000000000010700000000000000"; // checkstyle.off: LineLength
    String wkbBe = "00000000070000000500000000017ff80000000000007ff800000000000000000000020000000000000000070000000000000000070000000200000000070000000000000000070000000000000000070000000300000000040000000000000000040000000100000000017ff80000000000007ff8000000000000000000000700000003000000000500000000000000000600000000000000000700000000"; // checkstyle.off: LineLength
    checkWkbParsing(wkbLe, wkbBe, GeoTypeId.GEOMETRY_COLLECTION, true);
    checkWkbRoundTrip(wkbLe, wkbBe);
  }

  @Test
  public void testGeometryCollection2D_AllTypes() {
    // WKT: GEOMETRYCOLLECTION(POINT EMPTY,LINESTRING EMPTY,POLYGON EMPTY,MULTIPOINT EMPTY,
    //      MULTILINESTRING EMPTY,MULTIPOLYGON EMPTY,GEOMETRYCOLLECTION(POINT EMPTY,
    //      LINESTRING EMPTY,POLYGON EMPTY,MULTIPOINT EMPTY,MULTILINESTRING EMPTY,
    //      MULTIPOLYGON EMPTY))
    String wkbLe = "0107000000070000000101000000000000000000f87f000000000000f87f0102000000000000000103000000000000000104000000000000000105000000000000000106000000000000000107000000060000000101000000000000000000f87f000000000000f87f010200000000000000010300000000000000010400000000000000010500000000000000010600000000000000"; // checkstyle.off: LineLength
    String wkbBe = "00000000070000000700000000017ff80000000000007ff800000000000000000000020000000000000000030000000000000000040000000000000000050000000000000000060000000000000000070000000600000000017ff80000000000007ff8000000000000000000000200000000000000000300000000000000000400000000000000000500000000000000000600000000"; // checkstyle.off: LineLength
    checkWkbParsing(wkbLe, wkbBe, GeoTypeId.GEOMETRY_COLLECTION, true);
    checkWkbRoundTrip(wkbLe, wkbBe);
  }

  @Test
  public void testGeometryCollection2D_Complex() {
    // WKT: GEOMETRYCOLLECTION(POINT(1 2),LINESTRING(1 2,3 4),POLYGON((0 0,1 0,0 1,0 0)),
    //      MULTIPOINT(EMPTY,(1 2)),MULTILINESTRING(EMPTY,(1 2,3 4)),
    //      MULTIPOLYGON(EMPTY,((0 0,1 0,0 1,0 0))),GEOMETRYCOLLECTION(POINT(1 2),
    //      LINESTRING(1 2,3 4),POLYGON((0 0,1 0,0 1,0 0)),MULTIPOINT(EMPTY,(1 2)),
    //      MULTILINESTRING(EMPTY,(1 2,3 4)),MULTIPOLYGON(EMPTY,((0 0,1 0,0 1,0 0)))))
    String wkbLe = "0107000000070000000101000000000000000000f03f0000000000000040010200000002000000000000000000f03f00000000000000400000000000000840000000000000104001030000000100000004000000000000000000000000000000000000000000000000000000000000000000f03f000000000000f03f0000000000000000000000000000000000000000000000000104000000020000000101000000000000000000f87f000000000000f87f0101000000000000000000f03f0000000000000040010500000002000000010200000000000000010200000002000000000000000000f03f00000000000000400000000000000840000000000000104001060000000200000001030000000000000001030000000100000004000000000000000000000000000000000000000000000000000000000000000000f03f000000000000f03f0000000000000000000000000000000000000000000000000107000000060000000101000000000000000000f03f0000000000000040010200000002000000000000000000f03f00000000000000400000000000000840000000000000104001030000000100000004000000000000000000000000000000000000000000000000000000000000000000f03f000000000000f03f0000000000000000000000000000000000000000000000000104000000020000000101000000000000000000f87f000000000000f87f0101000000000000000000f03f0000000000000040010500000002000000010200000000000000010200000002000000000000000000f03f00000000000000400000000000000840000000000000104001060000000200000001030000000000000001030000000100000004000000000000000000000000000000000000000000000000000000000000000000f03f000000000000f03f000000000000000000000000000000000000000000000000"; // checkstyle.off: LineLength
    String wkbBe = "00000000070000000700000000013ff000000000000040000000000000000000000002000000023ff0000000000000400000000000000040080000000000004010000000000000000000000300000001000000040000000000000000000000000000000000000000000000003ff00000000000003ff000000000000000000000000000000000000000000000000000000000000000000000040000000200000000017ff80000000000007ff800000000000000000000013ff000000000000040000000000000000000000005000000020000000002000000000000000002000000023ff0000000000000400000000000000040080000000000004010000000000000000000000600000002000000000300000000000000000300000001000000040000000000000000000000000000000000000000000000003ff00000000000003ff000000000000000000000000000000000000000000000000000000000000000000000070000000600000000013ff000000000000040000000000000000000000002000000023ff0000000000000400000000000000040080000000000004010000000000000000000000300000001000000040000000000000000000000000000000000000000000000003ff00000000000003ff000000000000000000000000000000000000000000000000000000000000000000000040000000200000000017ff80000000000007ff800000000000000000000013ff000000000000040000000000000000000000005000000020000000002000000000000000002000000023ff0000000000000400000000000000040080000000000004010000000000000000000000600000002000000000300000000000000000300000001000000040000000000000000000000000000000000000000000000003ff00000000000003ff0000000000000000000000000000000000000000000000000000000000000"; // checkstyle.off: LineLength
    checkWkbParsing(wkbLe, wkbBe, GeoTypeId.GEOMETRY_COLLECTION, false);
    checkWkbRoundTrip(wkbLe, wkbBe);
  }

  // ========== Additional Parsing Tests ==========

  @Test
  public void testInvalidWkbTooShort() {
    byte[] invalidWkb = {0x01};
    WkbReader reader = new WkbReader();
    Assertions.assertThrows(WkbParseException.class, () -> reader.read(invalidWkb, 0));
  }

  @Test
  public void testInvalidByteOrder() {
    byte[] invalidWkb = {0x02, 0x01, 0x00, 0x00, 0x00};
    WkbReader reader = new WkbReader();
    Assertions.assertThrows(WkbParseException.class, () -> reader.read(invalidWkb, 0));
  }

  @Test
  public void testUnsupportedGeometryType() {
    // Invalid type 99
    byte[] invalidWkb = hexToBytes("0163000000000000000000f03f0000000000000040");
    WkbReader reader = new WkbReader();
    Assertions.assertThrows(WkbParseException.class, () -> reader.read(invalidWkb, 0));
  }

  // ========== Coordinate Verification Tests ==========

  @Test
  public void testLineStringCoordinates() {
    // WKT: LINESTRING(1 2,3 4)
    String wkbLe = "010200000002000000000000000000f03f000000000000004000000000000008400000000000001040"; // checkstyle.off: LineLength
    byte[] wkb = hexToBytes(wkbLe);
    WkbReader reader = new WkbReader();
    LineString ls = (LineString) reader.read(wkb, 0);

    Assertions.assertEquals(2, ls.getNumPoints());
    List<Point> points = ls.getPoints();
    Assertions.assertEquals(1.0, points.get(0).getX(), 0.0001);
    Assertions.assertEquals(2.0, points.get(0).getY(), 0.0001);
    Assertions.assertEquals(3.0, points.get(1).getX(), 0.0001);
    Assertions.assertEquals(4.0, points.get(1).getY(), 0.0001);
  }

  @Test
  public void testPolygonCoordinates() {
    // WKT: POLYGON((0 0,10 0,0 10,0 0),(1 1,1 2,2 1,1 1))
    String wkbLe = "010300000002000000040000000000000000000000000000000000000000000000000024400000000000000000000000000000000000000000000024400000000000000000000000000000000004000000000000000000f03f000000000000f03f000000000000f03f00000000000000400000000000000040000000000000f03f000000000000f03f000000000000f03f"; // checkstyle.off: LineLength
    byte[] wkb = hexToBytes(wkbLe);
    WkbReader reader = new WkbReader();
    Polygon poly = (Polygon) reader.read(wkb, 0);

    Assertions.assertEquals(2, poly.getRings().size());
    Ring exteriorRing = poly.getRings().get(0);
    Assertions.assertEquals(4, exteriorRing.getNumPoints());
    Assertions.assertEquals(0.0, exteriorRing.getPoints().get(0).getX(), 0.0001);
    Assertions.assertEquals(0.0, exteriorRing.getPoints().get(0).getY(), 0.0001);
    Assertions.assertEquals(10.0, exteriorRing.getPoints().get(1).getX(), 0.0001);
    Assertions.assertEquals(0.0, exteriorRing.getPoints().get(1).getY(), 0.0001);
  }

  @Test
  public void testMultiPointCoordinates() {
    // WKT: MULTIPOINT((1 2),(3 4))
    String wkbLe = "0104000000020000000101000000000000000000f03f0000000000000040010100000000000000000008400000000000001040"; // checkstyle.off: LineLength
    byte[] wkb = hexToBytes(wkbLe);
    WkbReader reader = new WkbReader();
    MultiPoint mp = (MultiPoint) reader.read(wkb, 0);

    Assertions.assertEquals(2, mp.getNumGeometries());
    List<Point> points = mp.getPoints();
    Assertions.assertEquals(1.0, points.get(0).getX(), 0.0001);
    Assertions.assertEquals(2.0, points.get(0).getY(), 0.0001);
    Assertions.assertEquals(3.0, points.get(1).getX(), 0.0001);
    Assertions.assertEquals(4.0, points.get(1).getY(), 0.0001);
  }

  @Test
  public void testGeometryCollectionStructure() {
    // WKT: GEOMETRYCOLLECTION(POINT(1 2),LINESTRING(1 2,3 4))
    String wkbLe = "0107000000020000000101000000000000000000f03f0000000000000040010200000002000000000000000000f03f000000000000004000000000000008400000000000001040"; // checkstyle.off: LineLength
    byte[] wkb = hexToBytes(wkbLe);
    WkbReader reader = new WkbReader();
    GeometryCollection gc = (GeometryCollection) reader.read(wkb, 0);

    Assertions.assertEquals(2, gc.getNumGeometries());
    Assertions.assertTrue(gc.getGeometries().get(0) instanceof Point);
    Assertions.assertTrue(gc.getGeometries().get(1) instanceof LineString);
  }

  // ========== SRID Preservation Tests ==========

  @Test
  public void testSridPreservation() {
    String wkbLe = "0101000000000000000000f03f0000000000000040";
    byte[] wkb = hexToBytes(wkbLe);
    WkbReader reader = new WkbReader();
    GeometryModel geom = reader.read(wkb, 4326);

    Assertions.assertEquals(4326, geom.srid());
  }
}
