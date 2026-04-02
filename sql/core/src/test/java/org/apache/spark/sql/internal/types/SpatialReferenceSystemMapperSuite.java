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

package org.apache.spark.sql.internal.types;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

public class SpatialReferenceSystemMapperSuite {

  // Tests for the SpatialReferenceSystemInformation class.

  @Test
  void testSpatialReferenceSystemInformation() {
    // Test case schema.
    record TestCase(int srid, String stringId, boolean isGeographic) {}
    // Define test cases.
    List<TestCase> testCases = List.of(
      new TestCase(0, "SRID:0", false),
      new TestCase(3857, "EPSG:3857", false),
      new TestCase(4326, "OGC:CRS84", true)
    );
    // Execute test cases.
    for (TestCase tc : testCases) {
      SpatialReferenceSystemInformation srsInfo =
        new SpatialReferenceSystemInformation(tc.srid(), tc.stringId(), tc.isGeographic());
      Assertions.assertEquals(tc.srid(), srsInfo.srid());
      Assertions.assertEquals(tc.stringId(), srsInfo.stringId());
      Assertions.assertEquals(tc.isGeographic(), srsInfo.isGeographic());
    }
  }

  // Tests for the SpatialReferenceSystemMapper classes.

  @Test
  public void getStringIdReturnsCorrectStringIdForValidSrid() {
    // GEOMETRY: Spark-specific and OGC override entries.
    Assertions.assertEquals("SRID:0", CartesianSpatialReferenceSystemMapper.getStringId(0));
    Assertions.assertEquals("EPSG:3857", CartesianSpatialReferenceSystemMapper.getStringId(3857));
    Assertions.assertEquals("OGC:CRS84", CartesianSpatialReferenceSystemMapper.getStringId(4326));
    Assertions.assertEquals("OGC:CRS27", CartesianSpatialReferenceSystemMapper.getStringId(4267));
    Assertions.assertEquals("OGC:CRS83", CartesianSpatialReferenceSystemMapper.getStringId(4269));
    // GEOMETRY: PROJ-sourced registry entries (no override).
    Assertions.assertEquals("EPSG:32601", CartesianSpatialReferenceSystemMapper.getStringId(32601));
    Assertions.assertEquals(
        "ESRI:102013", CartesianSpatialReferenceSystemMapper.getStringId(102013));
    // GEOGRAPHY: Spark-specific and OGC override entries.
    Assertions.assertEquals("OGC:CRS84", GeographicSpatialReferenceSystemMapper.getStringId(4326));
    Assertions.assertEquals("OGC:CRS27", GeographicSpatialReferenceSystemMapper.getStringId(4267));
    Assertions.assertEquals("OGC:CRS83", GeographicSpatialReferenceSystemMapper.getStringId(4269));
    // GEOGRAPHY: PROJ-sourced registry entries (no override).
    Assertions.assertEquals("EPSG:4612", GeographicSpatialReferenceSystemMapper.getStringId(4612));
    Assertions.assertEquals(
        "ESRI:37001", GeographicSpatialReferenceSystemMapper.getStringId(37001));
  }

  @Test
  public void getStringIdReturnsNullForInvalidSrid() {
    // GEOMETRY: SRIDs not in the registry.
    Assertions.assertNull(CartesianSpatialReferenceSystemMapper.getStringId(-2));
    Assertions.assertNull(CartesianSpatialReferenceSystemMapper.getStringId(-1));
    Assertions.assertNull(CartesianSpatialReferenceSystemMapper.getStringId(1));
    Assertions.assertNull(CartesianSpatialReferenceSystemMapper.getStringId(2));
    Assertions.assertNull(CartesianSpatialReferenceSystemMapper.getStringId(9999));
    Assertions.assertNull(CartesianSpatialReferenceSystemMapper.getStringId(999999));
    // GEOGRAPHY: non-geographic or invalid SRIDs.
    Assertions.assertNull(GeographicSpatialReferenceSystemMapper.getStringId(-2));
    Assertions.assertNull(GeographicSpatialReferenceSystemMapper.getStringId(-1));
    Assertions.assertNull(GeographicSpatialReferenceSystemMapper.getStringId(0));
    Assertions.assertNull(GeographicSpatialReferenceSystemMapper.getStringId(1));
    Assertions.assertNull(GeographicSpatialReferenceSystemMapper.getStringId(2));
    Assertions.assertNull(GeographicSpatialReferenceSystemMapper.getStringId(3857));
    Assertions.assertNull(GeographicSpatialReferenceSystemMapper.getStringId(9999));
    Assertions.assertNull(GeographicSpatialReferenceSystemMapper.getStringId(999999));
  }

  @Test
  public void getSridReturnsCorrectSridForValidStringId() {
    // GEOMETRY.
    Assertions.assertEquals(0, CartesianSpatialReferenceSystemMapper.getSrid("SRID:0"));
    Assertions.assertEquals(3857, CartesianSpatialReferenceSystemMapper.getSrid("EPSG:3857"));
    Assertions.assertEquals(4326, CartesianSpatialReferenceSystemMapper.getSrid("OGC:CRS84"));
    Assertions.assertEquals(4267, CartesianSpatialReferenceSystemMapper.getSrid("OGC:CRS27"));
    Assertions.assertEquals(4269, CartesianSpatialReferenceSystemMapper.getSrid("OGC:CRS83"));
    Assertions.assertEquals(4326, CartesianSpatialReferenceSystemMapper.getSrid("EPSG:4326"));
    Assertions.assertEquals(4267, CartesianSpatialReferenceSystemMapper.getSrid("EPSG:4267"));
    Assertions.assertEquals(4269, CartesianSpatialReferenceSystemMapper.getSrid("EPSG:4269"));
    Assertions.assertEquals(32601, CartesianSpatialReferenceSystemMapper.getSrid("EPSG:32601"));
    // GEOGRAPHY.
    Assertions.assertEquals(4326, GeographicSpatialReferenceSystemMapper.getSrid("OGC:CRS84"));
  }

  @Test
  public void getSridReturnsNullForInvalidStringId() {
    // GEOMETRY: String IDs not in the registry.
    Assertions.assertNull(CartesianSpatialReferenceSystemMapper.getSrid("INVALID:ID"));
    Assertions.assertNull(CartesianSpatialReferenceSystemMapper.getSrid("EPSG:999999"));
    // GEOGRAPHY: non-geographic or invalid string IDs.
    Assertions.assertNull(GeographicSpatialReferenceSystemMapper.getSrid("INVALID:ID"));
    Assertions.assertNull(GeographicSpatialReferenceSystemMapper.getSrid("EPSG:999999"));
    Assertions.assertNull(GeographicSpatialReferenceSystemMapper.getSrid("SRID:0"));
    Assertions.assertNull(GeographicSpatialReferenceSystemMapper.getSrid("EPSG:3857"));
  }

  /**
   * The SRID map and CRS string-id map must be consistent inverses: canonical string ID from an
   * SRID entry resolves back to that entry, and every CRS key resolves to the canonical entry for
   * its SRID.
   */
  @Test
  public void testMapValidity() {
    SpatialReferenceSystemCache cache = SpatialReferenceSystemCache.getInstance();
    Map<Integer, SpatialReferenceSystemInformation> sridToSrs = cache.getSridToSrs();
    Map<String, SpatialReferenceSystemInformation> stringIdToSrs = cache.getStringIdToSrs();

    for (Map.Entry<Integer, SpatialReferenceSystemInformation> e : sridToSrs.entrySet()) {
      int srid = e.getKey();
      SpatialReferenceSystemInformation bySrid = e.getValue();
      Assertions.assertEquals(
          srid,
          bySrid.srid(),
          "sridToSrs key must match SpatialReferenceSystemInformation.srid()");
      SpatialReferenceSystemInformation byCanonicalCrs =
          stringIdToSrs.get(bySrid.stringId());
      Assertions.assertNotNull(
          byCanonicalCrs,
          "Canonical CRS string ID must exist in stringIdToSrs: srid=" + srid);
      Assertions.assertSame(
          bySrid,
          byCanonicalCrs,
          "sridToSrs entry must match stringIdToSrs for canonical string ID");
    }

    for (Map.Entry<String, SpatialReferenceSystemInformation> e : stringIdToSrs.entrySet()) {
      String crsKey = e.getKey();
      SpatialReferenceSystemInformation byCrs = e.getValue();
      SpatialReferenceSystemInformation bySrid = sridToSrs.get(byCrs.srid());
      Assertions.assertNotNull(
          bySrid,
          "CRS key must refer to a known SRID: crsKey=" + crsKey);
      Assertions.assertSame(
          bySrid,
          byCrs,
          "stringIdToSrs entry must match sridToSrs for that SRID (crsKey=" + crsKey + ")");
    }
  }

  /**
   * Validates expected registry scale: geographic SRID count and CRS string-id counts by
   * authority prefix (empty authority for {@code SRID:0}, plus OGC, EPSG, ESRI).
   */
  @Test
  public void testSpatialReferenceSystemContents() {
    SpatialReferenceSystemCache cache = SpatialReferenceSystemCache.getInstance();
    Map<Integer, SpatialReferenceSystemInformation> sridToSrs = cache.getSridToSrs();

    int emptyAuthoritySrid0 = 0;
    int ogc = 0;
    int epsg = 0;
    int esri = 0;
    for (SpatialReferenceSystemInformation srs : sridToSrs.values()) {
      String stringId = srs.stringId();
      if ("SRID:0".equals(stringId)) {
        emptyAuthoritySrid0++;
      } else if (stringId.startsWith("OGC:")) {
        ogc++;
      } else if (stringId.startsWith("EPSG:")) {
        epsg++;
      } else if (stringId.startsWith("ESRI:")) {
        esri++;
      }
    }
    Assertions.assertEquals(1, emptyAuthoritySrid0, "SRID:0 (empty authority)");
    Assertions.assertEquals(3, ogc, "OGC CRS string IDs");
    Assertions.assertEquals(7720, epsg, "EPSG CRS string IDs");
    Assertions.assertEquals(2919, esri, "ESRI CRS string IDs");
    Assertions.assertEquals(
        10643,
        sridToSrs.size(),
        "total SRID entries (SRID:0 + OGC + EPSG + ESRI)");
  }
}
