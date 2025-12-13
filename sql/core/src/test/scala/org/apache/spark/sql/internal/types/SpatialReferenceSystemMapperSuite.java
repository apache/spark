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
    // GEOMETRY.
    Assertions.assertEquals("SRID:0", CartesianSpatialReferenceSystemMapper.getStringId(0));
    Assertions.assertEquals("EPSG:3857", CartesianSpatialReferenceSystemMapper.getStringId(3857));
    Assertions.assertEquals("OGC:CRS84", CartesianSpatialReferenceSystemMapper.getStringId(4326));
    // GEOGRAPHY.
    Assertions.assertEquals("OGC:CRS84", GeographicSpatialReferenceSystemMapper.getStringId(4326));
  }

  @Test
  public void getStringIdReturnsNullForInvalidSrid() {
    // GEOMETRY.
    Assertions.assertNull(CartesianSpatialReferenceSystemMapper.getStringId(-2));
    Assertions.assertNull(CartesianSpatialReferenceSystemMapper.getStringId(-1));
    Assertions.assertNull(CartesianSpatialReferenceSystemMapper.getStringId(1));
    Assertions.assertNull(CartesianSpatialReferenceSystemMapper.getStringId(2));
    Assertions.assertNull(CartesianSpatialReferenceSystemMapper.getStringId(9999));
    // GEOGRAPHY.
    Assertions.assertNull(GeographicSpatialReferenceSystemMapper.getStringId(-2));
    Assertions.assertNull(GeographicSpatialReferenceSystemMapper.getStringId(-1));
    Assertions.assertNull(GeographicSpatialReferenceSystemMapper.getStringId(0));
    Assertions.assertNull(GeographicSpatialReferenceSystemMapper.getStringId(1));
    Assertions.assertNull(GeographicSpatialReferenceSystemMapper.getStringId(2));
    Assertions.assertNull(GeographicSpatialReferenceSystemMapper.getStringId(3857));
    Assertions.assertNull(GeographicSpatialReferenceSystemMapper.getStringId(9999));
  }

  @Test
  public void getSridReturnsCorrectSridForValidStringId() {
    // GEOMETRY.
    Assertions.assertEquals(0, CartesianSpatialReferenceSystemMapper.getSrid("SRID:0"));
    Assertions.assertEquals(3857, CartesianSpatialReferenceSystemMapper.getSrid("EPSG:3857"));
    Assertions.assertEquals(4326, CartesianSpatialReferenceSystemMapper.getSrid("OGC:CRS84"));
    // GEOGRAPHY.
    Assertions.assertEquals(4326, GeographicSpatialReferenceSystemMapper.getSrid("OGC:CRS84"));
  }

  @Test
  public void getSridReturnsNullForInvalidStringId() {
    // GEOMETRY.
    Assertions.assertNull(CartesianSpatialReferenceSystemMapper.getSrid("INVALID:ID"));
    Assertions.assertNull(CartesianSpatialReferenceSystemMapper.getSrid("EPSG:9999"));
    // GEOGRAPHY.
    Assertions.assertNull(GeographicSpatialReferenceSystemMapper.getSrid("INVALID:ID"));
    Assertions.assertNull(GeographicSpatialReferenceSystemMapper.getSrid("EPSG:9999"));
    Assertions.assertNull(GeographicSpatialReferenceSystemMapper.getSrid("SRID:0"));
    Assertions.assertNull(GeographicSpatialReferenceSystemMapper.getSrid("EPSG:3857"));
  }
}
