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

package org.apache.spark.sql.types;

import org.apache.spark.sql.internal.types.CartesianSpatialReferenceSystemMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import org.apache.spark.SparkIllegalArgumentException;

import java.util.stream.Stream;

public class JavaGeometryTypeSuite {

  /*
   * Test cases GeometryType construction based on SRID.
   */

  @Test
  public void geometryTypeWithSpecifiedValidSridTest() {
    // Valid SRID values for GEOMETRY.
    Stream.of(0, 3857, 4326).forEach(srid -> {
      DataType geometryType = DataTypes.createGeometryType(srid);
      Assertions.assertEquals("GEOMETRY(" + srid + ")", geometryType.sql());
      Assertions.assertEquals("geometry(" + srid + ")", geometryType.typeName());
      Assertions.assertEquals("geometry(" + srid + ")", geometryType.simpleString());
    });
  }

  @Test
  public void geometryTypeWithSpecifiedInvalidSridTest() {
    // Invalid SRID values for GEOMETRY.
    Stream.of(-1, -2, 1, 2).forEach(srid -> {
      try {
        DataTypes.createGeometryType(srid);
        Assertions.fail("Expected SparkIllegalArgumentException for SRID: " + srid);
      } catch (SparkIllegalArgumentException e) {
        Assertions.assertEquals("ST_INVALID_SRID_VALUE", e.getCondition());
        Assertions.assertEquals(String.valueOf(srid), e.getMessageParameters().get("srid"));
      }
    });
  }

  /*
   * Test cases GeometryType construction based on CRS.
   */

  @Test
  public void geometryTypeWithSpecifiedValidCrsTest() {
    // Valid CRS values for GEOMETRY.
    Stream.of("SRID:0", "EPSG:3857", "OGC:CRS84").forEach(crs -> {
      Integer srid = CartesianSpatialReferenceSystemMapper.getSrid(crs);
      DataType geometryType = DataTypes.createGeometryType(crs);
      Assertions.assertEquals("GEOMETRY(" + srid + ")", geometryType.sql());
      Assertions.assertEquals("geometry(" + srid + ")", geometryType.typeName());
      Assertions.assertEquals("geometry(" + srid + ")", geometryType.simpleString());
    });
  }

  @Test
  public void geometryTypeWithSpecifiedInvalidCrsTest() {
    // Invalid CRS values for GEOMETRY.
    Stream.of("0", "SRID", "SRID:-1", "SRID:4326", "CRS84", "").forEach(crs -> {
      try {
        DataTypes.createGeometryType(crs);
        Assertions.fail("Expected SparkIllegalArgumentException for CRS: " + crs);
      } catch (SparkIllegalArgumentException e) {
        Assertions.assertEquals("ST_INVALID_CRS_VALUE", e.getCondition());
        Assertions.assertEquals(crs, e.getMessageParameters().get("crs"));
      }
    });
  }

  @Test
  public void geometryTypeWithSpecifiedAnyTest() {
    // Special string value "ANY" in place of CRS is used to denote a mixed GEOMETRY type.
    DataType geometryType = DataTypes.createGeometryType("ANY");
    Assertions.assertEquals("GEOMETRY(ANY)", geometryType.sql());
    Assertions.assertEquals("geometry(any)", geometryType.typeName());
    Assertions.assertEquals("geometry(any)", geometryType.simpleString());
  }
}
