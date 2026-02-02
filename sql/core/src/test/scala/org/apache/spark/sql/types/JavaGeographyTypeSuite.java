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

import org.apache.spark.sql.internal.types.GeographicSpatialReferenceSystemMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import org.apache.spark.SparkIllegalArgumentException;

import java.util.stream.Stream;

public class JavaGeographyTypeSuite {

  /*
   * Test cases GeographyType construction based on SRID.
   */

  @Test
  public void geographyTypeWithSpecifiedValidSridTest() {
    // Valid SRID values for GEOGRAPHY. Note that only 4326 is supported for now.
    Stream.of(4326).forEach(srid -> {
      DataType geographyType = DataTypes.createGeographyType(srid);
      Assertions.assertEquals("GEOGRAPHY(" + srid + ")", geographyType.sql());
      Assertions.assertEquals("geography(" + srid + ")", geographyType.typeName());
      Assertions.assertEquals("geography(" + srid + ")", geographyType.simpleString());
    });
  }

  @Test
  public void geographyTypeWithSpecifiedInvalidSridTest() {
    // Invalid SRID values for GEOGRAPHY.
    Stream.of(-1, -2, 0, 1, 2).forEach(srid -> {
      try {
        DataTypes.createGeographyType(srid);
        Assertions.fail("Expected SparkIllegalArgumentException for SRID: " + srid);
      } catch (SparkIllegalArgumentException e) {
        Assertions.assertEquals("ST_INVALID_SRID_VALUE", e.getCondition());
        Assertions.assertEquals(String.valueOf(srid), e.getMessageParameters().get("srid"));
      }
    });
  }

  /*
   * Test cases GeographyType construction based on CRS.
   */

  @Test
  public void geographyTypeWithSpecifiedValidCrsTest() {
    // Valid CRS values for GEOGRAPHY. Note that only "OGC:CRS84" is supported for now.
    Stream.of("OGC:CRS84").forEach(crs -> {
      Integer srid = GeographicSpatialReferenceSystemMapper.getSrid(crs);
      DataType geographyType = DataTypes.createGeographyType(crs);
      Assertions.assertEquals("GEOGRAPHY(" + srid + ")", geographyType.sql());
      Assertions.assertEquals("geography(" + srid + ")", geographyType.typeName());
      Assertions.assertEquals("geography(" + srid + ")", geographyType.simpleString());
    });
  }

  @Test
  public void geographyTypeWithSpecifiedInvalidCrsTest() {
    // Invalid CRS values for GEOGRAPHY.
    Stream.of("0", "SRID", "SRID:-1", "SRID:4326", "CRS84", "").forEach(crs -> {
      try {
        DataTypes.createGeographyType(crs);
        Assertions.fail("Expected SparkIllegalArgumentException for CRS: " + crs);
      } catch (SparkIllegalArgumentException e) {
        Assertions.assertEquals("ST_INVALID_CRS_VALUE", e.getCondition());
        Assertions.assertEquals(crs, e.getMessageParameters().get("crs"));
      }
    });
  }

  @Test
  public void geographyTypeWithSpecifiedAnyTest() {
    // Special string value "ANY" in place of CRS is used to denote a mixed GEOGRAPHY type.
    DataType geographyType = DataTypes.createGeographyType("ANY");
    Assertions.assertEquals("GEOGRAPHY(ANY)", geographyType.sql());
    Assertions.assertEquals("geography(any)", geographyType.typeName());
    Assertions.assertEquals("geography(any)", geographyType.simpleString());
  }
}
