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

public class SpatialReferenceSystemMapperSuite {

  // Singleton instance of the SpatialReferenceSystemMapper to use across tests.
  private final SpatialReferenceSystemMapper srsMapper = SpatialReferenceSystemMapper.get();

  // Spatial types for GEOMETRY and GEOGRAPHY reference system mapping.
  private final SpatialReferenceSystemMapper.Type GEOM =
    SpatialReferenceSystemMapper.Type.GEOMETRY;
  private final SpatialReferenceSystemMapper.Type GEOG =
    SpatialReferenceSystemMapper.Type.GEOGRAPHY;

  @Test
  public void getStringIdReturnsCorrectStringIdForValidSrid() {
    // GEOMETRY.
    Assertions.assertEquals("SRID:0", srsMapper.getStringId(0, GEOM));
    Assertions.assertEquals("EPSG:3857", srsMapper.getStringId(3857, GEOM));
    Assertions.assertEquals("OGC:CRS84", srsMapper.getStringId(4326, GEOM));
    // GEOGRAPHY.
    Assertions.assertEquals("OGC:CRS84", srsMapper.getStringId(4326, GEOG));
  }

  @Test
  public void getStringIdReturnsNullForInvalidSrid() {
    // GEOMETRY.
    Assertions.assertNull(srsMapper.getStringId(-2, GEOM));
    Assertions.assertNull(srsMapper.getStringId(-1, GEOM));
    Assertions.assertNull(srsMapper.getStringId(1, GEOM));
    Assertions.assertNull(srsMapper.getStringId(2, GEOM));
    Assertions.assertNull(srsMapper.getStringId(9999, GEOM));
    // GEOGRAPHY.
    Assertions.assertNull(srsMapper.getStringId(-2, GEOG));
    Assertions.assertNull(srsMapper.getStringId(-1, GEOG));
    Assertions.assertNull(srsMapper.getStringId(0, GEOG));
    Assertions.assertNull(srsMapper.getStringId(1, GEOG));
    Assertions.assertNull(srsMapper.getStringId(2, GEOG));
    Assertions.assertNull(srsMapper.getStringId(3857, GEOG));
    Assertions.assertNull(srsMapper.getStringId(9999, GEOG));
  }

  @Test
  public void getSridReturnsCorrectSridForValidStringId() {
    // GEOMETRY.
    Assertions.assertEquals(0, srsMapper.getSrid("SRID:0", GEOM));
    Assertions.assertEquals(3857, srsMapper.getSrid("EPSG:3857", GEOM));
    Assertions.assertEquals(4326, srsMapper.getSrid("OGC:CRS84", GEOM));
    // GEOGRAPHY.
    Assertions.assertEquals(4326, srsMapper.getSrid("OGC:CRS84", GEOG));
  }

  @Test
  public void getSridReturnsNullForInvalidStringId() {
    // GEOMETRY.
    Assertions.assertNull(srsMapper.getSrid("INVALID:ID", GEOM));
    Assertions.assertNull(srsMapper.getSrid("EPSG:9999", GEOM));
    // GEOGRAPHY.
    Assertions.assertNull(srsMapper.getSrid("INVALID:ID", GEOG));
    Assertions.assertNull(srsMapper.getSrid("EPSG:9999", GEOG));
    Assertions.assertNull(srsMapper.getSrid("SRID:0", GEOG));
    Assertions.assertNull(srsMapper.getSrid("EPSG:3857", GEOG));
  }
}
