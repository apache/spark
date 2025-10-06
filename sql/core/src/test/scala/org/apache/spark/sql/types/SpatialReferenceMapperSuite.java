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
 
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SpatialReferenceMapperSuite {

  @Test
  public void getStringIdReturnsCorrectStringIdForValidSrid() {
    SpatialReferenceMapper srMapper = SpatialReferenceMapper.get();
    Assertions.assertEquals("SRID:0", srMapper.getStringId(0));
    Assertions.assertEquals("EPSG:3857", srMapper.getStringId(3857));
    Assertions.assertEquals("OGC:CRS84", srMapper.getStringId(4326));
  }

  @Test
  public void getStringIdReturnsNullForInvalidSrid() {
    SpatialReferenceMapper srMapper = SpatialReferenceMapper.get();
    Assertions.assertNull(srMapper.getStringId(-1));
    Assertions.assertNull(srMapper.getStringId(9999));
  }

  @Test
  public void getSridReturnsCorrectSridForValidStringId() {
    SpatialReferenceMapper srMapper = SpatialReferenceMapper.get();
    Assertions.assertEquals(0, srMapper.getSrid("SRID:0"));
    Assertions.assertEquals(3857, srMapper.getSrid("EPSG:3857"));
    Assertions.assertEquals(4326, srMapper.getSrid("OGC:CRS84"));
  }

  @Test
  public void getSridReturnsNullForInvalidStringId() {
    SpatialReferenceMapper srMapper = SpatialReferenceMapper.get();
    Assertions.assertNull(srMapper.getSrid("INVALID:ID"));
    Assertions.assertNull(srMapper.getSrid("EPSG:9999"));
  }
}
