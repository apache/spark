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

/**
 * Enum type for geometry type IDs. The values chosen match the WKB specification
 * for 2D geometries and align with the values for 3DZ, 3DM, and 4D geometries.
 * <p>
 * These map to the type IDs used in the WKB (Well-Known Binary) format.
 **/
enum GeoTypeId {
  POINT(1, "POINT"),
  LINESTRING(2, "LINESTRING"),
  POLYGON(3, "POLYGON"),
  MULTI_POINT(4, "MULTIPOINT"),
  MULTI_LINESTRING(5, "MULTILINESTRING"),
  MULTI_POLYGON(6, "MULTIPOLYGON"),
  GEOMETRY_COLLECTION(7, "GEOMETRYCOLLECTION");

  private final long value;
  private final String wktName;

  GeoTypeId(long value, String wktName) {
    this.value = value;
    this.wktName = wktName;
  }

  long getValue() {
    return value;
  }

  String getWktName() {
    return wktName;
  }

  static GeoTypeId fromValue(long value) {
    for (GeoTypeId type : values()) {
      if (type.value == value) {
        return type;
      }
    }
    throw new IllegalArgumentException("Invalid GeoTypeId value: " + value);
  }
}
