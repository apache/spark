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

/**
 * Enum type for geometry type IDs. The values chosen match the WKB specification
 * for 2D geometries and align with the values for 3DZ, 3DM, and 4D geometries.
 */
public enum GeoTypeId {
  ABSTRACT_GEOMETRY(-1),
  DYNAMIC_GEOMETRY(0),
  POINT(1),
  LINESTRING(2),
  POLYGON(3),
  MULTI_POINT(4),
  MULTI_LINESTRING(5),
  MULTI_POLYGON(6),
  GEOMETRY_COLLECTION(7),
  BOX(1729);

  private final long value;

  GeoTypeId(long value) {
    this.value = value;
  }

  public long getValue() {
    return value;
  }

  public static GeoTypeId fromValue(long value) {
    for (GeoTypeId type : values()) {
      if (type.value == value) {
        return type;
      }
    }
    throw new IllegalArgumentException("Invalid GeoTypeId value: " + value);
  }
}

