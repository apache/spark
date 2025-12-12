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

import java.util.List;

/**
 * Represents a GeometryCollection geometry.
 */
public class GeometryCollection extends Geometry {
  private final List<Geometry> geometries;

  public GeometryCollection(List<Geometry> geometries, int srid) {
    super(GeoTypeId.GEOMETRY_COLLECTION, srid);
    this.geometries = geometries;
  }

  public List<Geometry> getGeometries() {
    return geometries;
  }

  public int getNumGeometries() {
    return geometries.size();
  }

  @Override
  public boolean isEmpty() {
    return geometries.isEmpty();
  }

  @Override
  public String toString() {
    if (isEmpty()) {
      return "GEOMETRYCOLLECTION EMPTY";
    }
    return "GEOMETRYCOLLECTION (" + geometries.size() + " geometries)";
  }
}

