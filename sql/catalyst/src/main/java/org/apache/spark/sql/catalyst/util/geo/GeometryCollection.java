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

import java.util.List;

/**
 * Represents a GeometryCollection geometry.
 */
class GeometryCollection extends GeometryModel {
  private final List<GeometryModel> geometries;

  GeometryCollection(List<GeometryModel> geometries, int srid, boolean hasZ, boolean hasM) {
    super(GeoTypeId.GEOMETRY_COLLECTION, srid, hasZ, hasM);
    this.geometries = geometries;
  }

  List<GeometryModel> getGeometries() {
    return geometries;
  }

  int getNumGeometries() {
    return geometries.size();
  }

  @Override
  boolean isEmpty() {
    return geometries.isEmpty() || geometries.stream().allMatch(GeometryModel::isEmpty);
  }

  @Override
  int getDimensionCount() {
    return 2 + (hasZ ? 1 : 0) + (hasM ? 1 : 0);
  }
}
