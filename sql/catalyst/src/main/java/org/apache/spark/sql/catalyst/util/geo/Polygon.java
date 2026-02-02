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
 * Represents a Polygon geometry.
 */
class Polygon extends GeometryModel {
  private final List<Ring> rings;

  Polygon(List<Ring> rings, int srid, boolean hasZ, boolean hasM) {
    super(GeoTypeId.POLYGON, srid, hasZ, hasM);
    this.rings = rings;
  }

  List<Ring> getRings() {
    return rings;
  }

  Ring getExteriorRing() {
    return rings.isEmpty() ? null : rings.get(0);
  }

  int getNumInteriorRings() {
    return Math.max(0, rings.size() - 1);
  }

  Ring getInteriorRingN(int n) {
    return rings.get(n + 1);
  }

  @Override
  boolean isEmpty() {
    return rings.isEmpty() || rings.stream().allMatch(Ring::isEmpty);
  }

  @Override
  int getDimensionCount() {
    return 2 + (hasZ ? 1 : 0) + (hasM ? 1 : 0);
  }
}
