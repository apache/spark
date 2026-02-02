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
 * Represents a point geometry with coordinates.
 */
class Point extends GeometryModel {
  private final double[] coordinates;
  private final boolean isEmpty;

  /**
   * Creates a Point with coordinates, SRID, and dimension flags.
   *
   * @param coordinates the coordinate array (X, Y, and optionally Z and/or M)
   * @param srid the spatial reference ID
   * @param hasZ true if the point has a Z coordinate
   * @param hasM true if the point has an M coordinate
   */
  Point(double[] coordinates, int srid, boolean hasZ, boolean hasM) {
    super(GeoTypeId.POINT, srid, hasZ, hasM);
    this.coordinates = coordinates;
    // Check if the point is empty (any coordinate is NaN)
    boolean empty = false;
    for (double coord : coordinates) {
      if (Double.isNaN(coord)) {
        empty = true;
        break;
      }
    }
    this.isEmpty = empty;
  }

  /**
   * Creates a Point with coordinates and SRID.
   * Dimension flags are inferred from coordinate array length:
   * - 2 coordinates: 2D (X, Y)
   * - 3 coordinates: 3DZ (X, Y, Z)
   * - 4 coordinates: 4D (X, Y, Z, M)
   */
  Point(double[] coordinates, int srid) {
    this(coordinates, srid,
        coordinates.length >= 3,  // hasZ if 3 or more coordinates
        coordinates.length >= 4); // hasM if 4 coordinates
  }

  double getX() {
    return coordinates.length > 0 ? coordinates[0] : Double.NaN;
  }

  double getY() {
    return coordinates.length > 1 ? coordinates[1] : Double.NaN;
  }

  double getZ() {
    // Z is at index 2 only if hasZ is true
    return hasZ && coordinates.length > 2 ? coordinates[2] : Double.NaN;
  }

  double getM() {
    // M is at index 3 for XYZM (4D), or index 2 for XYM (3DM)
    if (!hasM) {
      return Double.NaN;
    }
    if (hasZ) {
      // XYZM: M is at index 3
      return coordinates.length > 3 ? coordinates[3] : Double.NaN;
    } else {
      // XYM: M is at index 2
      return coordinates.length > 2 ? coordinates[2] : Double.NaN;
    }
  }

  double[] getCoordinates() {
    return coordinates;
  }

  @Override
  boolean isEmpty() {
    return isEmpty;
  }

  @Override
  int getDimensionCount() {
    return coordinates.length;
  }
}
