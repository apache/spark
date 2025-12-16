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

import java.util.Arrays;

/**
 * Represents a point geometry with coordinates.
 */
class Point extends GeometryModel {
  private final double[] coordinates;
  private final boolean is_Empty;


  Point(double[] coordinates, int srid) {
    super(GeoTypeId.POINT, srid);
    this.coordinates = coordinates;
    // Check if the point is empty (any coordinate is NaN)
    boolean empty = false;
    for (double coord : coordinates) {
      if (Double.isNaN(coord)) {
        empty = true;
        break;
      }
    }
    this.is_Empty = empty;
  }

  double getX() {
    return coordinates.length > 0 ? coordinates[0] : Double.NaN;
  }

  double getY() {
    return coordinates.length > 1 ? coordinates[1] : Double.NaN;
  }

  double getZ() {
    return coordinates.length > 2 ? coordinates[2] : Double.NaN;
  }

  double getM() {
    return coordinates.length > 3 ? coordinates[3] : Double.NaN;
  }

  double[] getCoordinates() {
    return coordinates;
  }

  @Override
  boolean isEmpty() {
    return is_Empty;
  }

  @Override
  public String toString() {
    if (is_Empty) {
      return "POINT EMPTY";
    }
    return "POINT (" + Arrays.toString(coordinates) + ")";
  }
}

