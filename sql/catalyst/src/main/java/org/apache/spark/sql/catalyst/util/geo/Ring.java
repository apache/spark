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
 * Represents a ring (closed linestring) in a polygon.
 *
 * In line with OGC standards, a ring must have at least 4 points,
 * with the first and last point being identical to close the ring.
 */
class Ring {
  private final List<Point> points;

  Ring(List<Point> points) {
    this.points = points;
  }

  List<Point> getPoints() {
    return points;
  }

  int getNumPoints() {
    return points.size();
  }

  boolean isEmpty() {
    return points.isEmpty();
  }

  boolean isClosed() {
    if (points.size() < 4) {
      return false;
    }
    Point first = points.get(0);
    Point last = points.get(points.size() - 1);
    return coordinatesEqual(first.getCoordinates(), last.getCoordinates());
  }

  /**
   * Compares two coordinate arrays for equality.
   */
  private static boolean coordinatesEqual(double[] coords1, double[] coords2) {
    if (coords1.length != coords2.length) {
      return false;
    }
    for (int i = 0; i < coords1.length; i++) {
      if (coords1[i] != coords2[i]) {
        return false;
      }
    }
    return true;
  }
}
