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
    return coordinatesEqual(points.get(0), points.get(points.size() - 1));
  }

  /**
   * Compares two points for coordinate equality (X and Y only).
   * Z and M coordinates are NOT considered, per OGC standards.
   */
  private static boolean coordinatesEqual(Point p1, Point p2) {
    return p1.getX() == p2.getX() && p1.getY() == p2.getY();
  }

  /**
   * Appends the ring coordinates wrapped in parentheses for WKT output.
   */
  void appendCoordinatesToWkt(StringBuilder sb) {
    sb.append("(");
    for (int i = 0; i < points.size(); i++) {
      if (i > 0) {
        sb.append(",");
      }
      points.get(i).appendWktContent(sb);
    }
    sb.append(")");
  }
}
