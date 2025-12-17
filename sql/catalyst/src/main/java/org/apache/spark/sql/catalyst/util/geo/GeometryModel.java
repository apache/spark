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
 * Abstract base class for specific geometry types (Point, LineString, Polygon, etc.).
 * This class provides common functionality needed by geometry subclasses.
 */
public abstract class GeometryModel {

  /** GeometryModel internal implementation. */

  // Geometry type and SRID information
  protected GeoTypeId typeId;
  protected int sridValue;

  /** GeometryModel constants. */

  // The default SRID value for GEOMETRY values.
  static int DEFAULT_SRID = 0;

  /** GeometryModel constructors. */

  // Protected constructor for subclasses
  protected GeometryModel(GeoTypeId typeId, int srid) {
    this.typeId = typeId;
    this.sridValue = srid;
  }

  /** GeometryModel getters and instance methods. */

  // Returns the geometry type ID
  GeoTypeId getTypeId() {
    return typeId;
  }

  // Returns the SRID value
  int srid() {
    return sridValue;
  }

  // Sets the SRID value
  void setSrid(int srid) {
    this.sridValue = srid;
  }

  // Returns whether this geometry is empty (subclasses should override)
  boolean isEmpty() {
    return false;
  }

  // Returns the dimension count (2 for 2D, 3 for 3DZ/3DM, 4 for 4D)
  // Subclasses should override this method
  int getDimensionCount() {
    return 2; // Default to 2D
  }

  // Returns true if this geometry has Z coordinate
  // Subclasses should override this method
  boolean hasZ() {
    return false;
  }

  // Returns true if this geometry has M coordinate
  // Subclasses should override this method
  boolean hasM() {
    return false;
  }

  /** Type checking methods for GeometryModel subclasses. */

  // Returns true if this geometry is a Point
  boolean isPoint() {
    return this instanceof Point;
  }

  // Returns true if this geometry is a LineString
  boolean isLineString() {
    return this instanceof LineString;
  }

  // Returns true if this geometry is a Polygon
  boolean isPolygon() {
    return this instanceof Polygon;
  }

  // Returns true if this geometry is a MultiPoint
  boolean isMultiPoint() {
    return this instanceof MultiPoint;
  }

  // Returns true if this geometry is a MultiLineString
  boolean isMultiLineString() {
    return this instanceof MultiLineString;
  }

  // Returns true if this geometry is a MultiPolygon
  boolean isMultiPolygon() {
    return this instanceof MultiPolygon;
  }

  // Returns true if this geometry is a GeometryCollection
  boolean isGeometryCollection() {
    return this instanceof GeometryCollection;
  }

  /** Type casting methods for GeometryModel subclasses. */

  // Casts this geometry to Point if it is an instance of Point
  Point asPoint() {
    if (isPoint()) {
      return (Point) this;
    }
    throw new ClassCastException("Cannot cast " + getClass().getSimpleName() + " to Point");
  }

  // Casts this geometry to LineString if it is an instance of LineString
  LineString asLineString() {
    if (isLineString()) {
      return (LineString) this;
    }
    throw new ClassCastException("Cannot cast " + getClass().getSimpleName() + " to LineString");
  }

  // Casts this geometry to Polygon if it is an instance of Polygon
  Polygon asPolygon() {
    if (isPolygon()) {
      return (Polygon) this;
    }
    throw new ClassCastException("Cannot cast " + getClass().getSimpleName() + " to Polygon");
  }

  // Casts this geometry to MultiPoint if it is an instance of MultiPoint
  MultiPoint asMultiPoint() {
    if (isMultiPoint()) {
      return (MultiPoint) this;
    }
    throw new ClassCastException("Cannot cast " + getClass().getSimpleName() + " to MultiPoint");
  }

  // Casts this geometry to MultiLineString if it is an instance of MultiLineString
  MultiLineString asMultiLineString() {
    if (isMultiLineString()) {
      return (MultiLineString) this;
    }
    throw new ClassCastException(
      "Cannot cast " + getClass().getSimpleName() + " to MultiLineString");
  }

  // Casts this geometry to MultiPolygon if it is an instance of MultiPolygon
  MultiPolygon asMultiPolygon() {
    if (isMultiPolygon()) {
      return (MultiPolygon) this;
    }
    throw new ClassCastException("Cannot cast " + getClass().getSimpleName() + " to MultiPolygon");
  }

  // Casts this geometry to GeometryCollection if it is an instance of GeometryCollection
  GeometryCollection asGeometryCollection() {
    if (isGeometryCollection()) {
      return (GeometryCollection) this;
    }
    throw new ClassCastException(
      "Cannot cast " + getClass().getSimpleName() + " to GeometryCollection");
  }

}

