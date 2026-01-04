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
 * Utility class for WKB-related constants and helper methods.
 */
public class WkbConstants {
  // WKB byte order values
  public static final byte BIG_ENDIAN = 0;
  public static final byte LITTLE_ENDIAN = 1;

  // WKB base type codes (2D geometries) - matches GeoTypeId values
  public static final int WKB_POINT = 1;
  public static final int WKB_LINESTRING = 2;
  public static final int WKB_POLYGON = 3;
  public static final int WKB_MULTIPOINT = 4;
  public static final int WKB_MULTILINESTRING = 5;
  public static final int WKB_MULTIPOLYGON = 6;
  public static final int WKB_GEOMETRYCOLLECTION = 7;

  // Min and max valid base type codes
  public static final int WKB_MIN_TYPE = WKB_POINT;
  public static final int WKB_MAX_TYPE = WKB_GEOMETRYCOLLECTION;

  // Dimensional offset multipliers (following OGC WKB spec)
  // Type = BaseType + DIM_OFFSET * DimFactor
  public static final int DIM_OFFSET_2D = 0;
  public static final int DIM_OFFSET_Z = 1000;
  public static final int DIM_OFFSET_M = 2000;
  public static final int DIM_OFFSET_ZM = 3000;

  // WKB type codes for 3DZ geometries (Z coordinate)
  public static final int WKB_POINT_Z = WKB_POINT + DIM_OFFSET_Z;
  public static final int WKB_LINESTRING_Z = WKB_LINESTRING + DIM_OFFSET_Z;
  public static final int WKB_POLYGON_Z = WKB_POLYGON + DIM_OFFSET_Z;
  public static final int WKB_MULTIPOINT_Z = WKB_MULTIPOINT + DIM_OFFSET_Z;
  public static final int WKB_MULTILINESTRING_Z = WKB_MULTILINESTRING + DIM_OFFSET_Z;
  public static final int WKB_MULTIPOLYGON_Z = WKB_MULTIPOLYGON + DIM_OFFSET_Z;
  public static final int WKB_GEOMETRYCOLLECTION_Z = WKB_GEOMETRYCOLLECTION + DIM_OFFSET_Z;

  // WKB type codes for 3DM geometries (M coordinate)
  public static final int WKB_POINT_M = WKB_POINT + DIM_OFFSET_M;
  public static final int WKB_LINESTRING_M = WKB_LINESTRING + DIM_OFFSET_M;
  public static final int WKB_POLYGON_M = WKB_POLYGON + DIM_OFFSET_M;
  public static final int WKB_MULTIPOINT_M = WKB_MULTIPOINT + DIM_OFFSET_M;
  public static final int WKB_MULTILINESTRING_M = WKB_MULTILINESTRING + DIM_OFFSET_M;
  public static final int WKB_MULTIPOLYGON_M = WKB_MULTIPOLYGON + DIM_OFFSET_M;
  public static final int WKB_GEOMETRYCOLLECTION_M = WKB_GEOMETRYCOLLECTION + DIM_OFFSET_M;

  // WKB type codes for 4D geometries (Z and M coordinates)
  public static final int WKB_POINT_ZM = WKB_POINT + DIM_OFFSET_ZM;
  public static final int WKB_LINESTRING_ZM = WKB_LINESTRING + DIM_OFFSET_ZM;
  public static final int WKB_POLYGON_ZM = WKB_POLYGON + DIM_OFFSET_ZM;
  public static final int WKB_MULTIPOINT_ZM = WKB_MULTIPOINT + DIM_OFFSET_ZM;
  public static final int WKB_MULTILINESTRING_ZM = WKB_MULTILINESTRING + DIM_OFFSET_ZM;
  public static final int WKB_MULTIPOLYGON_ZM = WKB_MULTIPOLYGON + DIM_OFFSET_ZM;
  public static final int WKB_GEOMETRYCOLLECTION_ZM = WKB_GEOMETRYCOLLECTION + DIM_OFFSET_ZM;

  // Size constants
  public static final int BYTE_SIZE = 1;
  public static final int INT_SIZE = 4;
  public static final int DOUBLE_SIZE = 8;

  // Default SRID
  public static final int DEFAULT_SRID = 0;

  /**
   * Returns true if the type value is a valid base type (1-7).
   */
  static boolean isValidBaseType(int type) {
    return type >= WkbConstants.WKB_MIN_TYPE && type <= WkbConstants.WKB_MAX_TYPE;
  }

  /**
   * Extracts the base type from a WKB type value (removing dimension offset).
   * For example: 1001 (Point Z) returns 1 (Point).
   */
  static int getBaseType(int wkbType) {
    if (wkbType >= WkbConstants.DIM_OFFSET_ZM + WkbConstants.WKB_MIN_TYPE
      && wkbType <= WkbConstants.DIM_OFFSET_ZM + WkbConstants.WKB_MAX_TYPE) {
      return wkbType - WkbConstants.DIM_OFFSET_ZM;
    } else if (wkbType >= WkbConstants.DIM_OFFSET_M + WkbConstants.WKB_MIN_TYPE
      && wkbType <= WkbConstants.DIM_OFFSET_M + WkbConstants.WKB_MAX_TYPE) {
      return wkbType - WkbConstants.DIM_OFFSET_M;
    } else if (wkbType >= WkbConstants.DIM_OFFSET_Z + WkbConstants.WKB_MIN_TYPE
      && wkbType <= WkbConstants.DIM_OFFSET_Z + WkbConstants.WKB_MAX_TYPE) {
      return wkbType - WkbConstants.DIM_OFFSET_Z;
    } else {
      return wkbType;
    }
  }

  /**
   * Returns the dimension count (number of coordinates per point) from a WKB type value.
   * 2D: 2, 3DZ: 3, 3DM: 3, 4D: 4
   */
  static int getDimensionCount(int wkbType) {
    if (wkbType >= WkbConstants.DIM_OFFSET_ZM + WkbConstants.WKB_MIN_TYPE
      && wkbType <= WkbConstants.DIM_OFFSET_ZM + WkbConstants.WKB_MAX_TYPE) {
      return 4;
    } else if (wkbType >= WkbConstants.DIM_OFFSET_M + WkbConstants.WKB_MIN_TYPE
      && wkbType <= WkbConstants.DIM_OFFSET_M + WkbConstants.WKB_MAX_TYPE) {
      return 3;
    } else if (wkbType >= WkbConstants.DIM_OFFSET_Z + WkbConstants.WKB_MIN_TYPE
      && wkbType <= WkbConstants.DIM_OFFSET_Z + WkbConstants.WKB_MAX_TYPE) {
      return 3;
    } else {
      return 2;
    }
  }

  /**
   * Returns true if the WKB type has Z coordinate (3DZ or 4D).
   */
  static boolean hasZ(int wkbType) {
    int baseType = getBaseType(wkbType);
    int offset = wkbType - baseType;
    return offset == DIM_OFFSET_Z || offset == DIM_OFFSET_ZM;
  }

  /**
   * Returns true if the WKB type has M coordinate (3DM or 4D).
   */
  static boolean hasM(int wkbType) {
    int baseType = getBaseType(wkbType);
    int offset = wkbType - baseType;
    return offset == DIM_OFFSET_M || offset == DIM_OFFSET_ZM;
  }

  /**
   * Returns true if the WKB type represents a valid type (2D, 3DZ, 3DM, or 4D).
   */
  static boolean isValidWkbType(int wkbType) {
    int baseType = getBaseType(wkbType);
    if (!isValidBaseType(baseType)) {
      return false;
    }
    // Check it's exactly one of the valid dimensional offsets
    int offset = wkbType - baseType;
    return offset == WkbConstants.DIM_OFFSET_2D || offset == WkbConstants.DIM_OFFSET_Z
      || offset == WkbConstants.DIM_OFFSET_M || offset == WkbConstants.DIM_OFFSET_ZM;
  }

  private WkbConstants() {
    // Utility class, no instances
  }
}


