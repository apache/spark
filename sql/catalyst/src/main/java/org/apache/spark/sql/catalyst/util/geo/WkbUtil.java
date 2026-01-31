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
class WkbUtil {
  // WKB byte order values
  static final byte BIG_ENDIAN = 0;
  static final byte LITTLE_ENDIAN = 1;

  // Min and max valid base type codes
  static final int WKB_MIN_TYPE = (int) GeoTypeId.POINT.getValue();
  static final int WKB_MAX_TYPE = (int) GeoTypeId.GEOMETRY_COLLECTION.getValue();

  // Dimensional offset multipliers (following OGC WKB spec)
  // Type = BaseType + DIM_OFFSET * DimFactor
  static final int DIM_OFFSET_2D = 0;
  static final int DIM_OFFSET_Z = 1000;
  static final int DIM_OFFSET_M = 2000;
  static final int DIM_OFFSET_ZM = 3000;

  // Size constants
  static final int BYTE_SIZE = 1;
  static final int INT_SIZE = 4;
  static final int DOUBLE_SIZE = 8;
  static final int TYPE_SIZE = INT_SIZE; // WKB type is a 4-byte integer

  private WkbUtil() {
    // Utility class, no instances
  }

  /**
   * Returns true if the type value is a valid base type (1-7).
   */
  static boolean isValidBaseType(int type) {
    return type >= WkbUtil.WKB_MIN_TYPE && type <= WkbUtil.WKB_MAX_TYPE;
  }

  /**
   * Extracts the base type value from a WKB type value (removing dimension offset).
   * For example: 1001 (Point Z) returns 1 (Point).
   */
  private static int getBaseTypeValue(int wkbType) {
    if (wkbType >= WkbUtil.DIM_OFFSET_ZM + WkbUtil.WKB_MIN_TYPE
      && wkbType <= WkbUtil.DIM_OFFSET_ZM + WkbUtil.WKB_MAX_TYPE) {
      return wkbType - WkbUtil.DIM_OFFSET_ZM;
    } else if (wkbType >= WkbUtil.DIM_OFFSET_M + WkbUtil.WKB_MIN_TYPE
      && wkbType <= WkbUtil.DIM_OFFSET_M + WkbUtil.WKB_MAX_TYPE) {
      return wkbType - WkbUtil.DIM_OFFSET_M;
    } else if (wkbType >= WkbUtil.DIM_OFFSET_Z + WkbUtil.WKB_MIN_TYPE
      && wkbType <= WkbUtil.DIM_OFFSET_Z + WkbUtil.WKB_MAX_TYPE) {
      return wkbType - WkbUtil.DIM_OFFSET_Z;
    } else {
      return wkbType;
    }
  }

  /**
   * Extracts the base type from a WKB type value (removing dimension offset).
   * For example: 1001 (Point Z) returns GeoTypeId.POINT.
   */
  static GeoTypeId getBaseType(int wkbType) {
    return GeoTypeId.fromValue(getBaseTypeValue(wkbType));
  }

  /**
   * Returns the dimension count (number of coordinates per point) from a WKB type value.
   * 2D: 2, 3DZ: 3, 3DM: 3, 4D: 4
   */
  static int getDimensionCount(int wkbType) {
    if (wkbType >= WkbUtil.DIM_OFFSET_ZM + WkbUtil.WKB_MIN_TYPE
      && wkbType <= WkbUtil.DIM_OFFSET_ZM + WkbUtil.WKB_MAX_TYPE) {
      return 4;
    } else if (wkbType >= WkbUtil.DIM_OFFSET_M + WkbUtil.WKB_MIN_TYPE
      && wkbType <= WkbUtil.DIM_OFFSET_M + WkbUtil.WKB_MAX_TYPE) {
      return 3;
    } else if (wkbType >= WkbUtil.DIM_OFFSET_Z + WkbUtil.WKB_MIN_TYPE
      && wkbType <= WkbUtil.DIM_OFFSET_Z + WkbUtil.WKB_MAX_TYPE) {
      return 3;
    } else {
      return 2;
    }
  }

  /**
   * Returns true if the WKB type has Z coordinate (3DZ or 4D).
   */
  static boolean hasZ(int wkbType) {
    int baseType = getBaseTypeValue(wkbType);
    int offset = wkbType - baseType;
    return offset == DIM_OFFSET_Z || offset == DIM_OFFSET_ZM;
  }

  /**
   * Returns true if the WKB type has M coordinate (3DM or 4D).
   */
  static boolean hasM(int wkbType) {
    int baseType = getBaseTypeValue(wkbType);
    int offset = wkbType - baseType;
    return offset == DIM_OFFSET_M || offset == DIM_OFFSET_ZM;
  }

  /**
   * Returns true if the WKB type represents a valid type (2D, 3DZ, 3DM, or 4D).
   */
  static boolean isValidWkbType(int wkbType) {
    int baseType = getBaseTypeValue(wkbType);
    if (!isValidBaseType(baseType)) {
      return false;
    }
    // Check it's exactly one of the valid dimensional offsets
    int offset = wkbType - baseType;
    return offset == WkbUtil.DIM_OFFSET_2D || offset == WkbUtil.DIM_OFFSET_Z
      || offset == WkbUtil.DIM_OFFSET_M || offset == WkbUtil.DIM_OFFSET_ZM;
  }
}
