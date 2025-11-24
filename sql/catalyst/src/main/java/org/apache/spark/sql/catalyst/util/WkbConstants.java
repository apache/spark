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

/**
 * Utility class for WKB-related constants and helper methods.
 */
public class WkbConstants {
  // WKB byte order values
  public static final byte BIG_ENDIAN = 0;
  public static final byte LITTLE_ENDIAN = 1;

  // WKB type codes for 2D geometries
  public static final int WKB_POINT = 1;
  public static final int WKB_LINESTRING = 2;
  public static final int WKB_POLYGON = 3;
  public static final int WKB_MULTIPOINT = 4;
  public static final int WKB_MULTILINESTRING = 5;
  public static final int WKB_MULTIPOLYGON = 6;
  public static final int WKB_GEOMETRYCOLLECTION = 7;

  // WKB type codes for 3DZ geometries (Z coordinate)
  public static final int WKB_POINT_Z = 1001;
  public static final int WKB_LINESTRING_Z = 1002;
  public static final int WKB_POLYGON_Z = 1003;
  public static final int WKB_MULTIPOINT_Z = 1004;
  public static final int WKB_MULTILINESTRING_Z = 1005;
  public static final int WKB_MULTIPOLYGON_Z = 1006;
  public static final int WKB_GEOMETRYCOLLECTION_Z = 1007;

  // WKB type codes for 3DM geometries (M coordinate)
  public static final int WKB_POINT_M = 2001;
  public static final int WKB_LINESTRING_M = 2002;
  public static final int WKB_POLYGON_M = 2003;
  public static final int WKB_MULTIPOINT_M = 2004;
  public static final int WKB_MULTILINESTRING_M = 2005;
  public static final int WKB_MULTIPOLYGON_M = 2006;
  public static final int WKB_GEOMETRYCOLLECTION_M = 2007;

  // WKB type codes for 4D geometries (Z and M coordinates)
  public static final int WKB_POINT_ZM = 3001;
  public static final int WKB_LINESTRING_ZM = 3002;
  public static final int WKB_POLYGON_ZM = 3003;
  public static final int WKB_MULTIPOINT_ZM = 3004;
  public static final int WKB_MULTILINESTRING_ZM = 3005;
  public static final int WKB_MULTIPOLYGON_ZM = 3006;
  public static final int WKB_GEOMETRYCOLLECTION_ZM = 3007;

  // EWKB flags
  public static final int EWKB_SRID_FLAG = 0x20000000;
  public static final int EWKB_Z_FLAG = 0x80000000;
  public static final int EWKB_M_FLAG = 0x40000000;
  public static final int EWKB_TYPE_MASK = 0x000000FF;

  // Size constants
  public static final int BYTE_SIZE = 1;
  public static final int INT_SIZE = 4;
  public static final int DOUBLE_SIZE = 8;

  // Default SRID
  public static final int DEFAULT_SRID = 0;

  private WkbConstants() {
    // Utility class, no instances
  }
}

