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

package org.apache.spark.sql.internal.types;

import java.util.HashMap;
import java.util.HashSet;

/*
 * Class for maintaining mappings between supported SRID values and the string ID of the
 * corresponding CRS.
 */
public class SpatialReferenceSystemMapper {

  // We implement this class as a singleton (we disallow construction).
  private SpatialReferenceSystemMapper() {}

  private static final SpatialReferenceSystemMapper Instance = new SpatialReferenceSystemMapper();

  // Returns the unique instance of this class.
  public static SpatialReferenceSystemMapper get() {
    return Instance;
  }

  // The `SpatialReferenceSystemMapper` class can be used for both GEOGRAPHY and GEOMETRY types.
  // This enumeration defines the two spatial type options, in the context of SRS mapping.
  public enum Type {
    GEOGRAPHY,
    GEOMETRY
  }

  // Hash maps for defining the mappings to/from SRID and string ID for a CRS.
  private static final HashMap<Integer, String> sridToStringId = buildSridToStringIdMap();
  private static final HashMap<String, Integer> stringIdToSrid = buildStringIdToSridMap();

  // Hash set for keeping track of the supported SRID values for geographic CRS.
  public static final HashSet<Integer> geographicSrids = buildGeographicSridSet();

  // Returns the string ID corresponding to the input SRID. If the input SRID is not supported,
  // `null` is returned.
  public String getStringId(int srid, Type type) {
    String stringId = sridToStringId.get(srid);
    return switch (type) {
      case GEOGRAPHY -> geographicSrids.contains(srid) ? stringId : null;
      case GEOMETRY -> stringId;
    };
  }

  // Returns the SRID corresponding to the input string ID. If the input string ID is not
  // supported, `null` is returned.
  public Integer getSrid(String stringId, Type type) {
    Integer srid = stringIdToSrid.get(stringId);
      return switch (type) {
        case GEOGRAPHY -> geographicSrids.contains(srid) ? srid : null;
        case GEOMETRY -> srid;
      };
  }

  // Currently, we only support a limited set of SRID / CRS mappings. However, we will soon extend
  // this to support all the SRIDs supported by relevant authorities and libraries. The methods
  // below will be updated accordingly, in order to populate the mappings with more complete data.

  // Helper method for building the SRID-to-string-ID mapping.
  private static HashMap<Integer, String> buildSridToStringIdMap() {
    HashMap<Integer, String> map = new HashMap<>();
    map.put(0, "SRID:0"); // Unspecified
    map.put(3857, "EPSG:3857"); // Web Mercator
    map.put(4326, "OGC:CRS84"); // WGS84
    return map;
  }

  // Helper method for building the string-ID-to-SRID mapping.
  private static HashMap<String, Integer> buildStringIdToSridMap() {
    HashMap<String, Integer> map = new HashMap<>();
    map.put("SRID:0", 0); // Unspecified
    map.put("EPSG:3857", 3857); // Web Mercator
    map.put("OGC:CRS84", 4326); // WGS84
    return map;
  }

  // Helper method for building the set of supported geographic SRID values.
  private static HashSet<Integer> buildGeographicSridSet() {
    HashSet<Integer> set = new HashSet<>();
    set.add(4326); // WGS84
    return set;
  }
}
