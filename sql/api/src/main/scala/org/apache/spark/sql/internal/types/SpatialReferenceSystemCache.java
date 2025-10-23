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

import org.apache.spark.sql.internal.util.SingletonBase;

import java.util.HashMap;
import java.util.List;

/**
 * Class for maintaining the mappings between supported SRID/CRS values and the corresponding SRS.
 */
public class SpatialReferenceSystemCache extends SingletonBase<SpatialReferenceSystemCache> {

  // Private constructor to prevent external instantiation.
  private SpatialReferenceSystemCache() {
    populateSpatialReferenceSystemInformationMapping();
  }

  @Override
  protected SpatialReferenceSystemCache createInstance() {
    return new SpatialReferenceSystemCache();
  }

  public static SpatialReferenceSystemCache getInstance() {
    return SingletonBase.getInstance(new SpatialReferenceSystemCache());
  }

  // Hash map for defining the mappings from the integer SRID value to the full SRS information.
  private final HashMap<Integer, SpatialReferenceSystemInformation> sridToSrs =
      new HashMap<>();

  // Hash map for defining the mappings from the string ID value to the full SRS information.
  private final HashMap<String, SpatialReferenceSystemInformation> stringIdToSrs =
      new HashMap<>();

  // Helper method for building the SRID-to-SRS and stringID-to-SRS mappings.
  private void populateSpatialReferenceSystemInformationMapping() {
    // Currently, we only support a limited set of SRID / CRS values. However, we will soon extend
    // this to support all the SRIDs supported by relevant authorities and libraries. The SRS list
    // below will be updated accordingly, and the maps will be populated with more complete data.
    List<SpatialReferenceSystemInformation> srsInformationList = List.of(
      new SpatialReferenceSystemInformation(0, "SRID:0", false),
      new SpatialReferenceSystemInformation(3857, "EPSG:3857", false),
      new SpatialReferenceSystemInformation(4326, "OGC:CRS84", true)
    );
    // Populate the mappings using the same SRS information objects, avoiding any duplication.
    for (SpatialReferenceSystemInformation srsInformation: srsInformationList) {
      sridToSrs.put(srsInformation.srid(), srsInformation);
      stringIdToSrs.put(srsInformation.stringId(), srsInformation);
    }
  }

  // Utility method that can be used to check whether an SRID value is valid or not.
  public boolean isValidSrid(int srid) {
    return sridToSrs.containsKey(srid);
  }

  // Utility method that can be used to check whether an SRID value is geographic or not.
  public boolean isGeographicSrid(int srid) {
    return isValidSrid(srid) && sridToSrs.get(srid).isGeographic();
  }

  // Utility method that can be used to check whether a string ID value is valid or not.
  public boolean isValidStringId(String stringId) {
    return stringIdToSrs.containsKey(stringId);
  }

  // Utility method that can be used to check whether a string ID value is geographic or not.
  public boolean isGeographicStringId(String stringId) {
    return isValidStringId(stringId) && stringIdToSrs.get(stringId).isGeographic();
  }

  // Returns the SRS corresponding to the input SRID. If not supported, returns `null`.
  public SpatialReferenceSystemInformation getSrsInfo(int srid) {
    return sridToSrs.getOrDefault(srid, null);
  }

  // Returns the SRS corresponding to the input string ID. If not supported, returns `null`.
  public SpatialReferenceSystemInformation getSrsInfo(String stringId) {
    return stringIdToSrs.getOrDefault(stringId, null);
  }
}
