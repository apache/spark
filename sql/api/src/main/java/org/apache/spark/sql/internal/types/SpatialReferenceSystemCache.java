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

import org.apache.spark.annotation.Unstable;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Class for maintaining the mappings between supported SRID/CRS values and the corresponding SRS.
 *
 * The registry is populated from a CSV resource file generated from the PROJ library's EPSG
 * database (see dev/generate_srs_registry.py). Additional Spark-specific entries and aliases
 * are added for storage format compatibility (e.g. OGC:CRS84 for Parquet/Delta/Iceberg).
 */
@Unstable
public class SpatialReferenceSystemCache {

  private static final String SRS_REGISTRY_RESOURCE =
      "/org/apache/spark/sql/srs_registry.csv";

  // Private constructor to prevent external instantiation of this singleton class.
  private SpatialReferenceSystemCache() {
    populateSpatialReferenceSystemInformationMapping();
  }

  // The singleton `instance` is created lazily, meaning that it is not instantiated until the
  // `getInstance()` method is called for the first time. Note that this solution is thread-safe.
  private static volatile SpatialReferenceSystemCache instance = null;

  // The `getInstance` method uses double-checked locking to ensure efficient and safe instance
  // creation. The singleton instance is created only once, even in a multithreaded environment.
  public static SpatialReferenceSystemCache getInstance() {
    if (instance == null) {
      synchronized (SpatialReferenceSystemCache.class) {
        if (instance == null) {
          instance = new SpatialReferenceSystemCache();
        }
      }
    }
    return instance;
  }

  // Hash map for defining the mappings from the integer SRID value to the full SRS information.
  private final HashMap<Integer, SpatialReferenceSystemInformation> sridToSrs =
      new HashMap<>();

  // Hash map for defining the mappings from the string ID value to the full SRS information.
  private final HashMap<String, SpatialReferenceSystemInformation> stringIdToSrs =
      new HashMap<>();

  // Helper method for building the SRID-to-SRS and stringID-to-SRS mappings.
  private void populateSpatialReferenceSystemInformationMapping() {
    loadSrsRegistryCsv();
    addSparkSpecificEntries();
  }

  /**
   * Load SRS entries from the CSV resource file generated from the PROJ EPSG database.
   * The CSV has a header row (srid,string_id,is_geographic) and comment lines starting with '#'.
   */
  private void loadSrsRegistryCsv() {
    try (InputStream is = getClass().getResourceAsStream(SRS_REGISTRY_RESOURCE)) {
      if (is == null) {
        throw new IllegalStateException(
            "SRS registry resource not found: " + SRS_REGISTRY_RESOURCE);
      }
      try (BufferedReader reader =
               new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
        String line;
        while ((line = reader.readLine()) != null) {
          if (line.startsWith("#") || line.startsWith("srid,")) {
            continue;
          }
          String[] parts = line.split(",", 3);
          if (parts.length != 3) {
            continue;
          }
          int srid = Integer.parseInt(parts[0].trim());
          String stringId = parts[1].trim();
          boolean isGeographic = Boolean.parseBoolean(parts[2].trim());
          SpatialReferenceSystemInformation srsInfo =
              new SpatialReferenceSystemInformation(srid, stringId, isGeographic);
          sridToSrs.put(srid, srsInfo);
          stringIdToSrs.put(stringId, srsInfo);
        }
      }
    } catch (IllegalStateException e) {
      throw e;
    } catch (Exception e) {
      throw new IllegalStateException(
          "Failed to load SRS registry from " + SRS_REGISTRY_RESOURCE, e);
    }
  }

  /**
   * Add Spark-specific SRS entries and aliases that are not part of the PROJ EPSG database.
   */
  private void addSparkSpecificEntries() {
    // SRID 0: Spark convention for Cartesian coordinate system with no defined SRS.
    SpatialReferenceSystemInformation srid0 =
        new SpatialReferenceSystemInformation(0, "SRID:0", false);
    sridToSrs.put(0, srid0);
    stringIdToSrs.put("SRID:0", srid0);

    // OGC standardizations: override primary string IDs, keep EPSG as aliases.
    addOgcOverride(4326, "OGC:CRS84");  // OGC standardization of WGS 84
    addOgcOverride(4267, "OGC:CRS27");  // OGC standardization of NAD27
    addOgcOverride(4269, "OGC:CRS83");  // OGC standardization of NAD83
  }

  /**
   * Override a PROJ EPSG entry with an OGC string ID, keeping the EPSG string ID as an alias.
   */
  private void addOgcOverride(int srid, String ogcStringId) {
    SpatialReferenceSystemInformation existing = sridToSrs.get(srid);
    if (existing == null) {
      throw new RuntimeException("SRID " + srid + " should already be in the SRS registry");
    }
    SpatialReferenceSystemInformation ogcEntry =
        new SpatialReferenceSystemInformation(srid, ogcStringId, existing.isGeographic());
    sridToSrs.put(srid, ogcEntry);
    stringIdToSrs.put(ogcStringId, ogcEntry);
    stringIdToSrs.put(existing.stringId(), ogcEntry);
  }

  // Returns the SRS corresponding to the input SRID. If not supported, returns `null`.
  public SpatialReferenceSystemInformation getSrsInfo(int srid) {
    return sridToSrs.getOrDefault(srid, null);
  }

  // Returns the SRS corresponding to the input string ID. If not supported, returns `null`.
  public SpatialReferenceSystemInformation getSrsInfo(String stringId) {
    return stringIdToSrs.getOrDefault(stringId, null);
  }

  /**
   * Returns an unmodifiable view of the SRID-to-SRS map.
   */
  public Map<Integer, SpatialReferenceSystemInformation> getSridToSrs() {
    return Collections.unmodifiableMap(sridToSrs);
  }

  /**
   * Returns an unmodifiable view of the string CRS ID-to-SRS map.
   */
  public Map<String, SpatialReferenceSystemInformation> getStringIdToSrs() {
    return Collections.unmodifiableMap(stringIdToSrs);
  }
}
