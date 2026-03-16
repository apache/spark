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

/**
 * Class for providing SRS mappings for geographic spatial reference systems.
 */
@Unstable
public class GeographicSpatialReferenceSystemMapper extends SpatialReferenceSystemMapper {
  // Returns the string ID corresponding to the input SRID. If not supported, returns `null`.
  public static String getStringId(int srid) {
    SpatialReferenceSystemInformation srsInfo = srsCache.getSrsInfo(srid);
    return srsInfo != null && srsInfo.isGeographic() ? srsInfo.stringId() : null;
  }

  // Returns the SRID corresponding to the input string ID. If not supported, returns `null`.
  public static Integer getSrid(String stringId) {
    SpatialReferenceSystemInformation srsInfo = srsCache.getSrsInfo(stringId);
    return srsInfo != null && srsInfo.isGeographic() ? srsInfo.srid() : null;
  }
}
