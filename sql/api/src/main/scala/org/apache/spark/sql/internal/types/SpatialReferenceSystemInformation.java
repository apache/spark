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
 * Class for maintaining information about a spatial reference system (SRS).
 */
@Unstable
public record SpatialReferenceSystemInformation(
  // Field storing the spatial reference identifier (SRID) value of this SRS.
  int srid,
  // Field storing the string ID of the corresponding coordinate reference system (CRS).
  String stringId,
  // Field indicating whether the spatial reference system (SRS) is geographic or not.
  boolean isGeographic
) {}
