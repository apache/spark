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

package org.apache.spark.sql.pipelines.autocdc

import org.apache.spark.sql.types.{DataType, MapType, StringType}

/**
 * Per-leaf sequencing clocks for SCD1 reconciliation.
 *
 * An SCD1 target row can combine values authored by different CDC events. The version map records
 * which event currently authors each non-key user-data leaf.
 */
private[pipelines] object Scd1VersionMap {

  /**
   * Schema of the version map: `Map(String, sequencingType)`.
   *
   * Each key is the field path of a user-data leaf. Each value is the sequencing value of the
   * event that authored the leaf, or null if no event has authored it.
   *
   * @param sequencingType The resolved data type of the flow's sequencing expression.
   */
  def mapType(sequencingType: DataType): MapType =
    MapType(StringType, sequencingType, valueContainsNull = true)
}
