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
 * The map type is `Map(String, sequencingType)`. Keys are field paths rendered as fully quoted
 * multipart identifiers using
 * [[org.apache.spark.sql.catalyst.util.QuotingUtils.quoteNameParts]]. Quoting each name part
 * distinguishes a nested path from a column whose name contains dots. Name parts use the
 * persisted target schema's canonical spelling. Values are the non-null sequencing clocks that
 * determined those leaves.
 *
 * Below, the stored leaf value referes to the column's actual data value in the SCD1 row.
 *
 * If the stored leaf value is null:
 *
 *   - If the leaf does not have an entry in the version map, it is unauthored and has no
 *     sequencing clock.
 *   - If the leaf has an entry in the version map, the null was authored at the entry's
 *     sequencing clock.
 *
 * If the stored leaf value is non-null:
 *
 *   - If the leaf does not have an entry in the version map, it was authored at the row's upsert
 *     sequence.
 *   - If the leaf has an entry in the version map, it was authored at the entry's sequencing
 *     clock.
 *
 * Thus, authored nulls and non-null leaves carrying a clock other than the row's upsert sequence
 * require entries; unauthored nulls and non-null leaves authored by that upsert omit them.
 */
private[pipelines] object Scd1VersionMap {

  /**
   * The version map's Spark data type.
   */
  def mapType(sequencingType: DataType): MapType =
    MapType(StringType, sequencingType, valueContainsNull = false)
}
