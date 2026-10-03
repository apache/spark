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

import org.apache.spark.sql.{functions => F, Column}
import org.apache.spark.sql.catalyst.analysis.Resolver
import org.apache.spark.sql.catalyst.util.QuotingUtils
import org.apache.spark.sql.types.{DataType, MapType, StringType, StructType}

/**
 * Per-leaf sequencing clocks for SCD1 reconciliation.
 *
 * An SCD1 target row can combine values authored by different CDC events. The version map records
 * which event currently authors each non-key user-data leaf.
 *
 * Concretely, the contract of the version map is as follows.
 * 1. Every user-data leaf present in the target-table-aligned row when the map is written receives
 *    an entry.
 * 2. A non-null entry is the sequencing clock of the event that authored the leaf's current stored
 *    value. A delete authors a null value until a later upsert reauthors the leaf.
 * 3. A null entry means no event has authored the leaf so far.
 * 4. A leaf absent from the map implies it was added through later schema evolution. It is treated
 *    as unauthored, as in (3), until the map is next rewritten; then a null entry is explicitly
 *    materialized.
 *
 * Unlike the SCD2 version map, the SCD1 version map is mutable reconciliation state. When a newer
 * CDC event authors a leaf, reconciliation updates that leaf's stored value and advances its
 * sequencing clock in the map without changing the clocks of other leaves.
 *
 * A user-data leaf is a non-framework field obtained by recursively expanding structs. A
 * target-table-aligned row uses the target's field set, order, and spelling. Alignment ensures
 * every target leaf has a stable map entry across reductive schema evolution and case differences.
 *
 * Version-map keys are field paths rendered as fully quoted multipart identifiers using
 * [[org.apache.spark.sql.catalyst.util.QuotingUtils.quoteNameParts]]. Quoting each name part
 * distinguishes a nested path from a column whose name contains dots. Name parts use the persisted
 * target schema's canonical spelling.
 */
private[pipelines] object Scd1VersionMap {

  /**
   * The version map's Spark data type.
   */
  def mapType(sequencingType: DataType): MapType =
    MapType(StringType, sequencingType, valueContainsNull = true)

  /**
   * Builds the version map for one ingested upsert, independently of other events for its key.
   *
   * Every leaf column in `schema` receives an entry. A leaf receives a null clock when it is
   * selected by `ignoreNullSelection` and its value is null; every other leaf receives
   * `upsertSequence`.
   *
   * @param schema The schema whose leaf columns receive version-map entries.
   * @param ignoreNullSelection The selection identifying leaves whose null values are unauthored.
   * @param upsertSequence The upsert event's non-null sequencing value.
   * @param sequencingType The data type of `upsertSequence`.
   * @param resolver The resolver used for column-name matching.
   */
  def buildVersionMap(
      schema: StructType,
      ignoreNullSelection: ColumnSelection,
      upsertSequence: Column,
      sequencingType: DataType,
      resolver: Resolver): Column = {
    val ignoreNullSchema = ColumnSelection.applyToSchema(
      schemaName = "ignoreNullSelection",
      schema = schema,
      columnSelection = Some(ignoreNullSelection),
      resolver = resolver
    )
    val ignoreNullLeafPaths =
      AutoCdcSchemaUtils.flattenStructFieldPaths(ignoreNullSchema).toSet

    val keyValueColumns = AutoCdcSchemaUtils.flattenStructFieldPaths(schema).flatMap { path =>
      val versionMapKey = QuotingUtils.quoteNameParts(path)
      val leafSequence =
        if (ignoreNullLeafPaths.contains(path)) {
          F.when(F.col(versionMapKey).isNotNull, upsertSequence)
        } else {
          upsertSequence
        }
      Seq(F.lit(versionMapKey), leafSequence)
    }

    F.map(keyValueColumns: _*).cast(mapType(sequencingType))
  }
}
