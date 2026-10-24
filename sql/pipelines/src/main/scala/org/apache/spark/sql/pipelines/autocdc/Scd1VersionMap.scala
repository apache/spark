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
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.util.QuotingUtils
import org.apache.spark.sql.classic.ExpressionUtils
import org.apache.spark.sql.types.{DataType, MapType, StringType, StructType}

/**
 * Per-leaf authorship sequences for SCD1 reconciliation.
 *
 * An SCD1 target row can combine values authored by different CDC events. The version map records
 * which event currently authors each non-key user-data leaf.
 *
 * Concretely, the contract of the version map is as follows.
 * 1. Every user-data leaf present in the row when the map is written receives an entry.
 * 2. A non-null entry is the sequencing clock of the event that authored the leaf's current stored
 *    value.
 * 3. A null entry means the leaf has no authored value.
 *
 * A user-data leaf is a non-key, non-framework field obtained by recursively expanding structs.
 *
 * Version-map keys are field paths rendered as fully quoted multipart identifiers using
 * [[org.apache.spark.sql.catalyst.util.QuotingUtils.quoteNameParts]]. Quoting each name part
 * distinguishes a nested path from a column whose name contains dots.
 */
private[pipelines] object Scd1VersionMap {

  /**
   * Schema of the version map: `Map(String, sequencingType)`.
   *
   * Each key is the field path of a user-data leaf. Each value is the leaf's authorship sequence:
   * the sequencing value of the event that authored the leaf, or null if no event has authored it.
   *
   * @param sequencingType The resolved data type of the flow's sequencing expression.
   */
  def mapType(sequencingType: DataType): MapType =
    MapType(StringType, sequencingType, valueContainsNull = true)

  /** Serializes a leaf field path into its version-map key. */
  def serializeKey(path: Seq[String]): String = QuotingUtils.quoteNameParts(path)

  /**
   * Builds a version map of type `mapType(sequencingType)` with one entry per leaf.
   *
   * @param entries Each leaf's raw, unquoted name parts, paired with its authorship sequence. Keys
   *                are serialized with [[serializeKey]].
   * @param sequencingType The data type the sequences are cast to.
   */
  private def fromEntries(entries: Seq[(Seq[String], Column)], sequencingType: DataType): Column = {
    if (entries.isEmpty) {
      // Without arguments, `map()` is `map<string, string>` under
      // spark.sql.legacy.createEmptyCollectionUsingStringType, which cannot be cast to struct or
      // array sequence types.
      ExpressionUtils.column(Literal.create(Map.empty, mapType(sequencingType)))
    } else {
      val keyValueColumns = entries.flatMap { case (leafPath, leafSequence) =>
        Seq(F.lit(serializeKey(leafPath)), leafSequence)
      }
      F.map(keyValueColumns: _*).cast(mapType(sequencingType))
    }
  }

  /**
   * Builds a column that computes the version map of an upsert row from that row's column values.
   *
   * The map has an entry for every leaf in `schema`. A leaf selected by `ignoreNullSelection` is
   * read by path from the row: a null value receives a null entry, and a non-null value receives
   * `upsertSequence`. Every other leaf receives `upsertSequence` regardless of its value.
   *
   * @param schema The user-data schema, without keys or framework columns, whose leaves receive
   *               version-map entries. Keys take each name part's spelling verbatim from this
   *               schema, so callers that persist the map must supply a spelling that stays the
   *               same across runs. Its leaves selected by `ignoreNullSelection` must resolve by
   *               path in the DataFrame on which the returned column is evaluated.
   * @param ignoreNullSelection The selection identifying leaves whose null values are unauthored.
   * @param upsertSequence The upsert event's non-null sequencing value.
   * @param sequencingType The data type of `upsertSequence`.
   * @param resolver The resolver used for column-name matching.
   * @return A column of type `mapType(sequencingType)` holding the upsert's version map.
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

    val entries = AutoCdcSchemaUtils.flattenStructFieldPaths(schema).map { path =>
      val leafSequence =
        if (ignoreNullLeafPaths.contains(path)) {
          F.when(F.col(serializeKey(path)).isNotNull, upsertSequence)
        } else {
          upsertSequence
        }
      path -> leafSequence
    }

    fromEntries(entries, sequencingType)
  }
}
