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
import org.apache.spark.sql.catalyst.expressions.{CreateMap, If, Literal, RaiseError}
import org.apache.spark.sql.catalyst.util.QuotingUtils
import org.apache.spark.sql.classic.ExpressionUtils
import org.apache.spark.sql.types.{BooleanType, MapType, StringType, StructType}

/**
 * Per-row column authorship tracker for SCD2 ignore-null semantics.
 *
 * Recall in SCD2, every materialized row traces back to an upsert event that created it (and
 * if the row is closed then also a delete/succession event that closed it, but that's not
 * relevant here). Every data column in the row is at least partially derived by the
 * corresponding data column in the upsert event that spawned the row.
 *
 * For columns where ignore-null was not applied, the data column in the row is fully derived
 * (authored) by the corresponding data column in the upsert event. For columns where
 * ignore-null was applied however, if the data column in the upsert event was null
 * (unauthored), then we need to look backwards to deduce the corresponding inherited data
 * column for the row.
 *
 * Non-null values in an event are always considered authored, regardless of whether the column
 * in the event was included in the ignore-null configuration or not. Null values however, as
 * mentioned above, may or may not be considered authored -- it depends on whether they are
 * specified for a column that was included in the ignore-null configuration.
 *
 * In SCD2 the version map helps us answer per row: for all the columns that received a null
 * value in the upsert event that created this row, which nulls are considered authored vs
 * unauthored?
 *
 * Concretely, the contract of the version map is as follows.
 * 1. Every null considered authored receives an entry of (column name, true).
 * 2. Every null considered unauthored receives an entry of (column name, false).
 * 3. Once a non-null map is established, every column added by later schema evolution has an
 *    unauthored null but initially has no map entry. An entry will be added as per (2).
 *
 * In a single sentence: if a null column in an SCD2 row with a non-null version map is either
 * absent from the map or has a false value, the null is considered unauthored by the upsert event
 * that spawned this row. Otherwise the row explicitly authored the null.
 */
private[pipelines] object Scd2VersionMap {

  /**
   * Schema of the version map: `Map(String, Boolean)`.
   *
   * Keys are field paths rendered as fully quoted multipart identifiers using
   * [[QuotingUtils.quoteNameParts]]. Quoting each name part distinguishes a nested path from a
   * column whose name contains dots. Name parts use the persisted target schema's canonical
   * spelling.
   *
   * Values indicate authorship: `true` means authored-null, `false` means unauthored-null.
   * Null values never appear in the map.
   *
   * Lack of entry in a non-null map for a null-valued leaf column implies the column was
   * schema-evolved with an unauthored-null.
   */
  def mapType: MapType = MapType(StringType, BooleanType, valueContainsNull = false)

  /**
   * Builds a version map from a row's values and an ignore-null selection. For each null leaf,
   * the map records whether the row authored that null or left the leaf unauthored under the
   * selection. Used both when ingesting a new upsert and when lazily establishing a map for an
   * existing row that has none.
   *
   * @param schema The schema whose leaves the version map covers. Null-authorship is tracked
   *   for every leaf column in this schema, as per the version map contract.
   * @param ignoreNullSelection The ignore-null column selection this schema is being ingested
   *   under.
   * @param resolver Case-sensitivity resolver for column name matching.
   * @return A [[Column]] of [[mapType]] schema.
   */
  def buildVersionMap(
      schema: StructType,
      ignoreNullSelection: ColumnSelection,
      resolver: Resolver): Column = {
    val ignoreNullLeafPaths =
      resolveIgnoreNullLeafPaths(schema, ignoreNullSelection, resolver).toSet

    // For each leaf, build a nullable struct (key, value). The struct is non-null only when
    // the leaf column's runtime value is null (meaning the leaf needs a version map entry).
    // The value is a non-nullable BooleanType literal indicating authorship: true if the upsert
    // event authored the null, false if the event left the leaf unauthored.
    val candidateEntries = AutoCdcSchemaUtils.flattenStructFieldPaths(schema).map { path =>
      val isIgnoreNullLeaf = ignoreNullLeafPaths.contains(path)
      val leafIsNull = F.col(QuotingUtils.quoteNameParts(path)).isNull

      // If the leaf is not null, this candidate entry will simply resolve to null and will not be
      // added to the version map during construction below.
      F.when(
        leafIsNull,
        buildVersionMapEntry(path, authored = !isIgnoreNullLeaf))
    }

    if (candidateEntries.isEmpty) {
      F.map().cast(mapType)
    } else {
      val nonNullEntries = F.filter(F.array(candidateEntries: _*), (e: Column) => e.isNotNull)
      F.map_from_entries(nonNullEntries)
    }
  }

  /**
   * Resolves an ignore-null selection against the top-level fields of `schema`, then flattens
   * selected structs into leaf paths. Arrays and maps remain opaque leaves.
   *
   * @param schema Schema against which to resolve and canonicalize the selection.
   * @param ignoreNullSelection Top-level columns selected for ignore-null handling.
   * @param resolver Case-sensitivity resolver for column name matching.
   * @return Raw, unquoted leaf-name parts in schema order, with spelling taken exactly from
   *   `schema`.
   */
  private[autocdc] def resolveIgnoreNullLeafPaths(
      schema: StructType,
      ignoreNullSelection: ColumnSelection,
      resolver: Resolver): Seq[Seq[String]] = {
    val ignoreNullColumns = ColumnSelection.applyToSchema(
      schemaName = "ignoreNullSelection",
      schema = schema,
      columnSelection = Some(ignoreNullSelection),
      resolver = resolver
    )
    AutoCdcSchemaUtils.flattenStructFieldPaths(ignoreNullColumns)
  }

  /**
   * Returns the authorship value for `columnPath`, or null if the map or entry is absent.
   *
   * @param versionMap Version map containing keys serialized with
   *   [[QuotingUtils.quoteNameParts]].
   * @param columnPath Raw, unquoted leaf-name parts. Their spelling must exactly match the path
   *   used to construct the version-map key, including casing and special characters.
   */
  private[autocdc] def entryValue(
      versionMap: Column,
      columnPath: Seq[String]): Column =
    versionMap(QuotingUtils.quoteNameParts(columnPath))

  /**
   * Asserts that if the version map claims `columnPath` authored a null, then
   * `currentColumnValue` is indeed null. Authored nulls are never overwritten by coalescing,
   * so a `true` entry paired with a non-null stored value indicates data corruption or a bug.
   *
   * @param authorshipEntry Authorship value read from the version map, or null if absent.
   * @param currentColumnValue Current stored value for the same leaf.
   * @param columnPath Raw leaf-name parts used to identify the column in an error.
   * @return A column that evaluates to true for a valid pairing and raises an internal error
   *   otherwise.
   */
  private[autocdc] def validateAuthoredNullEntry(
      authorshipEntry: Column,
      currentColumnValue: Column,
      columnPath: Seq[String]): Column = {
    val authoredNullButValueNonNull =
      F.coalesce(authorshipEntry, F.lit(false)) && currentColumnValue.isNotNull
    val errorMessage = "Version map entry is true (authored null) " +
      "but stored value is non-null for column " + QuotingUtils.quoteNameParts(columnPath)
    ExpressionUtils.column(
      If(
        predicate = ExpressionUtils.expression(!authoredNullButValueNonNull),
        trueValue = Literal(true, BooleanType),
        falseValue = RaiseError(
          Literal("INTERNAL_ERROR"),
          CreateMap(Seq(Literal("message"), Literal(errorMessage))),
          BooleanType)
      )
    )
  }

  /**
   * Returns whether the [[currentColumnValue]] was authored by the upsert event that derived this
   * row, according to the [[versionMap]].
   *
   * Evaluating the returned expression raises `INTERNAL_ERROR` if a non-null value has an explicit
   * authored entry, because that combination violates the version map contract.
   *
   * @param versionMap Version map for the row, or null when the row has no authorship record.
   * @param currentColumnValue Current stored value for the leaf.
   * @param columnPath Raw leaf-name parts matching the key's canonical schema spelling.
   * @return Whether the originating upsert authored the stored leaf value.
   */
  private[autocdc] def isAuthored(
      versionMap: Column,
      currentColumnValue: Column,
      columnPath: Seq[String]): Column = {
    val authorshipEntry = entryValue(versionMap, columnPath)
    // A null map carries no per-column authorship record. Callers that want ignore-null semantics
    // initialize upsert maps before consulting them; treating any remaining null map as authored
    // keeps delete-encoded rows from contributing user-data authorship.
    val allWritesAreTriviallyAuthored = versionMap.isNull
    // Otherwise, look at the non-null version map to deduce authorship.
    val authoredAccordingToVersionMap = F.when(
        currentColumnValue.isNull,
        // Cases 1 and 2: a present entry is the source of truth for a stored null; an absent
        // entry means the leaf was retroactively schema evolved and is unauthored.
        F.coalesce(authorshipEntry, F.lit(false))
      )
      .otherwise({
        // Case 3: Was the value non-null in the event, or was it null and it inherited
        // another row's value instead?
        //
        // The stored value is currently non-null. If the version map has an entry for this
        // column, that means it was originally null but inherited a non-null value due to the
        // ignore-null selection. Otherwise if the version map does not contain an entry for
        // this column, it must have been non-null in the event itself, and therefore
        // necessarily authored.
        validateAuthoredNullEntry(authorshipEntry, currentColumnValue, columnPath) &&
          authorshipEntry.isNull
      })

    allWritesAreTriviallyAuthored || authoredAccordingToVersionMap
  }

  /**
   * Returns whether a null column needs to retroactively gain an entry in the version map.
   *
   * This can only happen on retroactive schema evolution for an existing row, where the default
   * value on schema evolution is null.
   *
   * @param versionMap Version map for the row, or null when ignore-null authorship is not tracked.
   * @param currentColumnValue Current stored value for the leaf.
   * @param columnPath Raw leaf-name parts matching the key's canonical schema spelling.
   * @param valueToInherit Value the row would inherit from a preceding row. A null may represent
   *   either a null value to inherit or the absence of a value to inherit; both cases are treated
   *   identically by this method.
   * @return Whether to materialize an explicit unauthored entry for this leaf.
   */
  private[autocdc] def needsSchemaEvolutionEntry(
      versionMap: Column,
      currentColumnValue: Column,
      columnPath: Seq[String],
      valueToInherit: Column): Column = {
    val isIgnoreNullOn = versionMap.isNotNull
    val isRetroactiveSchemaEvolution =
      currentColumnValue.isNull && entryValue(versionMap, columnPath).isNull

    // A null stored value with an absent entry unambiguously denotes an unauthored schema-evolved
    // leaf. If no non-null value is available to inherit, the row remains null and the sparse map
    // representation can remain unchanged. Once the row inherits a non-null value, however,
    // isAuthored would interpret that value as authored if its entry remained absent. Materializing
    // an explicit unauthored entry when inheritance occurs preserves authorship for subsequent
    // reconciliations.
    val hasValueToInherit = valueToInherit.isNotNull

    isIgnoreNullOn && isRetroactiveSchemaEvolution && hasValueToInherit
  }

  /**
   * Builds a `(key, value)` entry that follows the version map's path-serialization contract.
   *
   * @param columnPath Raw, unquoted leaf-name parts to serialize with
   *   [[QuotingUtils.quoteNameParts]].
   * @param authored Authorship Boolean to store directly as the entry value.
   * @return A non-null `(key: String, value: Boolean)` struct.
   */
  private[autocdc] def buildVersionMapEntry(
      columnPath: Seq[String],
      authored: Boolean): Column =
    F.struct(
      F.lit(QuotingUtils.quoteNameParts(columnPath)).as("key"),
      F.lit(authored).as("value")
    )
}
