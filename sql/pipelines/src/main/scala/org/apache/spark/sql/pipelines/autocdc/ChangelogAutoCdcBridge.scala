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

import org.apache.spark.sql.{functions => F, AnalysisException, Column}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.streaming.StreamingRelationV2
import org.apache.spark.sql.catalyst.util.QuotingUtils
import org.apache.spark.sql.classic.DataFrame
import org.apache.spark.sql.connector.catalog.{Changelog, ChangelogContext}
import org.apache.spark.sql.execution.datasources.v2.{ChangelogTable, DataSourceV2Relation}
import org.apache.spark.sql.types.{LongType, StringType, StructType, TimestampType}

/**
 * Bridges Spark's native CDC [[Changelog]] connector API into the AutoCDC engine.
 *
 * When an AutoCDC flow's source is a native changelog read (e.g. a view defined as
 * `spark.readStream.changes(table)` or `SELECT * FROM STREAM table CHANGES ...`), its rows carry
 * the standardized CDC metadata columns `_change_type`, `_commit_version`, and
 * `_commit_timestamp`. This object detects such sources and derives the AutoCDC
 * [[ChangeArgs]] (delete detection and metadata-column exclusion) from the changelog contract,
 * so users do not have to hand-map `_change_type` to `apply_as_deletes` or manually exclude the
 * metadata columns.
 *
 * Detection is decided by locating a [[ChangelogTable]] relation in the resolved source plan and
 * confirming the output schema still carries the three typed metadata columns. A source fed by a
 * materialized table (where the [[ChangelogTable]] node no longer appears in the plan) is
 * intentionally not auto-detected; such users continue to configure the flow manually.
 */
private[pipelines] object ChangelogAutoCdcBridge {

  /** The CDC metadata column holding the change kind (insert/delete/update_*). */
  val ChangeTypeColumn: String = "_change_type"

  /** The CDC metadata column holding the commit version that emitted the change row. */
  val CommitVersionColumn: String = "_commit_version"

  /** The CDC metadata column holding the commit timestamp of the change row. */
  val CommitTimestampColumn: String = "_commit_timestamp"

  /** All native CDC metadata column names, in schema order. */
  val MetadataColumns: Seq[String] = Seq(ChangeTypeColumn, CommitVersionColumn, CommitTimestampColumn)

  /**
   * A detected native-CDC changelog source, with the bits of the [[Changelog]] contract that
   * AutoCDC needs to derive its configuration and validate the read.
   *
   * @param changeTypeColumn               The actual (case-preserving) name of the `_change_type`
   *                                       column in the source schema.
   * @param representsUpdateAsDeleteAndInsert Whether the connector encodes updates as raw
   *                                       delete+insert pairs rather than pre/post-images.
   * @param computeUpdates                 Whether the changelog read materializes
   *                                       `update_preimage`/`update_postimage` rows.
   * @param containsCarryoverRows          Whether the connector may surface copy-on-write
   *                                       carry-over insert/delete pairs.
   * @param removesCarryoverRows           Whether the changelog read removes carry-over pairs
   *                                       (i.e. `deduplicationMode` is not `none`).
   */
  case class ChangelogSource(
      changeTypeColumn: String,
      representsUpdateAsDeleteAndInsert: Boolean,
      computeUpdates: Boolean,
      containsCarryoverRows: Boolean,
      removesCarryoverRows: Boolean)

  /**
   * Detects whether the given resolved source DataFrame reads from a native CDC changelog.
   * Returns a [[ChangelogSource]] descriptor when so, or `None` otherwise.
   */
  def analyzeSource(df: DataFrame, caseSensitive: Boolean): Option[ChangelogSource] = {
    findChangelogTable(df.queryExecution.analyzed).flatMap { table =>
      if (hasChangelogMetadataColumns(df.schema, caseSensitive)) {
        Some(
          ChangelogSource(
            changeTypeColumn = resolvedColumnName(df.schema, ChangeTypeColumn, caseSensitive),
            representsUpdateAsDeleteAndInsert =
              table.changelog.representsUpdateAsDeleteAndInsert(),
            computeUpdates = table.changelogContext.computeUpdates(),
            containsCarryoverRows = table.changelog.containsCarryoverRows(),
            removesCarryoverRows =
              table.changelogContext.deduplicationMode() !=
                ChangelogContext.DeduplicationMode.NONE
          )
        )
      } else {
        None
      }
    }
  }

  /**
   * Validates that a detected changelog source is configured such that AutoCDC can unambiguously
   * classify each row as an upsert or a delete, and order events deterministically. Intended to be
   * the single entry point for all changelog-source validation; called once at
   * [[org.apache.spark.sql.pipelines.graph.AutoCdcMergeFlow]] construction.
   *
   * AutoCDC sequences events by a single ordering expression and keeps the max-by-sequence event
   * per key within a microbatch. Its correctness premise is that, after pre-image filtering, no
   * two surviving rows for the same key share the same sequence value with conflicting
   * upsert/delete meaning -- otherwise the per-key dedup ties and resolves non-deterministically.
   * Three changelog configurations break this:
   *
   *  - Updates arriving as raw delete+insert pairs (`representsUpdateAsDeleteAndInsert` without
   *    `computeUpdates`): the delete and insert halves share a `_commit_version`. Requiring
   *    `computeUpdates = true` materializes them as `update_preimage`/`update_postimage` (the
   *    pre-image is then filtered out), leaving one surviving upsert per key per commit.
   *  - Copy-on-write carry-over pairs (`containsCarryoverRows` with `deduplicationMode = none`):
   *    an unchanged rewritten row is emitted as an identical insert+delete pair at the same
   *    `_commit_version`, and the delete half would match the derived delete condition. Requiring
   *    a deduplication mode that removes carry-overs (`dropCarryovers` or `netChanges`) drops
   *    these pairs before they reach AutoCDC.
   *  - Sequencing by anything other than `_commit_version`: the [[Changelog]] contract only
   *    guarantees that `_commit_version`'s natural ordering matches commit order (see the metadata
   *    column contract on [[Changelog]]). Any other column (including `_commit_timestamp`, which
   *    is only strictly increasing across micro-batches, not within one) may tie or mis-order two
   *    changes to the same key, so AutoCDC requires the sequencing expression to reference
   *    `_commit_version`.
   *
   * @param source          The detected changelog source descriptor.
   * @param df              The resolved source change-data feed.
   * @param sequencing      The user-provided sequencing expression.
   * @param destinationName The quoted destination table name, surfaced in error messages.
   * @param caseSensitive   Whether column-name comparison is case-sensitive.
   */
  def validateSource(
      source: ChangelogSource,
      df: DataFrame,
      sequencing: Column,
      destinationName: String,
      caseSensitive: Boolean): Unit = {
    if (source.representsUpdateAsDeleteAndInsert && !source.computeUpdates) {
      throw new AnalysisException(
        errorClass = "AUTOCDC_CHANGELOG_REQUIRES_COMPUTE_UPDATES",
        messageParameters = Map("tableName" -> destinationName)
      )
    }
    if (source.containsCarryoverRows && !source.removesCarryoverRows) {
      throw new AnalysisException(
        errorClass = "AUTOCDC_CHANGELOG_REQUIRES_CARRYOVER_REMOVAL",
        messageParameters = Map("tableName" -> destinationName)
      )
    }
    requireCommitVersionSequencing(df, sequencing, destinationName, caseSensitive)
  }

  /**
   * Fails when a changelog source is sequenced by something other than `_commit_version`. The
   * [[Changelog]] contract only guarantees that `_commit_version` orders rows in commit order, so
   * it is the only safe choice for AutoCDC's out-of-order event reconciliation. If the sequencing
   * expression cannot be analyzed (e.g. it references an unresolved column), validation is skipped
   * rather than failing on an indeterminate result.
   */
  private def requireCommitVersionSequencing(
      df: DataFrame,
      sequencing: Column,
      destinationName: String,
      caseSensitive: Boolean): Unit = {
    val sequencesByCommitVersion = scala.util.Try {
      df.select(sequencing).queryExecution.analyzed.references
        .exists(attr => nameMatches(attr.name, CommitVersionColumn, caseSensitive))
    }.getOrElse(true)
    if (!sequencesByCommitVersion) {
      throw new AnalysisException(
        errorClass = "AUTOCDC_CHANGELOG_REQUIRES_COMMIT_VERSION_SEQUENCING",
        messageParameters = Map("tableName" -> destinationName)
      )
    }
  }

  /**
   * Derives the effective [[ChangeArgs]] for a changelog-backed source: fills in the delete
   * condition from `_change_type` when the user did not provide one (an explicit user condition
   * wins), and excludes the native CDC metadata columns from the target output.
   */
  def deriveEffectiveChangeArgs(
      base: ChangeArgs,
      sourceSchema: StructType,
      caseSensitive: Boolean): ChangeArgs = {
    val changeTypeCol = resolvedColumnName(sourceSchema, ChangeTypeColumn, caseSensitive)
    val derivedDeleteCondition =
      base.deleteCondition.orElse(Some(deleteCondition(changeTypeCol)))
    val metadataExclusions = metadataColumnsToExclude(sourceSchema, caseSensitive)
    val mergedSelection = excludeColumns(base.columnSelection, metadataExclusions, caseSensitive)
    base.copy(deleteCondition = derivedDeleteCondition, columnSelection = mergedSelection)
  }

  /**
   * Filters out `update_preimage` rows from a changelog feed. The pre-image carries the old value
   * of an updated row; for SCD1 upsert semantics only the post-image (and inserts) matter, and
   * keeping the pre-image would apply a stale upsert.
   */
  def filterOutPreimages(df: DataFrame, changeTypeColumn: String): DataFrame = {
    df.filter(
      F.col(QuotingUtils.quoteIdentifier(changeTypeColumn)) =!= Changelog.CHANGE_TYPE_UPDATE_PREIMAGE
    )
  }

  /** The delete condition derived from the `_change_type` column for a changelog source. */
  private def deleteCondition(changeTypeColumn: String): Column = {
    F.col(QuotingUtils.quoteIdentifier(changeTypeColumn)) === Changelog.CHANGE_TYPE_DELETE
  }

  /**
   * Locates a [[ChangelogTable]]-backed relation in a resolved logical plan. The changelog read is
   * resolved to a [[DataSourceV2Relation]] (batch) or [[StreamingRelationV2]] (streaming) wrapping
   * a [[ChangelogTable]], possibly under post-processing operators injected by the analyzer.
   */
  private def findChangelogTable(plan: LogicalPlan): Option[ChangelogTable] = {
    plan
      .collect {
        case r: DataSourceV2Relation => r.table
        case r: StreamingRelationV2 => r.table
      }
      .collectFirst { case table: ChangelogTable => table }
  }

  /** Whether the schema carries all three native CDC metadata columns with the expected types. */
  private def hasChangelogMetadataColumns(schema: StructType, caseSensitive: Boolean): Boolean = {
    def field(name: String) = findField(schema, name, caseSensitive)
    field(ChangeTypeColumn).exists(_.dataType == StringType) &&
    field(CommitVersionColumn).exists(f => f.dataType == LongType || f.dataType == StringType) &&
    field(CommitTimestampColumn).exists(_.dataType == TimestampType)
  }

  /** The metadata columns actually present in the schema, as [[UnqualifiedColumnName]]s. */
  private def metadataColumnsToExclude(
      schema: StructType,
      caseSensitive: Boolean): Seq[UnqualifiedColumnName] = {
    MetadataColumns.flatMap(name => findField(schema, name, caseSensitive)).map { field =>
      UnqualifiedColumnName(Seq(field.name))
    }
  }

  /** Returns the actual (case-preserving) field name matching `name`, or `name` if absent. */
  private def resolvedColumnName(
      schema: StructType,
      name: String,
      caseSensitive: Boolean): String = {
    findField(schema, name, caseSensitive).map(_.name).getOrElse(name)
  }

  private def findField(schema: StructType, name: String, caseSensitive: Boolean) = {
    schema.fields.find { f =>
      if (caseSensitive) f.name == name else f.name.equalsIgnoreCase(name)
    }
  }

  private def nameMatches(attrName: String, colName: String, caseSensitive: Boolean): Boolean = {
    if (caseSensitive) attrName == colName else attrName.equalsIgnoreCase(colName)
  }

  /** Merges the metadata-column exclusions into the user's column selection. */
  private def excludeColumns(
      selection: Option[ColumnSelection],
      toExclude: Seq[UnqualifiedColumnName],
      caseSensitive: Boolean): Option[ColumnSelection] = {
    if (toExclude.isEmpty) {
      selection
    } else {
      selection match {
        case None =>
          Some(ColumnSelection.ExcludeColumns(toExclude))
        case Some(ColumnSelection.ExcludeColumns(cols)) =>
          Some(ColumnSelection.ExcludeColumns((cols ++ toExclude).distinct))
        case Some(ColumnSelection.IncludeColumns(cols)) =>
          val excludeNames = toExclude.map(_.name)
          def matches(a: String, b: String): Boolean =
            if (caseSensitive) a == b else a.equalsIgnoreCase(b)
          Some(
            ColumnSelection.IncludeColumns(
              cols.filterNot(c => excludeNames.exists(matches(_, c.name)))
            )
          )
      }
    }
  }
}
