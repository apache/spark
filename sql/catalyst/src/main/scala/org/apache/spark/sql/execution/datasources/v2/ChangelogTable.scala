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

package org.apache.spark.sql.execution.datasources.v2

import java.util.{EnumSet => JEnumSet, Set => JSet}

import org.apache.spark.sql.connector.catalog.{Changelog, ChangelogContext, Column, Identifier, SupportsRead, Table, TableCapability, TableCatalog}
import org.apache.spark.sql.connector.catalog.ChangelogRange.{TimestampRange, UnboundedRange, VersionRange}
import org.apache.spark.sql.connector.catalog.TableCapability.{BATCH_READ, MICRO_BATCH_READ}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.types.{DataType, LongType, StringType, TimestampType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * An internal wrapper that adapts a connector's [[Changelog]] into a DSv2 [[Table]] with
 * [[SupportsRead]], enabling reuse of [[DataSourceV2Relation]] without logical plan changes.
 *
 * This class is NOT part of the connector API. Connectors implement [[Changelog]]; Spark
 * wraps it in [[ChangelogTable]] during analysis.
 */
case class ChangelogTable(
    changelog: Changelog,
    changelogContext: ChangelogContext,
    resolved: Boolean = false) extends Table with SupportsRead {

  // Capture the metadata used to build the analyzer's CDC post-processing operators.
  private val containsCarryoverRows = changelog.containsCarryoverRows()
  private val containsIntermediateChanges = changelog.containsIntermediateChanges()
  private val representsUpdateAsDeleteAndInsert = changelog.representsUpdateAsDeleteAndInsert()

  private[sql] val requiresCarryoverRemoval =
    changelogContext.deduplicationMode() != ChangelogContext.DeduplicationMode.NONE &&
      containsCarryoverRows
  private[sql] val requiresUpdateDetection =
    changelogContext.computeUpdates() && representsUpdateAsDeleteAndInsert
  private[sql] val requiresNetChangeCollapse =
    changelogContext.deduplicationMode() == ChangelogContext.DeduplicationMode.NET_CHANGES &&
      containsIntermediateChanges

  // Validate the schema and option combinations before capturing the referenced fields.
  ChangelogTable.validateSchema(changelog)
  if (requiresUpdateDetection && containsCarryoverRows && !requiresCarryoverRemoval) {
    throw QueryCompilationErrors.cdcUpdateDetectionRequiresCarryOverRemoval(name)
  }

  private val rowId = if (
      requiresCarryoverRemoval || requiresUpdateDetection || requiresNetChangeCollapse) {
    changelog.rowId().toVector.map(_.fieldNames().toVector)
  } else {
    Vector.empty
  }
  private val rowVersion = if (requiresCarryoverRemoval) {
    Option(changelog.rowVersion()).map(_.fieldNames().toVector)
  } else {
    None
  }

  override def name: String = changelog.name

  override def columns: Array[Column] = changelog.columns

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    changelog.newScanBuilder(options)
  }

  override def capabilities: JSet[TableCapability] = JEnumSet.of(BATCH_READ, MICRO_BATCH_READ)

  private[sql] def isBounded: Boolean = changelogContext.range() match {
    case range: VersionRange => range.endingVersion().isPresent
    case range: TimestampRange => range.endingTimestamp().isPresent
    case _: UnboundedRange => false
  }

  /** Checks that refreshing the changelog preserves the already analyzed CDC rewrites. */
  def validateRefresh(current: ChangelogTable): Unit = {
    if (resolved) {
      val changedProperties = Seq(
        "containsCarryoverRows" ->
          (requiresCarryoverRemoval != current.requiresCarryoverRemoval),
        "containsIntermediateChanges" ->
          (requiresNetChangeCollapse != current.requiresNetChangeCollapse),
        "representsUpdateAsDeleteAndInsert" ->
          (requiresUpdateDetection != current.requiresUpdateDetection),
        "rowId" -> (rowId != current.rowId),
        "rowVersion" -> (rowVersion != current.rowVersion)).collect {
        case (property, true) => property
      }
      if (changedProperties.nonEmpty) {
        throw QueryCompilationErrors.changelogChangedAfterAnalysis(name, changedProperties)
      }
    }
  }
}

object ChangelogTable {

  def load(
      catalog: TableCatalog,
      ident: Identifier,
      context: ChangelogContext,
      stateOptions: CaseInsensitiveStringMap): ChangelogTable = {
    val changelog = try {
      catalog.loadChangelog(ident, context, stateOptions)
    } catch {
      case _: UnsupportedOperationException =>
        throw QueryCompilationErrors.cdcNotSupportedError(catalog.name())
    }
    ChangelogTable(changelog, context)
  }

  private[v2] def validateSchema(cl: Changelog): Unit = {
    val byName = cl.columns.map(c => c.name -> c).toMap
    def check(name: String, expected: DataType*): Unit = {
      val col = byName.getOrElse(name,
        throw QueryCompilationErrors.changelogMissingColumnError(cl.name, name))
      if (expected.nonEmpty && col.dataType != expected.head) {
        throw QueryCompilationErrors.changelogInvalidColumnTypeError(
          cl.name, name, expected.head.sql, col.dataType.sql)
      }
    }
    check("_change_type", StringType)
    // `_commit_version` must be either `LongType` or `StringType`. Connectors must
    // additionally guarantee that the column's natural ordering (numeric /
    // lexicographic) matches commit order, because the netChanges post-processing path
    // sorts rows by this column. These two types cover every realistic CDC source;
    // broader atomic types like `IntegerType` are strict subsets of `LongType`, and
    // `TimestampType` duplicates the role of `_commit_timestamp`. The narrower
    // contract can always be relaxed later (relaxing is non-breaking; restricting is
    // not).
    val versionCol = byName.getOrElse("_commit_version",
      throw QueryCompilationErrors.changelogMissingColumnError(cl.name, "_commit_version"))
    if (versionCol.dataType != LongType && versionCol.dataType != StringType) {
      throw QueryCompilationErrors.changelogInvalidColumnTypeError(
        cl.name, "_commit_version", "BIGINT or STRING", versionCol.dataType.sql)
    }
    check("_commit_timestamp", TimestampType)

    // Only call `rowId()` / `rowVersion()` when a capability requires them; a connector
    // that advertises a capability without overriding the method surfaces the default
    // UnsupportedOperationException directly.
    val needsRowId = cl.containsCarryoverRows() ||
      cl.representsUpdateAsDeleteAndInsert() ||
      cl.containsIntermediateChanges()
    if (needsRowId) {
      val rowIds = cl.rowId()
      if (rowIds == null || rowIds.isEmpty) {
        throw QueryCompilationErrors.changelogMissingRowIdError(cl.name)
      }
    }
    val needsRowVersion = cl.containsCarryoverRows() ||
      cl.representsUpdateAsDeleteAndInsert()
    if (needsRowVersion) {
      cl.rowVersion()
    }
  }
}
