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

import org.apache.spark.sql.connector.catalog.{Changelog, ChangelogInfo, Column, SupportsRead, Table, TableCapability}
import org.apache.spark.sql.connector.catalog.TableCapability.{BATCH_READ, MICRO_BATCH_READ}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.types.{AtomicType, DataType, StringType, TimestampType}
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
    changelogInfo: ChangelogInfo,
    resolved: Boolean = false) extends Table with SupportsRead {

  // Validate that the connector returned a schema with the required CDC metadata columns
  // and correct types.
  ChangelogTable.validateSchema(changelog)

  override def name: String = changelog.name

  override def columns: Array[Column] = changelog.columns

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    changelog.newScanBuilder(options)
  }

  override def capabilities: JSet[TableCapability] = JEnumSet.of(BATCH_READ, MICRO_BATCH_READ)
}

object ChangelogTable {

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
    // `_commit_version` is connector-defined but must be an atomic orderable type. Both
    // the batch (Catalyst `SortOrder`) and the streaming netChanges (typed Comparable
    // ordering inside the stateful processor) paths require an orderable scalar.
    val versionCol = byName.getOrElse("_commit_version",
      throw QueryCompilationErrors.changelogMissingColumnError(cl.name, "_commit_version"))
    if (!versionCol.dataType.isInstanceOf[AtomicType]) {
      throw QueryCompilationErrors.changelogInvalidColumnTypeError(
        cl.name, "_commit_version", "an atomic orderable type", versionCol.dataType.sql)
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
