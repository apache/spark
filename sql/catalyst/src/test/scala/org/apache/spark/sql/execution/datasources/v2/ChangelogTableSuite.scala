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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.connector.catalog.{ChangelogContext, ChangelogProperties, Column, InMemoryChangelog}
import org.apache.spark.sql.connector.catalog.ChangelogContext.DeduplicationMode
import org.apache.spark.sql.connector.catalog.ChangelogRange.UnboundedRange
import org.apache.spark.sql.connector.expressions.NamedReference
import org.apache.spark.sql.types.LongType

class ChangelogTableSuite extends SparkFunSuite {

  private val dataColumns = Seq("id", "other_id", "row_version", "other_version")
    .map(name => Column.create(name, LongType, false)).toArray
  private val properties = ChangelogProperties(
    containsCarryoverRows = true,
    containsIntermediateChanges = true,
    representsUpdateAsDeleteAndInsert = true,
    rowIdNames = Seq("id"),
    rowVersionName = Some("row_version"))

  private def table(
      props: ChangelogProperties = properties,
      mode: DeduplicationMode = DeduplicationMode.NET_CHANGES,
      computeUpdates: Boolean = true,
      resolved: Boolean = true): ChangelogTable = {
    ChangelogTable(
      new InMemoryChangelog("changes", dataColumns, Seq.empty, props),
      new ChangelogContext(new UnboundedRange(), mode, computeUpdates),
      resolved)
  }

  private def checkMetadataChange(
      captured: ChangelogTable,
      current: ChangelogTable,
      changedProperties: String): Unit = {
    checkError(
      intercept[AnalysisException] { captured.validateRefresh(current) },
      condition = "INCOMPATIBLE_TABLE_CHANGE_AFTER_ANALYSIS.CHANGELOG_METADATA_MISMATCH",
      parameters = Map(
        "tableName" -> "`changes`",
        "changedProperties" -> changedProperties))
  }

  Seq(
    properties.copy(containsCarryoverRows = false) -> "containsCarryoverRows, rowVersion",
    properties.copy(containsIntermediateChanges = false) -> "containsIntermediateChanges",
    properties.copy(representsUpdateAsDeleteAndInsert = false) ->
      "representsUpdateAsDeleteAndInsert",
    properties.copy(rowIdNames = Seq("other_id")) -> "rowId",
    properties.copy(rowVersionName = Some("other_version")) -> "rowVersion"
  ).foreach { case (changed, changedProperties) =>
    test(s"resolved changelog refresh rejects changed $changedProperties") {
      checkMetadataChange(table(), table(changed), changedProperties)
    }
  }

  test("changelog refresh compares metadata and references by value") {
    table().validateRefresh(table())
  }

  test("raw changelog refresh ignores references unused by post-processing") {
    val changed = properties.copy(
      rowIdNames = Seq("other_id"),
      rowVersionName = Some("other_version"))
    table(mode = DeduplicationMode.NONE, computeUpdates = false).validateRefresh(
      table(changed, mode = DeduplicationMode.NONE, computeUpdates = false))
  }

  test("update-only changelog refresh ignores unused row-version references") {
    val updateOnly = properties.copy(
      containsCarryoverRows = false,
      containsIntermediateChanges = false)
    table(updateOnly).validateRefresh(
      table(updateOnly.copy(rowVersionName = Some("other_version"))))
  }

  test("changelog refresh preserves a snapshot of mutable row reference paths") {
    val mutablePath = Array("id")
    val changelog = new InMemoryChangelog("changes", dataColumns, Seq.empty, properties) {
      override def rowId(): Array[NamedReference] = Array(new NamedReference {
        override def fieldNames(): Array[String] = mutablePath
      })
    }
    val context = new ChangelogContext(
      new UnboundedRange(), DeduplicationMode.NET_CHANGES, true)
    val captured = ChangelogTable(changelog, context, resolved = true)
    mutablePath(0) = "other_id"
    checkMetadataChange(captured, ChangelogTable(changelog, context), "rowId")
  }

  test("unresolved changelog refresh permits metadata changes before post-processing") {
    table(resolved = false).validateRefresh(
      table(properties.copy(rowIdNames = Seq("other_id"))))
  }
}
