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

import java.util.Optional

import scala.jdk.CollectionConverters._

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog.{
  ChangelogContext, ChangelogProperties, ChangelogRange, Column, Identifier, InMemoryChangelog,
  InMemoryChangelogCatalog}
import org.apache.spark.sql.connector.catalog.ChangelogContext.DeduplicationMode
import org.apache.spark.sql.connector.catalog.ChangelogRange.{TimestampRange, UnboundedRange, VersionRange}
import org.apache.spark.sql.connector.expressions.NamedReference
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.unsafe.types.UTF8String

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
      resolved: Boolean = true,
      range: ChangelogRange = new UnboundedRange()): ChangelogTable = {
    ChangelogTable(
      new InMemoryChangelog("changes", dataColumns, Seq.empty, props),
      new ChangelogContext(range, mode, computeUpdates),
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

  gridTest("changelog range is bounded only when it has an ending bound")(Seq(
    new VersionRange("1", Optional.of("2"), true, true) -> true,
    new VersionRange("1", Optional.empty[String](), true, true) -> false,
    new TimestampRange(1L, Optional.of[java.lang.Long](2L), true, true) -> true,
    new TimestampRange(1L, Optional.empty[java.lang.Long](), true, true) -> false,
    new UnboundedRange() -> false)) { case (range, expected) =>
    assert(table(range = range).isBounded == expected)
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

  test("raw changelog refresh ignores metadata unused by post-processing") {
    val changed = properties.copy(
      containsCarryoverRows = false,
      containsIntermediateChanges = false,
      representsUpdateAsDeleteAndInsert = false,
      rowIdNames = Seq("other_id"),
      rowVersionName = Some("other_version"))
    table(mode = DeduplicationMode.NONE, computeUpdates = false).validateRefresh(
      table(changed, mode = DeduplicationMode.NONE, computeUpdates = false))
  }

  test("carryover-only changelog refresh ignores unused intermediate and update metadata") {
    val changed = properties.copy(
      containsIntermediateChanges = false,
      representsUpdateAsDeleteAndInsert = false)
    table(mode = DeduplicationMode.DROP_CARRYOVERS, computeUpdates = false).validateRefresh(
      table(
        changed,
        mode = DeduplicationMode.DROP_CARRYOVERS,
        computeUpdates = false))
  }

  gridTest("changelog construction rejects invalid post-processing options")(
      Seq(false, true)) { resolved =>
    checkError(
      intercept[AnalysisException] {
        table(mode = DeduplicationMode.NONE, computeUpdates = true, resolved = resolved)
      },
      condition = "INVALID_CDC_OPTION.UPDATE_DETECTION_REQUIRES_CARRY_OVER_REMOVAL",
      parameters = Map("changelogName" -> "changes"))
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

  test("changelog relations compare by connector equality, context, identifier, and options") {
    val catalog = new InMemoryChangelogCatalog
    catalog.initialize("catalog", CaseInsensitiveStringMap.empty())
    val identifier = Identifier.of(Array("ns"), "table")
    val options = new CaseInsensitiveStringMap(Map("state" -> "base").asJava)

    def relation(
        changelogTable: ChangelogTable = table(),
        ident: Identifier = identifier,
        relationOptions: CaseInsensitiveStringMap = options): DataSourceV2Relation = {
      DataSourceV2Relation.create(
        changelogTable, Some(catalog), Some(ident), relationOptions)
    }

    val base = relation()
    val equivalent = relation()
    assert(!(base.table.asInstanceOf[ChangelogTable].changelog eq
      equivalent.table.asInstanceOf[ChangelogTable].changelog))
    assert(base.sameResult(equivalent))
    assert(base.semanticHash() == equivalent.semanticHash())
    assert(!base.sameResult(
      relation(table(mode = DeduplicationMode.DROP_CARRYOVERS))))
    assert(!base.sameResult(relation(table(computeUpdates = false))))
    assert(!base.sameResult(relation(table(
      range = new VersionRange("1", Optional.of("2"), true, true)))))
    assert(!base.sameResult(
      relation(ident = Identifier.of(Array("ns"), "other_table"))))
    assert(!base.sameResult(relation(
      relationOptions = new CaseInsensitiveStringMap(Map("state" -> "other").asJava))))

    val row = InternalRow(1L, 2L, 3L, 4L, UTF8String.fromString("insert"), 5L, 6L)
    val changedData = new InMemoryChangelog("changes", dataColumns, Seq(row), properties)
    assert(!base.sameResult(relation(table().copy(changelog = changedData))))
  }

  test("changelog relations preserve connector identity equality") {
    val catalog = new InMemoryChangelogCatalog
    catalog.initialize("catalog", CaseInsensitiveStringMap.empty())
    val identifier = Identifier.of(Array("ns"), "table")

    def identityTable(): ChangelogTable = {
      val changelog = new InMemoryChangelog("changes", dataColumns, Seq.empty, properties) {
        override def equals(other: Any): Boolean = other match {
          case ref: AnyRef => this eq ref
          case _ => false
        }

        override def hashCode(): Int = System.identityHashCode(this)
      }
      table().copy(changelog = changelog)
    }

    def relation(changelogTable: ChangelogTable): DataSourceV2Relation = {
      DataSourceV2Relation.create(changelogTable, Some(catalog), Some(identifier))
    }

    val captured = identityTable()
    val valueCompared = table().changelog
    assert(valueCompared != captured.changelog)
    assert(captured.changelog != valueCompared)
    val base = relation(captured)
    val sameConnector = relation(captured.copy())
    assert(base.sameResult(sameConnector))
    assert(base.semanticHash() == sameConnector.semanticHash())
    assert(!base.sameResult(relation(identityTable())))
  }
}
