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

package org.apache.spark.sql.catalyst.analysis

import java.util

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.V2Reference.{TemporaryViewContext, TransactionContext, WriteTargetContext}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Statistics}
import org.apache.spark.sql.connector.catalog.{ChangelogContext, Column, Identifier, InMemoryChangelog, InMemoryTable, InMemoryTableCatalog, Table}
import org.apache.spark.sql.connector.catalog.ChangelogContext.DeduplicationMode
import org.apache.spark.sql.connector.catalog.ChangelogRange.UnboundedRange
import org.apache.spark.sql.execution.datasources.v2.{ChangelogTable, DataSourceV2Relation}
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class V2ReferenceSuite extends PlanTest {

  private val catalog = new InMemoryTableCatalog
  catalog.initialize("catalog", CaseInsensitiveStringMap.empty())
  private val identifier = Identifier.of(Array("ns"), "table")
  private val options = new CaseInsensitiveStringMap(util.Map.of("branch", "main"))
  private val columns = Array(Column.create("id", LongType))
  private val changelogContext = new ChangelogContext(
    new UnboundedRange(), DeduplicationMode.NONE, false)

  private def table(isChangelog: Boolean, dataColumns: Array[Column] = columns): Table = {
    if (isChangelog) {
      ChangelogTable(
        new InMemoryChangelog("changes", dataColumns, Seq.empty),
        changelogContext,
        resolved = true)
    } else {
      new InMemoryTable("table", dataColumns, Array.empty, util.Map.of(), id = "table-id")
    }
  }

  private def relation(table: Table): DataSourceV2Relation = {
    DataSourceV2Relation.create(table, Some(catalog), Some(identifier), options)
  }

  gridTest("reference factories preserve captured metadata and choose the reference type")(
      for {
        isChangelog <- Seq(false, true)
        context <- Seq(TemporaryViewContext(Seq("view")), TransactionContext, WriteTargetContext)
      } yield (isChangelog, context)) { case (isChangelog, context) =>
    val original = relation(table(isChangelog))
    original.setTagValue(LogicalPlan.PLAN_ID_TAG, 42L)
    val ref = context match {
      case TemporaryViewContext(viewName) => V2Reference.createForTempView(original, viewName)
      case TransactionContext => V2Reference.createForTransaction(original)
      case WriteTargetContext => V2Reference.createForWriteTarget(original)
    }

    assert(ref.isInstanceOf[V2ChangelogReference] == isChangelog)
    assert(ref.isInstanceOf[V2TableReference] != isChangelog)
    assert(ref.catalog eq catalog)
    assert(ref.identifier == identifier)
    assert(ref.options eq options)
    assert(ref.context == context)
    assert(ref.info.tableId == Option(original.table.id()))
    assert(ref.info.columns == original.table.columns().toSeq)
    assert(ref.output == original.output)
    assert(ref.getTagValue(LogicalPlan.PLAN_ID_TAG).contains(42L))
    assert(ref.name == original.name)
    assert(ref.computeStats() == Statistics.DUMMY)
    assert(ref.simpleString(10).startsWith(
      if (isChangelog) "ChangelogReference[" else "TableReference["))
    ref match {
      case changelogRef: V2ChangelogReference =>
        assert(changelogRef.changelog eq original.table)
        assert(changelogRef.changelog.changelogContext eq changelogContext)
        assert(changelogRef.changelog.resolved)
      case _: V2TableReference =>
    }

    val fresh = ref.newInstance()
    assert(fresh.getClass == ref.getClass)
    assert(fresh.context == context)
    assert(fresh.info == ref.info)
    assert(fresh.options eq options)
    assert(fresh.output.map(_.name) == ref.output.map(_.name))
    assert(fresh.output.zip(ref.output).forall { case (a, b) => a.exprId != b.exprId })
    comparePlans(ref.toRelation(original.table), original)
  }

  gridTest("both reference types validate captured schemas for temporary views")(
      Seq(false, true)) { isChangelog =>
    val original = relation(table(isChangelog))
    val ref = V2Reference.createForTempView(original, Seq("view"))
    V2ReferenceUtils.validateLoadedTable(
      table(isChangelog, columns :+ Column.create("added", LongType)), ref)
    checkError(
      intercept[AnalysisException] {
        V2ReferenceUtils.validateLoadedTable(table(isChangelog, Array.empty), ref)
      },
      condition = "INCOMPATIBLE_COLUMN_CHANGES_AFTER_VIEW_WITH_PLAN_CREATION",
      parameters = Map(
        "viewName" -> "`view`",
        "tableName" -> "`catalog`.`ns`.`table`",
        "colType" -> "data",
        "errors" -> "- `id` BIGINT has been removed"))
  }

  gridTest("both reference types reject changed schemas in transactions")(
      Seq(false, true)) { isChangelog =>
    val ref = V2Reference.createForTransaction(relation(table(isChangelog)))
    checkError(
      intercept[AnalysisException] {
        V2ReferenceUtils.validateLoadedTable(table(isChangelog, Array.empty), ref)
      },
      condition = "INCOMPATIBLE_TABLE_CHANGE_AFTER_ANALYSIS.COLUMNS_MISMATCH",
      parameters = Map(
        "tableName" -> "`catalog`.`ns`.`table`",
        "errors" -> "- `id` BIGINT has been removed"))
  }
}
