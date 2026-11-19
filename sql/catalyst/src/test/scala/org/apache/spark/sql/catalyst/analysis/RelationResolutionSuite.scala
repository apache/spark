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

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{doThrow, mock, never, verify, verifyNoInteractions, when}

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType, SessionCatalog, TemporaryViewRelation}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.connector.catalog.{CatalogManager, CatalogPlugin, Column, Identifier, RelationCatalog, Table, TableCatalog, TableContext, V1Table, V1View, View, ViewCatalog}
import org.apache.spark.sql.types.{IntegerType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class RelationResolutionSuite extends PlanTest {
  private val nameParts = Seq("cat", "ns", "t")
  private val ident = Identifier.of(Array("ns"), "t")
  private val schema = new StructType().add("id", IntegerType)

  private def relationResolution(catalog: CatalogPlugin): RelationResolution = {
    val sessionCatalog = mock(classOf[SessionCatalog])
    when(sessionCatalog.getRawLocalOrGlobalTempView(any[Seq[String]])).thenReturn(None)
    val manager = mock(classOf[CatalogManager])
    when(manager.v1SessionCatalog).thenReturn(sessionCatalog)
    when(manager.catalog("cat")).thenReturn(catalog)
    when(manager.currentCatalog).thenReturn(catalog)
    when(manager.currentNamespace).thenReturn(Array("ns"))
    new RelationResolution(manager, RelationCache.empty)
  }

  private def table: Table = {
    val result = mock(classOf[Table])
    when(result.columns()).thenReturn(Array(Column.create("id", IntegerType)))
    result
  }

  private def tableMetadata(tableType: CatalogTableType): CatalogTable = {
    CatalogTable(
      TableIdentifier("t", Some("ns")),
      tableType,
      CatalogStorageFormat.empty,
      schema)
  }

  gridTest("DDL lookup gives temporary views precedence over persistent objects")(
      for {
        name <- Seq(Seq("t"), Seq("global_temp", "t"))
        viewOnly <- Seq(false, true)
      } yield (name, viewOnly)) { case (name, viewOnly) =>
    val catalog = mock(classOf[RelationCatalog])
    val resolution = relationResolution(catalog)
    val metadata = tableMetadata(CatalogTableType.VIEW)
    when(resolution.v1SessionCatalog.getRawLocalOrGlobalTempView(name))
      .thenReturn(Some(TemporaryViewRelation(metadata)))

    val result = resolution.lookupTableOrView(name, viewOnly)
    assert(result.contains(ResolvedTempView(
      Identifier.of(name.dropRight(1).toArray, name.last), metadata)))
    verifyNoInteractions(catalog)
  }

  gridTest("DDL relation catalog lookup uses one load for tables and views")(
      for {
        isView <- Seq(false, true)
        viewOnly <- Seq(false, true)
        name <- Seq(nameParts, Seq("t"))
      } yield (isView, viewOnly, name)) { case (isView, viewOnly, name) =>
    val catalog = mock(classOf[RelationCatalog])
    when(catalog.name()).thenReturn("cat")
    val resolution = relationResolution(catalog)
    val expected = if (isView) {
      val view = new View.Builder()
        .withSchema(schema)
        .withQueryText("SELECT * FROM missing_table")
        .build()
      when(catalog.loadRelation(ident)).thenReturn(view)
      ResolvedPersistentView(catalog, ident, view)
    } else {
      val target = table
      when(catalog.loadRelation(ident)).thenReturn(target)
      ResolvedTable.create(catalog, ident, target)
    }

    comparePlans(resolution.lookupTableOrView(name, viewOnly).get, expected)
    verify(catalog).loadRelation(ident)
    verify(catalog, never()).loadTable(any[Identifier])
    verify(catalog, never()).loadView(any[Identifier])
  }

  test("DDL lookup resolves pure view catalogs without requiring table support") {
    val catalog = mock(classOf[ViewCatalog])
    when(catalog.name()).thenReturn("cat")
    val view = new View.Builder()
      .withSchema(schema)
      .withQueryText("SELECT * FROM missing_table")
      .build()
    when(catalog.loadView(ident)).thenReturn(view)
    val resolution = relationResolution(catalog)

    assert(resolution.lookupTableOrView(nameParts, viewOnly = true)
      .contains(ResolvedPersistentView(catalog, ident, view)))
    verify(catalog).loadView(ident)
  }

  test("DDL view-only lookup rejects catalogs without view support before loading") {
    val catalog = mock(classOf[TableCatalog])
    when(catalog.name()).thenReturn("cat")
    val resolution = relationResolution(catalog)

    checkError(
      exception = intercept[AnalysisException] {
        resolution.lookupTableOrView(nameParts, viewOnly = true)
      },
      condition = "MISSING_CATALOG_ABILITY.VIEWS",
      parameters = Map("plugin" -> "cat"))
    verify(catalog, never()).loadTable(
      any[Identifier], any[TableContext], any[CaseInsensitiveStringMap])
  }

  gridTest("DDL lookup preserves session catalog table and view metadata")(
      Seq(CatalogTableType.MANAGED, CatalogTableType.VIEW)) { tableType =>
    val catalog = mock(classOf[TableCatalog])
    when(catalog.name()).thenReturn(CatalogManager.SESSION_CATALOG_NAME)
    val metadata = tableMetadata(tableType)
    val target = V1Table(metadata)
    when(catalog.tableStateOptionKeys()).thenReturn(java.util.Set.of[String]())
    when(catalog.loadTable(any[Identifier], any[TableContext], any[CaseInsensitiveStringMap]))
      .thenReturn(target)
    val resolution = relationResolution(catalog)

    val result = resolution.lookupTableOrView(nameParts).get
    result match {
      case view: ResolvedPersistentView =>
        assert(tableType == CatalogTableType.VIEW)
        assert(view.identifier == ident)
        assert(view.info.asInstanceOf[V1View].v1Table eq metadata)
      case resolved: ResolvedTable =>
        assert(tableType == CatalogTableType.MANAGED)
        assert(resolved.table eq target)
        assert(resolved.identifier == ident)
      case other => fail(s"Unexpected DDL target: $other")
    }
  }

  gridTest("DDL lookup returns None for absent relations and propagates other failures")(
      Seq(false, true)) { viewsOnly =>
    val failure = new IllegalStateException("catalog failure")
    val (resolution, failLookup) = if (viewsOnly) {
      val catalog = mock(classOf[ViewCatalog])
      when(catalog.name()).thenReturn("cat")
      when(catalog.loadView(ident)).thenThrow(new NoSuchViewException(ident))
      (relationResolution(catalog), () => doThrow(failure).when(catalog).loadView(ident))
    } else {
      val catalog = mock(classOf[RelationCatalog])
      when(catalog.name()).thenReturn("cat")
      when(catalog.loadRelation(ident)).thenThrow(new NoSuchTableException(nameParts))
      (relationResolution(catalog), () => doThrow(failure).when(catalog).loadRelation(ident))
    }

    assert(resolution.lookupTableOrView(nameParts).isEmpty)
    failLookup()
    assert(intercept[IllegalStateException] {
      resolution.lookupTableOrView(nameParts)
    } eq failure)
  }
}
