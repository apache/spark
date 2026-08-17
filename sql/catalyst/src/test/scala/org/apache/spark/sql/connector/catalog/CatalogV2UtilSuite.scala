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

package org.apache.spark.sql.connector.catalog

import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.{any, eq => mockEq}
import org.mockito.Mockito.{mock, verify, when}

import org.apache.spark.{SparkFunSuite, SparkIllegalArgumentException}
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.{
  AsOfTimestamp, AsOfVersion, TimeTravelSpec, UnresolvedRelation}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{IntegerType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class CatalogV2UtilSuite extends SparkFunSuite {

  private def catalogWithStateOptions(keys: java.util.Set[String]): TableCatalog = {
    val catalog = mock(classOf[TableCatalog])
    when(catalog.tableStateOptionKeys()).thenReturn(keys)
    catalog
  }

  // CatalogV2Util.getTable routes through the options-aware TableCatalog.loadTable, whose default
  // implementation dispatches to the existing overloads. Stub only that method to run the real
  // default so the dispatch is exercised; the leaf overloads stay as plain mock methods (returning
  // null) that we then `verify`.
  private def mockCatalogWithRealDispatch(): TableCatalog = {
    val testCatalog = mock(classOf[TableCatalog])
    when(testCatalog.tableStateOptionKeys()).thenCallRealMethod()
    when(testCatalog.loadTable(
      any[Identifier], any[TableContext], any[CaseInsensitiveStringMap])).thenCallRealMethod()
    testCatalog
  }

  test("Load relation should encode the identifiers for V2Relations") {
    val testCatalog = mockCatalogWithRealDispatch()
    val ident = mock(classOf[Identifier])
    val table = mock(classOf[Table])
    when(table.columns()).thenReturn(Array(Column.create("i", IntegerType)))
    when(testCatalog.loadTable(ident)).thenReturn(table)
    val r = CatalogV2Util.loadRelation(testCatalog, ident)
    assert(r.isDefined)
    assert(r.get.isInstanceOf[DataSourceV2Relation])
    val v2Relation = r.get.asInstanceOf[DataSourceV2Relation]
    assert(v2Relation.catalog.exists(_ == testCatalog))
    assert(v2Relation.identifier.exists(_ == ident))
  }

  private def getTableAndVerifyDispatch(
      timeTravelSpec: Option[TimeTravelSpec],
      writePrivilegesString: Option[String])(
      verifyOverload: TableCatalog => Unit): Unit = {
    val testCatalog = mockCatalogWithRealDispatch()
    val ident = mock(classOf[Identifier])
    CatalogV2Util.getTable(testCatalog, ident, timeTravelSpec, writePrivilegesString)
    verifyOverload(testCatalog)
  }

  test("getTable dispatches to loadTable(ident) with no time travel and no write privileges") {
    getTableAndVerifyDispatch(None, None) { c => verify(c).loadTable(any[Identifier]) }
  }

  test("getTable dispatches to loadTable(ident, writePrivileges) with write privileges") {
    getTableAndVerifyDispatch(None, Some("INSERT,DELETE")) { c =>
      verify(c).loadTable(
        any[Identifier],
        mockEq(java.util.Set.of(TableWritePrivilege.INSERT, TableWritePrivilege.DELETE)))
    }
  }

  test("getTable dispatches to loadTable(ident, version) for version time travel") {
    getTableAndVerifyDispatch(Some(AsOfVersion("v1")), None) { c =>
      verify(c).loadTable(any[Identifier], mockEq("v1"))
    }
  }

  test("getTable dispatches to loadTable(ident, timestamp) for timestamp time travel") {
    getTableAndVerifyDispatch(Some(AsOfTimestamp(123L)), None) { c =>
      verify(c).loadTable(any[Identifier], mockEq(123L))
    }
  }

  test("getTable rejects combining time travel and write privileges") {
    val testCatalog = mockCatalogWithRealDispatch()
    val ident = mock(classOf[Identifier])
    val e = intercept[SparkIllegalArgumentException] {
      CatalogV2Util.getTable(testCatalog, ident, Some(AsOfVersion("v1")), Some("INSERT"))
    }
    assert(e.getMessage.contains("Cannot set both time travel and write privileges"))
  }

  test("loadTableForV2Write forwards write privileges and only table-state options") {
    val testCatalog = mock(classOf[TableCatalog])
    when(testCatalog.tableStateOptionKeys()).thenReturn(java.util.Set.of("state"))
    val ident = mock(classOf[Identifier])
    val options = new CaseInsensitiveStringMap(
      java.util.Map.of("state", "branch", "custom", "value"))
    val contextCaptor = ArgumentCaptor.forClass(classOf[TableContext])

    CatalogV2Util.loadTableForV2Write(
      testCatalog, ident, Set(TableWritePrivilege.INSERT, TableWritePrivilege.DELETE), options)

    val expectedStateOptions =
      new CaseInsensitiveStringMap(java.util.Map.of("state", "branch"))
    verify(testCatalog).loadTable(
      mockEq(ident), contextCaptor.capture(), mockEq(expectedStateOptions))
    assert(contextCaptor.getValue.timeTravel().isEmpty)
    assert(contextCaptor.getValue.writePrivileges() ===
      java.util.Set.of(TableWritePrivilege.INSERT, TableWritePrivilege.DELETE))
  }

  test("loadTableForV2Write rejects configured time travel options") {
    val testCatalog = mock(classOf[TableCatalog])
    when(testCatalog.name()).thenReturn("testcat")
    val ident = Identifier.of(Array("ns"), "table")
    val conf = new SQLConf
    conf.setConf(SQLConf.TIME_TRAVEL_VERSION_KEY, "customVersion")
    conf.setConf(SQLConf.TIME_TRAVEL_TIMESTAMP_KEY, "customTimestamp")

    SQLConf.withExistingConf(conf) {
      Seq("customVersion", "customTimestamp").foreach { key =>
        val options = new CaseInsensitiveStringMap(java.util.Map.of(key, "value"))
        val e = intercept[AnalysisException] {
          CatalogV2Util.loadTableForV2Write(
            testCatalog, ident, Set(TableWritePrivilege.INSERT), options)
        }
        assert(e.getCondition === "UNSUPPORTED_FEATURE.TIME_TRAVEL")
      }
    }
  }

  test("UnresolvedRelation preserves option key case while updating write privileges") {
    val options = new CaseInsensitiveStringMap(java.util.Map.of(
      "targetLoadOption", "loadValue",
      "targetWriteOption", "writeValue"))
    val relation = UnresolvedRelation(Seq("catalog", "table"), options)

    val withPrivileges = relation.requireWritePrivileges(Set(TableWritePrivilege.INSERT))
    assert(withPrivileges.options.asCaseSensitiveMap().containsKey("targetLoadOption"))
    assert(withPrivileges.options.asCaseSensitiveMap().containsKey("targetWriteOption"))
    assert(!withPrivileges.options.asCaseSensitiveMap().containsKey("targetloadoption"))
    assert(withPrivileges.options.get(UnresolvedRelation.REQUIRED_WRITE_PRIVILEGES) === "INSERT")

    val cleared = withPrivileges.clearWritePrivileges
    assert(cleared.options.asCaseSensitiveMap() === options.asCaseSensitiveMap())
  }

  test("TableContext normalizes null time travel and null write privileges to empty") {
    val context = new TableContext(null, null)
    assert(context.timeTravel().isEmpty)
    assert(context.writePrivileges().isEmpty)
  }

  test("TableContext equals / hashCode / toString") {
    val emptyPrivileges = java.util.Set.of[TableWritePrivilege]()
    val a = new TableContext(new TimeTravel.AsOfVersion("v1"), emptyPrivileges)
    val b = new TableContext(new TimeTravel.AsOfVersion("v1"), emptyPrivileges)
    val c = new TableContext(new TimeTravel.AsOfTimestamp(1L), emptyPrivileges)
    assert(a == b)
    assert(a.hashCode() == b.hashCode())
    assert(a != c)
    assert(a.toString.contains("timeTravel"))
    assert(a.toString.contains("writePrivileges"))
  }

  test("getTable forwards only declared table-state options") {
    val catalog = mock(classOf[TableCatalog])
    when(catalog.tableStateOptionKeys()).thenReturn(java.util.Set.of("snapshot"))
    val ident = mock(classOf[Identifier])
    val options = new CaseInsensitiveStringMap(
      java.util.Map.of("snapshot", "s1", "split-size", "5"))

    CatalogV2Util.getTable(catalog, ident, options = options)

    val expected = new CaseInsensitiveStringMap(java.util.Map.of("snapshot", "s1"))
    verify(catalog).loadTable(
      mockEq(ident),
      any[TableContext],
      mockEq(expected))
  }

  test("getTable forwards no options when a catalog declares no table-state options") {
    val catalog = mock(classOf[TableCatalog])
    when(catalog.tableStateOptionKeys()).thenCallRealMethod()
    val ident = mock(classOf[Identifier])
    val options = new CaseInsensitiveStringMap(
      java.util.Map.of("snapshot", "s1", "split-size", "5"))

    CatalogV2Util.getTable(catalog, ident, options = options)

    verify(catalog).loadTable(
      mockEq(ident),
      any[TableContext],
      mockEq(CaseInsensitiveStringMap.empty()))
  }

  test("extractTableStateOptions projects declared keys case-insensitively") {
    val catalog = catalogWithStateOptions(java.util.Set.of("BrAnCh", "tag"))
    val options = new CaseInsensitiveStringMap(java.util.Map.of(
      "branch", "Main",
      "TAG", "Release",
      "split-size", "5"))

    val stateOptions = CatalogV2Util.extractTableStateOptions(catalog, options)

    assert(stateOptions.size() == 2)
    assert(stateOptions.get("BRANCH") == "Main")
    assert(stateOptions.get("tag") == "Release")
    assert(!stateOptions.containsKey("split-size"))
  }

  test("extractTableStateOptions compares option keys case-insensitively") {
    val catalog = catalogWithStateOptions(java.util.Set.of("SnApShOt"))
    val lowerCaseKey = CatalogV2Util.extractTableStateOptions(
      catalog,
      new CaseInsensitiveStringMap(java.util.Map.of("snapshot", "main")))
    val upperCaseKey = CatalogV2Util.extractTableStateOptions(
      catalog,
      new CaseInsensitiveStringMap(java.util.Map.of("SNAPSHOT", "main")))

    assert(lowerCaseKey == upperCaseKey)
  }

  test("extractTableStateOptions compares option values case-sensitively") {
    val catalog = catalogWithStateOptions(java.util.Set.of("snapshot"))
    val lowerCaseValue = CatalogV2Util.extractTableStateOptions(
      catalog,
      new CaseInsensitiveStringMap(java.util.Map.of("snapshot", "main")))
    val upperCaseValue = CatalogV2Util.extractTableStateOptions(
      catalog,
      new CaseInsensitiveStringMap(java.util.Map.of("snapshot", "MAIN")))

    assert(lowerCaseValue != upperCaseValue)
  }

  test("extractTableStateOptions returns no options by default") {
    val catalog = mock(classOf[TableCatalog])
    when(catalog.tableStateOptionKeys()).thenCallRealMethod()
    val options = new CaseInsensitiveStringMap(
      java.util.Map.of("branch", "Main", "split-size", "5"))

    val stateOptions = CatalogV2Util.extractTableStateOptions(catalog, options)

    assert(stateOptions.isEmpty)
  }

  test("viewInfoBuilderFrom preserves the dependency list") {
    val dependencies = DependencyList.of(Array(Dependency.table(Array("cat", "ns", "events"))))
    val existing = viewWithDependencies(Some(dependencies))
    val rebuilt = CatalogV2Util.viewInfoBuilderFrom(existing).build()
    assert(rebuilt.viewDependencies() === dependencies)
  }

  test("viewInfoBuilderFrom leaves an absent dependency list absent") {
    val existing = viewWithDependencies(None)
    val rebuilt = CatalogV2Util.viewInfoBuilderFrom(existing).build()
    assert(rebuilt.viewDependencies() === null)
  }

  private def viewWithDependencies(dependencies: Option[DependencyList]): View = {
    val builder = new View.Builder()
      .withSchema(new StructType().add("i", IntegerType))
      .withQueryText("SELECT i FROM cat.ns.events")
    dependencies.foreach(builder.withViewDependencies)
    builder.build()
  }
}
