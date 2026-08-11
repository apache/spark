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

import org.mockito.ArgumentMatchers.{any, eq => mockEq}
import org.mockito.Mockito.{mock, verify, when}

import org.apache.spark.{SparkFunSuite, SparkIllegalArgumentException}
import org.apache.spark.sql.catalyst.analysis.{AsOfTimestamp, AsOfVersion, TimeTravelSpec}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.types.{IntegerType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class CatalogV2UtilSuite extends SparkFunSuite {

  // CatalogV2Util.getTable routes through the options-aware TableCatalog.loadTable, whose default
  // implementation dispatches to the existing overloads. Stub only that method to run the real
  // default so the dispatch is exercised; the leaf overloads stay as plain mock methods (returning
  // null) that we then `verify`.
  private def mockCatalogWithRealDispatch(): TableCatalog = {
    val testCatalog = mock(classOf[TableCatalog])
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
