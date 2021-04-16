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

import java.io.File

import scala.collection.JavaConverters._

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.scalatest.matchers.must.Matchers

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogDatabase, CatalogStorageFormat, CatalogTable, CatalogTableType, ExternalCatalog, InMemoryCatalog, SessionCatalog}
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.connector.{InMemoryTable, InMemoryTableCatalog}
import org.apache.spark.sql.connector.catalog.{CatalogManager, CatalogNotFoundException, Identifier, Table}
import org.apache.spark.sql.types._

class TableLookupCacheSuite extends AnalysisTest with Matchers {
  private def getAnalyzer(externalCatalog: ExternalCatalog, databasePath: File): Analyzer = {
    val v1Catalog = new SessionCatalog(externalCatalog, FunctionRegistry.builtin)
    v1Catalog.createDatabase(
      CatalogDatabase("default", "", databasePath.toURI, Map.empty),
      ignoreIfExists = false)
    v1Catalog.createTable(
      CatalogTable(
        TableIdentifier("t1", Some("default")),
        CatalogTableType.MANAGED,
        CatalogStorageFormat.empty,
        StructType(Seq(StructField("a", IntegerType)))),
      ignoreIfExists = false)
    val v2Catalog = new InMemoryTableCatalog {
      override def loadTable(ident: Identifier): Table = {
        val catalogTable = externalCatalog.getTable("default", ident.name)
        new InMemoryTable(
          catalogTable.identifier.table,
          catalogTable.schema,
          Array.empty,
          Map.empty[String, String].asJava)
      }
      override def name: String = CatalogManager.SESSION_CATALOG_NAME
    }
    val catalogManager = mock(classOf[CatalogManager])
    when(catalogManager.catalog(any())).thenAnswer((invocation: InvocationOnMock) => {
      invocation.getArgument[String](0) match {
        case CatalogManager.SESSION_CATALOG_NAME => v2Catalog
        case name =>
          throw new CatalogNotFoundException(s"No such catalog: $name")
      }
    })
    when(catalogManager.v1SessionCatalog).thenReturn(v1Catalog)
    when(catalogManager.currentCatalog).thenReturn(v2Catalog)
    when(catalogManager.currentNamespace).thenReturn(Array("default"))

    new Analyzer(catalogManager)
  }

  test("table lookups to external catalog are cached") {
    withTempDir { tempDir =>
      val inMemoryCatalog = new InMemoryCatalog
      val catalog = spy(inMemoryCatalog)
      val analyzer = getAnalyzer(catalog, tempDir)
      reset(catalog)
      analyzer.execute(table("t1").join(table("t1")).join(table("t1")))
      verify(catalog, times(1)).getTable("default", "t1")
    }
  }

  test("table lookups via nested views are cached") {
    withTempDir { tempDir =>
      val inMemoryCatalog = new InMemoryCatalog
      val catalog = spy(inMemoryCatalog)
      val analyzer = getAnalyzer(catalog, tempDir)
      val viewDef = CatalogTable(
        TableIdentifier("view", Some("default")),
        CatalogTableType.VIEW,
        CatalogStorageFormat.empty,
        StructType(Seq(StructField("a", IntegerType, nullable = true))),
        viewText = Some("select * from t1")
      )
      catalog.createTable(viewDef, ignoreIfExists = false)
      reset(catalog)
      analyzer.execute(table("t1").join(table("view")).join(table("view")))
      verify(catalog, times(1)).getTable("default", "t1")
    }
  }
}
