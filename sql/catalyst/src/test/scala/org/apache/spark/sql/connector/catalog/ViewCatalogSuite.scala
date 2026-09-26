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

import scala.jdk.CollectionConverters._

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class ViewCatalogSuite extends SparkFunSuite {

  test("default alterView applies changes in order and preserves view metadata") {
    val catalog = new InMemoryRelationCatalog
    catalog.initialize("test", CaseInsensitiveStringMap.empty())
    val ident = Identifier.of(Array("ns"), "view")
    val dependencies = DependencyList.of(
      Array(Dependency.table(Array("source_catalog", "source_ns", "source"))))
    val original = new View.Builder()
      .withColumns(Array(Column.create("id", IntegerType)))
      .withProperties(Map("first" -> "old", "second" -> "remove").asJava)
      .withQueryText("SELECT id FROM source_catalog.source_ns.source")
      .withCurrentCatalog("source_catalog")
      .withCurrentNamespace(Array("source_ns"))
      .withSqlConfigs(Map("spark.sql.ansi.enabled" -> "true").asJava)
      .withSchemaMode("BINDING")
      .withQueryColumnNames(Array("id"))
      .withViewDependencies(dependencies)
      .build()
    catalog.createView(ident, original)

    val updated = catalog.alterView(
      ident,
      ViewChange.setProperty("first", "intermediate"),
      ViewChange.removeProperty("first"),
      ViewChange.setProperty("first", "new"),
      ViewChange.removeProperty("second"))

    assert(updated.properties.get("first") === "new")
    assert(!updated.properties.containsKey("second"))
    assert(updated.columns.sameElements(original.columns))
    assert(updated.queryText === original.queryText)
    assert(updated.currentCatalog === original.currentCatalog)
    assert(updated.currentNamespace.sameElements(original.currentNamespace))
    assert(updated.sqlConfigs === original.sqlConfigs)
    assert(updated.schemaMode === original.schemaMode)
    assert(updated.queryColumnNames.sameElements(original.queryColumnNames))
    assert(updated.viewDependencies === original.viewDependencies)
  }

  test("default alterView invalidates cached metadata before loading the current view") {
    var invalidations = 0
    val catalog = new InMemoryRelationCatalog {
      private var cachedView: View = _

      override def loadView(ident: Identifier): View = {
        if (cachedView == null) {
          cachedView = super.loadView(ident)
        }
        cachedView
      }

      override def invalidateView(ident: Identifier): Unit = {
        invalidations += 1
        cachedView = null
      }
    }
    catalog.initialize("test", CaseInsensitiveStringMap.empty())
    val ident = Identifier.of(Array("ns"), "view")
    val original = new View.Builder()
      .withColumns(Array(Column.create("id", IntegerType)))
      .withProperties(Map("original" -> "value").asJava)
      .withQueryText("SELECT 1 AS id")
      .build()
    catalog.createView(ident, original)
    catalog.loadView(ident)

    val concurrent = CatalogV2Util.viewInfoBuilderFrom(original)
      .withProperties(Map("concurrent" -> "value").asJava)
      .withQueryText("SELECT 2 AS id")
      .build()
    catalog.replaceView(ident, concurrent)

    val updated = catalog.alterView(ident, ViewChange.setProperty("new", "value"))

    assert(invalidations === 1)
    assert(updated.queryText === concurrent.queryText)
    assert(updated.properties.asScala === Map("concurrent" -> "value", "new" -> "value"))
  }
}
