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

import java.net.URI

import scala.collection.JavaConverters._

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.analysis.{EmptyFunctionRegistry, FakeV2SessionCatalog, NoSuchNamespaceException}
import org.apache.spark.sql.catalyst.catalog.{CatalogDatabase, InMemoryCatalog, SessionCatalog}
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.connector.InMemoryTableCatalog
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class CatalogManagerSuite extends SparkFunSuite with SQLHelper {

  private def createSessionCatalog(): SessionCatalog = {
    val catalog = new InMemoryCatalog()
    catalog.createDatabase(
      CatalogDatabase(SessionCatalog.DEFAULT_DATABASE, "", new URI("fake"), Map.empty),
      ignoreIfExists = true)
    new SessionCatalog(catalog, EmptyFunctionRegistry)
  }

  test("CatalogManager should reflect the changes of default catalog") {
    val catalogManager = new CatalogManager(FakeV2SessionCatalog, createSessionCatalog())
    assert(catalogManager.currentCatalog.name() == CatalogManager.SESSION_CATALOG_NAME)
    assert(catalogManager.currentNamespace.sameElements(Array("default")))

    withSQLConf("spark.sql.catalog.dummy" -> classOf[DummyCatalog].getName,
      SQLConf.DEFAULT_CATALOG.key -> "dummy") {
      // The current catalog should be changed if the default catalog is set.
      assert(catalogManager.currentCatalog.name() == "dummy")
      assert(catalogManager.currentNamespace.sameElements(Array("a", "b")))
    }
  }

  test("CatalogManager should keep the current catalog once set") {
    val catalogManager = new CatalogManager(FakeV2SessionCatalog, createSessionCatalog())
    assert(catalogManager.currentCatalog.name() == CatalogManager.SESSION_CATALOG_NAME)
    withSQLConf("spark.sql.catalog.dummy" -> classOf[DummyCatalog].getName) {
      catalogManager.setCurrentCatalog("dummy")
      assert(catalogManager.currentCatalog.name() == "dummy")
      assert(catalogManager.currentNamespace.sameElements(Array("a", "b")))

      withSQLConf("spark.sql.catalog.dummy2" -> classOf[DummyCatalog].getName,
        SQLConf.DEFAULT_CATALOG.key -> "dummy2") {
        // The current catalog shouldn't be changed if it's set before.
        assert(catalogManager.currentCatalog.name() == "dummy")
      }
    }
  }

  test("current namespace should be updated when switching current catalog") {
    val catalogManager = new CatalogManager(FakeV2SessionCatalog, createSessionCatalog())
    withSQLConf("spark.sql.catalog.dummy" -> classOf[DummyCatalog].getName) {
      catalogManager.setCurrentCatalog("dummy")
      assert(catalogManager.currentNamespace.sameElements(Array("a", "b")))
      catalogManager.setCurrentNamespace(Array("a"))
      assert(catalogManager.currentNamespace.sameElements(Array("a")))

      // If we set current catalog to the same catalog, current namespace should stay the same.
      catalogManager.setCurrentCatalog("dummy")
      assert(catalogManager.currentNamespace.sameElements(Array("a")))

      // If we switch to a different catalog, current namespace should be reset.
      withSQLConf("spark.sql.catalog.dummy2" -> classOf[DummyCatalog].getName) {
        catalogManager.setCurrentCatalog("dummy2")
        assert(catalogManager.currentNamespace.sameElements(Array("a", "b")))
      }
    }
  }

  test("set current namespace") {
    val v1SessionCatalog = createSessionCatalog()
    v1SessionCatalog.createDatabase(
      CatalogDatabase(
        "test", "", v1SessionCatalog.getDefaultDBPath("test"), Map.empty),
      ignoreIfExists = false)
    val catalogManager = new CatalogManager(FakeV2SessionCatalog, v1SessionCatalog)

    // If the current catalog is session catalog, setting current namespace actually sets
    // `SessionCatalog.currentDb`.
    catalogManager.setCurrentNamespace(Array("test"))
    assert(catalogManager.currentNamespace.sameElements(Array("test")))
    assert(v1SessionCatalog.getCurrentDatabase == "test")

    intercept[NoSuchNamespaceException] {
      catalogManager.setCurrentNamespace(Array("ns1", "ns2"))
    }

    // when switching current catalog, `SessionCatalog.currentDb` should be reset.
    withSQLConf("spark.sql.catalog.dummy" -> classOf[DummyCatalog].getName) {
      catalogManager.setCurrentCatalog("dummy")
      assert(v1SessionCatalog.getCurrentDatabase == "default")
      catalogManager.setCurrentNamespace(Array("test2"))
      assert(v1SessionCatalog.getCurrentDatabase == "default")

      // Check namespace existence if currentCatalog implements SupportsNamespaces.
      withSQLConf("spark.sql.catalog.testCatalog" -> classOf[InMemoryTableCatalog].getName) {
        catalogManager.setCurrentCatalog("testCatalog")
        catalogManager.currentCatalog.asInstanceOf[InMemoryTableCatalog]
          .createNamespace(Array("test3"), Map.empty[String, String].asJava)
        assert(v1SessionCatalog.getCurrentDatabase == "default")
        catalogManager.setCurrentNamespace(Array("test3"))
        assert(v1SessionCatalog.getCurrentDatabase == "default")

        intercept[NoSuchNamespaceException] {
          catalogManager.setCurrentNamespace(Array("ns1", "ns2"))
        }
      }
    }
  }
}

class DummyCatalog extends CatalogPlugin {
  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    _name = name
  }
  private var _name: String = null
  override def name(): String = _name
  override def defaultNamespace(): Array[String] = Array("a", "b")
}
