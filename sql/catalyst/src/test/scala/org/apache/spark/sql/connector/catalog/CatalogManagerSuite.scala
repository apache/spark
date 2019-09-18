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

import java.util

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.analysis.FakeV2SessionCatalog
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class CatalogManagerSuite extends SparkFunSuite {

  test("CatalogManager should reflect the changes of default catalog") {
    val conf = new SQLConf
    val catalogManager = new CatalogManager(conf, FakeV2SessionCatalog)
    assert(catalogManager.currentCatalog.isEmpty)
    assert(catalogManager.currentNamespace.sameElements(Array("default")))

    conf.setConfString("spark.sql.catalog.dummy", classOf[DummyCatalog].getName)
    conf.setConfString(SQLConf.DEFAULT_V2_CATALOG.key, "dummy")

    // The current catalog should be changed if the default catalog is set.
    assert(catalogManager.currentCatalog == Some("dummy"))
    assert(catalogManager.currentNamespace.sameElements(Array("a", "b")))
  }

  test("CatalogManager should keep the current catalog once set") {
    val conf = new SQLConf
    val catalogManager = new CatalogManager(conf, FakeV2SessionCatalog)
    assert(catalogManager.currentCatalog.isEmpty)
    conf.setConfString("spark.sql.catalog.dummy", classOf[DummyCatalog].getName)
    catalogManager.setCurrentCatalog("dummy")
    assert(catalogManager.currentCatalog == Some("dummy"))
    assert(catalogManager.currentNamespace.sameElements(Array("a", "b")))

    conf.setConfString("spark.sql.catalog.dummy2", classOf[DummyCatalog].getName)
    conf.setConfString(SQLConf.DEFAULT_V2_CATALOG.key, "dummy2")
    // The current catalog shouldn't be changed if it's set before.
    assert(catalogManager.currentCatalog == Some("dummy"))
  }

  test("current namespace should be updated when switching current catalog") {
    val conf = new SQLConf
    val catalogManager = new CatalogManager(conf, FakeV2SessionCatalog)
    catalogManager.setCurrentNamespace(Array("abc"))
    assert(catalogManager.currentNamespace.sameElements(Array("abc")))

    conf.setConfString("spark.sql.catalog.dummy", classOf[DummyCatalog].getName)
    catalogManager.setCurrentCatalog("dummy")
    assert(catalogManager.currentNamespace.sameElements(Array("a", "b")))
  }
}

class DummyCatalog extends SupportsNamespaces {
  override def defaultNamespace(): Array[String] = Array("a", "b")

  override def listNamespaces(): Array[Array[String]] = {
    throw new UnsupportedOperationException
  }
  override def listNamespaces(namespace: Array[String]): Array[Array[String]] = {
    throw new UnsupportedOperationException
  }
  override def loadNamespaceMetadata(namespace: Array[String]): util.Map[String, String] = {
    throw new UnsupportedOperationException
  }
  override def createNamespace(
      namespace: Array[String], metadata: util.Map[String, String]): Unit = {
    throw new UnsupportedOperationException
  }
  override def alterNamespace(namespace: Array[String], changes: NamespaceChange*): Unit = {
    throw new UnsupportedOperationException
  }
  override def dropNamespace(namespace: Array[String]): Boolean = {
    throw new UnsupportedOperationException
  }
  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {}
  override def name(): String = "dummy"
}
