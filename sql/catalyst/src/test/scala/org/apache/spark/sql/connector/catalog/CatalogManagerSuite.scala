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

import scala.jdk.CollectionConverters._

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.{EmptyFunctionRegistry, FakeV2SessionCatalog, NoSuchNamespaceException}
import org.apache.spark.sql.catalyst.catalog.{CatalogDatabase, InMemoryCatalog => V1InMemoryCatalog, SessionCatalog}
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.connector.catalog.CatalogManager.{CurrentSchemaEntry, LiteralPathEntry}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class CatalogManagerSuite extends SparkFunSuite with SQLHelper {

  private def createSessionCatalog(): SessionCatalog = {
    val catalog = new V1InMemoryCatalog()
    catalog.createDatabase(
      CatalogDatabase(SessionCatalog.DEFAULT_DATABASE, "", new URI("fake"), Map.empty),
      ignoreIfExists = true)
    new SessionCatalog(catalog, EmptyFunctionRegistry)
  }

  test("CatalogManager should reflect the changes of default catalog") {
    val catalogManager = new DefaultCatalogManager(FakeV2SessionCatalog, createSessionCatalog())
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
    val catalogManager = new DefaultCatalogManager(FakeV2SessionCatalog, createSessionCatalog())
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
    val catalogManager = new DefaultCatalogManager(FakeV2SessionCatalog, createSessionCatalog())
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
    val catalogManager = new DefaultCatalogManager(FakeV2SessionCatalog, v1SessionCatalog)

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

  test("deserializePathEntries parses valid payloads") {
    val stored =
      """[["spark_catalog","default"],["system","builtin"],["spark_catalog","db1","ns1"]]"""
    assert(CatalogManager.deserializePathEntries(stored).contains(Seq(
      Seq("spark_catalog", "default"),
      Seq("system", "builtin"),
      Seq("spark_catalog", "db1", "ns1"))))
    assert(CatalogManager.deserializePathEntries("[]").contains(Seq.empty))
  }

  test("deserializePathEntries returns None for malformed payloads") {
    val malformedPayloads = Seq(
      "",
      "not_json",
      "{}",
      """["spark_catalog"]""",
      """[["spark_catalog"], 1]""",
      """[[1]]""")
    malformedPayloads.foreach { payload =>
      assert(CatalogManager.deserializePathEntries(payload).isEmpty, s"payload=$payload")
    }
  }

  test("serializePathEntries round-trips through deserialize for typical inputs") {
    val cases = Seq(
      Seq(Seq("spark_catalog", "default"), Seq("system", "builtin")),
      Seq(Seq("system", "session")),
      Seq.empty[Seq[String]])
    cases.foreach { entries =>
      val payload = CatalogManager.serializePathEntries(entries)
      val parsed = CatalogManager.deserializePathEntries(payload)
        .getOrElse(fail(s"Expected payload to round-trip: $payload"))
      assert(parsed === entries, s"Round-trip mismatch for $entries; got $parsed")
    }
  }

  test("serializePathEntries round-trips multi-level and quoted identifiers") {
    val entries = Seq(
      Seq("cat", "ns1", "ns2"),
      Seq("spark_catalog", "sch.with.dots"),
      Seq("spark_catalog", "schema with spaces"))
    val payload = CatalogManager.serializePathEntries(entries)
    val parsed = CatalogManager.deserializePathEntries(payload)
      .getOrElse(fail(s"Expected payload to round-trip: $payload"))
    assert(parsed === entries)
  }

  test("deserializePathEntriesOrFail raises a clear AnalysisException for bad payloads") {
    val e = intercept[AnalysisException] {
      CatalogManager.deserializePathEntriesOrFail(
        storedPathStr = "{bad-json",
        objectType = "view",
        objectName = "default.v_broken")
    }
    assert(e.getMessage.contains("Invalid stored SQL path metadata for view"))
    assert(e.getMessage.contains("default.v_broken"))
  }

  // ---------------------------------------------------------------------------
  // Direct unit tests for [[PathElement.validateNoStaticDuplicates]]. The end-to-end
  // `SetPathSuite` exercises this via SQL, but the duplicate-detection rules
  // (literal-vs-literal, current_schema-vs-current_schema, case-sensitivity) are pure
  // data and benefit from focused tests close to the implementation.
  // ---------------------------------------------------------------------------

  private def literalEntry(parts: String*): LiteralPathEntry = LiteralPathEntry(parts.toSeq)

  test("validateNoStaticDuplicates: no duplicates returns the input unchanged") {
    val entries = Seq(
      literalEntry("spark_catalog", "default"),
      literalEntry("system", "builtin"),
      CurrentSchemaEntry)
    assert(PathElement.validateNoStaticDuplicates(entries, caseSensitive = false) === entries)
  }

  test("validateNoStaticDuplicates: duplicate literal under case-insensitive collation") {
    val entries = Seq(
      literalEntry("spark_catalog", "default"),
      literalEntry("Spark_Catalog", "DEFAULT"))
    val e = intercept[AnalysisException] {
      PathElement.validateNoStaticDuplicates(entries, caseSensitive = false)
    }
    assert(e.getCondition == "DUPLICATE_SQL_PATH_ENTRY")
    assert(e.getMessageParameters.get("pathEntry") == "Spark_Catalog.DEFAULT")
  }

  test("validateNoStaticDuplicates: case-sensitive mode keeps differently cased entries") {
    val entries = Seq(
      literalEntry("spark_catalog", "DEFAULT"),
      literalEntry("spark_catalog", "default"))
    assert(PathElement.validateNoStaticDuplicates(entries, caseSensitive = true) === entries)
  }

  test("validateNoStaticDuplicates: repeated CurrentSchemaEntry is rejected") {
    val entries = Seq(CurrentSchemaEntry, CurrentSchemaEntry)
    val e = intercept[AnalysisException] {
      PathElement.validateNoStaticDuplicates(entries, caseSensitive = false)
    }
    assert(e.getCondition == "DUPLICATE_SQL_PATH_ENTRY")
    assert(e.getMessageParameters.get("pathEntry") == "current_schema")
  }

  test("validateNoStaticDuplicates: literal-vs-CurrentSchemaEntry collision is tolerated") {
    // The CurrentSchemaEntry marker resolves dynamically against USE SCHEMA, so a literal
    // that happens to match the live current schema is intentionally not flagged here.
    val entries = Seq(
      literalEntry("spark_catalog", "default"),
      CurrentSchemaEntry,
      literalEntry("system", "builtin"))
    assert(PathElement.validateNoStaticDuplicates(entries, caseSensitive = false) === entries)
  }

  test("validateNoStaticDuplicates: identifier containing a dot is quoted in the error") {
    val entries = Seq(
      literalEntry("spark_catalog", "weird.schema"),
      literalEntry("spark_catalog", "weird.schema"))
    val e = intercept[AnalysisException] {
      PathElement.validateNoStaticDuplicates(entries, caseSensitive = false)
    }
    assert(e.getMessageParameters.get("pathEntry") == "spark_catalog.`weird.schema`")
  }

  test("validateNoStaticDuplicates: multi-level namespace duplicate is flagged") {
    val entries = Seq(
      literalEntry("cat", "db", "ns"),
      literalEntry("cat", "db", "ns"))
    val e = intercept[AnalysisException] {
      PathElement.validateNoStaticDuplicates(entries, caseSensitive = false)
    }
    assert(e.getMessageParameters.get("pathEntry") == "cat.db.ns")
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
