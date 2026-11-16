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
import org.apache.spark.sql.catalyst.catalog.{
  CatalogDatabase,
  CatalogTable,
  InMemoryCatalog => V1InMemoryCatalog,
  SessionCatalog
}
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.connector.catalog.CatalogManager.{CurrentCatalogEntry, CurrentSchemaEntry, LiteralPathEntry}
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
    import CatalogManager.ResolvedPathEntry
    val stored = CatalogManager.serializePathEntries(Seq(
      ResolvedPathEntry.schema(Seq("spark_catalog", "default")),
      ResolvedPathEntry.schema(Seq("system", "builtin")),
      ResolvedPathEntry.schema(Seq("spark_catalog", "db1", "ns1")),
      ResolvedPathEntry.catalog(Seq("system"))))
    assert(CatalogManager.deserializePathEntries(stored).contains(Seq(
      ResolvedPathEntry.schema(Seq("spark_catalog", "default")),
      ResolvedPathEntry.schema(Seq("system", "builtin")),
      ResolvedPathEntry.schema(Seq("spark_catalog", "db1", "ns1")),
      ResolvedPathEntry.catalog(Seq("system")))))
    assert(CatalogManager.deserializePathEntries("[]").contains(Seq.empty))
  }

  test("deserializePathEntries returns None for malformed payloads") {
    val malformedPayloads = Seq(
      "",
      "not_json",
      "{}",
      """["spark_catalog"]""",
      """[["spark_catalog","default"]]""",
      """[{"parts":["spark_catalog"],"role":"other"}]""",
      """[{"parts":[1],"role":"schema"}]""")
    malformedPayloads.foreach { payload =>
      assert(CatalogManager.deserializePathEntries(payload).isEmpty, s"payload=$payload")
    }
  }

  test("serializePathEntries round-trips through deserialize for typical inputs") {
    import CatalogManager.ResolvedPathEntry
    val cases = Seq(
      Seq(
        ResolvedPathEntry.schema(Seq("spark_catalog", "default")),
        ResolvedPathEntry.schema(Seq("system", "builtin")),
        ResolvedPathEntry.catalog(Seq("system")),
        ResolvedPathEntry.catalog(Seq("spark_catalog"))),
      Seq(ResolvedPathEntry.schema(Seq("system", "session"))),
      // Collapsed current schema and explicit catalog stay distinct after reload.
      Seq(
        ResolvedPathEntry.schema(Seq("testcat")),
        ResolvedPathEntry.catalog(Seq("testcat"))),
      Seq.empty[ResolvedPathEntry])
    cases.foreach { entries =>
      val payload = CatalogManager.serializePathEntries(entries)
      val parsed = CatalogManager.deserializePathEntries(payload)
        .getOrElse(fail(s"Expected payload to round-trip: $payload"))
      assert(parsed === entries, s"Round-trip mismatch for $entries; got $parsed")
    }
  }

  test("serializePathEntries round-trips multi-level and quoted identifiers") {
    import CatalogManager.ResolvedPathEntry
    val entries = Seq(
      ResolvedPathEntry.schema(Seq("cat", "ns1", "ns2")),
      ResolvedPathEntry.schema(Seq("spark_catalog", "sch.with.dots")),
      ResolvedPathEntry.schema(Seq("spark_catalog", "schema with spaces")))
    val payload = CatalogManager.serializePathEntries(entries)
    val parsed = CatalogManager.deserializePathEntries(payload)
      .getOrElse(fail(s"Expected payload to round-trip: $payload"))
    assert(parsed === entries)
  }

  test("legacy persisted path keeps collapsed schemas and appends catalog roles") {
    import CatalogManager.ResolvedPathEntry
    // Version-0 payloads are a JSON array of string arrays. Collapsed CURRENT_SCHEMA names
    // (single catalog part) are schema-role entries and must not be dropped.
    val stored =
      """[["system","builtin"],["testcat"],["spark_catalog","default"]]"""

    def read(persistentCatalogFirst: Boolean): Seq[ResolvedPathEntry] =
      CatalogManager.deserializePathEntriesForAnalysisOrFail(
        stored,
        metadataVersion = 0,
        creationCatalog = Some("spark_catalog"),
        persistentCatalogFirst = persistentCatalogFirst,
        objectType = "view",
        objectName = "v",
        currentMetadataVersion = CatalogTable.CURRENT_VIEW_METADATA_VERSION)

    val schemaEntries = Seq(
      ResolvedPathEntry.schema(Seq("system", "builtin")),
      ResolvedPathEntry.schema(Seq("testcat")),
      ResolvedPathEntry.schema(Seq("spark_catalog", "default")))
    assert(read(persistentCatalogFirst = false) ===
      schemaEntries ++ Seq(
        ResolvedPathEntry.catalog(Seq("system")),
        ResolvedPathEntry.catalog(Seq("spark_catalog"))))
    assert(read(persistentCatalogFirst = true) ===
      schemaEntries ++ Seq(
        ResolvedPathEntry.catalog(Seq("spark_catalog")),
        ResolvedPathEntry.catalog(Seq("system"))))
  }

  test("current metadata version preserves an intentionally schema-only path") {
    import CatalogManager.ResolvedPathEntry
    val entries = Seq(ResolvedPathEntry.schema(Seq("pathcat", "ns")))
    val stored = CatalogManager.serializePathEntries(entries)
    assert(CatalogManager.deserializePathEntriesForAnalysisOrFail(
      stored,
      metadataVersion = CatalogTable.CURRENT_VIEW_METADATA_VERSION,
      creationCatalog = Some("spark_catalog"),
      persistentCatalogFirst = false,
      objectType = "view",
      objectName = "v",
      currentMetadataVersion = CatalogTable.CURRENT_VIEW_METADATA_VERSION) === entries)
  }

  test("unsupported metadata version fails before PATH parse") {
    val e = intercept[AnalysisException] {
      CatalogManager.deserializePathEntriesForAnalysisOrFail(
        storedPathStr = "{bad-json",
        metadataVersion = CatalogTable.CURRENT_VIEW_METADATA_VERSION + 1,
        creationCatalog = Some("spark_catalog"),
        persistentCatalogFirst = false,
        objectType = "view",
        objectName = "default.v_future",
        currentMetadataVersion = CatalogTable.CURRENT_VIEW_METADATA_VERSION)
    }
    assert(e.getMessage.contains("Unsupported stored metadata version"))
    assert(e.getMessage.contains("default.v_future"))
    assert(!e.getMessage.contains("Invalid stored SQL path metadata"))
  }

  test("negative metadata version is rejected") {
    import CatalogManager.ResolvedPathEntry
    val e = intercept[AnalysisException] {
      CatalogManager.deserializePathEntriesForAnalysisOrFail(
        storedPathStr = CatalogManager.serializePathEntries(
          Seq(ResolvedPathEntry.schema(Seq("spark_catalog", "default")))),
        metadataVersion = -1,
        creationCatalog = Some("spark_catalog"),
        persistentCatalogFirst = false,
        objectType = "view",
        objectName = "v",
        currentMetadataVersion = CatalogTable.CURRENT_VIEW_METADATA_VERSION)
    }
    assert(e.getMessage.contains("Unsupported stored metadata version '-1'"))
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

  test("isBuiltinFirstOnPath: SPARK-57758 gate for the built-in function fast-path") {
    // The function-resolution built-in fast-path is safe (cannot be shadowed) only when
    // `system.builtin` is the FIRST schema-role entry. Catalog-role entries do not participate.
    import CatalogManager.ResolvedPathEntry
    val builtin = ResolvedPathEntry.schema(Seq("system", "builtin"))
    val session = ResolvedPathEntry.schema(Seq("system", "session"))
    val schema = ResolvedPathEntry.schema(Seq("spark_catalog", "some_schema"))
    val catalogOnly = ResolvedPathEntry.catalog(Seq("spark_catalog"))
    val collapsed = ResolvedPathEntry.schema(Seq("testcat"))
    // Default `sessionOrder` modes `second` / `last` keep `system.builtin` first -> safe.
    assert(CatalogManager.isBuiltinFirstOnPath(Seq(builtin, session)))
    assert(CatalogManager.isBuiltinFirstOnPath(Seq(builtin)))
    assert(CatalogManager.isBuiltinFirstOnPath(Seq(builtin, schema, session)))
    // An uncovered catalog-role entry before builtin disables the fast-path. A covered
    // catalog-role duplicate is skipped for one-part, but an earlier schema still disables
    // the fast-path when it precedes builtin.
    assert(!CatalogManager.isBuiltinFirstOnPath(Seq(catalogOnly, builtin)))
    assert(CatalogManager.isBuiltinFirstOnPath(Seq(builtin, catalogOnly)))
    assert(!CatalogManager.isBuiltinFirstOnPath(Seq(
      ResolvedPathEntry.schema(Seq("spark_catalog", "default")), catalogOnly, builtin)))
    // Case-insensitive match on the well-known entry.
    assert(CatalogManager.isBuiltinFirstOnPath(
      Seq(ResolvedPathEntry.schema(Seq("System", "Builtin")), session)))
    // A collapsed current schema before builtin disables the fast-path.
    assert(!CatalogManager.isBuiltinFirstOnPath(Seq(collapsed, builtin)))
    // `first` mode (session before builtin) and custom shapes that place a schema before
    // `system.builtin` -> not safe.
    assert(!CatalogManager.isBuiltinFirstOnPath(Seq(session, builtin)))
    assert(!CatalogManager.isBuiltinFirstOnPath(Seq(schema, builtin)))
    assert(!CatalogManager.isBuiltinFirstOnPath(Seq(schema)))
    assert(!CatalogManager.isBuiltinFirstOnPath(Seq.empty))
  }

  test("identifierPathCandidates selects PATH entries by identifier role") {
    import CatalogManager.ResolvedPathEntry
    val path = Seq(
      ResolvedPathEntry.schema(Seq("system", "builtin")),
      ResolvedPathEntry.schema(Seq("spark_catalog", "default")),
      ResolvedPathEntry.catalog(Seq("system")),
      ResolvedPathEntry.catalog(Seq("spark_catalog")))
    // Schema-only PATH: no catalog-role entries. Two-part names have nothing to resolve against.
    val schemaOnlyPath = Seq(
      ResolvedPathEntry.schema(Seq("spark_catalog", "path_db_a")),
      ResolvedPathEntry.schema(Seq("system", "builtin")))
    val noCatalogs = (_: String) => false
    val sparkCatalogRegistered = Set("spark_catalog").contains _
    val catRegistered = Set("cat").contains _

    // One-part: schema-role entries plus uncovered catalog-role entries, in declared order.
    assert(CatalogManager.identifierPathEntries(path, Seq("f"), noCatalogs) ===
      Seq(Seq("system", "builtin"), Seq("spark_catalog", "default")))
    // Collapsed current schema (schema role) participates; a covered catalog-role duplicate
    // of the same catalog does not. An uncovered catalog-role entry does.
    val emptyNsPath = Seq(
      ResolvedPathEntry.schema(Seq("system", "builtin")),
      ResolvedPathEntry.schema(Seq("testcat")),
      ResolvedPathEntry.catalog(Seq("system")),
      ResolvedPathEntry.catalog(Seq("pathcat")))
    assert(CatalogManager.identifierPathEntries(emptyNsPath, Seq("t"), noCatalogs) ===
      Seq(Seq("system", "builtin"), Seq("testcat"), Seq("pathcat")))
    // Two-part names use catalog-role entries only; schema prefixes do not invent catalogs.
    assert(CatalogManager.identifierPathEntries(
      path, Seq("session", "f"), noCatalogs) === Seq(Seq("system"), Seq("spark_catalog")))
    assert(CatalogManager.identifierPathEntries(
      schemaOnlyPath, Seq("default", "t"), sparkCatalogRegistered) === Seq.empty)
    assert(CatalogManager.identifierPathEntries(
      path, Seq("cat", "f"), catRegistered) ===
      Seq(Seq("system", "builtin"), Seq("spark_catalog", "default")))
    assert(CatalogManager.identifierPathEntries(
      path, Seq("cat", "ns", "f"), noCatalogs) ===
      Seq(Seq("system", "builtin"), Seq("spark_catalog", "default")))
    assert(CatalogManager.identifierPathCandidates(path, Seq("f"), noCatalogs) === Seq(
      Seq("system", "builtin", "f")))
    assert(CatalogManager.identifierPathCandidates(
      path, Seq("f"), sparkCatalogRegistered) === Seq(
      Seq("system", "builtin", "f"),
      Seq("spark_catalog", "default", "f")))
    assert(CatalogManager.identifierPathCandidates(
      emptyNsPath, Seq("t"), Set("testcat").contains _) === Seq(
      Seq("system", "builtin", "t"),
      Seq("testcat", "t")))
    assert(CatalogManager.identifierPathCandidates(
      path, Seq("session", "f"), sparkCatalogRegistered) === Seq(
      Seq("system", "session", "f"),
      Seq("spark_catalog", "session", "f")))
    assert(CatalogManager.identifierPathCandidates(
      path, Seq("session", "f"), noCatalogs) === Seq(
      Seq("system", "session", "f")))
    assert(CatalogManager.identifierPathCandidates(
      schemaOnlyPath, Seq("default", "t"), sparkCatalogRegistered) === Seq.empty)
    assert(CatalogManager.identifierPathCandidates(
      path, Seq("cat", "f"), catRegistered) === Seq(Seq("cat", "f")))
    assert(CatalogManager.identifierPathCandidates(
      path, Seq("cat", "ns", "f"), noCatalogs) === Seq(Seq("cat", "ns", "f")))
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

  test("validateNoStaticDuplicates: repeated CurrentCatalogEntry is rejected") {
    val entries = Seq(CurrentCatalogEntry, CurrentCatalogEntry)
    val e = intercept[AnalysisException] {
      PathElement.validateNoStaticDuplicates(entries, caseSensitive = false)
    }
    assert(e.getCondition == "DUPLICATE_SQL_PATH_ENTRY")
    assert(e.getMessageParameters.get("pathEntry") == "current_catalog")
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
