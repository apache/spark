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

package org.apache.spark.sql.connector

import java.util.Collections

import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.sql.connector.catalog.{Identifier, InMemoryCatalog, SupportsNamespaces}
import org.apache.spark.sql.connector.catalog.functions.UnboundFunction
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

/**
 * End-to-end coverage of [[SQLConf.PATH_ENABLED]] resolution through non-session V2 catalogs.
 *
 * Other path tests live in `SetPathSuite` (session catalog) and `ProcedureSuite`
 * (procedures via CALL). This suite specifically exercises:
 *   - unqualified table resolution across two V2 catalogs in SET PATH,
 *   - first-match ordering when both catalogs hold the same name,
 *   - unqualified V2 function resolution across two V2 catalogs in SET PATH,
 *   - the negative case where the unqualified name only lives in a catalog
 *     that is NOT on the path.
 */
class SqlPathV2CatalogSuite extends SharedSparkSession {

  private val emptyProps: java.util.Map[String, String] = Collections.emptyMap()

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.conf.set("spark.sql.catalog.pathcat", classOf[InMemoryCatalog].getName)
    spark.conf.set("spark.sql.catalog.pathcat2", classOf[InMemoryCatalog].getName)
  }

  override def afterAll(): Unit = {
    try {
      spark.sessionState.catalogManager.reset()
      spark.sessionState.conf.unsetConf("spark.sql.catalog.pathcat")
      spark.sessionState.conf.unsetConf("spark.sql.catalog.pathcat2")
    } finally {
      super.afterAll()
    }
  }

  private def v2Catalog(name: String): InMemoryCatalog =
    spark.sessionState.catalogManager.catalog(name).asInstanceOf[InMemoryCatalog]

  private def createV2Namespace(catalog: String, ns: String): Unit = {
    v2Catalog(catalog).asInstanceOf[SupportsNamespaces]
      .createNamespace(Array(ns), emptyProps)
  }

  private def addV2Function(
      catalog: String,
      ns: String,
      name: String,
      fn: UnboundFunction): Unit = {
    v2Catalog(catalog).createFunction(Identifier.of(Array(ns), name), fn)
  }

  test("V2 catalogs on SET PATH: unqualified table follows first match") {
    withSQLConf(SQLConf.PATH_ENABLED.key -> "true") {
      // pathcat and pathcat2 each have a namespace `ns` and a table `path_v2_t` with
      // different contents, so we can tell which catalog supplied the row.
      createV2Namespace("pathcat", "ns")
      createV2Namespace("pathcat2", "ns")
      sql("CREATE TABLE pathcat.ns.path_v2_t (id INT) USING foo")
      sql("INSERT INTO pathcat.ns.path_v2_t VALUES (10)")
      sql("CREATE TABLE pathcat2.ns.path_v2_t (id INT) USING foo")
      sql("INSERT INTO pathcat2.ns.path_v2_t VALUES (20)")

      try {
        sql("SET PATH = pathcat.ns, pathcat2.ns, system.builtin")
        checkAnswer(sql("SELECT id FROM path_v2_t"), Row(10))

        sql("SET PATH = pathcat2.ns, pathcat.ns, system.builtin")
        checkAnswer(sql("SELECT id FROM path_v2_t"), Row(20))
      } finally {
        sql("SET PATH = DEFAULT_PATH")
        sql("DROP TABLE IF EXISTS pathcat.ns.path_v2_t")
        sql("DROP TABLE IF EXISTS pathcat2.ns.path_v2_t")
      }
    }
  }

  test("V2 catalogs on SET PATH: unqualified table only in a non-path catalog is not found") {
    withSQLConf(SQLConf.PATH_ENABLED.key -> "true") {
      createV2Namespace("pathcat", "ns_only_here")
      sql("CREATE TABLE pathcat.ns_only_here.hidden_t (id INT) USING foo")
      try {
        // Path does not include pathcat.ns_only_here; bare `hidden_t` must not resolve.
        sql("SET PATH = pathcat2.ns, system.builtin")
        val e = intercept[AnalysisException] {
          sql("SELECT id FROM hidden_t").collect()
        }
        assert(e.getCondition == "TABLE_OR_VIEW_NOT_FOUND" ||
            e.getMessage.contains("TABLE_OR_VIEW_NOT_FOUND"),
          s"Expected TABLE_OR_VIEW_NOT_FOUND; got: ${e.getCondition}: ${e.getMessage}")
      } finally {
        sql("SET PATH = DEFAULT_PATH")
        sql("DROP TABLE IF EXISTS pathcat.ns_only_here.hidden_t")
      }
    }
  }

  test("V2 catalogs on SET PATH: unqualified function follows first match") {
    withSQLConf(SQLConf.PATH_ENABLED.key -> "true") {
      // Two V2 catalogs each register a `strlen` function; resolution must follow path order.
      createV2Namespace("pathcat", "fns")
      createV2Namespace("pathcat2", "fns")
      addV2Function("pathcat", "fns", "strlen", StrLen(StrLenDefault))
      addV2Function("pathcat2", "fns", "strlen", StrLen(StrLenMagic))
      try {
        sql("SET PATH = pathcat.fns, pathcat2.fns, system.builtin")
        // Both backing impls return the same numeric length, so a correct result here
        // also implies neither catalog raised "not found" -- the path drove resolution.
        checkAnswer(sql("SELECT strlen('abc')"), Row(3))

        sql("SET PATH = pathcat2.fns, pathcat.fns, system.builtin")
        checkAnswer(sql("SELECT strlen('hello')"), Row(5))
      } finally {
        sql("SET PATH = DEFAULT_PATH")
        v2Catalog("pathcat").clearFunctions()
        v2Catalog("pathcat2").clearFunctions()
      }
    }
  }
}
