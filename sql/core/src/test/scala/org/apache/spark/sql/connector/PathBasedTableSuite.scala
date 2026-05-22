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

import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.connector.catalog.InMemoryTableCatalog
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Non-transactional tests for SQL resolution of path-based tables surfaced by a
 * [[org.apache.spark.sql.connector.catalog.SupportsCatalogOptions]] data source
 * (e.g. `pathformat.`/path/to/t``). [[FakePathBasedSource]] routes resolution to a
 * dedicated `pathformat_cat` catalog rather than the session catalog, so assertions
 * against that catalog unambiguously confirm the SCO seam fired — without SCO,
 * `CatalogAndIdentifier`'s fallback lands in the (default) session catalog and the
 * named catalog stays empty.
 */
class PathBasedTableSuite extends QueryTest with SharedSparkSession {

  import testImplicits._

  private val tablePath = "pathformat.`/path/to/t`"

  private def pathformatCat: InMemoryTableCatalog =
    spark.sessionState.catalogManager.catalog(FakePathBasedSource.CATALOG_NAME)
      .asInstanceOf[InMemoryTableCatalog]

  override def beforeEach(): Unit = {
    super.beforeEach()
    spark.conf.set(
      s"spark.sql.catalog.${FakePathBasedSource.CATALOG_NAME}",
      classOf[InMemoryTableCatalog].getName)
  }

  override def afterEach(): Unit = {
    // Start each test starts with a fresh InMemoryTableCatalog instance.
    spark.sessionState.catalogManager.reset()
    spark.conf.unset(s"spark.sql.catalog.${FakePathBasedSource.CATALOG_NAME}")
    super.afterEach()
  }

  test("CREATE then SELECT on path-based table") {
    sql(s"CREATE TABLE $tablePath (id INT, data STRING)")
    sql(s"INSERT INTO $tablePath VALUES (1, 'a'), (2, 'b')")
    checkAnswer(spark.table(tablePath), Row(1, "a") :: Row(2, "b") :: Nil)
    // The SCO resolver routed creation/reads to pathformat_cat. Without the seam,
    // CREATE would fall through to the (default) session catalog and fail.
    assert(pathformatCat.listTables(Array("pathformat")).map(_.name()).contains("/path/to/t"))
  }

  test("DESCRIBE TABLE resolves path-based table") {
    sql(s"CREATE TABLE $tablePath (id INT, data STRING)")
    checkAnswer(
      sql(s"DESCRIBE TABLE $tablePath").select("col_name", "data_type"),
      Row("id", "int") :: Row("data", "string") :: Nil)
  }

  test("ALTER TABLE on path-based table") {
    sql(s"CREATE TABLE $tablePath (id INT, data STRING)")
    sql(s"ALTER TABLE $tablePath ADD COLUMNS (extra DOUBLE)")
    val columns = sql(s"DESCRIBE TABLE $tablePath").collect().map(_.getString(0)).toSet
    assert(Set("id", "data", "extra").subsetOf(columns))
  }

  test("DROP TABLE on path-based table") {
    sql(s"CREATE TABLE $tablePath (id INT, data STRING)")
    sql(s"DROP TABLE $tablePath")
    intercept[AnalysisException] {
      sql(s"SELECT * FROM $tablePath")
    }
  }

  test("JOIN across two path-based tables") {
    val a = "pathformat.`/a`"
    val b = "pathformat.`/b`"
    sql(s"CREATE TABLE $a (id INT, x STRING)")
    sql(s"CREATE TABLE $b (id INT, y STRING)")
    sql(s"INSERT INTO $a VALUES (1, 'x1'), (2, 'x2')")
    sql(s"INSERT INTO $b VALUES (1, 'y1'), (2, 'y2')")
    checkAnswer(
      sql(s"SELECT a.id, a.x, b.y FROM $a a JOIN $b b ON a.id = b.id ORDER BY a.id"),
      Row(1, "x1", "y1") :: Row(2, "x2", "y2") :: Nil)
  }

  test("session-config catalog routes non-transactional reads") {
    val target = "tgt"
    withSQLConf(
        s"spark.sql.catalog.$target" -> classOf[InMemoryTableCatalog].getName,
        s"spark.datasource.pathformat2.catalog" -> target) {
      sql("CREATE TABLE pathformat2.`/p` (id INT, data STRING)")
      sql("INSERT INTO pathformat2.`/p` VALUES (1, 'a')")
      checkAnswer(spark.table("pathformat2.`/p`"), Row(1, "a") :: Nil)
      val tgt = spark.sessionState.catalogManager.catalog(target)
        .asInstanceOf[InMemoryTableCatalog]
      assert(tgt.listTables(Array("pathformat2")).map(_.name()).contains("/p"))
    }
  }

  test("v1 file format direct query still resolves") {
    withTempDir { dir =>
      val path = new java.io.File(dir, "p.parquet").getCanonicalPath
      Seq((1, "a"), (2, "b")).toDF("id", "data").write.parquet(path)
      checkAnswer(sql(s"SELECT * FROM parquet.`$path`"), Row(1, "a") :: Row(2, "b") :: Nil)
    }
  }

  test("unknown format head produces table-not-found, not ClassNotFoundException") {
    val e = intercept[AnalysisException] {
      sql("SELECT * FROM unknown_fmt.`/path/to/t`")
    }
    // The format head is not a registered data source and not a catalog. Resolution
    // falls through to a normal table-not-found error.
    assert(e.getCondition == "TABLE_OR_VIEW_NOT_FOUND")
  }

  test("VERSION AS OF on path-based table") {
    val base = "pathformat.`/p`"
    // InMemoryTableCatalog implements time travel by appending the version string to the
    // identifier name (see loadTable(ident, version) - it looks up name + version).
    val versioned = "pathformat.`/pv1`"
    sql(s"CREATE TABLE $base (id INT)")
    sql(s"CREATE TABLE $versioned (id INT)")
    sql(s"INSERT INTO $base VALUES (1)")
    sql(s"INSERT INTO $versioned VALUES (2)")
    checkAnswer(sql(s"SELECT * FROM $base VERSION AS OF 'v1'"), Row(2))
  }

  test("catalog precedence: same-named catalog wins over SCO data source") {
    // Register a catalog under the same name as the SCO data source short name. SQL
    // resolution should route to the catalog; the SCO resolver is consulted only when
    // no catalog claims the head.
    withSQLConf("spark.sql.catalog.pathformat" -> classOf[InMemoryTableCatalog].getName) {
      sql(s"CREATE TABLE $tablePath (id INT, data STRING)")
      sql(s"INSERT INTO $tablePath VALUES (1, 'a')")
      checkAnswer(spark.table(tablePath), Row(1, "a") :: Nil)

      // Table lives in the homonym catalog at identifier (ns=[], name="/path/to/t"),
      // and the SCO-targeted catalog (pathformat_cat) is untouched because the SCO
      // resolver was never consulted.
      val homonymCat = spark.sessionState.catalogManager.catalog("pathformat")
        .asInstanceOf[InMemoryTableCatalog]
      assert(homonymCat.listTables(Array.empty).map(_.name()).contains("/path/to/t"))
      assert(!pathformatCat.namespaceExists(Array("pathformat")))
    }
  }

  test("CREATE TABLE AS SELECT on path-based table") {
    withTable("source") {
      sql("CREATE TABLE source (id INT, data STRING)")
      sql("INSERT INTO source VALUES (1, 'a'), (2, 'b')")
      sql(s"CREATE TABLE $tablePath AS SELECT * FROM source")
      checkAnswer(spark.table(tablePath), Row(1, "a") :: Row(2, "b") :: Nil)
    }
  }

  test("REPLACE TABLE AS SELECT on path-based table") {
    withTable("source") {
      sql("CREATE TABLE source (id INT, data STRING)")
      sql("INSERT INTO source VALUES (1, 'a'), (2, 'b'), (3, 'c')")
      sql(s"CREATE TABLE $tablePath AS SELECT * FROM source")
      sql(s"REPLACE TABLE $tablePath AS SELECT id FROM source WHERE id > 1")
      checkAnswer(spark.table(tablePath), Row(2) :: Row(3) :: Nil)
    }
  }

  test("INSERT OVERWRITE on path-based table") {
    sql(s"CREATE TABLE $tablePath (id INT, data STRING)")
    sql(s"INSERT INTO $tablePath VALUES (1, 'a'), (2, 'b')")
    sql(s"INSERT OVERWRITE $tablePath VALUES (9, 'z')")
    checkAnswer(spark.table(tablePath), Row(9, "z") :: Nil)
  }

  test("DataFrame API: read resolves via SCO") {
    // Create via SQL (exercises the new LookupCatalog SCO seam), read via DataFrame
    // (exercises the pre-existing DataFrameReader SCO path in DataSourceV2Utils).
    // Both paths should land on the same Identifier in pathformat_cat.
    sql(s"CREATE TABLE $tablePath (id INT, data STRING)")
    sql(s"INSERT INTO $tablePath VALUES (1, 'a'), (2, 'b')")
    val df = spark.read.format("pathformat").load("/path/to/t")
    checkAnswer(df, Row(1, "a") :: Row(2, "b") :: Nil)
  }

  test("DataFrame API: write via SCO, read via SQL") {
    // Write through DataFrameWriter (exercises the refactored buildDsOptions in the v2
    // write path), read back via SQL to confirm both entry points land on the same
    // Identifier under pathformat_cat.
    Seq((1, "a"), (2, "b")).toDF("id", "data")
      .write.format("pathformat").save("/path/to/t")
    checkAnswer(spark.table(tablePath), Row(1, "a") :: Row(2, "b") :: Nil)
  }

  test("DataFrame API: time travel via SCO") {
    // InMemoryTableCatalog implements time travel by appending the version string to
    // the identifier name. SCO time-travel options flow through the refactored
    // extractCatalogAndIdentifier helper, so this also regresses that path.
    sql("CREATE TABLE pathformat.`/p` (id INT)")
    sql("CREATE TABLE pathformat.`/pv1` (id INT)")
    sql("INSERT INTO pathformat.`/p` VALUES (1)")
    sql("INSERT INTO pathformat.`/pv1` VALUES (2)")
    val df = spark.read.format("pathformat").option("versionAsOf", "v1").load("/p")
    checkAnswer(df, Row(2))
  }

  test("DataFrame API: pure write and read via SCO (no SQL)") {
    // Uses only DataFrameWriter/DataFrameReader, so it exercises the v2 SCO entry point
    // in DataSourceV2Utils.loadV2Source / the writer's getTableProviderCatalog branch
    // independently of LookupCatalog. Survives even when the SQL SCO seam is removed.
    Seq((1, "a"), (2, "b")).toDF("id", "data")
      .write.format("pathformat").save("/path/to/t")
    val df = spark.read.format("pathformat").load("/path/to/t")
    checkAnswer(df, Row(1, "a") :: Row(2, "b") :: Nil)
    assert(pathformatCat.listTables(Array("pathformat")).map(_.name()).contains("/path/to/t"))
  }

  test("DataFrame API: pure time travel via SCO (no SQL)") {
    // Pure DataFrame setup so the test does not depend on the SQL SCO seam.
    Seq(1).toDF("id").write.format("pathformat").save("/p")
    Seq(2).toDF("id").write.format("pathformat").save("/pv1")
    val df = spark.read.format("pathformat").option("versionAsOf", "v1").load("/p")
    checkAnswer(df, Row(2))
  }
}
