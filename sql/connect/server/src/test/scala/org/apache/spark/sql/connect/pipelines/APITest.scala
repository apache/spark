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

package org.apache.spark.sql.connect.pipelines

import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
// scalastyle:off
import org.scalatest.funsuite.AnyFunSuite
// scalastyle:on
import org.scalatest.matchers.should.Matchers

import org.apache.spark.sql.QueryTest.checkAnswer
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.pipelines.utils.TestGraphRegistrationContext.{DEFAULT_CATALOG, DEFAULT_DATABASE}

case class TestPipelineSpec(catalog: String, database: String, include: Seq[String])

case class TestPipelineConfiguration(
    pipelineSpec: TestPipelineSpec,
    // additional configuration options not included in the pipeline spec that should also be tested
    dryRun: Boolean = false,
    fullRefreshAll: Boolean = false,
    fullRefreshSelection: Seq[String] = Seq.empty,
    refreshSelection: Seq[String] = Seq.empty)

case class File(name: String, contents: String)

/*
  Extendable traits for PipelineReference and UpdateReference to allow different level of
  implementations which stores pipeline execution and update execution specific information.
 */
trait PipelineReference {}

trait UpdateReference {}

trait APITest
    extends AnyFunSuite // scalastyle:ignore funsuite
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with Matchers {

  protected def spark: SparkSession
  protected def catalogInPipelineSpec: Option[String]
  protected def databaseInPipelineSpec: Option[String]

  def createAndRunPipeline(
      config: TestPipelineConfiguration,
      sources: Seq[File]): (PipelineReference, UpdateReference)
  def awaitPipelineUpdateTermination(update: UpdateReference): Unit
  def stopPipelineUpdate(update: UpdateReference): Unit

  /* SQL Language Tests */
  test("SQL Pipeline with mv, st, and flows") {
    val pipelineSpec =
      TestPipelineSpec(
        catalog = catalogInPipelineSpec.getOrElse(DEFAULT_CATALOG),
        database = databaseInPipelineSpec.getOrElse(DEFAULT_DATABASE),
        include = Seq("mv.sql", "st.sql"))
    val testConfig = TestPipelineConfiguration(pipelineSpec)
    val sources = Seq(
      File(
        name = "st.sql",
        contents = s"""
                     |CREATE STREAMING TABLE ${pipelineSpec.catalog}.${pipelineSpec.database}.st;
                     |CREATE FLOW f AS INSERT INTO st BY NAME SELECT * FROM STREAM mv WHERE id > 2;
                     |""".stripMargin),
      File(
        name = "mv.sql",
        contents = s"""
                     |CREATE MATERIALIZED VIEW ${pipelineSpec.database}.mv
                     |AS SELECT * FROM RANGE(5);
                     |""".stripMargin))
    val (_, update) = createAndRunPipeline(testConfig, sources)
    awaitPipelineUpdateTermination(update)

    checkAnswer(
      spark.sql(s"SELECT * FROM ${pipelineSpec.catalog}.${pipelineSpec.database}.st"),
      Seq(Row(3), Row(4)))
  }

  test("SQL Pipeline with CTE") {
    val pipelineSpec =
      TestPipelineSpec(
        catalog = catalogInPipelineSpec.getOrElse(DEFAULT_CATALOG),
        database = databaseInPipelineSpec.getOrElse(DEFAULT_DATABASE),
        include = Seq("*.sql"))
    val testConfig = TestPipelineConfiguration(pipelineSpec)
    val sources = Seq(
      File(
        name = "definition.sql",
        contents = """
                     |CREATE MATERIALIZED VIEW a AS SELECT 1;
                     |CREATE MATERIALIZED VIEW d AS
                     |WITH c AS (
                     | WITH b AS (
                     |   SELECT * FROM a
                     | )
                     | SELECT * FROM b
                     |)
                     |SELECT * FROM c;
                     |""".stripMargin))

    val (_, update) = createAndRunPipeline(testConfig, sources)
    awaitPipelineUpdateTermination(update)

    checkAnswer(
      spark.sql(s"SELECT * FROM ${pipelineSpec.catalog}.${pipelineSpec.database}.d"),
      Seq(Row(1)))
  }

  test("SQL Pipeline with subquery") {
    val pipelineSpec =
      TestPipelineSpec(
        catalog = catalogInPipelineSpec.getOrElse(DEFAULT_CATALOG),
        database = databaseInPipelineSpec.getOrElse(DEFAULT_DATABASE),
        include = Seq("definition.sql"))
    val testConfig = TestPipelineConfiguration(pipelineSpec)
    val sources = Seq(
      File(
        name = "definition.sql",
        contents = """
                     |CREATE MATERIALIZED VIEW a AS SELECT * FROM RANGE(5);
                     |CREATE MATERIALIZED VIEW b AS SELECT * FROM RANGE(5)
                     |WHERE id = (SELECT max(id) FROM a);
                     |""".stripMargin))

    val (_, update) = createAndRunPipeline(testConfig, sources)
    awaitPipelineUpdateTermination(update)
    checkAnswer(
      spark.sql(s"SELECT * FROM ${pipelineSpec.catalog}.${pipelineSpec.database}.b"),
      Seq(Row(4)))
  }

  test("SQL Pipeline with join") {
    val pipelineSpec =
      TestPipelineSpec(
        catalog = catalogInPipelineSpec.getOrElse(DEFAULT_CATALOG),
        database = databaseInPipelineSpec.getOrElse(DEFAULT_DATABASE),
        include = Seq("definition.sql"))
    val testConfig = TestPipelineConfiguration(pipelineSpec)
    val sources = Seq(
      File(
        name = "definition.sql",
        contents = """
                  |CREATE TEMPORARY VIEW a AS SELECT id FROM range(1,3);
                  |CREATE TEMPORARY VIEW b AS SELECT id FROM range(1,3);
                  |CREATE MATERIALIZED VIEW c AS SELECT a.id AS id1, b.id AS id2
                  |FROM a JOIN b ON a.id=b.id
                  |""".stripMargin))

    val (_, update) = createAndRunPipeline(testConfig, sources)
    awaitPipelineUpdateTermination(update)
    checkAnswer(
      spark.sql(s"SELECT * FROM ${pipelineSpec.catalog}.${pipelineSpec.database}.c"),
      Seq(Row(1, 1), Row(2, 2)))
  }

  test("SQL Pipeline with aggregation") {
    val pipelineSpec =
      TestPipelineSpec(
        catalog = catalogInPipelineSpec.getOrElse(DEFAULT_CATALOG),
        database = databaseInPipelineSpec.getOrElse(DEFAULT_DATABASE),
        include = Seq("definition.sql"))
    val testConfig = TestPipelineConfiguration(pipelineSpec)
    val sources = Seq(
      File(
        name = "definition.sql",
        contents = """
         |CREATE MATERIALIZED VIEW a AS SELECT id AS value, (id % 2) AS isOdd FROM range(1,10);
         |CREATE MATERIALIZED VIEW b AS SELECT isOdd, max(value) AS
         |maximum FROM a GROUP BY isOdd LIMIT 2;
         |""".stripMargin))

    val (_, update) = createAndRunPipeline(testConfig, sources)
    awaitPipelineUpdateTermination(update)
    checkAnswer(
      spark.sql(s"SELECT * FROM ${pipelineSpec.catalog}.${pipelineSpec.database}.b"),
      Seq(Row(0, 8), Row(1, 9)))
  }

  test("SQL Pipeline with table properties") {
    val pipelineSpec =
      TestPipelineSpec(
        catalog = catalogInPipelineSpec.getOrElse(DEFAULT_CATALOG),
        database = databaseInPipelineSpec.getOrElse(DEFAULT_DATABASE),
        include = Seq("definition.sql"))
    val testConfig = TestPipelineConfiguration(pipelineSpec)
    val sources = Seq(
      File(
        name = "definition.sql",
        contents = """
           |CREATE MATERIALIZED VIEW mv TBLPROPERTIES ('prop1'='foo1', 'prop2'='bar2')
           |AS SELECT 1;
           |CREATE STREAMING TABLE st TBLPROPERTIES ('prop3'='foo3', 'prop4'='bar4')
           |AS SELECT * FROM STREAM(mv);
           |""".stripMargin))

    val (_, update) = createAndRunPipeline(testConfig, sources)
    awaitPipelineUpdateTermination(update)
    // verify table properties
    val mv = spark.sessionState.catalog
      .getTableMetadata(TableIdentifier("mv", Some(pipelineSpec.database)))
    assert(mv.properties.get("prop1").contains("foo1"))
    assert(mv.properties.get("prop2").contains("bar2"))

    val st = spark.sessionState.catalog
      .getTableMetadata(TableIdentifier("st", Some(pipelineSpec.database)))
    assert(st.properties.get("prop3").contains("foo3"))
    assert(st.properties.get("prop4").contains("bar4"))
  }

  test("SQL Pipeline with schema") {
    val pipelineSpec =
      TestPipelineSpec(
        catalog = catalogInPipelineSpec.getOrElse(DEFAULT_CATALOG),
        database = databaseInPipelineSpec.getOrElse(DEFAULT_DATABASE),
        include = Seq("definition.sql"))
    val testConfig = TestPipelineConfiguration(pipelineSpec)
    val sources = Seq(
      File(
        name = "definition.sql",
        contents = """
               |CREATE MATERIALIZED VIEW a (id LONG COMMENT 'comment') AS SELECT * FROM RANGE(5);
               |CREATE STREAMING TABLE b (id LONG COMMENT 'comment') AS SELECT * FROM STREAM a;
               |""".stripMargin))

    val (_, update) = createAndRunPipeline(testConfig, sources)
    awaitPipelineUpdateTermination(update)

    val a = spark.sessionState.catalog
      .getTableMetadata(TableIdentifier("a", Some(pipelineSpec.database)))
    assert(a.schema.fields.length == 1)
    assert(a.schema.fields(0).name == "id")

    val b = spark.sessionState.catalog
      .getTableMetadata(TableIdentifier("b", Some(pipelineSpec.database)))
    assert(b.schema.fields.length == 1)
    assert(b.schema.fields(0).name == "id")
  }

  /* Mixed Language Tests */
  test("Pipeline with Python and SQL") {
    val pipelineSpec =
      TestPipelineSpec(
        catalog = catalogInPipelineSpec.getOrElse(DEFAULT_CATALOG),
        database = databaseInPipelineSpec.getOrElse(DEFAULT_DATABASE),
        include = Seq("definition.sql", "definition.py"))
    val testConfig = TestPipelineConfiguration(pipelineSpec)
    val sources = Seq(
      File(
        name = "definition.sql",
        contents = """
                     |CREATE STREAMING TABLE c;
                     |CREATE MATERIALIZED VIEW a AS SELECT * FROM RANGE(5);
                     |""".stripMargin),
      File(
        name = "definition.py",
        contents = """
                     |from pyspark import pipelines as dp
                     |from pyspark.sql import DataFrame, SparkSession
                     |
                     |spark = SparkSession.active()
                     |
                     |@dp.append_flow(target = "c", name = "append_to_c")
                     |def flow():
                     |  return spark.readStream.table("b").filter("id >= 3")
                     |
                     |@dp.materialized_view
                     |def b():
                     |  return spark.read.table("a")
                     |""".stripMargin))

    val (_, update) = createAndRunPipeline(testConfig, sources)
    awaitPipelineUpdateTermination(update)
    checkAnswer(
      spark.sql(s"SELECT * FROM ${pipelineSpec.catalog}.${pipelineSpec.database}.b"),
      Seq(Row(0), Row(1), Row(2), Row(3), Row(4)))
    checkAnswer(
      spark.sql(s"SELECT * FROM ${pipelineSpec.catalog}.${pipelineSpec.database}.c"),
      Seq(Row(3), Row(4)))
  }

  test("Pipeline referencing internal datasets") {
    val pipelineSpec =
      TestPipelineSpec(
        catalog = catalogInPipelineSpec.getOrElse(DEFAULT_CATALOG),
        database = databaseInPipelineSpec.getOrElse(DEFAULT_DATABASE),
        include = Seq("mv.py", "st.py", "definition.sql"))
    val testConfig = TestPipelineConfiguration(pipelineSpec)
    val sources = Seq(
      File(
        name = "mv.py",
        contents = """
                     |from pyspark import pipelines as dp
                     |from pyspark.sql import DataFrame, SparkSession
                     |
                     |spark = SparkSession.active()
                     |
                     |@dp.materialized_view
                     |def src():
                     |  return spark.range(5)
                     |""".stripMargin),
      File(
        name = "st.py",
        contents = """
                     |from pyspark import pipelines as dp
                     |from pyspark.sql import DataFrame, SparkSession
                     |
                     |spark = SparkSession.active()
                     |
                     |@dp.materialized_view
                     |def a():
                     |  return spark.read.table("src")
                     |
                     |@dp.table
                     |def b():
                     |  return spark.readStream.table("src")
                     |""".stripMargin),
      File(
        name = "definition.sql",
        contents = """
                     |CREATE STREAMING TABLE c;
                     |CREATE FLOW f AS INSERT INTO c BY NAME SELECT * FROM STREAM b WHERE id > 2;
                     |""".stripMargin))
    val (_, update) = createAndRunPipeline(testConfig, sources)
    awaitPipelineUpdateTermination(update)

    checkAnswer(
      spark.sql(s"SELECT * FROM ${pipelineSpec.catalog}.${pipelineSpec.database}.a"),
      Seq(Row(0), Row(1), Row(2), Row(3), Row(4)))
    checkAnswer(
      spark.sql(s"SELECT * FROM ${pipelineSpec.catalog}.${pipelineSpec.database}.b"),
      Seq(Row(0), Row(1), Row(2), Row(3), Row(4)))
    checkAnswer(
      spark.sql(s"SELECT * FROM ${pipelineSpec.catalog}.${pipelineSpec.database}.c"),
      Seq(Row(3), Row(4)))
  }

  test("Pipeline referencing external datasets") {
    val pipelineSpec =
      TestPipelineSpec(
        catalog = catalogInPipelineSpec.getOrElse(DEFAULT_CATALOG),
        database = databaseInPipelineSpec.getOrElse(DEFAULT_DATABASE),
        include = Seq("definition.py", "definition.sql"))
    val testConfig = TestPipelineConfiguration(pipelineSpec)
    spark.sql(
      s"CREATE TABLE ${pipelineSpec.catalog}.${pipelineSpec.database}.src " +
        s"AS SELECT * FROM RANGE(5)")
    val sources = Seq(
      File(
        name = "definition.py",
        contents = """
                     |from pyspark import pipelines as dp
                     |from pyspark.sql import DataFrame, SparkSession
                     |
                     |spark = SparkSession.active()
                     |
                     |@dp.materialized_view
                     |def a():
                     |  return spark.read.table("src")
                     |
                     |@dp.table
                     |def b():
                     |  return spark.readStream.table("src")
                     |""".stripMargin),
      File(
        name = "definition.sql",
        contents = """
                     |CREATE STREAMING TABLE c;
                     |CREATE FLOW f AS INSERT INTO c BY NAME SELECT * FROM STREAM b WHERE id > 2;
                     |""".stripMargin))
    val (_, update) = createAndRunPipeline(testConfig, sources)
    awaitPipelineUpdateTermination(update)

    checkAnswer(
      spark.sql(s"SELECT * FROM ${pipelineSpec.catalog}.${pipelineSpec.database}.a"),
      Seq(Row(0), Row(1), Row(2), Row(3), Row(4)))
    checkAnswer(
      spark.sql(s"SELECT * FROM ${pipelineSpec.catalog}.${pipelineSpec.database}.b"),
      Seq(Row(0), Row(1), Row(2), Row(3), Row(4)))
    checkAnswer(
      spark.sql(s"SELECT * FROM ${pipelineSpec.catalog}.${pipelineSpec.database}.c"),
      Seq(Row(3), Row(4)))
  }

  /* Python Language Tests */
  test("Python Pipeline with materialized_view, create_streaming_table, and append_flow") {
    val pipelineSpec =
      TestPipelineSpec(
        catalog = catalogInPipelineSpec.getOrElse(DEFAULT_CATALOG),
        database = databaseInPipelineSpec.getOrElse(DEFAULT_DATABASE),
        include = Seq("st.py", "mv.py"))
    val testConfig = TestPipelineConfiguration(pipelineSpec)
    val sources = Seq(
      File(
        name = "st.py",
        contents = s"""
           |from pyspark import pipelines as dp
           |from pyspark.sql import DataFrame, SparkSession
           |
           |spark = SparkSession.active()
           |
           |dp.create_streaming_table(
           |  name = "${pipelineSpec.database}.a",
           |  schema = "id LONG",
           |  comment = "streaming table a",
           |)
           |
           |@dp.append_flow(target = "a", name = "append_to_a")
           |def flow():
           |  return spark.readStream.table("src")
           |""".stripMargin),
      File(
        name = "mv.py",
        contents = s"""
           |from pyspark import pipelines as dp
           |from pyspark.sql import DataFrame, SparkSession
           |
           |spark = SparkSession.active()
           |
           |@dp.materialized_view(
           |  name = "${pipelineSpec.catalog}.${pipelineSpec.database}.src",
           |  comment = "source table",
           |  schema = "id LONG"
           |)
           |def irrelevant():
           |  return spark.range(5)
           |""".stripMargin))
    val (_, update) = createAndRunPipeline(testConfig, sources)
    awaitPipelineUpdateTermination(update)

    checkAnswer(
      spark.sql(s"SELECT * FROM ${pipelineSpec.catalog}.${pipelineSpec.database}.a"),
      Seq(Row(0), Row(1), Row(2), Row(3), Row(4)))
  }

  test("Python Pipeline with temporary_view") {
    val pipelineSpec =
      TestPipelineSpec(
        catalog = catalogInPipelineSpec.getOrElse(DEFAULT_CATALOG),
        database = databaseInPipelineSpec.getOrElse(DEFAULT_DATABASE),
        include = Seq("definition.py"))
    val testConfig = TestPipelineConfiguration(pipelineSpec)
    spark.sql(
      s"CREATE TABLE ${pipelineSpec.catalog}.${pipelineSpec.database}.src " +
        s"AS SELECT * FROM RANGE(5)")
    val sources = Seq(
      File(
        name = "definition.py",
        contents = """
                     |from pyspark import pipelines as dp
                     |from pyspark.sql import DataFrame, SparkSession
                     |
                     |spark = SparkSession.active()
                     |
                     |@dp.temporary_view(
                     | name = "view_1",
                     | comment = "temporary view 1"
                     |)
                     |def irrelevant():
                     |  return spark.range(5)
                     |
                     |@dp.materialized_view(
                     | name = "mv_1",
                     |)
                     |def irrelevant_1():
                     |  return spark.read.table("view_1")
                     |""".stripMargin))
    val (_, update) = createAndRunPipeline(testConfig, sources)
    awaitPipelineUpdateTermination(update)

    // query the mv that depends on the temporary view
    checkAnswer(
      spark.sql(s"SELECT * FROM ${pipelineSpec.catalog}.${pipelineSpec.database}.mv_1"),
      Seq(Row(0), Row(1), Row(2), Row(3), Row(4)))
  }

  /* Below tests pipeline execution configurations */

  test("Pipeline with dry run") {
    val pipelineSpec =
      TestPipelineSpec(
        catalog = catalogInPipelineSpec.getOrElse(DEFAULT_CATALOG),
        database = databaseInPipelineSpec.getOrElse(DEFAULT_DATABASE),
        include = Seq("definition.sql"))
    val testConfig = TestPipelineConfiguration(pipelineSpec, dryRun = true)
    val sources = Seq(
      File(
        name = "definition.sql",
        contents = """
                     |CREATE MATERIALIZED VIEW a AS SELECT * FROM RANGE(5);
                     |CREATE MATERIALIZED VIEW b AS SELECT * FROM a WHERE id > 2;
                     |""".stripMargin))

    val (_, update) = createAndRunPipeline(testConfig, sources)
    awaitPipelineUpdateTermination(update)
    // ensure the table did not get created in dry run mode
    assert(
      !spark.catalog.tableExists(s"${pipelineSpec.catalog}.${pipelineSpec.database}.a"),
      "Table a should not exist in dry run mode")
    assert(
      !spark.catalog.tableExists(s"${pipelineSpec.catalog}.${pipelineSpec.database}.b"),
      "Table b should not exist in dry run mode")
  }

  Seq(
    SelectiveRefreshTestCase(
      name = "Pipeline with refresh all by default",
      fullRefreshAll = false,
      // all tables retain old + new data
      expectedA = Seq(Row(1), Row(2), Row(3)),
      expectedB = Seq(Row(1), Row(2), Row(3)),
      expectedMV = Seq(Row(1), Row(2), Row(3))),
    SelectiveRefreshTestCase(
      name = "Pipeline with full refresh all",
      fullRefreshAll = true,
      expectedA = Seq(Row(2), Row(3)),
      expectedB = Seq(Row(2), Row(3)),
      expectedMV = Seq(Row(2), Row(3))),
    SelectiveRefreshTestCase(
      name = "Pipeline with selective full refresh and refresh",
      fullRefreshAll = false,
      refreshSelection = Seq("b"),
      fullRefreshSelection = Seq("mv", "a"),
      expectedA = Seq(Row(2), Row(3)),
      expectedB = Seq(Row(1), Row(2), Row(3)), // b keeps old + new
      expectedMV = Seq(Row(2), Row(3))),
    SelectiveRefreshTestCase(
      name = "Pipeline with selective full_refresh",
      fullRefreshAll = false,
      fullRefreshSelection = Seq("a"),
      expectedA = Seq(Row(2), Row(3)),
      expectedB = Seq(Row(1)), // b not refreshed
      expectedMV = Seq(Row(1)) // mv not refreshed
    )).foreach(runSelectiveRefreshTest)

  private case class SelectiveRefreshTestCase(
      name: String,
      fullRefreshAll: Boolean,
      refreshSelection: Seq[String] = Seq.empty,
      fullRefreshSelection: Seq[String] = Seq.empty,
      expectedA: Seq[Row],
      expectedB: Seq[Row],
      expectedMV: Seq[Row])

  private def runSelectiveRefreshTest(tc: SelectiveRefreshTestCase): Unit = {
    test(tc.name) {
      val pipelineSpec = TestPipelineSpec(
        catalog = catalogInPipelineSpec.getOrElse("spark_catalog"),
        database = databaseInPipelineSpec.getOrElse("test_db"),
        include = Seq("st.sql", "mv.sql"))
      val externalTable = s"${pipelineSpec.catalog}.${pipelineSpec.database}.source_data"
      // create initial source table
      spark.sql(s"DROP TABLE IF EXISTS $externalTable")
      spark.sql(s"CREATE TABLE $externalTable AS SELECT * FROM RANGE(1, 2)")

      val sources = Seq(
        File(
          name = "st.sql",
          contents = s"""
                        |CREATE STREAMING TABLE a AS SELECT * FROM STREAM $externalTable;
                        |CREATE STREAMING TABLE b AS SELECT * FROM STREAM $externalTable;
                        |""".stripMargin),
        File(
          name = "mv.sql",
          contents = """
                       |CREATE MATERIALIZED VIEW mv AS SELECT * FROM a;
                       |""".stripMargin))

      val testConfig = TestPipelineConfiguration(pipelineSpec)

      // run pipeline with possible refresh/full refresh
      val (_, update) = createAndRunPipeline(testConfig, sources)
      awaitPipelineUpdateTermination(update)

      // Replace source data to simulate a streaming update
      spark.sql(
        s"INSERT OVERWRITE TABLE $externalTable " +
          "SELECT * FROM VALUES (2), (3) AS t(id)")

      val refreshConfig = testConfig.copy(
        refreshSelection = tc.refreshSelection,
        fullRefreshSelection = tc.fullRefreshSelection,
        fullRefreshAll = tc.fullRefreshAll)
      val (_, updateFullRefresh) = createAndRunPipeline(refreshConfig, sources)
      awaitPipelineUpdateTermination(updateFullRefresh)

      // clear caches to force reload
      Seq("a", "b", "mv").foreach { t =>
        spark.catalog.refreshTable(s"${pipelineSpec.catalog}.${pipelineSpec.database}.$t")
      }

      // verify results
      checkAnswer(
        spark.sql(s"SELECT * FROM ${pipelineSpec.catalog}.${pipelineSpec.database}.a"),
        tc.expectedA)
      checkAnswer(
        spark.sql(s"SELECT * FROM ${pipelineSpec.catalog}.${pipelineSpec.database}.b"),
        tc.expectedB)
      checkAnswer(
        spark.sql(s"SELECT * FROM ${pipelineSpec.catalog}.${pipelineSpec.database}.mv"),
        tc.expectedMV)
    }
  }
}
