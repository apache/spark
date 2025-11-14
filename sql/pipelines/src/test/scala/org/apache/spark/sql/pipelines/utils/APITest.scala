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

package org.apache.spark.sql.pipelines.utils

import scala.concurrent.duration._
import scala.concurrent.duration.Duration

import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
// scalastyle:off
import org.scalatest.funsuite.AnyFunSuite
// scalastyle:on
import org.scalatest.matchers.should.Matchers

import org.apache.spark.sql.QueryTest.checkAnswer
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.util.Utils

/**
 * Representation of a pipeline specification
 * @param catalog
 *   the catalog to publish data from the pipeline
 * @param database
 *   the database to publish data from the pipeline
 * @param include
 *   the list of source files to include in the pipeline spec
 */
case class TestPipelineSpec(
    catalog: Option[String] = None,
    database: Option[String] = None,
    include: Seq[String])

/**
 * Available configurations for running a test pipeline.
 *
 * @param pipelineSpec
 *   the pipeline specification to use Below are CLI options that affect execution, default is to
 *   update all datasets incrementally
 * @param dryRun
 *   if true, the pipeline will be validated but not executed
 * @param fullRefreshAll
 *   if true, perform a full graph reset and recompute
 * @param fullRefreshSelection
 *   if non-empty, only reset and recompute the subset
 * @param refreshSelection
 *   if non-empty, only update the specified subset of datasets
 */
case class TestPipelineConfiguration(
    pipelineSpec: TestPipelineSpec,
    dryRun: Boolean = false,
    fullRefreshAll: Boolean = false,
    fullRefreshSelection: Seq[String] = Seq.empty,
    refreshSelection: Seq[String] = Seq.empty)

/**
 * Logical representation of a source file to be included in the pipeline spec.
 */
case class PipelineSourceFile(name: String, contents: String)

/**
 * Extendable traits for PipelineReference and UpdateReference to allow different level of
 * implementations which stores pipeline execution and update execution specific information.
 */
trait PipelineReference {}

trait APITest
    extends AnyFunSuite // scalastyle:ignore funsuite
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with Matchers {

  protected def spark: SparkSession

  def createAndRunPipeline(
      config: TestPipelineConfiguration,
      sources: Seq[PipelineSourceFile]): PipelineReference
  def awaitPipelineTermination(pipeline: PipelineReference, timeout: Duration = 60.seconds): Unit
  def stopPipeline(pipeline: PipelineReference): Unit

  /* SQL Language Tests */
  test("SQL Pipeline with mv, st, and flows") {
    val pipelineSpec =
      TestPipelineSpec(include = Seq("transformations/mvs/**", "transformations/st.sql"))
    val pipelineConfig = TestPipelineConfiguration(pipelineSpec)
    val sources = Seq(
      PipelineSourceFile(
        name = "transformations/st.sql",
        contents = s"""
                     |CREATE STREAMING TABLE st;
                     |CREATE FLOW f AS INSERT INTO st BY NAME SELECT * FROM STREAM mv WHERE id > 2;
                     |""".stripMargin),
      PipelineSourceFile(
        name = "transformations/mvs/mv.sql",
        contents = s"""
                     |CREATE MATERIALIZED VIEW mv
                     |AS SELECT * FROM RANGE(5);
                     |""".stripMargin))
    val pipeline = createAndRunPipeline(pipelineConfig, sources)
    awaitPipelineTermination(pipeline)

    checkAnswer(spark.sql(s"SELECT * FROM st"), Seq(Row(3), Row(4)))
  }

  test("SQL Pipeline with CTE") {
    val pipelineSpec =
      TestPipelineSpec(include = Seq("transformations/**"))
    val pipelineConfig = TestPipelineConfiguration(pipelineSpec)
    val sources = Seq(
      PipelineSourceFile(
        name = "transformations/definition.sql",
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

    val pipeline = createAndRunPipeline(pipelineConfig, sources)
    awaitPipelineTermination(pipeline)

    checkAnswer(spark.sql(s"SELECT * FROM d"), Seq(Row(1)))
  }

  test("SQL Pipeline with subquery") {
    val pipelineSpec =
      TestPipelineSpec(include = Seq("transformations/**"))
    val pipelineConfig = TestPipelineConfiguration(pipelineSpec)
    val sources = Seq(
      PipelineSourceFile(
        name = "transformations/definition.sql",
        contents = """
                     |CREATE MATERIALIZED VIEW a AS SELECT * FROM RANGE(5);
                     |CREATE MATERIALIZED VIEW b AS SELECT * FROM RANGE(5)
                     |WHERE id = (SELECT max(id) FROM a);
                     |""".stripMargin))

    val pipeline = createAndRunPipeline(pipelineConfig, sources)
    awaitPipelineTermination(pipeline)
    checkAnswer(spark.sql(s"SELECT * FROM b"), Seq(Row(4)))
  }

  test("SQL Pipeline with join") {
    val pipelineSpec =
      TestPipelineSpec(include = Seq("transformations/**"))
    val pipelineConfig = TestPipelineConfiguration(pipelineSpec)
    val sources = Seq(
      PipelineSourceFile(
        name = "transformations/definition.sql",
        contents = """
                  |CREATE TEMPORARY VIEW a AS SELECT id FROM range(1,3);
                  |CREATE TEMPORARY VIEW b AS SELECT id FROM range(1,3);
                  |CREATE MATERIALIZED VIEW c AS SELECT a.id AS id1, b.id AS id2
                  |FROM a JOIN b ON a.id=b.id
                  |""".stripMargin))

    val pipeline = createAndRunPipeline(pipelineConfig, sources)
    awaitPipelineTermination(pipeline)
    checkAnswer(spark.sql(s"SELECT * FROM c"), Seq(Row(1, 1), Row(2, 2)))
  }

  test("SQL Pipeline with aggregation") {
    val pipelineSpec =
      TestPipelineSpec(include = Seq("transformations/**"))
    val pipelineConfig = TestPipelineConfiguration(pipelineSpec)
    val sources = Seq(
      PipelineSourceFile(
        name = "transformations/definition.sql",
        contents = """
         |CREATE MATERIALIZED VIEW a AS SELECT id AS value, (id % 2) AS isOdd FROM range(1,10);
         |CREATE MATERIALIZED VIEW b AS SELECT isOdd, max(value) AS
         |maximum FROM a GROUP BY isOdd LIMIT 2;
         |""".stripMargin))

    val pipeline = createAndRunPipeline(pipelineConfig, sources)
    awaitPipelineTermination(pipeline)
    checkAnswer(spark.sql(s"SELECT * FROM b"), Seq(Row(0, 8), Row(1, 9)))
  }

  test("SQL Pipeline with table properties") {
    val pipelineSpec =
      TestPipelineSpec(include = Seq("transformations/**"))
    val pipelineConfig = TestPipelineConfiguration(pipelineSpec)
    val sources = Seq(
      PipelineSourceFile(
        name = "transformations/definition.sql",
        contents = """
           |CREATE MATERIALIZED VIEW mv TBLPROPERTIES ('prop1'='foo1', 'prop2'='bar2')
           |AS SELECT 1;
           |CREATE STREAMING TABLE st TBLPROPERTIES ('prop3'='foo3', 'prop4'='bar4')
           |AS SELECT * FROM STREAM(mv);
           |""".stripMargin))

    val pipeline = createAndRunPipeline(pipelineConfig, sources)
    awaitPipelineTermination(pipeline)
    // verify table properties
    val mv = spark.sessionState.catalog
      .getTableMetadata(TableIdentifier("mv"))
    assert(mv.properties.get("prop1").contains("foo1"))
    assert(mv.properties.get("prop2").contains("bar2"))

    val st = spark.sessionState.catalog
      .getTableMetadata(TableIdentifier("st"))
    assert(st.properties.get("prop3").contains("foo3"))
    assert(st.properties.get("prop4").contains("bar4"))
  }

  test("SQL Pipeline with schema") {
    val pipelineSpec =
      TestPipelineSpec(include = Seq("transformations/**"))
    val pipelineConfig = TestPipelineConfiguration(pipelineSpec)
    val sources = Seq(
      PipelineSourceFile(
        name = "transformations/definition.sql",
        contents = """
               |CREATE MATERIALIZED VIEW a (id LONG COMMENT 'comment') AS SELECT * FROM RANGE(5);
               |CREATE STREAMING TABLE b (id LONG COMMENT 'comment') AS SELECT * FROM STREAM a;
               |""".stripMargin))

    val pipeline = createAndRunPipeline(pipelineConfig, sources)
    awaitPipelineTermination(pipeline)

    val a = spark.sessionState.catalog
      .getTableMetadata(TableIdentifier("a"))
    assert(a.schema.fields.length == 1)
    assert(a.schema.fields(0).name == "id")

    val b = spark.sessionState.catalog
      .getTableMetadata(TableIdentifier("b"))
    assert(b.schema.fields.length == 1)
    assert(b.schema.fields(0).name == "id")
  }

  /* Mixed Language Tests */
  test("Pipeline with Python and SQL") {
    val pipelineSpec =
      TestPipelineSpec(include = Seq("transformations/**"))
    val pipelineConfig = TestPipelineConfiguration(pipelineSpec)
    val sources = Seq(
      PipelineSourceFile(
        name = "transformations/definition.sql",
        contents = """
                     |CREATE STREAMING TABLE c;
                     |CREATE MATERIALIZED VIEW a AS SELECT * FROM RANGE(5);
                     |""".stripMargin),
      PipelineSourceFile(
        name = "transformations/definition.py",
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

    val pipeline = createAndRunPipeline(pipelineConfig, sources)
    awaitPipelineTermination(pipeline)
    checkAnswer(spark.sql(s"SELECT * FROM b"), Seq(Row(0), Row(1), Row(2), Row(3), Row(4)))
    checkAnswer(spark.sql(s"SELECT * FROM c"), Seq(Row(3), Row(4)))
  }

  test("Pipeline referencing internal datasets") {
    val pipelineSpec =
      TestPipelineSpec(include =
        Seq("transformations/mv.py", "transformations/st.py", "transformations/definition.sql"))
    val pipelineConfig = TestPipelineConfiguration(pipelineSpec)
    val sources = Seq(
      PipelineSourceFile(
        name = "transformations/mv.py",
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
      PipelineSourceFile(
        name = "transformations/st.py",
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
      PipelineSourceFile(
        name = "transformations/definition.sql",
        contents = """
                     |CREATE STREAMING TABLE c;
                     |CREATE FLOW f AS INSERT INTO c BY NAME SELECT * FROM STREAM b WHERE id > 2;
                     |""".stripMargin))
    val pipeline = createAndRunPipeline(pipelineConfig, sources)
    awaitPipelineTermination(pipeline)

    checkAnswer(spark.sql(s"SELECT * FROM a"), Seq(Row(0), Row(1), Row(2), Row(3), Row(4)))
    checkAnswer(spark.sql(s"SELECT * FROM b"), Seq(Row(0), Row(1), Row(2), Row(3), Row(4)))
    checkAnswer(spark.sql(s"SELECT * FROM c"), Seq(Row(3), Row(4)))
  }

  test("Pipeline referencing external datasets") {
    val pipelineSpec =
      TestPipelineSpec(include =
        Seq("transformations/definition.py", "transformations/definition.sql"))
    val pipelineConfig = TestPipelineConfiguration(pipelineSpec)
    spark.sql(
      s"CREATE TABLE src " +
        s"AS SELECT * FROM RANGE(5)")
    val sources = Seq(
      PipelineSourceFile(
        name = "transformations/definition.py",
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
      PipelineSourceFile(
        name = "transformations/definition.sql",
        contents = """
                     |CREATE STREAMING TABLE c;
                     |CREATE FLOW f AS INSERT INTO c BY NAME SELECT * FROM STREAM b WHERE id > 2;
                     |""".stripMargin))
    val pipeline = createAndRunPipeline(pipelineConfig, sources)
    awaitPipelineTermination(pipeline)

    checkAnswer(spark.sql(s"SELECT * FROM a"), Seq(Row(0), Row(1), Row(2), Row(3), Row(4)))
    checkAnswer(spark.sql(s"SELECT * FROM b"), Seq(Row(0), Row(1), Row(2), Row(3), Row(4)))
    checkAnswer(spark.sql(s"SELECT * FROM c"), Seq(Row(3), Row(4)))
  }

  /* Python Language Tests */
  test("Python Pipeline with materialized_view, create_streaming_table, and append_flow") {
    val pipelineSpec =
      TestPipelineSpec(include = Seq("transformations/**"))
    val pipelineConfig = TestPipelineConfiguration(pipelineSpec)
    val sources = Seq(
      PipelineSourceFile(
        name = "transformations/st.py",
        contents = s"""
           |from pyspark import pipelines as dp
           |from pyspark.sql import DataFrame, SparkSession
           |
           |spark = SparkSession.active()
           |
           |dp.create_streaming_table(
           |  name = "a",
           |  schema = "id LONG",
           |  comment = "streaming table a",
           |)
           |
           |@dp.append_flow(target = "a", name = "append_to_a")
           |def flow():
           |  return spark.readStream.table("src")
           |""".stripMargin),
      PipelineSourceFile(
        name = "transformations/mv.py",
        contents = s"""
           |from pyspark import pipelines as dp
           |from pyspark.sql import DataFrame, SparkSession
           |
           |spark = SparkSession.active()
           |
           |@dp.materialized_view(
           |  name = "src",
           |  comment = "source table",
           |  schema = "id LONG"
           |)
           |def irrelevant():
           |  return spark.range(5)
           |""".stripMargin))
    val pipeline = createAndRunPipeline(pipelineConfig, sources)
    awaitPipelineTermination(pipeline)

    checkAnswer(spark.sql(s"SELECT * FROM a"), Seq(Row(0), Row(1), Row(2), Row(3), Row(4)))
  }

  test("Python Pipeline with temporary_view") {
    val pipelineSpec =
      TestPipelineSpec(include = Seq("transformations/definition.py"))
    val pipelineConfig = TestPipelineConfiguration(pipelineSpec)
    spark.sql(
      s"CREATE TABLE src " +
        s"AS SELECT * FROM RANGE(5)")
    val sources = Seq(
      PipelineSourceFile(
        name = "transformations/definition.py",
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
    val pipeline = createAndRunPipeline(pipelineConfig, sources)
    awaitPipelineTermination(pipeline)

    // query the mv that depends on the temporary view
    checkAnswer(spark.sql(s"SELECT * FROM mv_1"), Seq(Row(0), Row(1), Row(2), Row(3), Row(4)))
  }

  Seq("parquet", "json").foreach { format =>
    test(s"Python Pipeline with $format sink") {
      val session = spark
      import session.implicits._

      // create source data
      spark.sql(s"CREATE TABLE src AS SELECT * FROM RANGE(5)")

      val dir = Utils.createTempDir()
      try {
        val pipelineSpec =
          TestPipelineSpec(include = Seq("transformations/definition.py"))
        val pipelineConfig = TestPipelineConfiguration(pipelineSpec)

        val sources = Seq(
          PipelineSourceFile(
            name = "transformations/definition.py",
            contents =
              s"""
                 |from pyspark import pipelines as dp
                 |from pyspark.sql import DataFrame, SparkSession
                 |
                 |spark = SparkSession.active()
                 |
                 |dp.create_sink(
                 |  "mySink",
                 |  format = "$format",
                 |  options = {"path": "${dir.getPath}"}
                 |)
                 |
                 |@dp.append_flow(
                 |  target = "mySink",
                 |)
                 |def mySinkFlow():
                 |  return spark.readStream.table("src")
                 |""".stripMargin
          )
        )

        val pipeline = createAndRunPipeline(pipelineConfig, sources)
        awaitPipelineTermination(pipeline)

        // verify sink output
        checkAnswer(
          spark.read.format(format).load(dir.getPath),
          Seq(0, 1, 2, 3, 4).toDF().collect().toSeq
        )
      } finally {
        // clean up temp directory
        Utils.deleteRecursively(dir)
      }
    }
  }

  test("Python Pipeline with partition columns") {
    val pipelineSpec =
      TestPipelineSpec(include = Seq("transformations/**"))
    val pipelineConfig = TestPipelineConfiguration(pipelineSpec)
    val sources = Seq(
      PipelineSourceFile(
        name = "transformations/definition.py",
        contents = """
                     |from pyspark import pipelines as dp
                     |from pyspark.sql import DataFrame, SparkSession
                     |from pyspark.sql.functions import col
                     |
                     |spark = SparkSession.active()
                     |
                     |@dp.materialized_view(partition_cols = ["id_mod"])
                     |def mv():
                     |  return spark.range(5).withColumn("id_mod", col("id") % 2)
                     |
                     |@dp.table(partition_cols = ["id_mod"])
                     |def st():
                     |  return spark.readStream.table("mv")
                     |""".stripMargin))
    val pipeline = createAndRunPipeline(pipelineConfig, sources)
    awaitPipelineTermination(pipeline)

    Seq("mv", "st").foreach { tbl =>
      val fullName = s"$tbl"
      checkAnswer(
        spark.sql(s"SELECT * FROM $fullName"),
        Seq(Row(0, 0), Row(1, 1), Row(2, 0), Row(3, 1), Row(4, 0)))
    }
  }

  test("Python Pipeline with cluster columns") {
    val pipelineSpec =
      TestPipelineSpec(include = Seq("transformations/**"))
    val pipelineConfig = TestPipelineConfiguration(pipelineSpec)
    val sources = Seq(
      PipelineSourceFile(
        name = "transformations/definition.py",
        contents = """
                     |from pyspark import pipelines as dp
                     |from pyspark.sql import DataFrame, SparkSession
                     |from pyspark.sql.functions import col
                     |
                     |spark = SparkSession.active()
                     |
                     |@dp.materialized_view(cluster_by = ["cluster_col1"])
                     |def mv():
                     |  df = spark.range(10)
                     |  df = df.withColumn("cluster_col1", col("id") % 3)
                     |  df = df.withColumn("cluster_col2", col("id") % 2)
                     |  return df
                     |
                     |@dp.table(cluster_by = ["cluster_col1"])
                     |def st():
                     |  return spark.readStream.table("mv")
                     |""".stripMargin))
    val pipeline = createAndRunPipeline(pipelineConfig, sources)
    awaitPipelineTermination(pipeline)

    // Verify tables have correct data
    Seq("mv", "st").foreach { tbl =>
      val fullName = s"$tbl"
      checkAnswer(
        spark.sql(s"SELECT * FROM $fullName ORDER BY id"),
        Seq(
          Row(0, 0, 0), Row(1, 1, 1), Row(2, 2, 0), Row(3, 0, 1), Row(4, 1, 0),
          Row(5, 2, 1), Row(6, 0, 0), Row(7, 1, 1), Row(8, 2, 0), Row(9, 0, 1)
        ))
    }

    // Verify clustering information is stored in catalog
    val catalog = spark.sessionState.catalogManager.currentCatalog
      .asInstanceOf[org.apache.spark.sql.connector.catalog.TableCatalog]
    // Check materialized view has clustering transform
    val mvIdentifier = org.apache.spark.sql.connector.catalog.Identifier
      .of(Array("default"), "mv")
    val mvTable = catalog.loadTable(mvIdentifier)
    val mvTransforms = mvTable.partitioning()
    assert(mvTransforms.length == 1)
    assert(mvTransforms.head.name() == "cluster_by")
    assert(mvTransforms.head.toString.contains("cluster_col1"))
    // Check streaming table has clustering transform
    val stIdentifier = org.apache.spark.sql.connector.catalog.Identifier
      .of(Array("default"), "st")
    val stTable = catalog.loadTable(stIdentifier)
    val stTransforms = stTable.partitioning()
    assert(stTransforms.length == 1)
    assert(stTransforms.head.name() == "cluster_by")
    assert(stTransforms.head.toString.contains("cluster_col1"))
  }

  /* Below tests pipeline execution configurations */

  test("Pipeline with dry run") {
    val pipelineSpec =
      TestPipelineSpec(include = Seq("transformations/definition.sql"))
    val pipelineConfig = TestPipelineConfiguration(pipelineSpec, dryRun = true)
    val sources = Seq(
      PipelineSourceFile(
        name = "transformations/definition.sql",
        contents = """
                     |CREATE MATERIALIZED VIEW a AS SELECT * FROM RANGE(5);
                     |CREATE MATERIALIZED VIEW b AS SELECT * FROM a WHERE id > 2;
                     |""".stripMargin))

    val pipeline = createAndRunPipeline(pipelineConfig, sources)
    awaitPipelineTermination(pipeline)
    // ensure the table did not get created in dry run mode
    assert(!spark.catalog.tableExists(s"a"), "Table a should not exist in dry run mode")
    assert(!spark.catalog.tableExists(s"b"), "Table b should not exist in dry run mode")
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
      val pipelineSpec = TestPipelineSpec(include =
        Seq("transformations/st.sql", "transformations/mv.sql"))
      val externalTable = s"source_data"
      // create initial source table
      spark.sql(s"DROP TABLE IF EXISTS $externalTable")
      spark.sql(s"CREATE TABLE $externalTable AS SELECT * FROM RANGE(1, 2)")

      val sources = Seq(
        PipelineSourceFile(
          name = "transformations/st.sql",
          contents = s"""
                        |CREATE STREAMING TABLE a AS SELECT * FROM STREAM $externalTable;
                        |CREATE STREAMING TABLE b AS SELECT * FROM STREAM $externalTable;
                        |""".stripMargin),
        PipelineSourceFile(
          name = "transformations/mv.sql",
          contents = """
                       |CREATE MATERIALIZED VIEW mv AS SELECT * FROM a;
                       |""".stripMargin))

      val pipelineConfig = TestPipelineConfiguration(pipelineSpec)

      // run pipeline with possible refresh/full refresh
      val pipeline = createAndRunPipeline(pipelineConfig, sources)
      awaitPipelineTermination(pipeline)

      // Replace source data to simulate a streaming update
      spark.sql(
        s"INSERT OVERWRITE TABLE $externalTable " +
          "SELECT * FROM VALUES (2), (3) AS t(id)")

      val refreshConfig = pipelineConfig.copy(
        refreshSelection = tc.refreshSelection,
        fullRefreshSelection = tc.fullRefreshSelection,
        fullRefreshAll = tc.fullRefreshAll)
      val secondUpdate = createAndRunPipeline(refreshConfig, sources)
      awaitPipelineTermination(secondUpdate)

      // clear caches to force reload
      Seq("a", "b", "mv").foreach { t =>
        spark.catalog.refreshTable(s"$t")
      }

      // verify results
      checkAnswer(spark.sql(s"SELECT * FROM a"), tc.expectedA)
      checkAnswer(spark.sql(s"SELECT * FROM b"), tc.expectedB)
      checkAnswer(spark.sql(s"SELECT * FROM mv"), tc.expectedMV)
    }
  }
}
