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
package org.apache.spark.sql.pipelines.graph

import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.pipelines.utils.{PipelineTest, TestGraphRegistrationContext}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{LongType, StructType}
import org.apache.spark.util.Utils

class SqlPipelineSuite extends PipelineTest with SharedSparkSession {
  private val externalTable1Ident = fullyQualifiedIdentifier("external_t1")
  private val externalTable2Ident = fullyQualifiedIdentifier("external_t2")

  override def beforeEach(): Unit = {
    super.beforeEach()
    // Create mock external tables that tests can reference, ex. to stream from.
    spark.sql(s"CREATE TABLE $externalTable1Ident AS SELECT * FROM RANGE(3)")
    spark.sql(s"CREATE TABLE $externalTable2Ident AS SELECT * FROM RANGE(4)")
  }

  override def afterEach(): Unit = {
    spark.sql(s"DROP TABLE IF EXISTS $externalTable1Ident")
    spark.sql(s"DROP TABLE IF EXISTS $externalTable2Ident")
    super.afterEach()
  }

  test("Simple register SQL dataset test") {
    val unresolvedDataflowGraph = unresolvedDataflowGraphFromSql(
      sqlText = s"""
                   |CREATE MATERIALIZED VIEW mv AS SELECT 1;
                   |CREATE STREAMING TABLE st AS SELECT * FROM STREAM $externalTable1Ident;
                   |CREATE VIEW v AS SELECT * FROM mv;
                   |CREATE FLOW f AS INSERT INTO st BY NAME
                   |SELECT * FROM STREAM $externalTable2Ident;
                   |""".stripMargin
    )
    val resolvedDataflowGraph = unresolvedDataflowGraph.resolve()

    assert(resolvedDataflowGraph.flows.size == 4)
    assert(resolvedDataflowGraph.tables.size == 2)
    assert(resolvedDataflowGraph.views.size == 1)

    val mvFlow =
      resolvedDataflowGraph.resolvedFlows
        .filter(_.identifier == fullyQualifiedIdentifier("mv"))
        .head
    assert(mvFlow.inputs.map(_.table) == Set())
    assert(mvFlow.destinationIdentifier == fullyQualifiedIdentifier("mv"))

    val stFlow =
      resolvedDataflowGraph.resolvedFlows
        .filter(_.identifier == fullyQualifiedIdentifier("st"))
        .head
    // The streaming table has 1 external input, and no internal (defined within pipeline) inputs
    assert(stFlow.funcResult.usedExternalInputs == Set(externalTable1Ident))
    assert(stFlow.inputs.isEmpty)
    assert(stFlow.destinationIdentifier == fullyQualifiedIdentifier("st"))

    val viewFlow =
      resolvedDataflowGraph.resolvedFlows
        .filter(_.identifier == fullyQualifiedIdentifier("v"))
        .head
    assert(viewFlow.inputs == Set(fullyQualifiedIdentifier("mv")))
    assert(viewFlow.destinationIdentifier == fullyQualifiedIdentifier("v"))

    val namedFlow =
      resolvedDataflowGraph.resolvedFlows.filter(_.identifier == fullyQualifiedIdentifier("f")).head
    assert(namedFlow.funcResult.usedExternalInputs == Set(externalTable2Ident))
    assert(namedFlow.inputs.isEmpty)
    assert(namedFlow.destinationIdentifier == fullyQualifiedIdentifier("st"))
  }

  test("Duplicate table name across different SQL files fails") {
    val graphRegistrationContext = new TestGraphRegistrationContext(spark)
    val sqlGraphRegistrationContext = new SqlGraphRegistrationContext(graphRegistrationContext)

    sqlGraphRegistrationContext.processSqlFile(
      sqlText = "CREATE STREAMING TABLE table;",
      sqlFilePath = "a.sql",
      spark = spark
    )

    sqlGraphRegistrationContext.processSqlFile(
      sqlText = """
                  |CREATE VIEW table AS SELECT 1;
                  |""".stripMargin,
      sqlFilePath = "b.sql",
      spark = spark
    )

    checkError(
      exception = intercept[AnalysisException] {
        graphRegistrationContext.toDataflowGraph
      },
      condition = "PIPELINE_DUPLICATE_IDENTIFIERS.DATASET",
      sqlState = Option("42710"),
      parameters = Map(
        "datasetName" -> fullyQualifiedIdentifier("table").quotedString,
        "datasetType1" -> "TABLE",
        "datasetType2" -> "VIEW"
      )
    )
  }

  test("Static pipeline dataset resolves correctly") {
    val unresolvedDataflowGraph = unresolvedDataflowGraphFromSql(
      sqlText =
        "CREATE MATERIALIZED VIEW a COMMENT 'this is a comment' AS SELECT * FROM range(1, 4)"
    )

    val resolvedDataflowGraph = unresolvedDataflowGraph.resolve()

    val flowA =
      resolvedDataflowGraph.resolvedFlows
        .filter(_.identifier == fullyQualifiedIdentifier("a"))
        .head
    checkAnswer(flowA.df, Seq(Row(1), Row(2), Row(3)))
  }

  test("Special characters in dataset name allowed when escaped") {
    val unresolvedDataflowGraph = unresolvedDataflowGraphFromSql(
      sqlText = """
                  |CREATE MATERIALIZED VIEW `hyphen-mv` AS SELECT * FROM range(1, 4);
                  |CREATE MATERIALIZED VIEW `other-hyphen-mv` AS SELECT * FROM `hyphen-mv`
                  |""".stripMargin
    )

    val resolvedDataflowGraph = unresolvedDataflowGraph.resolve()

    assert(
      resolvedDataflowGraph.resolvedFlows
        .exists(f => f.identifier == fullyQualifiedIdentifier("hyphen-mv") && !f.df.isStreaming)
    )

    assert(
      resolvedDataflowGraph.resolvedFlows
        .exists(f =>
          f.identifier == fullyQualifiedIdentifier("other-hyphen-mv") && !f.df.isStreaming)
    )
  }

  test("Pipeline with batch dependencies is correctly resolved") {
    val unresolvedDataflowGraph = unresolvedDataflowGraphFromSql(
      sqlText = """
                  |CREATE VIEW b as SELECT * FROM a;
                  |CREATE VIEW a AS SELECT * FROM range(1, 4);
                  |CREATE VIEW c AS SELECT * FROM `b`;
                  |CREATE VIEW d AS SELECT * FROM c
                  |""".stripMargin
    )

    val resolvedDataflowGraph = unresolvedDataflowGraph.resolve()

    Seq("a", "b", "c", "d").foreach { datasetName =>
      val backingFlow = resolvedDataflowGraph.resolvedFlows
        .find(_.identifier == fullyQualifiedIdentifier(datasetName))
        .head
      checkAnswer(backingFlow.df, Seq(Row(1), Row(2), Row(3)))
    }
  }

  test("Pipeline dataset can be referenced in subquery") {
    val unresolvedDataflowGraph = unresolvedDataflowGraphFromSql(
      sqlText = """
                  |CREATE MATERIALIZED VIEW A AS SELECT * FROM RANGE(5);
                  |CREATE MATERIALIZED VIEW B AS SELECT * FROM RANGE(5)
                  |WHERE id = (SELECT max(id) FROM A);
                  |""".stripMargin
    )

    startPipelineAndWaitForCompletion(unresolvedDataflowGraph)

    checkAnswer(
      spark
        .sql(s"SELECT * FROM ${fullyQualifiedIdentifier("B").quotedString}"),
      Row(4)
    )
  }

  test("Pipeline datasets can have dependency on streaming table") {
    val unresolvedDataflowGraph = unresolvedDataflowGraphFromSql(
      sqlText = s"""
                   |CREATE STREAMING TABLE a AS SELECT * FROM STREAM($externalTable1Ident);
                   |CREATE MATERIALIZED VIEW b AS SELECT * FROM a;
                   |""".stripMargin
    )

    startPipelineAndWaitForCompletion(unresolvedDataflowGraph)

    checkAnswer(
      spark
        .sql(s"SELECT * FROM ${fullyQualifiedIdentifier("b").quotedString}"),
        Seq(Row(0), Row(1), Row(2))
    )
  }

  test("SQL aggregation works") {
    val unresolvedDataflowGraph = unresolvedDataflowGraphFromSql(
      sqlText =
        """
          |CREATE MATERIALIZED VIEW a AS SELECT id AS value, (id % 2) AS isOdd FROM range(1,10);
          |CREATE MATERIALIZED VIEW b AS SELECT isOdd, max(value) AS
          |maximum FROM a GROUP BY isOdd LIMIT 2;
          |""".stripMargin
    )

    startPipelineAndWaitForCompletion(unresolvedDataflowGraph)

    checkAnswer(
      spark
        .sql(s"SELECT * FROM ${fullyQualifiedIdentifier("b").quotedString}"),
        Seq(Row(0, 8), Row(1, 9))
    )
  }

  test("SQL join works") {
    val unresolvedDataflowGraph = unresolvedDataflowGraphFromSql(
      sqlText = """
                  |CREATE TEMPORARY VIEW a AS SELECT id FROM range(1,3);
                  |CREATE TEMPORARY VIEW b AS SELECT id FROM range(1,3);
                  |CREATE MATERIALIZED VIEW c AS SELECT a.id AS id1, b.id AS id2
                  |FROM a JOIN b ON a.id=b.id
                  |""".stripMargin
    )

    startPipelineAndWaitForCompletion(unresolvedDataflowGraph)

    checkAnswer(
      spark
        .sql(s"SELECT * FROM ${fullyQualifiedIdentifier("c").quotedString}"),
        Seq(Row(1, 1), Row(2, 2))
    )
  }

  test("Partition cols correctly registered") {
    val unresolvedDataflowGraph = unresolvedDataflowGraphFromSql(
      sqlText = """
                  |CREATE MATERIALIZED VIEW a
                  |PARTITIONED BY (id1, id2)
                  |AS SELECT id as id1, id as id2 FROM range(1,2) """.stripMargin
    )
    val resolvedDataflowGraph = unresolvedDataflowGraph.resolve()

    assert(
      resolvedDataflowGraph.tables
        .find(_.identifier == fullyQualifiedIdentifier("a"))
        .head
        .partitionCols
        .contains(Seq("id1", "id2"))
    )
  }

  test("Exception is thrown when non-identity partition columns are used") {
    val graphRegistrationContext = new TestGraphRegistrationContext(spark)
    val sqlGraphRegistrationContext = new SqlGraphRegistrationContext(graphRegistrationContext)

    val ex = intercept[SqlGraphElementRegistrationException] {
      sqlGraphRegistrationContext.processSqlFile(
        sqlText = """
                    |CREATE MATERIALIZED VIEW a
                    |PARTITIONED BY (year(id1))
                    |AS SELECT id as id1, id as id2 FROM range(1,2)""".stripMargin,
        sqlFilePath = "a.sql",
        spark = spark
      )
    }

    assert(ex.getMessage.contains("Invalid partitioning transform"))
  }

  test("Table properties are correctly registered") {
    val unresolvedDataflowGraph = unresolvedDataflowGraphFromSql(
      sqlText = "CREATE STREAMING TABLE st TBLPROPERTIES ('prop1'='foo', 'prop2'='bar') AS SELECT 1"
    )
    val resolvedDataflowGraph = unresolvedDataflowGraph.resolve()
    assert(
      resolvedDataflowGraph.tables
        .find(_.identifier == fullyQualifiedIdentifier("st"))
        .head
        .properties == Map(
        "prop1" -> "foo",
        "prop2" -> "bar"
      )
    )
  }

  test("Spark confs are correctly registered") {
    val unresolvedDataflowGraph = unresolvedDataflowGraphFromSql(
      sqlText = """
                  |CREATE MATERIALIZED VIEW a AS SELECT id FROM range(1,2);
                  |SET conf.test = a;
                  |CREATE VIEW b AS SELECT id from range(1,2);
                  |SET conf.test = b;
                  |SET conf.test2 = c;
                  |CREATE STREAMING TABLE c AS SELECT id FROM range(1,2);
                  |""".stripMargin
    )

    val resolvedDataflowGraph = unresolvedDataflowGraph.resolve()

    assert(
      resolvedDataflowGraph.flows
        .find(_.identifier == fullyQualifiedIdentifier("a"))
        .head
        .sqlConf == Map.empty
    )

    assert(
      resolvedDataflowGraph.flows
        .find(_.identifier == fullyQualifiedIdentifier("b"))
        .head
        .sqlConf == Map(
        "conf.test" -> "a"
      )
    )

    assert(
      resolvedDataflowGraph.flows
        .find(_.identifier == fullyQualifiedIdentifier("c"))
        .head
        .sqlConf == Map(
        "conf.test" -> "b",
        "conf.test2" -> "c"
      )
    )
  }

  test("Setting dataset location is disallowed") {
    val graphRegistrationContext = new TestGraphRegistrationContext(spark)
    val sqlGraphRegistrationContext = new SqlGraphRegistrationContext(graphRegistrationContext)

    val ex = intercept[ParseException] {
      sqlGraphRegistrationContext.processSqlFile(
        sqlText = """CREATE STREAMING TABLE a
                    |LOCATION "/path/to/table"
                    |AS SELECT * FROM range(1,2)""".stripMargin,
        sqlFilePath = "a.sql",
        spark = spark
      )
    }

    assert(ex.getMessage.contains("Specifying location is not supported"))
  }

  test("Tables living in arbitrary schemas can be read from pipeline") {
    val database_name = "db_c215a150_c9c1_4c65_bc02_f7d50dea2f5d"
    val table_name = s"$database_name.tbl"
    spark.sql(s"CREATE DATABASE $database_name")
    spark.sql(s"CREATE TABLE $table_name AS SELECT * FROM range(1,4)")

    withDatabase(database_name) {
      withTable(table_name) {
        val unresolvedDataflowGraph = unresolvedDataflowGraphFromSql(
          sqlText = s"""
                       |CREATE MATERIALIZED VIEW a AS SELECT * FROM $table_name;
                       |CREATE STREAMING TABLE b AS SELECT * FROM STREAM($table_name);
                       |CREATE TEMPORARY VIEW c AS SELECT * FROM a;
                       |""".stripMargin
        )

        startPipelineAndWaitForCompletion(unresolvedDataflowGraph)

        Seq("a", "b").foreach { tableName =>
          checkAnswer(
            spark
              .sql(
                s"SELECT * FROM ${fullyQualifiedIdentifier(tableName).quotedString}"
              ),
            Seq(Row(1), Row(2), Row(3))
          )
        }
      }
    }
  }

  gridTest(s"Pipeline dataset can read from file based data sources")(
    Seq("parquet", "orc", "json", "csv")
  ) { fileFormat =>
    // TODO: streaming file data sources in SQL is not currently supported. If and when it is,
    //  streaming tables should also be able to directly stream from file based data sources. Until
    //  then, users must stream from a regular table that has loaded the file data. A streaming
    //  table reading from a materialized view or temp view is not supported.
    val tmpDir = Utils.createTempDir().getAbsolutePath
    spark.sql("SELECT * FROM RANGE(3)").write.format(fileFormat).mode("overwrite").save(tmpDir)

    val externalTableIdent = fullyQualifiedIdentifier("t")
    spark.sql(s"CREATE TABLE $externalTableIdent AS SELECT * FROM $fileFormat.`$tmpDir`")

    withTable(externalTableIdent.quotedString) {
      val unresolvedDataflowGraph = unresolvedDataflowGraphFromSql(
        sqlText =
          s"""
             |CREATE MATERIALIZED VIEW a AS SELECT * FROM $fileFormat.`$tmpDir`;
             |CREATE STREAMING TABLE b AS SELECT * FROM STREAM $externalTableIdent
             |""".stripMargin
      )

      startPipelineAndWaitForCompletion(unresolvedDataflowGraph)

      Seq("a", "b").foreach { datasetName =>
        val datasetFullyQualifiedName =
          fullyQualifiedIdentifier(datasetName).quotedString
        val expectedRows = if (fileFormat == "csv") {
          // CSV values are read as strings
          Set("0", "1", "2")
        } else {
          Set(0, 1, 2)
        }
        assert(
          spark.sql(s"SELECT * FROM $datasetFullyQualifiedName").collect().toSet ==
            expectedRows.map(Row(_))
        )
      }
    }
  }

  gridTest("Invalid reads produce correct error message")(
    Seq(
      ("csv.``", "The location name cannot be empty string, but `` was given."),
      ("csv.`/non/existing/file`", "Path does not exist: file:/non/existing/file")
    )
  ) {
    case (path, expectedErrorMsg) =>
      val unresolvedDataflowGraph = unresolvedDataflowGraphFromSql(
        sqlText = s"""
                     |CREATE MATERIALIZED VIEW a AS SELECT * FROM $path;
                     |""".stripMargin
      )

      val resolvedDataflowGraph = unresolvedDataflowGraph.resolve()

      assert(
        resolvedDataflowGraph.resolutionFailedFlows
          .find(_.identifier == fullyQualifiedIdentifier("a"))
          .head
          .failure
          .head
          .getMessage
          .contains(expectedErrorMsg)
      )
  }

  test("Pipeline dataset can be referenced in CTE") {
    val unresolvedDataflowGraph = unresolvedDataflowGraphFromSql(
      sqlText = """
                  |CREATE MATERIALIZED VIEW a AS SELECT 1;
                  |CREATE MATERIALIZED VIEW d AS
                  |WITH c AS (
                  | WITH b AS (
                  |   SELECT * FROM a
                  | )
                  | SELECT * FROM b
                  |)
                  |SELECT * FROM c;
                  |""".stripMargin
    )

    startPipelineAndWaitForCompletion(unresolvedDataflowGraph)

    checkAnswer(
      spark
        .sql(s"SELECT * FROM ${fullyQualifiedIdentifier("d").quotedString}"),
        Row(1)
    )
  }

  test("Unsupported SQL statements throws error") {
    val graphRegistrationContext = new TestGraphRegistrationContext(spark)
    val sqlGraphRegistrationContext = new SqlGraphRegistrationContext(graphRegistrationContext)

    val ex = intercept[SqlGraphElementRegistrationException] {
      sqlGraphRegistrationContext.processSqlFile(
        sqlText = "CREATE TABLE t AS SELECT 1",
        sqlFilePath = "a.sql",
        spark = spark
      )
    }

    assert(ex.getMessage.contains("Unsupported plan"))
  }

  test("Table schema is correctly parsed") {
    val unresolvedDataflowGraph = unresolvedDataflowGraphFromSql(
      sqlText = """
                  |CREATE MATERIALIZED VIEW a (id LONG COMMENT 'comment') AS SELECT * FROM RANGE(5);
                  |""".stripMargin
    )

    val resolvedDataflowGraph = unresolvedDataflowGraph.resolve()

    // Let inferred/declared schema mismatch detection execute
    resolvedDataflowGraph.validate()

    val expectedSchema = new StructType().add(name = "id", dataType = LongType, nullable = false)

    assert(
      resolvedDataflowGraph.resolvedFlows
        .find(_.identifier == fullyQualifiedIdentifier("a"))
        .head
        .schema == expectedSchema
    )
  }

  test("Multipart table names supported") {
    val database_name = "db_4159cf91_42c1_44d6_aa8c_9cd8a158230d"
    val database2_name = "db_a90d194f_9dfd_44bf_b473_26727e76be7a"
    spark.sql(s"CREATE DATABASE $database_name")
    spark.sql(s"CREATE DATABASE $database2_name")

    val unresolvedDataflowGraph = unresolvedDataflowGraphFromSql(
      sqlText = s"""
                   |CREATE MATERIALIZED VIEW $database_name.mv1 AS SELECT 1;
                   |CREATE MATERIALIZED VIEW $database2_name.mv2 AS SELECT * FROM $database_name.mv1
                   |""".stripMargin
    )
    startPipelineAndWaitForCompletion(unresolvedDataflowGraph)

    checkAnswer(spark.sql(s"SELECT * FROM $database_name.mv1"), Row(1))
    checkAnswer(spark.sql(s"SELECT * FROM $database2_name.mv2"), Row(1))
  }

  test("Flow cannot be created with multipart identifier") {
    val graphRegistrationContext = new TestGraphRegistrationContext(spark)
    val sqlGraphRegistrationContext = new SqlGraphRegistrationContext(graphRegistrationContext)

    val ex = intercept[AnalysisException] {
      sqlGraphRegistrationContext.processSqlFile(
        sqlText = s"""
                     |CREATE STREAMING TABLE st;
                     |CREATE FLOW some_database.f AS INSERT INTO st BY NAME
                     |SELECT * FROM STREAM $externalTable1Ident;
                     |""".stripMargin,
        sqlFilePath = "a.sql",
        spark = spark
      )
    }

    assert(ex.getMessage.contains("Flow with multipart name 'some_database.f' is not supported"))
  }

  test("Temporary view cannot be created with multipart identifier") {
    val graphRegistrationContext = new TestGraphRegistrationContext(spark)
    val sqlGraphRegistrationContext = new SqlGraphRegistrationContext(graphRegistrationContext)

    val ex = intercept[ParseException] {
      sqlGraphRegistrationContext.processSqlFile(
        sqlText = """
                    |CREATE TEMPORARY VIEW some_database.tv AS SELECT 1;
                    |CREATE MATERIALIZED VIEW mv AS SELECT * FROM some_database.tv;
                    |""".stripMargin,
        sqlFilePath = "a.sql",
        spark = spark
      )
    }

    assert(ex.errorClass.contains("TEMP_VIEW_NAME_TOO_MANY_NAME_PARTS"))
  }

  test("Use database and set catalog works") {
    val pipelineCatalog = TestGraphRegistrationContext.DEFAULT_CATALOG
    val pipelineDatabase = TestGraphRegistrationContext.DEFAULT_DATABASE
    val otherCatalog = "c_bb3e5598_be3c_4250_a3e1_92c2a75bd3ce"
    val otherDatabase = "db_caa1d504_ceb5_40e5_b444_ada891288f07"

    // otherDatabase2 will be created in the pipeline's catalog
    val otherDatabase2 = "db_8b1e9b89_99d8_4f5e_91af_da5b9091143c"

    spark.conf.set(
      key = s"spark.sql.catalog.$otherCatalog",
      value = "org.apache.spark.sql.connector.catalog.InMemoryTableCatalog"
    )
    spark.sql(s"CREATE DATABASE $otherCatalog.$otherDatabase")
    spark.sql(s"CREATE DATABASE $otherDatabase2")

    val unresolvedDataflowGraph = unresolvedDataflowGraphFromSqlFiles(
      sqlFiles = Seq(
        TestSqlFile(
          sqlText =
            s"""
               |-- Create table in default (pipeline) catalog and database
               |     CREATE MATERIALIZED VIEW mv AS SELECT * FROM RANGE(3);
               |
               |-- Change database
               |USE DATABASE $otherDatabase2;
               |
               |-- Create mv2 under new database, implicitly
               |CREATE MATERIALIZED VIEW mv2 AS SELECT * FROM RANGE(1, 5);
               |
               |-- Unqualified names in subquery should implicitly use current database.
               |-- This should work as current database lives under test_db catalog
               |-- Fully qualified names in subquery should use specified database.
               |CREATE MATERIALIZED VIEW mv3 AS
               |WITH mv AS (SELECT * FROM $pipelineCatalog.$pipelineDatabase.mv)
               |SELECT mv2.id FROM
               |mv JOIN mv2 ON mv.id=mv2.id;
               |
               |-- Change to database that lives in another catalog. Same behavior expected.
               |SET CATALOG $otherCatalog;
               |USE DATABASE $otherDatabase;
               |
               |-- Create temporary view. Temporary views should always be created in the pipeline
               |-- catalog and database, regardless of what the active catalog/database are.
               |CREATE TEMPORARY VIEW tv AS SELECT * FROM $pipelineCatalog.$pipelineDatabase.mv;
               |
               |CREATE MATERIALIZED VIEW mv4 AS
               |WITH mv2 AS (SELECT * FROM $pipelineCatalog.$otherDatabase2.mv2)
               |SELECT * FROM STREAM(mv2) WHERE mv2.id % 2 == 0;
               |
               |-- Use namespace command should also work, setting both catalog and database.
               |USE NAMESPACE $pipelineCatalog.$otherDatabase2;
               |-- mv2 was originally created in this same namespace, so implicit qualification
               |-- should work.
               |CREATE MATERIALIZED VIEW mv5 AS SELECT * FROM mv2;
               |
               |-- Temp views, which don't support name qualification, should always resolve to
               |-- pipeline catalog and database despite the active catalog/database
               |CREATE MATERIALIZED VIEW mv6 AS SELECT * FROM tv;
               |""".stripMargin,
          sqlFilePath = "file1.sql"
        ),
        TestSqlFile(
          sqlText =
            s"""
               |-- The previous file's current catalog/database should not impact other files;
               |-- the catalog/database should be reset to the pipeline's.
               |--
               |-- Should also be able to read dataset created in other file with custom catalog
               |-- and database.
               |CREATE MATERIALIZED VIEW mv6 AS SELECT * FROM $pipelineCatalog.$otherDatabase2.mv5;
               |""".stripMargin,
          sqlFilePath = "file2.sql"
        )
      )
    )

    startPipelineAndWaitForCompletion(unresolvedDataflowGraph)

    checkAnswer(
      spark.sql(s"SELECT * FROM $pipelineCatalog.$otherDatabase2.mv3"),
      Seq(Row(1), Row(2))
    )

    checkAnswer(
      spark.sql(s"SELECT * FROM $otherCatalog.$otherDatabase.mv4"),
      Seq(Row(2), Row(4))
    )

    checkAnswer(
      spark.sql(s"SELECT * FROM $otherDatabase2.mv5"),
      Seq(Row(1), Row(2), Row(3), Row(4))
    )

    checkAnswer(
      spark.sql(s"SELECT * FROM $otherDatabase2.mv6"),
      Seq(Row(0), Row(1), Row(2))
    )

    checkAnswer(
      spark.sql(s"SELECT * FROM $pipelineCatalog.$pipelineDatabase.mv6"),
      Seq(Row(1), Row(2), Row(3), Row(4))
    )
  }

  test("Writing/reading datasets from fully and partially qualified names works") {
    spark.catalog.setCurrentCatalog(TestGraphRegistrationContext.DEFAULT_CATALOG)
    spark.catalog.setCurrentDatabase(TestGraphRegistrationContext.DEFAULT_DATABASE)

    val otherCatalog = "c_25fcf574_171a_4058_b17d_2e38622b702b"
    val otherDatabase = "db_3558273c_9843_4eac_8b9c_cf7a7d144371"
    val otherDatabase2 = "db_6ae79691_11f8_43a6_9120_ca7c7dee662a"

    spark.conf.set(
      key = s"spark.sql.catalog.$otherCatalog",
      value = "org.apache.spark.sql.connector.catalog.InMemoryTableCatalog"
    )
    spark.sql(s"CREATE DATABASE $otherCatalog.$otherDatabase")
    spark.sql(s"CREATE DATABASE $otherDatabase2")

    // Note: we are intentionally not testing using streaming tables, as the InMemoryManagedCatalog
    // does not support streaming reads/writes, and checkpoint locations cannot be materialized
    // without catalog-provided hard storage.
    Seq(
      ("upstream_mv", "downstream_mv"),
      ("upstream_mv2", s"$otherDatabase2.downstream_mv2"),
      ("upstream_mv3", s"$otherCatalog.$otherDatabase.downstream_mv3"),
      (s"$otherDatabase2.upstream_mv4", "downstream_mv4"),
      (s"$otherCatalog.$otherDatabase.upstream_mv5", "downstream_mv5"),
      (s"$otherCatalog.$otherDatabase.upstream_mv6", s"$otherDatabase2.downstream_mv6")
    ).foreach { case (table1Ident, table2Ident) =>
      // The pipeline catalog is [[TestGraphRegistrationContext.DEFAULT_CATALOG]] and database is
      // [[TestGraphRegistrationContext.DEFAULT_DATABASE]].
      val unresolvedDataflowGraph = unresolvedDataflowGraphFromSql(
        sqlText =
          s"""
             |CREATE MATERIALIZED VIEW $table1Ident (id BIGINT) AS SELECT * FROM RANGE(10);
             |CREATE MATERIALIZED VIEW $table2Ident AS SELECT id FROM $table1Ident
             |WHERE (id%2)=0;
             |""".stripMargin
      )

      startPipelineAndWaitForCompletion(unresolvedDataflowGraph)

      checkAnswer(
        spark.sql(s"SELECT * FROM $table2Ident"),
        Seq(Row(0), Row(2), Row(4), Row(6), Row(8))
      )

      spark.sql(s"DROP TABLE $table1Ident")
      spark.sql(s"DROP TABLE $table2Ident")
    }
  }

  test("Creating streaming table without subquery works if streaming table is backed by flows") {
    val unresolvedDataflowGraph = unresolvedDataflowGraphFromSql(
      sqlText = s"""
                   |CREATE STREAMING TABLE st;
                   |CREATE FLOW f AS INSERT INTO st BY NAME
                   |SELECT * FROM STREAM $externalTable1Ident;
                   |""".stripMargin
    )

    startPipelineAndWaitForCompletion(unresolvedDataflowGraph)

    checkAnswer(
      spark.sql(s"SELECT * FROM ${fullyQualifiedIdentifier("st")}"),
      Seq(Row(0), Row(1), Row(2))
    )
  }

  test("Empty streaming table definition is disallowed") {
    val unresolvedDataflowGraph = unresolvedDataflowGraphFromSql(
      sqlText = "CREATE STREAMING TABLE st;"
    )

    checkError(
      exception = intercept[AnalysisException] {
        unresolvedDataflowGraph
          .resolve()
          .validate()
      },
      condition = "PIPELINE_DATASET_WITHOUT_FLOW",
      sqlState = Option("0A000"),
      parameters = Map("identifier" -> fullyQualifiedIdentifier("st").quotedString)
    )
  }

  test("Flow identifiers must be single part") {
    Seq("a.b", "a.b.c").foreach { flowIdentifier =>
      val ex = intercept[AnalysisException] {
        unresolvedDataflowGraphFromSql(
          sqlText =
            s"""
               |CREATE STREAMING TABLE st;
               |CREATE FLOW $flowIdentifier AS INSERT INTO st BY NAME
               |SELECT * FROM STREAM $externalTable1Ident
               |""".stripMargin
        )
      }
      checkError(
        exception = ex,
        condition = "MULTIPART_FLOW_NAME_NOT_SUPPORTED",
        parameters = Map("flowName" -> flowIdentifier)
      )
    }
  }

  test("Duplicate standalone flow identifiers throw an exception") {
    val ex = intercept[AnalysisException] {
      // even if flows are defined across multiple files, if there's a duplicate flow identifier an
      // exception should be thrown.
      unresolvedDataflowGraphFromSqlFiles(
        sqlFiles = Seq(
          TestSqlFile(
            sqlText =
              s"""
                 |CREATE STREAMING TABLE st;
                 |CREATE FLOW f AS INSERT INTO st BY NAME
                 |SELECT * FROM STREAM $externalTable1Ident
                 |""".stripMargin,
            sqlFilePath = "file1.sql"
          ),
          TestSqlFile(
            sqlText =
              s"""
                 |CREATE FLOW f AS INSERT INTO st BY NAME
                 |SELECT * FROM STREAM $externalTable1Ident
                 |""".stripMargin,
            sqlFilePath = "file2.sql"
          )
        )
      )
    }
    checkError(
      exception = ex,
      condition = "PIPELINE_DUPLICATE_IDENTIFIERS.FLOW",
      parameters = Map(
        "flowName" -> fullyQualifiedIdentifier("f").unquotedString,
        "datasetNames" -> fullyQualifiedIdentifier("st").quotedString
      )
    )
  }

  test("Duplicate standalone implicit flow identifier throws exception") {
    val ex = intercept[AnalysisException] {
      // even if flows are defined across multiple files, if there's a duplicate flow identifier an
      // exception should be thrown.
      unresolvedDataflowGraphFromSqlFiles(
        sqlFiles = Seq(
          TestSqlFile(
            sqlText =
              s"""
                 |CREATE STREAMING TABLE st AS SELECT * FROM STREAM $externalTable1Ident;
                 |CREATE STREAMING TABLE st2;
                 |""".stripMargin,
            sqlFilePath = "file1.sql"
          ),
          TestSqlFile(
            sqlText =
              s"""
                 |CREATE FLOW st AS INSERT INTO st2 BY NAME
                 |SELECT * FROM STREAM $externalTable2Ident
                 |""".stripMargin,
            sqlFilePath = "file2.sql"
          )
        )
      )
    }
    checkError(
      exception = ex,
      condition = "PIPELINE_DUPLICATE_IDENTIFIERS.FLOW",
      parameters = Map(
        "flowName" -> fullyQualifiedIdentifier("st").unquotedString,
        "datasetNames" -> Seq(fullyQualifiedIdentifier("st").quotedString,
          fullyQualifiedIdentifier("st2").quotedString).mkString(",")
      )
    )
  }

  test("No table defined pipeline fails with RUN_EMPTY_PIPELINE") {
    val graphRegistrationContext = new TestGraphRegistrationContext(spark)
    val sqlGraphRegistrationContext = new SqlGraphRegistrationContext(graphRegistrationContext)

    sqlGraphRegistrationContext.processSqlFile(sqlText = "", sqlFilePath = "a.sql", spark = spark)

    checkError(
      exception = intercept[AnalysisException] {
        graphRegistrationContext.toDataflowGraph
      },
      condition = "RUN_EMPTY_PIPELINE",
      sqlState = Option("42617"),
      parameters = Map.empty
    )
  }

  test("Pipeline with only temp views fails with RUN_EMPTY_PIPELINE") {
    val graphRegistrationContext = new TestGraphRegistrationContext(spark)
    val sqlGraphRegistrationContext = new SqlGraphRegistrationContext(graphRegistrationContext)

    sqlGraphRegistrationContext.processSqlFile(
      sqlText = s"""
                   |CREATE TEMPORARY VIEW a AS SELECT id FROM range(1,3);
                   |""".stripMargin,
      sqlFilePath = "a.sql",
      spark = spark
    )

    checkError(
      exception = intercept[AnalysisException] {
        graphRegistrationContext.toDataflowGraph
      },
      condition = "RUN_EMPTY_PIPELINE",
      sqlState = Option("42617"),
      parameters = Map.empty
    )
  }

  test("Pipeline with only flow fails with RUN_EMPTY_PIPELINE") {
    val graphRegistrationContext = new TestGraphRegistrationContext(spark)
    val sqlGraphRegistrationContext = new SqlGraphRegistrationContext(graphRegistrationContext)

    sqlGraphRegistrationContext.processSqlFile(
      sqlText = s"""
                   |CREATE FLOW f AS INSERT INTO a BY NAME
                   |SELECT 1;
                   |""".stripMargin,
      sqlFilePath = "a.sql",
      spark = spark
    )

    checkError(
      exception = intercept[AnalysisException] {
        graphRegistrationContext.toDataflowGraph
      },
      condition = "RUN_EMPTY_PIPELINE",
      sqlState = Option("42617"),
      parameters = Map.empty
    )
  }
}
