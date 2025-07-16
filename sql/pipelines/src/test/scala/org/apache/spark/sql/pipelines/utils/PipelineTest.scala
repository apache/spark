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

import java.io.{BufferedReader, FileNotFoundException, InputStreamReader}
import java.nio.file.Files

import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Try}
import scala.util.control.NonFatal

import org.scalactic.source
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Tag}
import org.scalatest.concurrent.Eventually
import org.scalatest.matchers.should.Matchers

import org.apache.spark.SparkFunSuite
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Column, QueryTest, Row, SQLContext, TypedColumn}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.classic.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.pipelines.graph.{DataflowGraph, PipelineUpdateContextImpl, SqlGraphRegistrationContext}
import org.apache.spark.sql.pipelines.utils.PipelineTest.{cleanupMetastore, createTempDir}

abstract class PipelineTest
    extends SparkFunSuite
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with Matchers
    with SparkErrorTestMixin
    with TargetCatalogAndDatabaseMixin
    with Logging
    with Eventually {

  final protected val storageRoot = createTempDir()

  protected def spark: SparkSession

  protected implicit def sqlContext: SQLContext

  def sql(text: String): DataFrame = spark.sql(text)

  protected def startPipelineAndWaitForCompletion(unresolvedDataflowGraph: DataflowGraph): Unit = {
    val updateContext = new PipelineUpdateContextImpl(
      unresolvedDataflowGraph, eventCallback = _ => ())
    updateContext.pipelineExecution.runPipeline()
    updateContext.pipelineExecution.awaitCompletion()
  }

  /** Returns the dataset name in the event log. */
  protected def eventLogName(
      name: String,
      catalog: Option[String] = catalogInPipelineSpec,
      database: Option[String] = databaseInPipelineSpec,
      isView: Boolean = false
  ): String = {
    fullyQualifiedIdentifier(name, catalog, database, isView).unquotedString
  }

  /** Returns the fully qualified identifier. */
  protected def fullyQualifiedIdentifier(
      name: String,
      catalog: Option[String] = catalogInPipelineSpec,
      database: Option[String] = databaseInPipelineSpec,
      isTemporaryView: Boolean = false
  ): TableIdentifier = {
    if (isTemporaryView) {
      TableIdentifier(name)
    } else {
      TableIdentifier(
        catalog = catalog,
        database = database,
        table = name
      )
    }
  }

  /** Helper class to represent a SQL file by its contents and path. */
  protected case class TestSqlFile(sqlText: String, sqlFilePath: String)

  /** Construct an unresolved DataflowGraph object from possibly multiple SQL files. */
  protected def unresolvedDataflowGraphFromSqlFiles(
      sqlFiles: Seq[TestSqlFile]
  ): DataflowGraph = {
    val graphRegistrationContext = new TestGraphRegistrationContext(spark)
    sqlFiles.foreach { sqlFile =>
      new SqlGraphRegistrationContext(graphRegistrationContext).processSqlFile(
        sqlText = sqlFile.sqlText,
        sqlFilePath = sqlFile.sqlFilePath,
        spark = spark
      )
    }
    graphRegistrationContext
      .toDataflowGraph
  }

  /** Construct an unresolved DataflowGraph object from a single SQL file, given the file contents
   * and path. */
  protected def unresolvedDataflowGraphFromSql(
      sqlText: String,
      sqlFilePath: String = "dataset.sql"
  ): DataflowGraph = {
    unresolvedDataflowGraphFromSqlFiles(
      Seq(TestSqlFile(sqlText = sqlText, sqlFilePath = sqlFilePath))
    )
  }

  protected override def beforeEach(): Unit = {
    super.beforeEach()
    cleanupMetastore(spark)
    (catalogInPipelineSpec, databaseInPipelineSpec) match {
      case (Some(catalog), Some(schema)) =>
        spark.sql(s"CREATE DATABASE IF NOT EXISTS `$catalog`.`$schema`")
      case _ =>
        databaseInPipelineSpec.foreach(s => spark.sql(s"CREATE DATABASE IF NOT EXISTS `$s`"))
    }
  }

  protected override def afterEach(): Unit = {
    cleanupMetastore(spark)
    super.afterEach()
  }

  override protected def gridTest[A](testNamePrefix: String, testTags: Tag*)(params: Seq[A])(
      testFun: A => Unit): Unit = {
    namedGridTest(testNamePrefix, testTags: _*)(params.map(a => a.toString -> a).toMap)(testFun)
  }

  override protected def test(testName: String, testTags: Tag*)(testFun: => Any /* Assertion */ )(
      implicit pos: source.Position): Unit = super.test(testName, testTags: _*) {
    runWithInstrumentation(testFun)
  }

  /**
   * Adds custom instrumentation for tests.
   *
   * This instrumentation runs after `beforeEach` and
   * before `afterEach` which lets us instrument the state of a test and its environment
   * after any setup and before any clean-up done for a test.
   */
  private def runWithInstrumentation(testFunc: => Any): Any = {
    testFunc
  }

  /**
   * Creates individual tests for all items in `params`.
   *
   * The full test name will be "<testNamePrefix> (<paramName> = <param>)" where <param> is one
   * item in `params`.
   *
   * @param testNamePrefix The test name prefix.
   * @param paramName A descriptive name for the parameter.
   * @param testTags Extra tags for the test.
   * @param params The list of parameters for which to generate tests.
   * @param testFun The actual test function. This function will be called with one argument of
   *                type `A`.
   * @tparam A The type of the params.
   */
  protected def gridTest[A](testNamePrefix: String, paramName: String, testTags: Tag*)(
      params: Seq[A])(testFun: A => Unit): Unit =
    namedGridTest(testNamePrefix, testTags: _*)(
      params.map(a => s"$paramName = $a" -> a).toMap
    )(testFun)

  /**
   * Specialized version of gridTest where the params are two boolean values - `true` and
   * `false`.
   */
  protected def booleanGridTest(testNamePrefix: String, paramName: String, testTags: Tag*)(
      testFun: Boolean => Unit): Unit = {
    gridTest(testNamePrefix, paramName, testTags: _*)(Seq(true, false))(testFun)
  }

  protected def namedGridTest[A](testNamePrefix: String, testTags: Tag*)(params: Map[String, A])(
      testFun: A => Unit): Unit = {
    for (param <- params) {
      test(testNamePrefix + s" (${param._1})", testTags: _*)(testFun(param._2))
    }
  }

  protected def namedGridIgnore[A](testNamePrefix: String, testTags: Tag*)(params: Map[String, A])(
      testFun: A => Unit): Unit = {
    for (param <- params) {
      ignore(testNamePrefix + s" (${param._1})", testTags: _*)(testFun(param._2))
    }
  }

  /** Loads a package resources as a Seq of lines. */
  protected def loadResource(path: String): Seq[String] = {
    val stream = Thread.currentThread.getContextClassLoader.getResourceAsStream(path)
    if (stream == null) {
      throw new FileNotFoundException(path)
    }
    val reader = new BufferedReader(new InputStreamReader(stream))
    val data = new ArrayBuffer[String]
    var line = reader.readLine()
    while (line != null) {
      data.append(line)
      line = reader.readLine()
    }
    data.toSeq
  }

  private def checkAnswerAndPlan(
      df: => DataFrame,
      expectedAnswer: Seq[Row],
      checkPlan: Option[SparkPlan => Unit]): Unit = {
    QueryTest.checkAnswer(df, expectedAnswer)

    // To help with test development, you can dump the plan to the log by passing
    // `--test_env=DUMP_PLAN=true` to `bazel test`.
    if (Option(System.getenv("DUMP_PLAN")).exists(s => java.lang.Boolean.valueOf(s))) {
      log.info(s"Spark plan:\n${df.queryExecution.executedPlan}")
    }
    checkPlan.foreach(_.apply(df.queryExecution.executedPlan))
  }

  /**
   * Runs the plan and makes sure the answer matches the expected result.
   *
   * @param df the `DataFrame` to be executed
   * @param expectedAnswer the expected result in a `Seq` of `Row`s.
   */
  protected def checkAnswer(df: => DataFrame, expectedAnswer: Seq[Row]): Unit = {
    checkAnswerAndPlan(df, expectedAnswer, None)
  }

  protected def checkAnswer(df: => DataFrame, expectedAnswer: Row): Unit = {
    checkAnswer(df, Seq(expectedAnswer))
  }

  case class ValidationArgs(
      ignoreFieldOrder: Boolean = false,
      ignoreFieldCase: Boolean = false
  )

  /**
   * Evaluates a dataset to make sure that the result of calling collect matches the given
   * expected answer.
   */
  protected def checkDataset[T](ds: => Dataset[T], expectedAnswer: T*): Unit = {
    val result = getResult(ds)

    if (!QueryTest.compare(result.toSeq, expectedAnswer)) {
      fail(s"""
              |Decoded objects do not match expected objects:
              |expected: $expectedAnswer
              |actual:   ${result.toSeq}
         """.stripMargin)
    }
  }

  /**
   * Evaluates a dataset to make sure that the result of calling collect matches the given
   * expected answer, after sort.
   */
  protected def checkDatasetUnorderly[T: Ordering](result: Array[T], expectedAnswer: T*): Unit = {
    if (!QueryTest.compare(result.toSeq.sorted, expectedAnswer.sorted)) {
      fail(s"""
              |Decoded objects do not match expected objects:
              |expected: $expectedAnswer
              |actual:   ${result.toSeq}
         """.stripMargin)
    }
  }

  protected def checkDatasetUnorderly[T: Ordering](ds: => Dataset[T], expectedAnswer: T*): Unit = {
    val result = getResult(ds)
    if (!QueryTest.compare(result.toSeq.sorted, expectedAnswer.sorted)) {
      fail(s"""
              |Decoded objects do not match expected objects:
              |expected: $expectedAnswer
              |actual:   ${result.toSeq}
         """.stripMargin)
    }
  }

  private def getResult[T](ds: => Dataset[T]): Array[T] = {
    ds

    try ds.collect()
    catch {
      case NonFatal(e) =>
        fail(
          s"""
             |Exception collecting dataset as objects
             |${ds.queryExecution}
           """.stripMargin,
          e
        )
    }
  }

  /** Holds a parsed version along with the original json of a test. */
  private case class TestSequence(json: Seq[String], rows: Seq[Row]) {
    require(json.size == rows.size)
  }

  /**
   * Helper method to verify unresolved column error message. We expect three elements to be present
   * in the message: error class, unresolved column name, list of suggested columns.
   */
  protected def verifyUnresolveColumnError(
      errorMessage: String,
      unresolved: String,
      suggested: Seq[String]): Unit = {
    assert(errorMessage.contains(unresolved))
    assert(
      errorMessage.contains("[UNRESOLVED_COLUMN.WITH_SUGGESTION]") ||
      errorMessage.contains("[MISSING_COLUMN]")
    )
    suggested.foreach { x =>
      if (errorMessage.contains("[UNRESOLVED_COLUMN.WITH_SUGGESTION]")) {
        assert(errorMessage.contains(s"`$x`"))
      } else {
        assert(errorMessage.contains(x))
      }
    }
  }

  /** Evaluates the given column and returns the result. */
  def eval(column: Column): Any = {
    spark.range(1).select(column).collect().head.get(0)
  }

  /** Evaluates a column as part of a query and returns the result */
  def eval[T](col: TypedColumn[Any, T]): T = {
    spark.range(1).select(col).head()
  }
}

/**
 * A trait that provides a way to specify the target catalog and schema for a test.
 */
trait TargetCatalogAndDatabaseMixin {

  protected def catalogInPipelineSpec: Option[String] = Option(
    TestGraphRegistrationContext.DEFAULT_CATALOG
  )

  protected def databaseInPipelineSpec: Option[String] = Option(
    TestGraphRegistrationContext.DEFAULT_DATABASE
  )
}

object PipelineTest extends Logging {

  /** System schemas per-catalog that's can't be directly deleted. */
  protected val systemDatabases: Set[String] = Set("default", "information_schema")

  /** System catalogs that are read-only and cannot be modified/dropped. */
  private val systemCatalogs: Set[String] = Set("samples")

  /** Catalogs that cannot be dropped but schemas or tables under it can be cleaned up. */
  private val undroppableCatalogs: Set[String] = Set(
    "hive_metastore",
    "spark_catalog",
    "system",
    "main"
  )

  /** Creates a temporary directory. */
  protected def createTempDir(): String = {
    Files.createTempDirectory(getClass.getSimpleName).normalize.toString
  }

  /**
   * Try to drop the schema in the catalog and return whether it is successfully dropped.
   */
  private def dropDatabaseIfPossible(
      spark: SparkSession,
      catalogName: String,
      databaseName: String): Boolean = {
    try {
      spark.sql(s"DROP DATABASE IF EXISTS `$catalogName`.`$databaseName` CASCADE")
      true
    } catch {
      case NonFatal(e) =>
        logInfo(
          s"Failed to drop database $databaseName in catalog $catalogName, ex:${e.getMessage}"
        )
        false
    }
  }

  /** Cleanup resources created in the metastore by tests. */
  def cleanupMetastore(spark: SparkSession): Unit = synchronized {
    // some tests stop the spark session and managed the cleanup by themself, so no need to
    // cleanup if no active spark session found
    if (spark.sparkContext.isStopped) {
      return
    }
    val catalogs =
      spark.sql(s"SHOW CATALOGS").collect().map(_.getString(0)).filterNot(systemCatalogs.contains)
    catalogs.foreach { catalog =>
      if (undroppableCatalogs.contains(catalog)) {
        val schemas =
          spark.sql(s"SHOW SCHEMAS IN `$catalog`").collect().map(_.getString(0))
        schemas.foreach { schema =>
          if (systemDatabases.contains(schema) || !dropDatabaseIfPossible(spark, catalog, schema)) {
            spark
              .sql(s"SHOW tables in `$catalog`.`$schema`")
              .collect()
              .map(_.getString(0))
              .foreach { table =>
                Try(spark.sql(s"DROP table IF EXISTS `$catalog`.`$schema`.`$table`")) match {
                  case Failure(e) =>
                    logInfo(
                      s"Failed to drop table $table in schema $schema in catalog $catalog, " +
                      s"ex:${e.getMessage}"
                    )
                  case _ =>
                }
              }
          }
        }
      } else {
        Try(spark.sql(s"DROP CATALOG IF EXISTS `$catalog` CASCADE")) match {
          case Failure(e) =>
            logInfo(s"Failed to drop catalog $catalog, ex:${e.getMessage}")
          case _ =>
        }
      }
    }
    spark.sessionState.catalog.reset()
  }
}
