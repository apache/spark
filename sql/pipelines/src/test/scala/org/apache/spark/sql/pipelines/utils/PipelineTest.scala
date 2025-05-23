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

import java.io.{BufferedReader, File, FileNotFoundException, InputStreamReader}
import java.nio.file.{Files, Paths}

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Try}
import scala.util.control.NonFatal

import org.scalactic.source
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Tag}
import org.scalatest.exceptions.TestFailedDueToTimeoutException
import org.scalatest.matchers.should.Matchers

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Column, QueryTest, Row, TypedColumn}
import org.apache.spark.sql.SparkSession.{clearActiveSession, setActiveSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.classic.{DataFrame, Dataset, SparkSession, SQLContext}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.pipelines.utils.PipelineTest.{cleanupMetastore, createTempDir}

abstract class PipelineTest
    extends SparkFunSuite
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with Matchers
//    with SQLImplicits
    with SparkErrorTestMixin
    with TargetCatalogAndSchemaMixin
    with Logging {

  final protected val storageRoot = createTempDir()

  var spark: SparkSession = createAndInitializeSpark()
  val originalSpark: SparkSession = spark.cloneSession()

  implicit def sqlContext: SQLContext = spark.sqlContext
  def sql(text: String): DataFrame = spark.sql(text)

  /**
   * Spark confs for [[originalSpark]]. Spark confs set here will be the default spark confs for
   * all spark sessions created in tests.
   */
  protected def sparkConf: SparkConf = {
    var conf = new SparkConf()
      .set("spark.sql.shuffle.partitions", "2")
      .set("spark.sql.session.timeZone", "UTC")

    if (schemaInPipelineSpec.isDefined) {
      conf = conf.set("pipelines.schema", schemaInPipelineSpec.get)
    }

    if (Option(System.getenv("ENABLE_SPARK_UI")).exists(s => java.lang.Boolean.valueOf(s))) {
      conf = conf.set("spark.ui.enabled", "true")
    }
    conf
  }

  /** Returns the dataset name in the event log. */
  protected def eventLogName(
      name: String,
      catalog: Option[String] = catalogInPipelineSpec,
      schema: Option[String] = schemaInPipelineSpec,
      isView: Boolean = false
  ): String = {
    fullyQualifiedIdentifier(name, catalog, schema, isView).unquotedString
  }

  /** Returns the fully qualified identifier. */
  protected def fullyQualifiedIdentifier(
      name: String,
      catalog: Option[String] = catalogInPipelineSpec,
      schema: Option[String] = schemaInPipelineSpec,
      isView: Boolean = false
  ): TableIdentifier = {
    if (isView) {
      TableIdentifier(name)
    } else {
      TableIdentifier(
        catalog = catalog,
        database = schema,
        table = name
      )
    }
  }

//  /** Returns the [[PipelineApiConf]] constructed from the current spark session */
//  def pipelineApiConf: PipelineApiConf = PipelineApiConf.instance

  /**
   * Runs the given function with the given spark conf, and resets the conf after the function
   * completes.
   */
  def withSparkConfs[T](confs: Map[String, String])(f: => T): T = {
    val originalConfs = confs.keys.map(k => k -> spark.conf.getOption(k)).toMap
    confs.foreach { case (k, v) => spark.conf.set(k, v) }
    try f
    finally originalConfs.foreach {
      case (k, v) =>
        v match {
          case Some(v) => spark.conf.set(k, v)
          case None => spark.conf.unset(k)
        }
    }
  }

  /**
   * This exists temporarily for compatibility with tests that become invalid when multiple
   * executors are available.
   */
  protected def master = "local[*]"

  /** Creates and returns a initialized spark session. */
  def createAndInitializeSpark(): SparkSession = {
    val newSparkSession = SparkSession
      .builder()
      .config(sparkConf)
      .master(master)
      .getOrCreate()
    newSparkSession
  }

  /** Set up the spark session before each test. */
  protected def initializeSparkBeforeEachTest(): Unit = {
    clearActiveSession()
    spark = originalSpark.newSession()
    setActiveSession(spark)
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    initializeSparkBeforeEachTest()
    cleanupMetastore(spark)
    (catalogInPipelineSpec, schemaInPipelineSpec) match {
      case (Some(catalog), Some(schema)) =>
        sql(s"CREATE SCHEMA IF NOT EXISTS `$catalog`.`$schema`")
      case _ =>
        schemaInPipelineSpec.foreach(s => sql(s"CREATE SCHEMA IF NOT EXISTS `$s`"))
    }
  }

  override def afterEach(): Unit = {
    cleanupMetastore(spark)
    super.afterEach()
  }

  override def afterAll(): Unit = {
    spark.stop()
  }

  protected def gridTest[A](testNamePrefix: String, testTags: Tag*)(params: Seq[A])(
      testFun: A => Unit): Unit = {
    namedGridTest(testNamePrefix, testTags: _*)(params.map(a => a.toString -> a).toMap)(testFun)
  }

  override def test(testName: String, testTags: Tag*)(testFun: => Any /* Assertion */ )(
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
    try {
      testFunc
    } catch {
      case e: TestFailedDueToTimeoutException =>
//        val stackTraces = StackTraceReporter.dumpAllStackTracesToString()
//        logInfo(
//          s"""
//             |Triggering thread dump since test failed with a timeout exception:
//             |$stackTraces
//             |""".stripMargin
//        )
        throw e
    }
  }

  /**
   * Creates individual tests for all items in [[params]].
   *
   * The full test name will be "<testNamePrefix> (<paramName> = <param>)" where <param> is one
   * item in [[params]].
   *
   * @param testNamePrefix The test name prefix.
   * @param paramName A descriptive name for the parameter.
   * @param testTags Extra tags for the test.
   * @param params The list of parameters for which to generate tests.
   * @param testFun The actual test function. This function will be called with one argument of
   *                type [[A]].
   * @tparam A The type of the params.
   */
  protected def gridTest[A](testNamePrefix: String, paramName: String, testTags: Tag*)(
      params: Seq[A])(testFun: A => Unit): Unit =
    namedGridTest(testNamePrefix, testTags: _*)(
      params.map(a => s"$paramName = $a" -> a).toMap
    )(testFun)

  /**
   * Specialized version of gridTest where the params are two boolean values - [[true]] and
   * [[false]].
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

  /**
   * Returns a [[Seq]] of JARs generated by compiling this test.
   *
   * Includes a "delta-pipelines-repo" to ensure the export_test, which compiles differently,
   * still succeeds.
   */
  protected def getTestJars: Seq[String] =
    getUniqueAbsoluteTestJarPaths.map(_.getName) :+ "delta-pipelines-repo"

  /**
   * Returns a [[Seq]] of absolute paths of all JAR files found in the
   * current directory. See [[getUniqueAbsoluteTestJarPaths]].
   */
  protected def getTestJarPaths: Seq[String] =
    getUniqueAbsoluteTestJarPaths.map(_.getAbsolutePath)

  /**
   * Returns a sequence of JARs found in the current directory. In a bazel test,
   * the current directory includes all jars that are required to run the test
   * (its run files). This allows us to include these jars in the class path
   * for the graph loading class loader.
   *
   * Because dependent jars can be included multiple times in this list, we deduplicate
   * by file name (ignoring the path).
   */
  private def getUniqueAbsoluteTestJarPaths: Seq[File] =
    Files
      .walk(Paths.get("."))
      .iterator()
      .asScala
      .map(_.toFile)
      .filter(
        f =>
          f.isFile &&
          // This filters JARs to match 2 main cases:
          // - JARs built by Bazel that are usually suffixed with deploy.jar;
          // - classpath.jar that Scala test template can also create if the classpath is too long.
          f.getName.matches("classpath.jar|.*deploy.jar")
      )
      .toSeq
      .groupBy(_.getName)
      .flatMap(_._2.headOption)
      .toSeq

//  /**
//   * Returns a [[DataFrame]] given the path to json encoded data stored in the project's
//   * test resources. Schema is parsed from first line.
//   */
//  protected def jsonData(path: String): DataFrame = {
//    val contents = loadResource(path)
//    val data = contents.tail
//    val schema = contents.head
//    jsonData(schema, data)
//  }
//
//  /** Returns a [[DataFrame]] given the string representation of it schema and data. */
//  protected def jsonData(schemaString: String, data: Seq[String]): DataFrame = {
//    val schema = DataType.fromJson(schemaString).asInstanceOf[StructType]
//    spark.read.schema(schema).json(data.toDS())
//  }

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
   * @param df the [[DataFrame]] to be executed
   * @param expectedAnswer the expected result in a [[Seq]] of [[Row]]s.
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

//  /**
//   * Runs the query and makes sure the answer matches the expected result.
//   *
//   * @param validateSchema Whether or not the exact schema fields are validated. This validates
//   *                       the schema types and field names, but does not validate field
//   *                       nullability.
//   */
//  protected def checkAnswer(
//      df: => DataFrame,
//      expectedAnswer: DataFrame,
//      validateSchema: Boolean = false,
//      validationArgs: ValidationArgs = ValidationArgs(),
//      checkPlan: Option[SparkPlan => Unit] = None
//  ): Unit = {
//    // Evaluate `df` so we get a constant DF.
//    val dfByVal = df
//    val actualSchema = dfByVal.schema
//    val expectedSchema = expectedAnswer.schema
//
//    def transformSchema(original: StructType): StructType = {
//      var result = original
//      if (validationArgs.ignoreFieldOrder) {
//        result = StructType(result.fields.sortBy(_.name))
//      }
//      if (validationArgs.ignoreFieldCase) {
//        result = StructType(result.fields.map { field =>
//          field.copy(name = field.name.toLowerCase(Locale.ROOT))
//        })
//      }
//      result
//    }
//
//    def transformDataFrame(original: DataFrame): DataFrame = {
//      var result = original
//      if (validationArgs.ignoreFieldOrder) {
//        result = result.select(
//          result.columns.sorted.map { columnName =>
//            result.col(UnresolvedAttribute.quoted(columnName).name)
//          }: _*
//        )
//      }
//      result
//    }
//
//    if (validateSchema) {
//      assert(
//        transformSchema(actualSchema.asNullable) == transformSchema(expectedSchema.asNullable),
//        s"Expected and actual schemas are different:\n" +
//        s"Expected: $expectedSchema\n" +
//        s"Actual: $actualSchema"
//      )
//    }
//    checkAnswerAndPlan(
//      transformDataFrame(dfByVal),
//      transformDataFrame(expectedAnswer).collect().toIndexedSeq,
//      checkPlan
//    )
//  }

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
   * in the message: error class, unresolved column name, list of suggested columns. There are three
   * significant differences between different versions of DBR:
   * - Error class changed in DBR 11.3 from `MISSING_COLUMN` to `UNRESOLVED_COLUMN.WITH_SUGGESTION`
   * - Name parts in suggested columns are escaped with backticks starting from DBR 11.3,
   *   e.g. table.column => `table`.`column`
   * - Starting from DBR 13.1 suggested columns qualification matches unresolved column, i.e. if
   *   unresolved column is a single-part identifier then suggested column will be as well. E.g.
   *   for unresolved column `x` suggested columns will omit catalog/schema or `LIVE` qualifier. For
   *   this reason we verify only last part of suggested column name.
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

//  /**
//   * Helper class to create a SQLPipeline that is eligible to resolve flows parallely.
//   */
//  class SQLPipelineWithParallelResolve(
//      queries: Seq[String],
//      notebookPath: Option[String] = None,
//      catalog: Option[String] = catalogInPipelineSpec,
//      schema: Option[String] = schemaInPipelineSpec
//  ) extends SQLPipeline(queries, notebookPath, catalog, schema) {
//    override def eligibleForResolvingFlowsParallely = true
//  }

//  /**
//   * Helper method to create a [[SQLPipelineWithParallelResolve]] with catalog and schema set to
//   * the test's catalog and schema.
//   */
//  protected def createSqlParallelPipeline(
//      queries: Seq[String],
//      notebookPath: Option[String] = None,
//      catalog: Option[String] = catalogInPipelineSpec,
//      schema: Option[String] = schemaInPipelineSpec
//  ): SQLPipelineWithParallelResolve = {
//    new SQLPipelineWithParallelResolve(
//      queries = queries,
//      notebookPath = notebookPath,
//      catalog = catalog,
//      schema = schema
//    )
//  }
}

/**
 * A trait that provides a way to specify the target catalog and schema for a test.
 */
trait TargetCatalogAndSchemaMixin {

  protected def catalogInPipelineSpec: Option[String] = Option(
    TestGraphRegistrationContext.DEFAULT_CATALOG
  )

  protected def schemaInPipelineSpec: Option[String] = Option(
    TestGraphRegistrationContext.DEFAULT_DATABASE
  )
}

object PipelineTest extends Logging {
  /** System schemas per-catalog that's can't be directly deleted. */
  protected val systemSchemas: Set[String] = Set("default", "information_schema")

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
  private def dropSchemaIfPossible(
      spark: SparkSession,
      catalogName: String,
      schemaName: String): Boolean = {
    try {
      spark.sql(s"DROP SCHEMA IF EXISTS `$catalogName`.`$schemaName` CASCADE")
      true
    } catch {
      case NonFatal(e) =>
        logInfo(
          s"Failed to drop schema $schemaName in catalog $catalogName, ex:${e.getMessage}"
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
          if (systemSchemas.contains(schema) || !dropSchemaIfPossible(spark, catalog, schema)) {
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
