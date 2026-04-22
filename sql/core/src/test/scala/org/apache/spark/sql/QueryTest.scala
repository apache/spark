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

package org.apache.spark.sql

import java.io.File
import java.net.URI
import java.nio.file.Files
import java.util.{Locale, TimeZone, UUID}
import java.util.regex.Pattern

import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.language.implicitConversions
import scala.util.control.NonFatal

import org.apache.hadoop.fs.Path
import org.scalactic.source.Position
import org.scalatest.{Assertions, BeforeAndAfterAll, Suite, Tag}
import org.scalatest.concurrent.Eventually

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.ExtendedAnalysisException
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.catalyst.catalog.SessionCatalog.DEFAULT_DATABASE
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.classic.ClassicConversions._
import org.apache.spark.sql.execution.{FilterExec, QueryExecution, SQLExecution}
import org.apache.spark.sql.execution.adaptive.DisableAdaptiveExecution
import org.apache.spark.sql.execution.columnar.InMemoryRelation
import org.apache.spark.sql.execution.datasources.DataSourceUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SQLTestData
import org.apache.spark.sql.util.QueryExecutionListener
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.ArrayImplicits._
import org.apache.spark.util.UninterruptibleThread
import org.apache.spark.util.Utils


trait QueryTestBase
  extends Eventually
  with BeforeAndAfterAll
  with SQLTestData
  with PlanTestBase { self: Suite =>

  /**
   * Runs the plan and makes sure the answer contains all of the keywords.
   */
  def checkKeywordsExist(df: DataFrame, keywords: String*): Unit = {
    val outputs = df.collect().map(_.mkString).mkString
    for (key <- keywords) {
      assert(outputs.contains(key), s"Failed for $df ($key doesn't exist in result)")
    }
  }

  /**
   * Runs the plan and makes sure the answer does NOT contain any of the keywords.
   */
  def checkKeywordsNotExist(df: DataFrame, keywords: String*): Unit = {
    val outputs = df.collect().map(_.mkString).mkString
    for (key <- keywords) {
      assert(!outputs.contains(key), s"Failed for $df ($key existed in the result)")
    }
  }

  /**
   * Evaluates a dataset to make sure that the result of calling collect matches the given
   * expected answer.
   */
  protected def checkDataset[T](
      ds: => Dataset[T],
      expectedAnswer: T*): Unit = {
    val result = getResult(ds)

    if (!QueryTest.compare(result.toSeq, expectedAnswer)) {
      fail(
        s"""
           |Decoded objects do not match expected objects:
           |expected: $expectedAnswer
           |actual:   ${result.toSeq}
           |${ds.exprEnc.deserializer.treeString}
         """.stripMargin)
    }
  }

  /**
   * Evaluates a dataset to make sure that the result of calling collect matches the given
   * expected answer, after sort.
   */
  protected def checkDatasetUnorderly[T : Ordering](
      ds: => Dataset[T],
      expectedAnswer: T*): Unit = {
    val result = getResult(ds)

    if (!QueryTest.compare(result.toSeq.sorted, expectedAnswer.sorted)) {
      fail(
        s"""
           |Decoded objects do not match expected objects:
           |expected: $expectedAnswer
           |actual:   ${result.toSeq}
           |${ds.exprEnc.deserializer.treeString}
         """.stripMargin)
    }
  }

  private def getResult[T](ds: => Dataset[T]): Array[T] = {
    val analyzedDS = try ds catch {
      case ae: ExtendedAnalysisException =>
        if (ae.plan.isDefined) {
          fail(
            s"""
               |Failed to analyze query: $ae
               |${ae.plan.get}
               |
               |${stackTraceToString(ae)}
             """.stripMargin)
        } else {
          throw ae
        }
    }
    assertEmptyMissingInput(analyzedDS)

    try ds.collect() catch {
      case e: Exception =>
        fail(
          s"""
             |Exception collecting dataset as objects
             |${ds.exprEnc}
             |${ds.exprEnc.deserializer.treeString}
             |${ds.queryExecution}
           """.stripMargin, e)
    }
  }

  /**
   * Runs the plan and makes sure the answer matches the expected result.
   *
   * @param df the [[DataFrame]] to be executed
   * @param expectedAnswer the expected result in a [[Seq]] of [[Row]]s.
   */
  protected def checkAnswer(df: => DataFrame, expectedAnswer: Seq[Row]): Unit = {
    val analyzedDF = try df catch {
      case ae: ExtendedAnalysisException =>
        if (ae.plan.isDefined) {
          fail(
            s"""
               |Failed to analyze query: $ae
               |${ae.plan.get}
               |
               |${stackTraceToString(ae)}
               |""".stripMargin)
        } else {
          throw ae
        }
    }

    assertEmptyMissingInput(analyzedDF)

    QueryTest.checkAnswer(analyzedDF, expectedAnswer)
  }

  protected def checkAnswer(df: => DataFrame, expectedAnswer: Row): Unit = {
    checkAnswer(df, Seq(expectedAnswer))
  }

  protected def checkAnswer(df: => DataFrame, expectedAnswer: DataFrame): Unit = {
    checkAnswer(df, expectedAnswer.collect().toImmutableArraySeq)
  }

  /**
   * Runs the plan and makes sure the answer matches the expected result.
   *
   * @param df the [[DataFrame]] to be executed
   * @param expectedAnswer the expected result in a [[Array]] of [[Row]]s.
   */
  protected def checkAnswer(df: => DataFrame, expectedAnswer: Array[Row]): Unit = {
    checkAnswer(df, expectedAnswer.toImmutableArraySeq)
  }

  /**
   * Runs the plan and makes sure the answer is within absTol of the expected result.
   *
   * @param dataFrame the [[DataFrame]] to be executed
   * @param expectedAnswer the expected result in a [[Seq]] of [[Row]]s.
   * @param absTol the absolute tolerance between actual and expected answers.
   */
  protected def checkAggregatesWithTol(dataFrame: DataFrame,
      expectedAnswer: Seq[Row],
      absTol: Double): Unit = {
    // TODO: catch exceptions in data frame execution
    val actualAnswer = dataFrame.collect()
    require(actualAnswer.length == expectedAnswer.length,
      s"actual num rows ${actualAnswer.length} != expected num of rows ${expectedAnswer.length}")

    actualAnswer.zip(expectedAnswer).foreach {
      case (actualRow, expectedRow) =>
        QueryTest.checkAggregatesWithTol(actualRow, expectedRow, absTol)
    }
  }

  protected def checkAggregatesWithTol(dataFrame: DataFrame,
      expectedAnswer: Row,
      absTol: Double): Unit = {
    checkAggregatesWithTol(dataFrame, Seq(expectedAnswer), absTol)
  }

  /**
   * Asserts that a given [[Dataset]] will be executed using the given number of cached results.
   */
  def assertCached(query: Dataset[_], numCachedTables: Int = 1): Unit = {
    val planWithCaching =
      query.queryExecution.withCachedData
    val cachedData = planWithCaching collect {
      case cached: InMemoryRelation => cached
    }

    assert(
      cachedData.size == numCachedTables,
      s"Expected query to contain $numCachedTables, but it actually had ${cachedData.size}\n" +
        planWithCaching)
  }

  /**
   * Asserts that a given [[Dataset]] will be executed using the cache with the given name and
   * storage level.
   */
  def assertCached(query: Dataset[_], cachedName: String, storageLevel: StorageLevel): Unit = {
    val planWithCaching =
      query.queryExecution.withCachedData
    val matched = planWithCaching.exists {
      case cached: InMemoryRelation =>
        val cacheBuilder = cached.cacheBuilder
        cachedName == cacheBuilder.tableName.get && (storageLevel == cacheBuilder.storageLevel)
      case _ => false
    }

    assert(matched, s"Expected query plan to hit cache $cachedName with storage " +
      s"level $storageLevel, but it doesn't.")
  }

  def assertNotCached(query: Dataset[_]): Unit = {
    assertCached(query, numCachedTables = 0)
  }

  /**
   * Asserts that a given [[Dataset]] does not have missing inputs in all the analyzed plans.
   */
  def assertEmptyMissingInput(query: Dataset[_]): Unit = {
    val qe = query.queryExecution
    assert(qe.analyzed.missingInput.isEmpty,
      s"The analyzed logical plan has missing inputs:\n${qe.analyzed}")
    assert(qe.optimizedPlan.missingInput.isEmpty,
      s"The optimized logical plan has missing inputs:\n${qe.optimizedPlan}")
    assert(qe.executedPlan.missingInput.isEmpty,
      s"The physical plan has missing inputs:\n${qe.executedPlan}")
  }

  protected def sparkContext = spark.sparkContext

  // Shorthand for running a query using our SparkSession
  protected lazy val sql: String => DataFrame = spark.sql _

  /**
   * A helper object for importing SQL implicits.
   *
   * Note that the alternative of importing `spark.implicits._` is not possible here.
   * This is because we create the `SparkSession` immediately before the first test is run,
   * but the implicits import is needed in the constructor.
   */
  protected object testImplicits
    extends classic.SQLImplicits
      with classic.ClassicConversions
      with classic.ColumnConversions {
    override protected def session: classic.SparkSession =
      self.spark.asInstanceOf[classic.SparkSession]
    override protected def converter: classic.ColumnNodeToExpressionConverter =
      self.spark.asInstanceOf[classic.SparkSession].converter
  }

  protected override def withSQLConf[T](pairs: (String, String)*)(f: => T): T = {
    SparkSession.setActiveSession(spark)
    super.withSQLConf(pairs: _*)(f)
  }

  /**
   * Drops functions after calling `f`. A function is represented by (functionName, isTemporary).
   */
  protected def withUserDefinedFunction(functions: (String, Boolean)*)(f: => Unit): Unit = {
    try {
      f
    } catch {
      case cause: Throwable => throw cause
    } finally {
      functions.foreach { case (functionName, isTemporary) =>
        val withTemporary = if (isTemporary) "TEMPORARY" else ""
        spark.sql(s"DROP $withTemporary FUNCTION IF EXISTS $functionName")
        assert(
          !spark.sessionState.catalog.functionExists(FunctionIdentifier(functionName)),
          s"Function $functionName should have been dropped. But, it still exists.")
      }
    }
  }

  /**
   * Drops temporary view `viewNames` after calling `f`.
   */
  protected def withTempView(viewNames: String*)(f: => Unit): Unit = {
    Utils.tryWithSafeFinally(f) {
      viewNames.foreach { viewName =>
        try spark.catalog.dropTempView(viewName) catch {
          case _: NoSuchTableException =>
        }
      }
    }
  }

  /**
   * Drops global temporary view `viewNames` after calling `f`.
   */
  protected def withGlobalTempView(viewNames: String*)(f: => Unit): Unit = {
    Utils.tryWithSafeFinally(f) {
      viewNames.foreach { viewName =>
        try spark.catalog.dropGlobalTempView(viewName) catch {
          case _: NoSuchTableException =>
        }
      }
    }
  }

  /**
   * Drops table `tableName` after calling `f`.
   */
  protected def withTable(tableNames: String*)(f: => Unit): Unit = {
    Utils.tryWithSafeFinally(f) {
      tableNames.foreach { name =>
        spark.sql(s"DROP TABLE IF EXISTS $name")
      }
    }
  }

  /**
   * Drops view `viewName` after calling `f`.
   */
  protected def withView(viewNames: String*)(f: => Unit): Unit = {
    Utils.tryWithSafeFinally(f)(
      viewNames.foreach { name =>
        spark.sql(s"DROP VIEW IF EXISTS $name")
      }
    )
  }

  /**
   * Drops cache `cacheName` after calling `f`.
   */
  protected def withCache(cacheNames: String*)(f: => Unit): Unit = {
    Utils.tryWithSafeFinally(f) {
      cacheNames.foreach { cacheName =>
        try uncacheTable(cacheName) catch {
          case _: AnalysisException =>
        }
      }
    }
  }

  // Blocking uncache table for tests
  protected def uncacheTable(tableName: String): Unit = {
    val tableIdent = spark.sessionState.sqlParser.parseTableIdentifier(tableName)
    val cascade = !spark.sessionState.catalog.isTempView(tableIdent)
    spark.sharedState.cacheManager.uncacheQuery(
      spark.table(tableName).asInstanceOf[classic.Dataset[_]],
      cascade = cascade,
      blocking = true)
  }

  /**
   * Creates a temporary database and switches current database to it before executing `f`.  This
   * database is dropped after `f` returns.
   *
   * Note that this method doesn't switch current database before executing `f`.
   */
  protected def withTempDatabase(f: String => Unit): Unit = {
    val dbName = s"db_${UUID.randomUUID().toString.replace('-', '_')}"

    try {
      spark.sql(s"CREATE DATABASE $dbName")
    } catch { case cause: Throwable =>
      fail("Failed to create temporary database", cause)
    }

    try f(dbName) finally {
      if (spark.catalog.currentDatabase == dbName) {
        spark.sql(s"USE $DEFAULT_DATABASE")
      }
      spark.sql(s"DROP DATABASE $dbName CASCADE")
    }
  }

  /**
   * Drops database `dbName` after calling `f`.
   */
  protected def withDatabase(dbNames: String*)(f: => Unit): Unit = {
    Utils.tryWithSafeFinally(f) {
      dbNames.foreach { name =>
        spark.sql(s"DROP DATABASE IF EXISTS $name CASCADE")
      }
      spark.sql(s"USE $DEFAULT_DATABASE")
    }
  }

  /**
   * Drops namespace `namespace` after calling `f`.
   *
   * Note that, if you switch current catalog/namespace in `f`, you should switch it back manually.
   */
  protected def withNamespace(namespaces: String*)(f: => Unit): Unit = {
    Utils.tryWithSafeFinally(f) {
      namespaces.foreach { name =>
        spark.sql(s"DROP NAMESPACE IF EXISTS $name CASCADE")
      }
    }
  }

  /**
   * Restores the current catalog/database after calling `f`.
   */
  protected def withCurrentCatalogAndNamespace(f: => Unit): Unit = {
    val curCatalog = sql("select current_catalog()").head().getString(0)
    val curDatabase = sql("select current_database()").head().getString(0)
    Utils.tryWithSafeFinally(f) {
      spark.sql(s"USE $curCatalog.$curDatabase")
    }
  }

  /**
   * Enables Locale `language` before executing `f`, then switches back to the default locale of
   * JVM after `f` returns.
   */
  protected def withLocale(language: String)(f: => Unit): Unit = {
    val originalLocale = Locale.getDefault
    try {
      // Add Locale setting
      Locale.setDefault(new Locale(language))
      f
    } finally {
      Locale.setDefault(originalLocale)
    }
  }

  /**
   * Drops temporary variable `variableName` after calling `f`.
   */
  protected def withSessionVariable(variableNames: String*)(f: => Unit): Unit = {
    Utils.tryWithSafeFinally(f) {
      variableNames.foreach { name =>
        spark.sql(s"DROP TEMPORARY VARIABLE IF EXISTS $name")
      }
    }
  }

  /**
   * Activates database `db` before executing `f`, then switches back to `default` database after
   * `f` returns.
   */
  protected def activateDatabase(db: String)(f: => Unit): Unit = {
    spark.sessionState.catalogManager.setCurrentNamespace(Array(db))
    Utils.tryWithSafeFinally(f)(
      spark.sessionState.catalogManager.setCurrentNamespace(Array("default")))
  }

  /**
   * Strip Spark-side filtering in order to check if a datasource filters rows correctly.
   */
  protected def stripSparkFilter(df: DataFrame): DataFrame = {
    val schema = df.schema
    val withoutFilters = df.queryExecution.executedPlan.transform {
      case FilterExec(_, child) => child
    }

    spark.asInstanceOf[classic.SparkSession]
      .internalCreateDataFrame(withoutFilters.execute(), schema)
  }

  /**
   * Turn a logical plan into a `DataFrame`. This should be removed once we have an easier
   * way to construct `DataFrame` directly out of local data without relying on implicits.
   */
  protected implicit def logicalPlanToSparkQuery(plan: LogicalPlan): classic.DataFrame = {
    classic.Dataset.ofRows(spark.asInstanceOf[classic.SparkSession], plan)
  }

  /**
   * This method is used to make the given path qualified, when a path
   * does not contain a scheme, this path will not be changed after the default
   * FileSystem is changed.
   */
  def makeQualifiedPath(path: String): URI = {
    val hadoopPath = new Path(path)
    val fs = hadoopPath.getFileSystem(spark.sessionState.newHadoopConf())
    fs.makeQualified(hadoopPath).toUri
  }

  /**
   * Returns full path to the given file in the resource folder
   */
  protected def testFile(fileName: String): String = {
    Thread.currentThread().getContextClassLoader.getResource(fileName).toString
  }

  /**
   * Returns the size of the local directory except the metadata file and the temporary file.
   */
  def getLocalDirSize(file: File): Long = {
    assert(file.isDirectory)
    Files.walk(file.toPath).iterator().asScala
      .filter(p => java.nio.file.Files.isRegularFile(p) &&
        DataSourceUtils.isDataFile(p.getFileName.toString))
      .map(_.toFile.length).sum
  }

}

/**
 * Helper trait that should be extended by all SQL test suites within the Spark code base.
 *
 * This allows subclasses to plugin a custom `SparkSession`. It comes with test data
 * prepared in advance as well as all implicit conversions used extensively by dataframes.
 * To use implicit methods, import `testImplicits._` instead of through the `SparkSession`.
 *
 * Subclasses should *not* create `SparkSession`s in the test suite constructor, which is
 * prone to leaving multiple overlapping [[org.apache.spark.SparkContext]]s in the same JVM.
 */
trait QueryTest extends SparkFunSuite with QueryTestBase with PlanTest {
  // Whether to materialize all test data before the first test is run
  private var loadTestDataBeforeTests = false

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    if (loadTestDataBeforeTests) {
      loadTestData()
    }
  }

  /**
   * Creates a temporary directory, which is then passed to `f` and will be deleted after `f`
   * returns.
   */
  protected override def withTempDir(f: File => Unit): Unit = {
    super.withTempDir { dir =>
      f(dir)
      waitForTasksToFinish()
    }
  }

  /**
   * A helper function for turning off/on codegen.
   */
  protected def testWithWholeStageCodegenOnAndOff(testName: String)(f: String => Unit): Unit = {
    Seq("false", "true").foreach { codegenEnabled =>
      val isTurnOn = if (codegenEnabled == "true") "on" else "off"
      test(s"$testName (whole-stage-codegen ${isTurnOn})") {
        withSQLConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> codegenEnabled) {
          f(codegenEnabled)
        }
      }
    }
  }

  /**
   * Materialize the test data immediately after the `SQLContext` is set up.
   * This is necessary if the data is accessed by name but not through direct reference.
   */
  protected def setupTestData(): Unit = {
    loadTestDataBeforeTests = true
  }

  /**
   * Disable stdout and stderr when running the test. To not output the logs to the console,
   * ConsoleAppender's `follow` should be set to `true` so that it will honor reassignments of
   * System.out or System.err. Otherwise, ConsoleAppender will still output to the console even if
   * we change System.out and System.err.
   */
  protected def testQuietly(name: String)(f: => Unit): Unit = {
    test(name) {
      quietly {
        f
      }
    }
  }

  override protected def test(testName: String, testTags: Tag*)(testFun: => Any)
      (implicit pos: Position): Unit = {
    if (testTags.exists(_.isInstanceOf[DisableAdaptiveExecution])) {
      super.test(testName, testTags: _*) {
        withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
          testFun
        }
      }
    } else {
      super.test(testName, testTags: _*)(testFun)
    }
  }

  /**
   * Run a test on a separate `UninterruptibleThread`.
   */
  protected def testWithUninterruptibleThread(name: String, quietly: Boolean = false)
    (body: => Unit): Unit = {
    val timeoutMillis = 10000
    @transient var ex: Throwable = null

    def runOnThread(): Unit = {
      val thread = new UninterruptibleThread(s"Testing thread for test $name") {
        override def run(): Unit = {
          try {
            body
          } catch {
            case NonFatal(e) =>
              ex = e
          }
        }
      }
      thread.setDaemon(true)
      thread.start()
      thread.join(timeoutMillis)
      if (thread.isAlive) {
        thread.interrupt()
        fail(
          s"Test '$name' running on o.a.s.util.UninterruptibleThread timed out after" +
            s" $timeoutMillis ms")
      } else if (ex != null) {
        throw ex
      }
    }

    if (quietly) {
      testQuietly(name) { runOnThread() }
    } else {
      test(name) { runOnThread() }
    }
  }

  /**
   * Copy file in jar's resource to a temp file, then pass it to `f`.
   * This function is used to make `f` can use the path of temp file(e.g. file:/), instead of
   * path of jar's resource which starts with 'jar:file:/'
   */
  protected def withResourceTempPath(resourcePath: String)(f: File => Unit): Unit = {
    val inputStream =
      Thread.currentThread().getContextClassLoader.getResourceAsStream(resourcePath)
    withTempDir { dir =>
      val tmpFile = new File(dir, "tmp")
      Files.copy(inputStream, tmpFile.toPath)
      f(tmpFile)
    }
  }

  /**
   * Waits for all tasks on all executors to be finished.
   */
  protected def waitForTasksToFinish(): Unit = {
    eventually(timeout(10.seconds)) {
      assert(spark.sparkContext.statusTracker
        .getExecutorInfos.map(_.numRunningTasks()).sum == 0)
    }
  }

  /**
   * Creates the specified number of temporary directories, which is then passed to `f` and will be
   * deleted after `f` returns.
   */
  protected def withTempPaths(numPaths: Int)(f: Seq[File] => Unit): Unit = {
    val files = Array.fill[File](numPaths)(Utils.createTempDir().getCanonicalFile)
    try f(files.toImmutableArraySeq) finally {
      // wait for all tasks to finish before deleting files
      waitForTasksToFinish()
      files.foreach(Utils.deleteRecursively)
    }
  }

  protected def getCurrentClassCallSitePattern: String = {
    val stack = Thread.currentThread().getStackTrace()
    val idx = stack.lastIndexWhere(_.getMethodName == "getCurrentClassCallSitePattern")
    val cs = stack(idx + 1)
    s"${cs.getClassName}\\..*\\(${cs.getFileName}:\\d+\\)"
  }

  protected def getNextLineCallSitePattern(lines: Int = 1): String = {
    val stack = Thread.currentThread().getStackTrace()
    val idx = stack.lastIndexWhere(_.getMethodName == "getNextLineCallSitePattern")
    val cs = stack(idx + 1)
    Pattern.quote(
      s"${cs.getClassName}.${cs.getMethodName}(${cs.getFileName}:${cs.getLineNumber + lines})")
  }
}

object QueryTest extends Assertions {
  /**
   * Runs the plan and makes sure the answer matches the expected result.
   *
   * @param df the DataFrame to be executed
   * @param expectedAnswer the expected result in a Seq of Rows.
   * @param checkToRDD whether to verify deserialization to an RDD. This runs the query twice.
   */
  def checkAnswer(df: DataFrame, expectedAnswer: Seq[Row], checkToRDD: Boolean = true): Unit = {
    getErrorMessageInCheckAnswer(df, expectedAnswer, checkToRDD) match {
      case Some(errorMessage) => fail(errorMessage)
      case None =>
    }
  }

  /**
   * Runs the plan and makes sure the answer matches the expected result.
   * If there was exception during the execution or the contents of the DataFrame does not
   * match the expected result, an error message will be returned. Otherwise, a None will
   * be returned.
   *
   * @param df the DataFrame to be executed
   * @param expectedAnswer the expected result in a Seq of Rows.
   * @param checkToRDD whether to verify deserialization to an RDD. This runs the query twice.
   */
  def getErrorMessageInCheckAnswer(
      df: DataFrame,
      expectedAnswer: Seq[Row],
      checkToRDD: Boolean = true): Option[String] = {
    val isSorted = df.logicalPlan.collectFirst { case s: logical.Sort => s }.nonEmpty
    if (checkToRDD) {
      SQLExecution.withSQLConfPropagated(df.sparkSession) {
        df.materializedRdd.count() // Also attempt to deserialize as an RDD [SPARK-15791]
      }
    }

    val sparkAnswer = try df.collect().toSeq catch {
      case e: Exception =>
        val errorMessage =
          s"""
            |Exception thrown while executing query:
            |${df.queryExecution}
            |== Exception ==
            |$e
            |${org.apache.spark.sql.catalyst.util.stackTraceToString(e)}
          """.stripMargin
        return Some(errorMessage)
    }

    sameRows(expectedAnswer, sparkAnswer, isSorted).map { results =>
        s"""
        |Results do not match for query:
        |Timezone: ${TimeZone.getDefault}
        |Timezone Env: ${sys.env.getOrElse("TZ", "")}
        |
        |${df.queryExecution}
        |== Results ==
        |$results
       """.stripMargin
    }
  }


  def prepareAnswer(answer: Seq[Row], isSorted: Boolean): Seq[Row] = {
    // Converts data to types that we can do equality comparison using Scala collections.
    // For BigDecimal type, the Scala type has a better definition of equality test (similar to
    // Java's java.math.BigDecimal.compareTo).
    // For binary arrays, we convert it to Seq to avoid of calling java.util.Arrays.equals for
    // equality test.
    val converted: Seq[Row] = answer.map(prepareRow)
    if (!isSorted) converted.sortBy(_.toString()) else converted
  }

  // We need to call prepareRow recursively to handle schemas with struct types.
  def prepareRow(row: Row): Row = {
    Row.fromSeq(row.toSeq.map {
      case null => null
      case bd: java.math.BigDecimal => BigDecimal(bd)
      // Equality of WrappedArray differs for AnyVal and AnyRef in Scala 2.12.2+
      case seq: Seq[_] => seq.map {
        case b: java.lang.Byte => b.byteValue
        case s: java.lang.Short => s.shortValue
        case i: java.lang.Integer => i.intValue
        case l: java.lang.Long => l.longValue
        case f: java.lang.Float => f.floatValue
        case d: java.lang.Double => d.doubleValue
        case x => x
      }
      // Convert array to Seq for easy equality check.
      case b: Array[_] => b.toSeq
      case r: Row => prepareRow(r)
      // SPARK-51349: "null" and null had the same precedence in sorting
      case "null" => "__null_string__"
      case o => o
    })
  }

  private def genError(
      expectedAnswer: Seq[Row],
      sparkAnswer: Seq[Row],
      isSorted: Boolean = false): String = {
    val getRowType: Option[Row] => String = row =>
      row.map(row =>
        if (row.schema == null) {
          "struct<>"
        } else {
          s"${row.schema.catalogString}"
        }).getOrElse("struct<>")

    s"""
       |== Results ==
       |${
      sideBySide(
        s"== Correct Answer - ${expectedAnswer.size} ==" +:
          getRowType(expectedAnswer.headOption) +:
          prepareAnswer(expectedAnswer, isSorted).map(_.toString()),
        s"== Spark Answer - ${sparkAnswer.size} ==" +:
          getRowType(sparkAnswer.headOption) +:
          prepareAnswer(sparkAnswer, isSorted).map(_.toString())).mkString("\n")
    }
    """.stripMargin
  }

  def includesRows(
      expectedRows: Seq[Row],
      sparkAnswer: Seq[Row]): Option[String] = {
    if (!prepareAnswer(expectedRows, true).toSet.subsetOf(prepareAnswer(sparkAnswer, true).toSet)) {
      return Some(genError(expectedRows, sparkAnswer, true))
    }
    None
  }

  def compare(obj1: Any, obj2: Any): Boolean = (obj1, obj2) match {
    case (null, null) => true
    case (null, _) => false
    case (_, null) => false
    case (a: Array[_], b: Array[_]) =>
      a.length == b.length && a.zip(b).forall { case (l, r) => compare(l, r)}
    case (a: Map[_, _], b: Map[_, _]) =>
      a.size == b.size && a.keys.forall { aKey =>
        b.keys.find(bKey => compare(aKey, bKey)).exists(bKey => compare(a(aKey), b(bKey)))
      }
    case (a: Iterable[_], b: Iterable[_]) =>
      a.size == b.size && a.zip(b).forall { case (l, r) => compare(l, r)}
    case (a: Product, b: Product) =>
      compare(a.productIterator.toSeq, b.productIterator.toSeq)
    case (a: Row, b: Row) =>
      compare(a.toSeq, b.toSeq)
    // 0.0 == -0.0, turn float/double to bits before comparison, to distinguish 0.0 and -0.0.
    // in some hardware NaN can be represented with different bits, so first check for it
    case (a: Double, b: Double) =>
      a.isNaN && b.isNaN ||
      java.lang.Double.doubleToRawLongBits(a) == java.lang.Double.doubleToRawLongBits(b)
    case (a: Float, b: Float) =>
      a.isNaN && b.isNaN ||
      java.lang.Float.floatToRawIntBits(a) == java.lang.Float.floatToRawIntBits(b)
    case (a, b) => a == b
  }

  def sameRows(
      expectedAnswer: Seq[Row],
      sparkAnswer: Seq[Row],
      isSorted: Boolean = false): Option[String] = {
    if (!compare(prepareAnswer(expectedAnswer, isSorted), prepareAnswer(sparkAnswer, isSorted))) {
      return Some(genError(expectedAnswer, sparkAnswer, isSorted))
    }
    None
  }

  def compareAnswers(
      sparkAnswer: Seq[Row],
      expectedAnswer: Seq[Row],
      sort: Boolean): Option[String] = {
    def prepareAnswer(answer: Seq[Row]): Seq[Row] = {
      // Converts data to types that we can do equality comparison using Scala collections.
      // For BigDecimal type, the Scala type has a better definition of equality test (similar to
      // Java's java.math.BigDecimal.compareTo).
      // For binary arrays, we convert it to Seq to avoid of calling java.util.Arrays.equals for
      // equality test.
      val converted: Seq[Row] = answer.map { s =>
        Row.fromSeq(s.toSeq.map {
          case d: java.math.BigDecimal => BigDecimal(d)
          case b: Array[Byte] => b.toSeq
          case o => o
        })
      }
      if (sort) {
        converted.sortBy(_.toString())
      } else {
        converted
      }
    }
    if (prepareAnswer(expectedAnswer) != prepareAnswer(sparkAnswer)) {
      val errorMessage =
        s"""
           | == Results ==
           | ${sideBySide(
          s"== Expected Answer - ${expectedAnswer.size} ==" +:
            prepareAnswer(expectedAnswer).map(_.toString()),
          s"== Actual Answer - ${sparkAnswer.size} ==" +:
            prepareAnswer(sparkAnswer).map(_.toString())).mkString("\n")}
      """.stripMargin
      Some(errorMessage)
    } else {
      None
    }
  }

  /**
   * Runs the plan and makes sure the answer is within absTol of the expected result.
   *
   * @param actualAnswer the actual result in a [[Row]].
   * @param expectedAnswer the expected result in a[[Row]].
   * @param absTol the absolute tolerance between actual and expected answers.
   */
  def checkAggregatesWithTol(actualAnswer: Row, expectedAnswer: Row, absTol: Double): Unit = {
    require(actualAnswer.length == expectedAnswer.length,
      s"actual answer length ${actualAnswer.length} != " +
        s"expected answer length ${expectedAnswer.length}")

    // TODO: support other numeric types besides Double
    // TODO: support struct types?
    actualAnswer.toSeq.zip(expectedAnswer.toSeq).foreach {
      case (actual: Double, expected: Double) =>
        assert(math.abs(actual - expected) < absTol,
          s"actual answer $actual not within $absTol of correct answer $expected")
      case (actual, expected) =>
        assert(actual == expected, s"$actual did not equal $expected")
    }
  }

  def checkAnswer(df: DataFrame, expectedAnswer: java.util.List[Row]): Unit = {
    getErrorMessageInCheckAnswer(df, expectedAnswer.asScala.toSeq) match {
      case Some(errorMessage) => fail(errorMessage)
      case None =>
    }
  }

  def withQueryExecutionsCaptured(spark: SparkSession)(thunk: => Unit): Seq[QueryExecution] = {
    var capturedQueryExecutions = Seq.empty[QueryExecution]

    val listener = new QueryExecutionListener {
      override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
        capturedQueryExecutions = capturedQueryExecutions :+ qe
      }
      override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {
        capturedQueryExecutions = capturedQueryExecutions :+ qe
      }
    }

    val classicSession = spark.asInstanceOf[classic.SparkSession]
    classicSession.sparkContext.listenerBus.waitUntilEmpty(15000)
    classicSession.listenerManager.register(listener)
    try {
      thunk
      classicSession.sparkContext.listenerBus.waitUntilEmpty(15000)
    } finally {
      classicSession.listenerManager.unregister(listener)
    }

    capturedQueryExecutions
  }

}

class QueryTestSuite extends QueryTest with test.SharedSparkSession {
  test("SPARK-16940: checkAnswer should raise TestFailedException for wrong results") {
    intercept[org.scalatest.exceptions.TestFailedException] {
      checkAnswer(sql("SELECT 1"), Row(2) :: Nil)
    }
  }

  test("SPARK-51349: null string and true null are distinguished") {
    checkAnswer(sql("select case when id == 0 then struct('null') else struct(null) end s " +
      "from range(2)"),
      Seq(Row(Row(null)), Row(Row("null"))))
  }
}
