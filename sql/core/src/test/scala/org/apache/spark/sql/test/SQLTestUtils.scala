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

package org.apache.spark.sql.test

import java.io.File
import java.net.URI
import java.nio.file.Files
import java.util.{Locale, UUID}

import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.language.implicitConversions
import scala.util.control.NonFatal

import org.apache.hadoop.fs.Path
import org.scalactic.source.Position
import org.scalatest.{BeforeAndAfterAll, Suite, Tag}
import org.scalatest.concurrent.Eventually

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.catalyst.catalog.SessionCatalog.DEFAULT_DATABASE
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.PlanTestBase
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.execution.FilterExec
import org.apache.spark.sql.execution.adaptive.DisableAdaptiveExecution
import org.apache.spark.sql.execution.datasources.DataSourceUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.ArrayImplicits._
import org.apache.spark.util.UninterruptibleThread
import org.apache.spark.util.Utils

/**
 * Helper trait that should be extended by all SQL test suites within the Spark
 * code base.
 *
 * This allows subclasses to plugin a custom `SparkSession`. It comes with test data
 * prepared in advance as well as all implicit conversions used extensively by dataframes.
 * To use implicit methods, import `testImplicits._` instead of through the `SparkSession`.
 *
 * Subclasses should *not* create `SparkSession`s in the test suite constructor, which is
 * prone to leaving multiple overlapping [[org.apache.spark.SparkContext]]s in the same JVM.
 */
private[sql] trait SQLTestUtils extends SparkFunSuite with SQLTestUtilsBase with PlanTest {
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
        // If this interrupt does not work, then this thread is most likely running something that
        // is not interruptible. There is not much point to wait for the thread to terminate, and
        // we rather let the JVM terminate the thread on exit.
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
}

/**
 * Helper trait that can be extended by all external SQL test suites.
 *
 * This allows subclasses to plugin a custom `SQLContext`.
 * To use implicit methods, import `testImplicits._` instead of through the `SQLContext`.
 *
 * Subclasses should *not* create `SQLContext`s in the test suite constructor, which is
 * prone to leaving multiple overlapping [[org.apache.spark.SparkContext]]s in the same JVM.
 */
private[sql] trait SQLTestUtilsBase
  extends Eventually
  with BeforeAndAfterAll
  with SQLTestData
  with PlanTestBase { self: Suite =>

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
  protected object testImplicits extends SQLImplicits {
    protected override def session: SparkSession = self.spark
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
      // If the test failed part way, we don't want to mask the failure by failing to remove
      // temp tables that never got created.
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
          // If the test failed part way, we don't want to mask the failure by failing to remove
          // temp views that never got created.
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
          // If the test failed part way, we don't want to mask the failure by failing to remove
          // global temp views that never got created.
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
      spark.table(tableName),
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
   * Enables Locale `language` before executing `f`, then switches back to the default locale of JVM
   * after `f` returns.
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
   * Activates database `db` before executing `f`, then switches back to `default` database after
   * `f` returns.
   */
  protected def activateDatabase(db: String)(f: => Unit): Unit = {
    spark.sessionState.catalog.setCurrentDatabase(db)
    Utils.tryWithSafeFinally(f)(spark.sessionState.catalog.setCurrentDatabase("default"))
  }

  /**
   * Strip Spark-side filtering in order to check if a datasource filters rows correctly.
   */
  protected def stripSparkFilter(df: DataFrame): DataFrame = {
    val schema = df.schema
    val withoutFilters = df.queryExecution.executedPlan.transform {
      case FilterExec(_, child) => child
    }

    spark.internalCreateDataFrame(withoutFilters.execute(), schema)
  }

  /**
   * Turn a logical plan into a `DataFrame`. This should be removed once we have an easier
   * way to construct `DataFrame` directly out of local data without relying on implicits.
   */
  protected implicit def logicalPlanToSparkQuery(plan: LogicalPlan): DataFrame = {
    Dataset.ofRows(spark, plan)
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
      .filter(p => Files.isRegularFile(p) && DataSourceUtils.isDataFile(p.getFileName.toString))
      .map(_.toFile.length).sum
  }
}

private[sql] object SQLTestUtils {

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
      // This function is copied from Catalyst's QueryTest
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
}
