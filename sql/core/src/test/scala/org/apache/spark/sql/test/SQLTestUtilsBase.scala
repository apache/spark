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
import scala.language.implicitConversions

import org.apache.hadoop.fs.Path
import org.scalatest.{BeforeAndAfterAll, Suite}
import org.scalatest.concurrent.Eventually

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.catalyst.catalog.SessionCatalog.DEFAULT_DATABASE
import org.apache.spark.sql.catalyst.plans.PlanTestBase
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.FilterExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.Utils

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

  // Shorthand for running a query using our SQLContext
  protected lazy val sql = spark.sql _

  /**
   * A helper object for importing SQL implicits.
   *
   * Note that the alternative of importing `spark.implicits._` is not possible here.
   * This is because we create the `SQLContext` immediately before the first test is run,
   * but the implicits import is needed in the constructor.
   */
  protected object testImplicits extends SQLImplicits {
    protected override def _sqlContext: SQLContext = self.spark.sqlContext
  }

  protected override def withSQLConf(pairs: (String, String)*)(f: => Unit): Unit = {
    SparkSession.setActiveSession(spark)
    super.withSQLConf(pairs: _*)(f)
  }

  /**
   * Generates a temporary path without creating the actual file/directory, then pass it to `f`. If
   * a file/directory is created there by `f`, it will be delete after `f` returns.
   *
   * @todo Probably this method should be moved to a more general place
   */
  protected def withTempPath(f: File => Unit): Unit = {
    val path = Utils.createTempDir()
    path.delete()
    try f(path) finally Utils.deleteRecursively(path)
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
   * Creates a temporary directory, which is then passed to `f` and will be deleted after `f`
   * returns.
   *
   * @todo Probably this method should be moved to a more general place
   */
  protected def withTempDir(f: File => Unit): Unit = {
    val dir = Utils.createTempDir().getCanonicalFile
    try f(dir) finally {
      // wait for all tasks to finish before deleting files
      waitForTasksToFinish()
      Utils.deleteRecursively(dir)
    }
  }

  /**
   * Creates the specified number of temporary directories, which is then passed to `f` and will be
   * deleted after `f` returns.
   */
  protected def withTempPaths(numPaths: Int)(f: Seq[File] => Unit): Unit = {
    val files = Array.fill[File](numPaths)(Utils.createTempDir().getCanonicalFile)
    try f(files) finally {
      // wait for all tasks to finish before deleting files
      waitForTasksToFinish()
      files.foreach(Utils.deleteRecursively)
    }
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
   * Drops temporary table `tableName` after calling `f`.
   */
  protected def withTempView(tableNames: String*)(f: => Unit): Unit = {
    try f finally {
      // If the test failed part way, we don't want to mask the failure by failing to remove
      // temp tables that never got created.
      try tableNames.foreach(spark.catalog.dropTempView) catch {
        case _: NoSuchTableException =>
      }
    }
  }

  /**
   * Drops table `tableName` after calling `f`.
   */
  protected def withTable(tableNames: String*)(f: => Unit): Unit = {
    try f finally {
      tableNames.foreach { name =>
        spark.sql(s"DROP TABLE IF EXISTS $name")
      }
    }
  }

  /**
   * Drops view `viewName` after calling `f`.
   */
  protected def withView(viewNames: String*)(f: => Unit): Unit = {
    try f finally {
      viewNames.foreach { name =>
        spark.sql(s"DROP VIEW IF EXISTS $name")
      }
    }
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
    try f finally {
      dbNames.foreach { name =>
        spark.sql(s"DROP DATABASE IF EXISTS $name CASCADE")
      }
      spark.sql(s"USE $DEFAULT_DATABASE")
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
    try f finally spark.sessionState.catalog.setCurrentDatabase("default")
  }

  /**
   * Strip Spark-side filtering in order to check if a datasource filters rows correctly.
   */
  protected def stripSparkFilter(df: DataFrame): DataFrame = {
    val schema = df.schema
    val withoutFilters = df.queryExecution.sparkPlan.transform {
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
}
