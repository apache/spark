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
import java.util.UUID

import scala.language.implicitConversions
import scala.util.Try

import org.apache.hadoop.conf.Configuration
import org.scalatest.BeforeAndAfterAll

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.execution.FilterExec
import org.apache.spark.util.Utils

/**
 * Helper trait that should be extended by all SQL test suites.
 *
 * This allows subclasses to plugin a custom [[SQLContext]]. It comes with test data
 * prepared in advance as well as all implicit conversions used extensively by dataframes.
 * To use implicit methods, import `testImplicits._` instead of through the [[SQLContext]].
 *
 * Subclasses should *not* create [[SQLContext]]s in the test suite constructor, which is
 * prone to leaving multiple overlapping [[org.apache.spark.SparkContext]]s in the same JVM.
 */
private[sql] trait SQLTestUtils
  extends SparkFunSuite
  with BeforeAndAfterAll
  with SQLTestData { self =>

  protected def sparkContext = spark.sparkContext

  // Whether to materialize all test data before the first test is run
  private var loadTestDataBeforeTests = false

  // Shorthand for running a query using our SQLContext
  protected lazy val sql = spark.sql _

  /**
   * A helper object for importing SQL implicits.
   *
   * Note that the alternative of importing `spark.implicits._` is not possible here.
   * This is because we create the [[SQLContext]] immediately before the first test is run,
   * but the implicits import is needed in the constructor.
   */
  protected object testImplicits extends SQLImplicits {
    protected override def _sqlContext: SQLContext = self.spark.sqlContext
  }

  /**
   * Materialize the test data immediately after the [[SQLContext]] is set up.
   * This is necessary if the data is accessed by name but not through direct reference.
   */
  protected def setupTestData(): Unit = {
    loadTestDataBeforeTests = true
  }

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    if (loadTestDataBeforeTests) {
      loadTestData()
    }
  }

  /**
   * Sets all SQL configurations specified in `pairs`, calls `f`, and then restore all SQL
   * configurations.
   *
   * @todo Probably this method should be moved to a more general place
   */
  protected def withSQLConf(pairs: (String, String)*)(f: => Unit): Unit = {
    val (keys, values) = pairs.unzip
    val currentValues = keys.map(key => Try(spark.conf.get(key)).toOption)
    (keys, values).zipped.foreach(spark.conf.set)
    try f finally {
      keys.zip(currentValues).foreach {
        case (key, Some(value)) => spark.conf.set(key, value)
        case (key, None) => spark.conf.unset(key)
      }
    }
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
   * Creates a temporary directory, which is then passed to `f` and will be deleted after `f`
   * returns.
   *
   * @todo Probably this method should be moved to a more general place
   */
  protected def withTempDir(f: File => Unit): Unit = {
    val dir = Utils.createTempDir().getCanonicalFile
    try f(dir) finally Utils.deleteRecursively(dir)
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
  protected def withTempTable(tableNames: String*)(f: => Unit): Unit = {
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

    try f(dbName) finally spark.sql(s"DROP DATABASE $dbName CASCADE")
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
   * Turn a logical plan into a [[DataFrame]]. This should be removed once we have an easier
   * way to construct [[DataFrame]] directly out of local data without relying on implicits.
   */
  protected implicit def logicalPlanToSparkQuery(plan: LogicalPlan): DataFrame = {
    Dataset.ofRows(spark, plan)
  }

  /**
   * Disable stdout and stderr when running the test. To not output the logs to the console,
   * ConsoleAppender's `follow` should be set to `true` so that it will honors reassignments of
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
