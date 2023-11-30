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

package org.apache.spark.sql.crossdbms

import java.io.File
import java.util.Locale

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SQLQueryTestSuite
import org.apache.spark.sql.catalyst.util.stringToFile
import org.apache.spark.util.Utils

// scalastyle:off line.size.limit
/**
 * See SQLQueryTestSuite.scala for more information. This class builds off of that to allow us
 * to generate golden files with other DBMS to perform cross-checking for correctness. Note that the
 * input directory path is currently limited because most, if not all, of our current SQL query
 * tests will not be compatible with other DBMSes. There will be more work in the future, such as
 * some kind of conversion, to increase coverage.
 *
 * If your SQL query test is not compatible with other DBMSes, please add it to the `ignoreList` at
 * the bottom of this file.
 *
 * You need to have a database server up before running this test.
 * For example, for postgres:
 * 1. Install PostgreSQL.
 *   a. On a mac: `brew install postgresql@13`
 * 2. After installing PostgreSQL, start the database server, then create a role named pg with
 *    superuser permissions: `createuser -s pg`` OR `psql> CREATE role pg superuser``
 *
 * To run the entire test suite:
 * {{{
 *   build/sbt "sql/testOnly org.apache.spark.sql.crossdbms.CrossDbmsQueryTestSuite"
 * }}}
 *
 * To re-generate golden files for entire suite, run:
 * {{{
 *   SPARK_GENERATE_GOLDEN_FILES=1 build/sbt "sql/testOnly org.apache.spark.sql.crossdbms.CrossDbmsQueryTestSuite"
 * }}}
 *
 * To re-generate golden file for a single test, run:
 * {{{
 *   SPARK_GENERATE_GOLDEN_FILES=1 build/sbt "sql/testOnly org.apache.spark.sql.crossdbms.CrossDbmsQueryTestSuite -- -z describe.sql"
 * }}}
 *
 * To specify a DBMS to use (the default is postgres):
 * {{{
 *   REF_DBMS=mysql SPARK_GENERATE_GOLDEN_FILES=1 build/sbt "sql/testOnly org.apache.spark.sql.crossdbms.CrossDbmsQueryTestSuite"
 * }}}
 */
// scalastyle:on line.size.limit
class CrossDbmsQueryTestSuite extends SQLQueryTestSuite with Logging {

  // Note: the below two functions have to be functions instead of variables because the superclass
  // runs the test first before the subclass variables can be instantiated.
  private def crossDbmsToGenerateGoldenFiles: String = {
    val userInputDbms = System.getenv(CrossDbmsQueryTestSuite.REF_DBMS_ARGUMENT)
    val selectedDbms = Option(userInputDbms).filter(_.nonEmpty).getOrElse(
      CrossDbmsQueryTestSuite.DEFAULT_DBMS)
    assert(CrossDbmsQueryTestSuite.SUPPORTED_DBMS.contains(selectedDbms),
      s"$selectedDbms is not currently supported.")
    selectedDbms
  }
  private def customConnectionUrl: String = System.getenv(
    CrossDbmsQueryTestSuite.REF_DBMS_CONNECTION_URL)

  override protected def runQueries(
      queries: Seq[String],
      testCase: TestCase,
      configSet: Seq[(String, String)]): Unit = {
    val localSparkSession = spark.newSession()

    var runner: Option[SQLQueryTestRunner] = None
    val outputs: Seq[QueryTestOutput] = queries.map { sql =>
      val output = {
        // Use the runner when generating golden files, and Spark when running the test against
        // the already generated golden files.
        if (regenerateGoldenFiles) {
          val connectionUrl = Option(customConnectionUrl).filter(_.nonEmpty)
          runner = runner.getOrElse(
            Some(CrossDbmsQueryTestSuite.DBMS_TO_CONNECTION_MAPPING(
              crossDbmsToGenerateGoldenFiles)(connectionUrl)))
          val sparkDf = spark.sql(sql)
          val output = runner.map(_.runQuery(sql)).get
          // Use Spark analyzed plan to check if the query result is already semantically sorted.
          val result = if (isSemanticallySorted(sparkDf.queryExecution.analyzed)) {
            output
          } else {
            // Sort the answer manually if it isn't sorted.
            output.sorted
          }
          result
        } else {
          val (_, output) = handleExceptions(
            getNormalizedQueryExecutionResult(localSparkSession, sql))
          output
        }
      }
      // We do some query canonicalization now.
      val executionOutput = ExecutionOutput(
        sql = sql,
        // Don't care about the schema for this test. Only care about correctness.
        schema = None,
        output = normalizeTestResults(output.mkString("\n")))
      if (testCase.isInstanceOf[CTETest]) {
        expandCTEQueryAndCompareResult(localSparkSession, sql, executionOutput)
      }
      executionOutput
    }
    runner.foreach(_.cleanUp())

    if (regenerateGoldenFiles) {
      val goldenOutput = {
        s"-- Automatically generated by ${getClass.getSimpleName}\n" +
          outputs.mkString("\n\n\n") + "\n"
      }
      val resultFile = new File(testCase.resultFile)
      val parent = resultFile.getParentFile
      if (!parent.exists()) {
        assert(parent.mkdirs(), "Could not create directory: " + parent)
      }
      stringToFile(resultFile, goldenOutput)
    }

    readGoldenFileAndCompareResults(testCase.resultFile, outputs, ExecutionOutput)
  }

  override def createScalaTestCase(testCase: TestCase): Unit = {
    testCase match {
      case _ if ignoreList.exists(t =>
        testCase.name.toLowerCase(Locale.ROOT).contains(t.toLowerCase(Locale.ROOT))) =>
        ignore(s"${testCase.name} is in the ignore list.") {
          log.debug(s"${testCase.name} is in the ignore list.")
        }
      case _: RegularTestCase =>
        // Create a test case to run this case.
        test(testCase.name) {
          runSqlTestCase(testCase, listTestCases)
        }
      case _ =>
        ignore(s"Ignoring test cases that are not [[RegularTestCase]] for now") {
          log.debug(s"${testCase.name} is not a RegularTestCase and is ignored.")
        }
    }
  }

  override protected def resultFileForInputFile(file: File): String = {
    val defaultResultsDir = new File(baseResourcePath, "results")
    val goldenFilePath = new File(
      defaultResultsDir, s"$crossDbmsToGenerateGoldenFiles-results").getAbsolutePath
    file.getAbsolutePath.replace(inputFilePath, goldenFilePath) + ".out"
  }

  override lazy val listTestCases: Seq[TestCase] = {
    listFilesRecursively(new File(inputFilePath)).flatMap { file =>
      var resultFile = resultFileForInputFile(file)
      // JDK-4511638 changes 'toString' result of Float/Double
      // JDK-8282081 changes DataTimeFormatter 'F' symbol
      if (Utils.isJavaVersionAtLeast21) {
        if (new File(resultFile + ".java21").exists()) resultFile += ".java21"
      }
      val absPath = file.getAbsolutePath
      val testCaseName = absPath.stripPrefix(inputFilePath).stripPrefix(File.separator)
      RegularTestCase(testCaseName, absPath, resultFile) :: Nil
    }.sortBy(_.name)
  }

  // Ignore all tests for now due to likely incompatibility.
  override def ignoreList: Set[String] = super.ignoreList ++ Set(
    "postgreSQL",
    "subquery",
    "ansi",
    "udtf",
    "udf",
    "timestampNTZ",
    "udaf",
    "typeCoercion",
    "group-by.sql",
    "natural-join.sql",
    "timestamp-ltz.sql",
    "csv-functions.sql",
    "datetime-formatting-invalid.sql",
    "except.sql",
    "string-functions.sql",
    "cte-command.sql",
    "identifier-clause.sql",
    "try_reflect.sql",
    "order-by-all.sql",
    "timestamp-ntz.sql",
    "url-functions.sql",
    "math.sql",
    "random.sql",
    "tablesample-negative.sql",
    "date.sql",
    "window.sql",
    "linear-regression.sql",
    "join-empty-relation.sql",
    "null-propagation.sql",
    "operators.sql",
    "change-column.sql",
    "count.sql",
    "mask-functions.sql",
    "decimalArithmeticOperations.sql",
    "column-resolution-sort.sql",
    "group-analytics.sql",
    "inline-table.sql",
    "comparator.sql",
    "ceil-floor-with-scale-param.sql",
    "show-tblproperties.sql",
    "timezone.sql",
    "ilike.sql",
    "parse-schema-string.sql",
    "charvarchar.sql",
    "ignored.sql",
    "cte.sql",
    "selectExcept.sql",
    "order-by-nulls-ordering.sql",
    "query_regex_column.sql",
    "show_columns.sql",
    "columnresolution-views.sql",
    "percentiles.sql",
    "hll.sql",
    "group-by-all.sql",
    "like-any.sql",
    "struct.sql",
    "show-create-table.sql",
    "try_cast.sql",
    "unpivot.sql",
    "describe-query.sql",
    "describe.sql",
    "columnresolution.sql",
    "union.sql",
    "order-by-ordinal.sql",
    "explain-cbo.sql",
    "intersect-all.sql",
    "ilike-all.sql",
    "try_aggregates.sql",
    "try-string-functions.sql",
    "show-views.sql",
    "xml-functions.sql",
    "datetime-parsing.sql",
    "cte-nested.sql",
    "columnresolution-negative.sql",
    "datetime-special.sql",
    "describe-table-after-alter-table.sql",
    "column-resolution-aggregate.sql",
    "keywords.sql",
    "table-aliases.sql",
    "bitwise.sql",
    "like-all.sql",
    "named-function-arguments.sql",
    "try_datetime_functions.sql",
    "datetime-legacy.sql",
    "outer-join.sql",
    "explain.sql",
    "sql-compatibility-functions.sql",
    "join-lateral.sql",
    "pred-pushdown.sql",
    "cast.sql",
    "except-all.sql",
    "predicate-functions.sql",
    "timestamp.sql",
    "group-by-filter.sql",
    "pivot.sql",
    "try_arithmetic.sql",
    "higher-order-functions.sql",
    "cte-legacy.sql",
    "misc-functions.sql",
    "describe-part-after-analyze.sql",
    "extract.sql",
    "datetime-formatting.sql",
    "interval.sql",
    "double-quoted-identifiers.sql",
    "datetime-formatting-legacy.sql",
    "group-by-ordinal.sql",
    "having.sql",
    "inner-join.sql",
    "null-handling.sql",
    "ilike-any.sql",
    "show-tables.sql",
    "sql-session-variables.sql",
    "using-join.sql",
    "subexp-elimination.sql",
    "cte-nonlegacy.sql",
    "group-by-all-duckdb.sql",
    "transform.sql",
    "map.sql",
    "table-valued-functions.sql",
    "comments.sql",
    "regexp-functions.sql",
    "datetime-parsing-legacy.sql",
    "cross-join.sql",
    "array.sql",
    "group-by-all-mosha.sql",
    "limit.sql",
    "non-excludable-rule.sql",
    "grouping_set.sql",
    "json-functions.sql",
    "datetime-parsing-invalid.sql",
    "try_element_at.sql",
    "explain-aqe.sql",
    "current_database_catalog.sql",
    "literals.sql"
  )
}

object CrossDbmsQueryTestSuite {

  // System argument to indicate which reference DBMS is being used.
  private final val REF_DBMS_ARGUMENT = "REF_DBMS"
  // System arguemnt to indicate a custom connection URL to the reference DBMS.
  private final val REF_DBMS_CONNECTION_URL = "REF_DBMS_CONNECTION_URL"

  private final val POSTGRES = "postgres"
  private final val SUPPORTED_DBMS = Seq(POSTGRES)
  private final val DEFAULT_DBMS = POSTGRES
  private final val DBMS_TO_CONNECTION_MAPPING = Map(
    POSTGRES -> ((connection_url: Option[String]) =>
      JdbcSQLQueryTestRunner(PostgresConnection(connection_url)))
  )
}
