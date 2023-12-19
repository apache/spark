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

import scala.util.control.NonFatal

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SQLQueryTestSuite
import org.apache.spark.sql.catalyst.util.{fileToString, stringToFile}
import org.apache.spark.util.ArrayImplicits.SparkArrayOps

// scalastyle:off line.size.limit
/**
 * IF YOU ADDED A NEW SQL TEST AND THIS SUITE IS FAILING, READ THIS:
 * Your new SQL test is automatically opted into this suite. It is likely failing because it is not
 * compatible with the default DBMS (currently postgres). You have two options:
 * 1. (Recommended) Modify your queries to be compatible with both systems, and generate golden
 *    files with the instructions below. This is recommended because it will run your queries
 *    against postgres, providing higher correctness testing confidence, and you won't have to
 *    manually verify the golden files generated with your test.
 * 2. Add this line to your .sql file: --SPARK_ONLY
 *
 * You need to have a database server up before running this test. Two options:
 * - Install Docker and use the bash script in ./bin/generate_golden_files.sh to run a DBMS
 *   container and generate golden files.
 * - Otherwise, do manual installation. For example, for postgres:
 *   1. Install PostgreSQL.
 *     a. On a mac: `brew install postgresql@13`
 *   2. After installing PostgreSQL, start the database server, then create a role named pg with
 *      superuser permissions: `createuser -s pg`` OR `psql> CREATE role pg superuser``
 *
 * To indicate that the SQL file is not eligible for testing with this suite, add the following
 * comment into the input file:
 * {{{
 *   --SPARK_ONLY
 * }}}
 *
 * And then, to run the entire test suite, with the default cross DBMS:
 * {{{
 *   build/sbt "sql/testOnly org.apache.spark.sql.crossdbms.PostgreSQLQueryTestSuite"
 * }}}
 *
 * To re-generate golden files for entire suite, either run:
 * 1. (Recommended) You need Docker on your machine.
 * {{{
 *   bash ./bin/generate_golden_files.sh
 * }}}
 * 2.
 * {{{
 *   SPARK_GENERATE_GOLDEN_FILES=1 build/sbt "sql/testOnly org.apache.spark.sql.crossdbms.PostgreSQLQueryTestSuite"
 * }}}
 *
 * To re-generate golden file for a single test, e.g. `describe.sql`, run:
 * {{{
 *   SPARK_GENERATE_GOLDEN_FILES=1 build/sbt "sql/testOnly org.apache.spark.sql.crossdbms.PostgreSQLQueryTestSuite -- -z describe.sql"
 * }}}
 *
 * To specify a DBMS to use (the default is postgres):
 * {{{
 *   SPARK_GENERATE_GOLDEN_FILES=1 build/sbt "sql/testOnly org.apache.spark.sql.crossdbms.PostgreSQLQueryTestSuite"
 * }}}
 */

class PostgreSQLQueryTestSuite extends CrossDbmsQueryTestSuite {

  def crossDbmsToGenerateGoldenFiles: String = CrossDbmsQueryTestSuite.POSTGRES

  // Reduce scope to subquery tests for now. That is where most correctness issues are.
  override def customInputFilePath: String = new File(inputFilePath, "subquery").getAbsolutePath

  override def getConnection: Option[String] => JdbcSQLQueryTestRunner =
    (connection_url: Option[String]) => JdbcSQLQueryTestRunner(PostgresConnection(connection_url))
}

/**
 * See SQLQueryTestSuite.scala for more information. This suite builds off of that to allow us
 * to generate golden files with other DBMS to perform cross-checking for correctness. It generates
 * another set of golden files. Note that this is not currently run on all SQL input files by
 * default because there is incompatibility between SQL dialects for Spark and the other DBMS.
 */
abstract class CrossDbmsQueryTestSuite extends SQLQueryTestSuite with Logging {

  protected def crossDbmsToGenerateGoldenFiles: String
  protected def customInputFilePath: String = inputFilePath
  protected def getConnection: Option[String] => JdbcSQLQueryTestRunner

  private def customConnectionUrl: String = System.getenv(
    CrossDbmsQueryTestSuite.REF_DBMS_CONNECTION_URL)

  override protected def runSqlTestCase(testCase: TestCase, listTestCases: Seq[TestCase]): Unit = {
    val input = fileToString(new File(testCase.inputFile))
    val (comments, code) = splitCommentsAndCodes(input)
    val queries = getQueries(code, comments)
    val settings = getSparkSettings(comments)

    val dbmsConfig = comments.filter(_.startsWith(CrossDbmsQueryTestSuite.ONLY_IF_ARG))
      .map(_.substring(CrossDbmsQueryTestSuite.ONLY_IF_ARG.length))
    // If `--ONLY_IF` is found, check if the DBMS being used is allowed.
    if (!dbmsConfig.contains(crossDbmsToGenerateGoldenFiles)) {
      log.info(s"This test case (${testCase.name}) is ignored because it indicates that it is " +
        "SPARK_ONLY")
    } else if (regenerateGoldenFiles) {
      runQueries(queries, testCase, settings.toImmutableArraySeq)
    } else {
      val configSets = getSparkConfigDimensions(comments)
      runQueriesWithSparkConfigDimensions(
        queries, testCase, settings, configSets)
    }
  }

  override protected def runQueries(
      queries: Seq[String],
      testCase: TestCase,
      sparkConfigSet: Seq[(String, String)]): Unit = {
    val localSparkSession = spark.newSession()
    var runner: Option[SQLQueryTestRunner] = None

    val outputs: Seq[QueryTestOutput] = queries.map { sql =>
      val output = {
        // Use the runner when generating golden files, and Spark when running the test against
        // the already generated golden files.
        if (regenerateGoldenFiles) {
          val connectionUrl = Option(customConnectionUrl).filter(_.nonEmpty)
          runner = runner.orElse(Some(getConnection(connectionUrl))
          try {
            // Either of the below two lines can error. If we go into the catch statement, then it
            // is likely one of the following scenarios:
            // 1. Error thrown in Spark analysis:
            //    a. The query is incompatible with either Spark and the other DBMS.
            //    b. There is an issue with the schema in Spark.
            //    c. The error is expected - it errors on both systems, but only the Spark error
            //       will be printed to the golden file, because it errors first.
            // 2. Error thrown in other DBMS execution:
            //    a. The query is either incompatible between Spark and the other DBMS
            //    b. Some test table/view is not created on the other DBMS.
            val sparkDf = localSparkSession.sql(sql)
            val output = runner.map(_.runQuery(sql)).get
            // Use Spark analyzed plan to check if the query result is already semantically sorted.
            if (isSemanticallySorted(sparkDf.queryExecution.analyzed)) {
              output
            } else {
              // Sort the answer manually if it isn't sorted.
              output.sorted
            }
          } catch {
            case NonFatal(e) => Seq(e.getClass.getName, e.getMessage)
          }
        } else {
          // Use Spark.
          // Execute the list of set operation in order to add the desired configs
          val setOperations = sparkConfigSet.map { case (key, value) => s"set $key=$value" }
          setOperations.foreach(localSparkSession.sql)
          val (_, output) = handleExceptions(
            getNormalizedQueryExecutionResult(localSparkSession, sql))
          output
        }
      }
      ExecutionOutput(
        sql = sql,
        // Don't care about the schema for this test. Only care about correctness.
        schema = None,
        output = normalizeTestResults(output.mkString("\n")))
    }
    if (runner.isDefined) {
      runner.foreach(_.cleanUp())
      runner = None
    }

    if (regenerateGoldenFiles) {
      val goldenOutput = {
        s"-- Automatically generated by ${getClass.getSimpleName} with " +
          s"$crossDbmsToGenerateGoldenFiles\n" + outputs.mkString("\n\n\n") + "\n"
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
    file.getAbsolutePath.replace(customInputFilePath, goldenFilePath) + ".out"
  }

  override lazy val listTestCases: Seq[TestCase] = {
    listFilesRecursively(new File(customInputFilePath)).flatMap { file =>
      val resultFile = resultFileForInputFile(file)
      val absPath = file.getAbsolutePath
      val testCaseName = absPath.stripPrefix(customInputFilePath).stripPrefix(File.separator)
      RegularTestCase(testCaseName, absPath, resultFile) :: Nil
    }.sortBy(_.name)
  }
}

object CrossDbmsQueryTestSuite {

  final val POSTGRES = "postgres"

  // System argument to indicate a custom connection URL to the reference DBMS.
  private final val REF_DBMS_CONNECTION_URL = "REF_DBMS_CONNECTION_URL"
  // Argument in input files to indicate that golden file should be generated with a reference DBMS
  private final val ONLY_IF_ARG = "--ONLY_IF "
}
