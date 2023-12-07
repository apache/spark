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
import org.apache.spark.sql.catalyst.util.{fileToString, stringToFile}

// scalastyle:off line.size.limit
/**
 * See SQLQueryTestSuite.scala for more information. This class builds off of that to allow us
 * to generate golden files with other DBMS to perform cross-checking for correctness. Note that
 * this is not currently run on all SQL input files by default because there is incompatibility
 * between Spark SQL and the DBMS SQL. More work will be done to increase coverage, focusing on
 * postgres.
 *
 * You need to have a database server up before running this test.
 * For example, for postgres:
 * 1. Install PostgreSQL.
 *   a. On a mac: `brew install postgresql@13`
 * 2. After installing PostgreSQL, start the database server, then create a role named pg with
 *    superuser permissions: `createuser -s pg`` OR `psql> CREATE role pg superuser``
 *
 * To indicate that the SQL file is eligible for testing with this suite, add the following comment
 * into the input file:
 * {{{
 *   --DBMS_TO_GENERATE_GOLDEN_FILE postgres
 * }}}
 *
 * And then, to run the entire test suite:
 * {{{
 *   build/sbt "sql/testOnly org.apache.spark.sql.crossdbms.CrossDbmsQueryTestSuite"
 * }}}
 *
 * To re-generate golden files for entire suite, run:
 * {{{
 *   SPARK_GENERATE_GOLDEN_FILES=1 build/sbt "sql/testOnly org.apache.spark.sql.crossdbms.CrossDbmsQueryTestSuite"
 * }}}
 *
 * To re-generate golden file for a single test, e.g. `describe.sql`, run:
 * {{{
 *   SPARK_GENERATE_GOLDEN_FILES=1 build/sbt "sql/testOnly org.apache.spark.sql.crossdbms.CrossDbmsQueryTestSuite -- -z describe.sql"
 * }}}
 *
 * To specify a DBMS to use (the default is postgres):
 * {{{
 *   REF_DBMS=postgres SPARK_GENERATE_GOLDEN_FILES=1 build/sbt "sql/testOnly org.apache.spark.sql.crossdbms.CrossDbmsQueryTestSuite"
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

  override protected def runSqlTestCase(testCase: TestCase, listTestCases: Seq[TestCase]): Unit = {
    val input = fileToString(new File(testCase.inputFile))
    val (comments, code) = splitCommentsAndCodes(input)
    val queries = getQueries(code, comments)
    val settings = getSparkSettings(comments)

    val dbmsConfig = comments.filter(_.startsWith(s"--${
      CrossDbmsQueryTestSuite.DBMS_TO_GENERATE_GOLDEN_FILE} ")).map(_.substring(31))
    // If `--DBMS_TO_GENERATE_GOLDEN_FILE` is not found, skip the test.
    if (!dbmsConfig.contains(crossDbmsToGenerateGoldenFiles)) {
      log.info(s"This test case (${testCase.name}) is ignored because it does not indicate " +
        s"testing with $crossDbmsToGenerateGoldenFiles")
      return
    }
    val configSets = getSparkConfigDimensions(comments)
    runQueriesWithSparkConfigDimensions(
      queries, testCase, settings, configSets)
  }

  override protected def runQueries(
      queries: Seq[String],
      testCase: TestCase,
      sparkConfigSet: Seq[(String, String)]): Unit = {
    val localSparkSession = spark.newSession()

    val outputs: Seq[QueryTestOutput] = queries.map { sql =>
      val output = {
        // Execute the list of set operation in order to add the desired configs
        val setOperations = sparkConfigSet.map { case (key, value) => s"set $key=$value" }
        setOperations.foreach(localSparkSession.sql)
        // Use the original SQL query to run on Spark.
        val (_, output) = handleExceptions(
          getNormalizedQueryExecutionResult(localSparkSession, sql))
        output
      }
      ExecutionOutput(
        sql = sql,
        // Don't care about the schema for this test. Only care about correctness.
        schema = None,
        output = normalizeTestResults(output.mkString("\n")))
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
    file.getAbsolutePath.replace(inputFilePath, goldenFilePath) + ".out"
  }

  override lazy val listTestCases: Seq[TestCase] = {
    listFilesRecursively(new File(inputFilePath)).flatMap { file =>
      val resultFile = resultFileForInputFile(file)
      val absPath = file.getAbsolutePath
      val testCaseName = absPath.stripPrefix(inputFilePath).stripPrefix(File.separator)
      RegularTestCase(testCaseName, absPath, resultFile) :: Nil
    }.sortBy(_.name)
  }
}

object CrossDbmsQueryTestSuite {

  // System argument to indicate which reference DBMS is being used.
  private final val REF_DBMS_ARGUMENT = "REF_DBMS"
  // Argument in input files to indicate that golden file should be generated with a reference DBMS
  private final val DBMS_TO_GENERATE_GOLDEN_FILE = "DBMS_TO_GENERATE_GOLDEN_FILE"

  private final val POSTGRES = "postgres"
  private final val SUPPORTED_DBMS = Seq(POSTGRES)
  private final val DEFAULT_DBMS = POSTGRES
}
