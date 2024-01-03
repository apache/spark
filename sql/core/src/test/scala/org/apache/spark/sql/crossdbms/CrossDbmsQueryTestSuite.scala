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

/**
 * See SQLQueryTestSuite.scala for more information. This suite builds off of that to allow us
 * to generate golden files with other DBMS to perform cross-checking for correctness. It generates
 * another set of golden files. Note that this is not currently run on all SQL input files by
 * default because there is incompatibility between SQL dialects for Spark and the other DBMS.
 *
 * This suite adds a new comment argument, --ONLY_IF. This comment is used to indicate the DBMS for
 * which is eligible for the SQL file. These strings are defined in the companion object. For
 * example, if you have a SQL file named `describe.sql`, and you want to indicate that Postgres is
 * incompatible, add the following comment into the input file:
 * --ONLY_IF spark
 */
abstract class CrossDbmsQueryTestSuite extends SQLQueryTestSuite with Logging {

  /**
   * A String representing the database system being used. A list of these is defined in the
   * companion object.
   */
  protected def crossDbmsToGenerateGoldenFiles: String
  assert(CrossDbmsQueryTestSuite.SUPPORTED_DBMS.contains(crossDbmsToGenerateGoldenFiles))

  /**
   * A custom input file path where SQL tests are located, if desired.
   */
  protected def customInputFilePath: String = inputFilePath

  /**
   * A function taking in an optional custom connection URL, and returns a [[SQLQueryTestRunner]]
   * specific to the database system.
   */
  protected def getConnection: Option[String] => SQLQueryTestRunner

  /**
   * Commands to be run on the DBMS before running queries to generate golden files, such as
   * defining custom functions.
   */
  protected def preprocessingCommands: Seq[String]

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
    if (dbmsConfig.nonEmpty && !dbmsConfig.contains(crossDbmsToGenerateGoldenFiles)) {
      log.info(s"This test case (${testCase.name}) is ignored because it indicates that it is " +
        s"not eligible with $crossDbmsToGenerateGoldenFiles.")
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
          runner = runner.orElse(Some(getConnection(connectionUrl)))
          preprocessingCommands.foreach(command => runner.foreach(_.runQuery(command)))

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
    val goldenFilePath = new File(baseResourcePath,
      CrossDbmsQueryTestSuite.RESULT_DIR).getAbsolutePath
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
  final val SUPPORTED_DBMS = Seq(POSTGRES)

  // Result directory for golden files produced by CrossDBMS. This means that all DBMS will share
  // the same golden files. This is only possible if we make the queries simple enough that they are
  // compatible with all DBMS.
  private final val RESULT_DIR = "crossdbms-results"
  // System argument to indicate a custom connection URL to the reference DBMS.
  private final val REF_DBMS_CONNECTION_URL = "REF_DBMS_CONNECTION_URL"
  // Argument in input files to indicate that the sql file is restricted to certain systems.
  private final val ONLY_IF_ARG = "--ONLY_IF "
}
