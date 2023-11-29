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
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, DescribeColumn, DescribeRelation, Distinct, Generate, Join, LogicalPlan, Sample, Sort}
import org.apache.spark.sql.catalyst.util.stringToFile
import org.apache.spark.sql.execution.command.{DescribeColumnCommand, DescribeCommandBase}
import org.apache.spark.util.Utils

// scalastyle:off line.size.limit
/**
 * See SQLQueryTestSuite.scala for the more information. This class builds off of that to allow us
 * to generate golden files with other DBMS to perform cross-checking for correctness. Note that the
 * input directory path is currently limited because most, if not all, of our current SQL query
 * tests will not be compatible with other DBMSes. There will be more work in the future, such as
 * some kind of conversion, to increase coverage.
 *
 * You need to have a database server up before running this test.
 * For postgres:
 * 1. On a mac: `brew install postgresql@13`
 * 2. After installing PostgreSQL, start the database server, then create a role named pg with
 * superuser permissions: `createuser -s pg`` OR `psql> CREATE role pg superuser``
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
 *   SPARK_GENERATE_GOLDEN_FILES=1 build/sbt "sql/testOnly org.apache.spark.sql.crossdbms.CrossDbmsQueryTestSuite" -- -z describe.sql"
 * }}}
 *
 * To specify a DBMS to use (the default is postgres):
 * {{{
 *   REF_DBMS=mysql SPARK_GENERATE_GOLDEN_FILES=1 build/sbt "sql/testOnly org.apache.spark.sql.crossdbms.CrossDbmsQueryTestSuite"
 * }}}
 */
// scalastyle:on line.size.limit
class CrossDbmsQueryTestSuite extends SQLQueryTestSuite with Logging {
  private val DEFAULT = "postgres"
  private val DBMS_MAPPING = Map(
    "postgres" ->((connection_url: Option[String]) =>
      JdbcSQLQueryTestRunner(PostgresConnection(connection_url))))

  private val crossDbmsToGenerateGoldenFiles: String = {
    val userInputDbms = System.getenv("REF_DBMS")
    if (userInputDbms.isEmpty) {
      DEFAULT
    } else {
      userInputDbms
    }
  }
  private val customConnectionUrl: String = System.getenv("REF_DBMS_CONNECTION_URL")

  // Currently using a separate directory for this because the current SQL tests we have are highly
  // unlikely to get compatible with the other DBMS.
  override protected val inputFilePath = {
    val originalInputs = new File(baseResourcePath, "inputs").getAbsolutePath
    val x = new File(originalInputs, s"$crossDbmsToGenerateGoldenFiles-crosstest").getAbsolutePath
    log.info("HERE " + originalInputs)
    log.info("HERE " + x)
    x
  }
  override protected val goldenFilePath = new File(
    baseResourcePath, s"$crossDbmsToGenerateGoldenFiles-results").getAbsolutePath

  private def isSemanticallySorted(plan: LogicalPlan): Boolean = plan match {
    case _: Join | _: Aggregate | _: Generate | _: Sample | _: Distinct => false
    case _: DescribeCommandBase
         | _: DescribeColumnCommand
         | _: DescribeRelation
         | _: DescribeColumn => true
    case PhysicalOperation(_, _, Sort(_, true, _)) => true
    case _ => plan.children.iterator.exists(isSemanticallySorted)
  }

  override protected def runQueries(
    queries: Seq[String],
    testCase: TestCase,
    configSet: Seq[(String, String)]): Unit = {
    val localSparkSession = spark.newSession()

    var runner: Option[SQLQueryTestRunner] = None
    val outputs: Seq[QueryTestOutput] = queries.map { sql =>
      val output =
        if (regenerateGoldenFiles) {
          if (runner.isEmpty) {
            val connectionUrl = if (customConnectionUrl.nonEmpty) {
              Some(customConnectionUrl)
            } else {
              None
            }
            runner = Some(DBMS_MAPPING(crossDbmsToGenerateGoldenFiles)(connectionUrl))
          }
          val sparkDf = spark.sql(sql)
          val output = runner.map(_.runQuery(sql)).get
          // Use Spark analyzed plan to check if the query result is already semantically sorted
          val result = if (isSemanticallySorted(sparkDf.queryExecution.analyzed)) {
            output.sorted
          } else {
            // Sort the answer manually if it isn't sorted.
            output
          }
          result
        } else {
          handleExceptions(getNormalizedQueryExecutionResult(localSparkSession, sql))._2
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
    if (ignoreList.exists(t =>
      testCase.name.toLowerCase(Locale.ROOT).contains(t.toLowerCase(Locale.ROOT)))) {
      ignore(testCase.name) {
        /* Do nothing */
      }
    } else {
      testCase match {
        case _: RegularTestCase =>
          // Create a test case to run this case.
          test(testCase.name) {
            runSqlTestCase(testCase, listTestCases)
          }
        case _ =>
          ignore(s"Ignoring test cases that are not [[RegularTestCase]] for now") {
            /* Do nothing */
          }
      }
    }
  }

  override lazy val listTestCases: Seq[TestCase] = {
    listFilesRecursively(new File(inputFilePath)).flatMap { file =>
      var resultFile = file.getAbsolutePath.replace(inputFilePath, goldenFilePath) + ".out"
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
}
