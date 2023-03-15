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

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.catalyst.util.stringToFile
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.tags.ExtendedSQLTest

// scalastyle:off line.size.limit
/**
 * This test suite implements end-to-end test cases using golden files for the purposes of
 * exercising the analysis of SQL queries. This is similar to the SQLQueryTestSuite, but the output
 * of each test case for this suite is the string representation of the logical plan returned as
 * output from the analyzer, rather than the result data from executing the query end-to-end.
 *
 * Each case is loaded from a file in "spark/sql/core/src/test/resources/analyzer-tests/inputs".
 * Each case has a golden result file in "spark/sql/core/src/test/resources/analyzer-tests/results".
 *
 * To run the entire test suite:
 * {{{
 *   build/sbt "sql/testOnly org.apache.spark.sql.SQLAnalyzerTestSuite"
 * }}}
 *
 * To run a single test file upon change:
 * {{{
 *   build/sbt "~sql/testOnly org.apache.spark.sql.SQLAnalyzerTestSuite -- -z basic-select.sql"
 * }}}
 *
 * To re-generate golden files for entire suite, run:
 * {{{
 *   SPARK_GENERATE_GOLDEN_FILES=1 build/sbt "sql/testOnly org.apache.spark.sql.SQLAnalyzerTestSuite"
 * }}}
 *
 * To re-generate golden file for a single test, run:
 * {{{
 *   SPARK_GENERATE_GOLDEN_FILES=1 build/sbt "sql/testOnly org.apache.spark.sql.SQLAnalyzerTestSuite -- -z basic-select.sql"
 * }}}
 *
 * The format for input files is simple:
 *  1. A list of SQL queries separated by semicolons by default. If the semicolon cannot effectively
 *     separate the SQL queries in the test file (e.g. bracketed comments), please use
 *     --QUERY-DELIMITER-START and --QUERY-DELIMITER-END. Lines starting with
 *     --QUERY-DELIMITER-START and --QUERY-DELIMITER-END represent the beginning and end of a query,
 *     respectively. Code that is not surrounded by lines that begin with --QUERY-DELIMITER-START
 *     and --QUERY-DELIMITER-END is still separated by semicolons.
 *  2. Lines starting with -- are treated as comments and ignored.
 *  3. Lines starting with --SET are used to specify the configs when running this testing file. You
 *     can set multiple configs in one --SET, using comma to separate them. Or you can use multiple
 *     --SET statements.
 *  4. Lines starting with --IMPORT are used to load queries from another test file.
 *  5. Lines starting with --CONFIG_DIM are used to specify config dimensions of this testing file.
 *     The dimension name is decided by the string after --CONFIG_DIM. For example, --CONFIG_DIM1
 *     belongs to dimension 1. One dimension can have multiple lines, each line representing one
 *     config set (one or more configs, separated by comma). Spark will run this testing file many
 *     times, each time picks one config set from each dimension, until all the combinations are
 *     tried. For example, if dimension 1 has 2 lines, dimension 2 has 3 lines, this testing file
 *     will be run 6 times (cartesian product).
 *
 * For example:
 * {{{
 *   -- this is a comment
 *   select 1, -1;
 *   select current_date;
 * }}}
 *
 * The format for golden result files look roughly like:
 * {{{
 *   -- some header information
 *
 *   -- !query
 *   select 1
 *   -- !result
 *   Project [1 AS 1#x]
 *   +- OneRowRelation
 *
 *
 *   -- !query
 *   ...
 * }}}
 */
// scalastyle:on line.size.limit
@ExtendedSQLTest
class SQLAnalyzerTestSuite extends QueryTest with SharedSparkSession with SQLHelper
  with SQLQueryTestHelper {

  val baseResourcePath = {
    // We use a path based on Spark home for 2 reasons:
    //   1. Maven can't get correct resource directory when resources in other jars.
    //   2. We test subclasses in the hive-thriftserver module.
    getWorkspaceFilePath("sql", "core", "src", "test", "resources", "analyzer-tests").toFile
  }

  val inputFilePath = new File(baseResourcePath, "inputs").getAbsolutePath
  val goldenFilePath = new File(baseResourcePath, "results").getAbsolutePath

  override def sparkConf: SparkConf = super.sparkConf
    // use Java 8 time API to handle negative years properly
    .set(SQLConf.DATETIME_JAVA8API_ENABLED, true)

  // Create all the test cases.
  listTestCases.foreach { testCase =>
    test(testCase.name) {
      runTest(testCase, listTestCases, runQueries)
    }
  }

  lazy val listTestCases: Seq[TestCase] = {
    listFilesRecursively(new File(inputFilePath)).flatMap { file =>
      val resultFile = file.getAbsolutePath.replace(inputFilePath, goldenFilePath) + ".out"
      val absPath = file.getAbsolutePath
      val testCaseName = absPath.stripPrefix(inputFilePath).stripPrefix(File.separator)

      RegularTestCase(testCaseName, absPath, resultFile) :: Nil
    }.sortBy(_.name)
  }

  private def runQueries(
      queries: Seq[String],
      testCase: TestCase,
      configSet: Seq[(String, String)]): Unit = {
    // Create a local SparkSession to have stronger isolation between different test cases.
    // This does not isolate catalog changes.
    val localSparkSession = spark.newSession()

    localSparkSession.conf.set(SQLConf.ANSI_ENABLED.key, false)

    if (configSet.nonEmpty) {
      // Execute the list of set operations in order to add the desired configurations.
      val setOperations = configSet.map { case (key, value) => s"set $key=$value" }
      logInfo(s"Setting configs: ${setOperations.mkString(", ")}")
      setOperations.foreach(localSparkSession.sql)
    }

    // Analyze the SQL queries preparing them for comparison.
    val outputs: Seq[QueryOutput] = queries.map { sql =>
      val (schema, output) = handleExceptions(getNormalizedAnalyzerOutput(localSparkSession, sql))
      // We might need to do some query canonicalization in the future.
      QueryOutput(
        sql = sql,
        schema = schema,
        output = output.mkString("\n").replaceAll("\\s+$", ""))
    }

    if (regenerateGoldenFiles) {
      // Again, we are explicitly not using multi-line string due to stripMargin removing "|".
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

    // This is a temporary workaround for SPARK-28894. The test names are truncated after
    // the last dot due to a bug in SBT. This makes easier to debug via Jenkins test result
    // report. See SPARK-28894.
    // See also SPARK-29127. It is difficult to see the version information in the failed test
    // cases so the version information related to Python was also added.
    val clue: String = s"${testCase.name}${System.lineSeparator()}"

    withClue(clue) {
      readGoldenFileAndCompareResults(testCase.resultFile, outputs)
    }
  }

  /**
   * Analyzes a query and returns the result as (schema of the output, normalized analyzer output).
   */
  protected def getNormalizedAnalyzerOutput(
      session: SparkSession, sql: String): (String, Seq[String]) = {
    val df = session.sql(sql)
    val schema = df.schema.catalogString
    // Get the output, but also get rid of the #1234 expression IDs that show up in plan strings.
    (schema, Seq(replaceNotIncludedMsg(df.queryExecution.analyzed.toString)))
  }
}
