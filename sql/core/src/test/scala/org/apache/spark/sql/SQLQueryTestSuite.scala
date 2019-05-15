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
import java.util.{Locale, TimeZone}

import scala.util.control.NonFatal

import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.catalyst.util.{fileToString, stringToFile}
import org.apache.spark.sql.execution.HiveResult.hiveResultString
import org.apache.spark.sql.execution.command.{DescribeColumnCommand, DescribeCommandBase}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.StructType

/**
 * End-to-end test cases for SQL queries.
 *
 * Each case is loaded from a file in "spark/sql/core/src/test/resources/sql-tests/inputs".
 * Each case has a golden result file in "spark/sql/core/src/test/resources/sql-tests/results".
 *
 * To run the entire test suite:
 * {{{
 *   build/sbt "sql/test-only *SQLQueryTestSuite"
 * }}}
 *
 * To run a single test file upon change:
 * {{{
 *   build/sbt "~sql/test-only *SQLQueryTestSuite -- -z inline-table.sql"
 * }}}
 *
 * To re-generate golden files for entire suite, run:
 * {{{
 *   SPARK_GENERATE_GOLDEN_FILES=1 build/sbt "sql/test-only *SQLQueryTestSuite"
 * }}}
 *
 * To re-generate golden file for a single test, run:
 * {{{
 *   SPARK_GENERATE_GOLDEN_FILES=1 build/sbt "sql/test-only *SQLQueryTestSuite -- -z describe.sql"
 * }}}
 *
 * The format for input files is simple:
 *  1. A list of SQL queries separated by semicolon.
 *  2. Lines starting with -- are treated as comments and ignored.
 *  3. Lines starting with --SET are used to run the file with the following set of configs.
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
 *   -- !query 0
 *   select 1, -1
 *   -- !query 0 schema
 *   struct<...schema...>
 *   -- !query 0 output
 *   ... data row 1 ...
 *   ... data row 2 ...
 *   ...
 *
 *   -- !query 1
 *   ...
 * }}}
 */
class SQLQueryTestSuite extends QueryTest with SharedSQLContext {

  private val regenerateGoldenFiles: Boolean = System.getenv("SPARK_GENERATE_GOLDEN_FILES") == "1"

  private val baseResourcePath = {
    // If regenerateGoldenFiles is true, we must be running this in SBT and we use hard-coded
    // relative path. Otherwise, we use classloader's getResource to find the location.
    if (regenerateGoldenFiles) {
      java.nio.file.Paths.get("src", "test", "resources", "sql-tests").toFile
    } else {
      val res = getClass.getClassLoader.getResource("sql-tests")
      new File(res.getFile)
    }
  }

  private val inputFilePath = new File(baseResourcePath, "inputs").getAbsolutePath
  private val goldenFilePath = new File(baseResourcePath, "results").getAbsolutePath

  private val validFileExtensions = ".sql"

  /** List of test cases to ignore, in lower cases. */
  private val blackList = Set(
    "blacklist.sql"   // Do NOT remove this one. It is here to test the blacklist functionality.
  )

  // Create all the test cases.
  listTestCases().foreach(createScalaTestCase)

  /** A test case. */
  private case class TestCase(name: String, inputFile: String, resultFile: String)

  /** A single SQL query's output. */
  private case class QueryOutput(sql: String, schema: String, output: String) {
    def toString(queryIndex: Int): String = {
      // We are explicitly not using multi-line string due to stripMargin removing "|" in output.
      s"-- !query $queryIndex\n" +
        sql + "\n" +
        s"-- !query $queryIndex schema\n" +
        schema + "\n" +
         s"-- !query $queryIndex output\n" +
        output
    }
  }

  private def createScalaTestCase(testCase: TestCase): Unit = {
    if (blackList.exists(t =>
        testCase.name.toLowerCase(Locale.ROOT).contains(t.toLowerCase(Locale.ROOT)))) {
      // Create a test case to ignore this case.
      ignore(testCase.name) { /* Do nothing */ }
    } else {
      // Create a test case to run this case.
      test(testCase.name) { runTest(testCase) }
    }
  }

  // For better test coverage, runs the tests on mixed config sets: WHOLESTAGE_CODEGEN_ENABLED
  // and CODEGEN_FACTORY_MODE.
  private lazy val codegenConfigSets = Array(
    ("true", "CODEGEN_ONLY"),
    ("false", "CODEGEN_ONLY"),
    ("false", "NO_CODEGEN")
  ).map { case (wholeStageCodegenEnabled, codegenFactoryMode) =>
    Array(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> wholeStageCodegenEnabled,
      SQLConf.CODEGEN_FACTORY_MODE.key -> codegenFactoryMode)
  }

  /** Run a test case. */
  private def runTest(testCase: TestCase): Unit = {
    val input = fileToString(new File(testCase.inputFile))

    val (comments, code) = input.split("\n").partition(_.startsWith("--"))

    // List of SQL queries to run
    // note: this is not a robust way to split queries using semicolon, but works for now.
    val queries = code.mkString("\n").split("(?<=[^\\\\]);").map(_.trim).filter(_ != "").toSeq

    // When we are regenerating the golden files, we don't need to set any config as they
    // all need to return the same result
    if (regenerateGoldenFiles) {
      runQueries(queries, testCase.resultFile, None)
    } else {
      val configSets = {
        val configLines = comments.filter(_.startsWith("--SET")).map(_.substring(5))
        val configs = configLines.map(_.split(",").map { confAndValue =>
          val (conf, value) = confAndValue.span(_ != '=')
          conf.trim -> value.substring(1).trim
        })

        if (configs.nonEmpty) {
          codegenConfigSets.flatMap { codegenConfig =>
            configs.map { config =>
              config ++ codegenConfig
            }
          }
        } else {
          codegenConfigSets
        }
      }

      configSets.foreach { configSet =>
        try {
          runQueries(queries, testCase.resultFile, Some(configSet))
        } catch {
          case e: Throwable =>
            val configs = configSet.map {
              case (k, v) => s"$k=$v"
            }
            logError(s"Error using configs: ${configs.mkString(",")}")
            throw e
        }
      }
    }
  }

  private def runQueries(
      queries: Seq[String],
      resultFileName: String,
      configSet: Option[Seq[(String, String)]]): Unit = {
    // Create a local SparkSession to have stronger isolation between different test cases.
    // This does not isolate catalog changes.
    val localSparkSession = spark.newSession()
    loadTestData(localSparkSession)

    if (configSet.isDefined) {
      // Execute the list of set operation in order to add the desired configs
      val setOperations = configSet.get.map { case (key, value) => s"set $key=$value" }
      logInfo(s"Setting configs: ${setOperations.mkString(", ")}")
      setOperations.foreach(localSparkSession.sql)
    }
    // Run the SQL queries preparing them for comparison.
    val outputs: Seq[QueryOutput] = queries.map { sql =>
      val (schema, output) = getNormalizedResult(localSparkSession, sql)
      // We might need to do some query canonicalization in the future.
      QueryOutput(
        sql = sql,
        schema = schema.catalogString,
        output = output.mkString("\n").trim)
    }

    if (regenerateGoldenFiles) {
      // Again, we are explicitly not using multi-line string due to stripMargin removing "|".
      val goldenOutput = {
        s"-- Automatically generated by ${getClass.getSimpleName}\n" +
        s"-- Number of queries: ${outputs.size}\n\n\n" +
        outputs.zipWithIndex.map{case (qr, i) => qr.toString(i)}.mkString("\n\n\n") + "\n"
      }
      val resultFile = new File(resultFileName)
      val parent = resultFile.getParentFile
      if (!parent.exists()) {
        assert(parent.mkdirs(), "Could not create directory: " + parent)
      }
      stringToFile(resultFile, goldenOutput)
    }

    // Read back the golden file.
    val expectedOutputs: Seq[QueryOutput] = {
      val goldenOutput = fileToString(new File(resultFileName))
      val segments = goldenOutput.split("-- !query.+\n")

      // each query has 3 segments, plus the header
      assert(segments.size == outputs.size * 3 + 1,
        s"Expected ${outputs.size * 3 + 1} blocks in result file but got ${segments.size}. " +
        s"Try regenerate the result files.")
      Seq.tabulate(outputs.size) { i =>
        QueryOutput(
          sql = segments(i * 3 + 1).trim,
          schema = segments(i * 3 + 2).trim,
          output = segments(i * 3 + 3).trim
        )
      }
    }

    // Compare results.
    assertResult(expectedOutputs.size, s"Number of queries should be ${expectedOutputs.size}") {
      outputs.size
    }

    outputs.zip(expectedOutputs).zipWithIndex.foreach { case ((output, expected), i) =>
      assertResult(expected.sql, s"SQL query did not match for query #$i\n${expected.sql}") {
        output.sql
      }
      assertResult(expected.schema,
        s"Schema did not match for query #$i\n${expected.sql}: $output") {
        output.schema
      }
      assertResult(expected.output, s"Result did not match for query #$i\n${expected.sql}") {
        output.output
      }
    }
  }

  /** Executes a query and returns the result as (schema of the output, normalized output). */
  private def getNormalizedResult(session: SparkSession, sql: String): (StructType, Seq[String]) = {
    // Returns true if the plan is supposed to be sorted.
    def isSorted(plan: LogicalPlan): Boolean = plan match {
      case _: Join | _: Aggregate | _: Generate | _: Sample | _: Distinct => false
      case _: DescribeCommandBase | _: DescribeColumnCommand => true
      case PhysicalOperation(_, _, Sort(_, true, _)) => true
      case _ => plan.children.iterator.exists(isSorted)
    }

    try {
      val df = session.sql(sql)
      val schema = df.schema
      val notIncludedMsg = "[not included in comparison]"
      val clsName = this.getClass.getCanonicalName
      // Get answer, but also get rid of the #1234 expression ids that show up in explain plans
      val answer = hiveResultString(df.queryExecution.executedPlan)
        .map(_.replaceAll("#\\d+", "#x")
        .replaceAll(
          s"Location.*/sql/core/spark-warehouse/$clsName/",
          s"Location ${notIncludedMsg}sql/core/spark-warehouse/")
        .replaceAll("Created By.*", s"Created By $notIncludedMsg")
        .replaceAll("Created Time.*", s"Created Time $notIncludedMsg")
        .replaceAll("Last Access.*", s"Last Access $notIncludedMsg")
        .replaceAll("Partition Statistics\t\\d+", s"Partition Statistics\t$notIncludedMsg")
        .replaceAll("\\*\\(\\d+\\) ", "*"))  // remove the WholeStageCodegen codegenStageIds

      // If the output is not pre-sorted, sort it.
      if (isSorted(df.queryExecution.analyzed)) (schema, answer) else (schema, answer.sorted)

    } catch {
      case a: AnalysisException =>
        // Do not output the logical plan tree which contains expression IDs.
        // Also implement a crude way of masking expression IDs in the error message
        // with a generic pattern "###".
        val msg = if (a.plan.nonEmpty) a.getSimpleMessage else a.getMessage
        (StructType(Seq.empty), Seq(a.getClass.getName, msg.replaceAll("#\\d+", "#x")))
      case NonFatal(e) =>
        // If there is an exception, put the exception class followed by the message.
        (StructType(Seq.empty), Seq(e.getClass.getName, e.getMessage))
    }
  }

  private def listTestCases(): Seq[TestCase] = {
    listFilesRecursively(new File(inputFilePath)).map { file =>
      val resultFile = file.getAbsolutePath.replace(inputFilePath, goldenFilePath) + ".out"
      val absPath = file.getAbsolutePath
      val testCaseName = absPath.stripPrefix(inputFilePath).stripPrefix(File.separator)
      TestCase(testCaseName, absPath, resultFile)
    }
  }

  /** Returns all the files (not directories) in a directory, recursively. */
  private def listFilesRecursively(path: File): Seq[File] = {
    val (dirs, files) = path.listFiles().partition(_.isDirectory)
    // Filter out test files with invalid extensions such as temp files created
    // by vi (.swp), Mac (.DS_Store) etc.
    val filteredFiles = files.filter(_.getName.endsWith(validFileExtensions))
    filteredFiles ++ dirs.flatMap(listFilesRecursively)
  }

  /** Load built-in test tables into the SparkSession. */
  private def loadTestData(session: SparkSession): Unit = {
    import session.implicits._

    (1 to 100).map(i => (i, i.toString)).toDF("key", "value").createOrReplaceTempView("testdata")

    ((Seq(1, 2, 3), Seq(Seq(1, 2, 3))) :: (Seq(2, 3, 4), Seq(Seq(2, 3, 4))) :: Nil)
      .toDF("arraycol", "nestedarraycol")
      .createOrReplaceTempView("arraydata")

    (Tuple1(Map(1 -> "a1", 2 -> "b1", 3 -> "c1", 4 -> "d1", 5 -> "e1")) ::
      Tuple1(Map(1 -> "a2", 2 -> "b2", 3 -> "c2", 4 -> "d2")) ::
      Tuple1(Map(1 -> "a3", 2 -> "b3", 3 -> "c3")) ::
      Tuple1(Map(1 -> "a4", 2 -> "b4")) ::
      Tuple1(Map(1 -> "a5")) :: Nil)
      .toDF("mapcol")
      .createOrReplaceTempView("mapdata")
  }

  private val originalTimeZone = TimeZone.getDefault
  private val originalLocale = Locale.getDefault

  override def beforeAll(): Unit = {
    super.beforeAll()
    // Timezone is fixed to America/Los_Angeles for those timezone sensitive tests (timestamp_*)
    TimeZone.setDefault(TimeZone.getTimeZone("America/Los_Angeles"))
    // Add Locale setting
    Locale.setDefault(Locale.US)
    RuleExecutor.resetMetrics()
  }

  override def afterAll(): Unit = {
    try {
      TimeZone.setDefault(originalTimeZone)
      Locale.setDefault(originalLocale)

      // For debugging dump some statistics about how much time was spent in various optimizer rules
      logWarning(RuleExecutor.dumpTimeSpent())
    } finally {
      super.afterAll()
    }
  }
}
