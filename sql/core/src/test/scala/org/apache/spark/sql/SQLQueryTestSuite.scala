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
import java.net.URI
import java.util.Locale

import org.apache.spark.{SparkConf, TestUtils}
import org.apache.spark.sql.catalyst.expressions.codegen.CodeGenerator
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.catalyst.plans.logical.{Command, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.catalyst.util.{fileToString, stringToFile}
import org.apache.spark.sql.catalyst.util.DateTimeConstants.NANOS_PER_SECOND
import org.apache.spark.sql.execution.WholeStageCodegenExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.TimestampTypes
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.tags.ExtendedSQLTest
import org.apache.spark.util.ArrayImplicits._
import org.apache.spark.util.Utils

// scalastyle:off line.size.limit
/**
 * End-to-end test cases for SQL queries.
 *
 * Each case is loaded from a file in "spark/sql/core/src/test/resources/sql-tests/inputs".
 * Each case has a golden result file in "spark/sql/core/src/test/resources/sql-tests/results".
 *
 * To run the entire test suite:
 * {{{
 *   build/sbt "sql/testOnly org.apache.spark.sql.SQLQueryTestSuite"
 * }}}
 *
 * To run a single test file upon change:
 * {{{
 *   build/sbt "~sql/testOnly org.apache.spark.sql.SQLQueryTestSuite -- -z inline-table.sql"
 * }}}
 *
 * To re-generate golden files for entire suite, run:
 * {{{
 *   SPARK_GENERATE_GOLDEN_FILES=1 build/sbt "sql/testOnly org.apache.spark.sql.SQLQueryTestSuite"
 * }}}
 *
 * To re-generate golden file for a single test, run:
 * {{{
 *   SPARK_GENERATE_GOLDEN_FILES=1 build/sbt "sql/testOnly org.apache.spark.sql.SQLQueryTestSuite -- -z describe.sql"
 * }}}
 *
 * The format for input files is simple:
 *  1. A list of SQL queries separated by semicolons by default. If the semicolon cannot effectively
 *     separate the SQL queries in the test file(e.g. bracketed comments), please use
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
 *   select 1, -1
 *   -- !query schema
 *   struct<...schema...>
 *   -- !query output
 *   ... data row 1 ...
 *   ... data row 2 ...
 *   ...
 *
 *   -- !query
 *   ...
 * }}}
 *
 * Note that UDF tests work differently. After the test files under 'inputs/udf' directory are
 * detected, it creates three test cases:
 *
 *  - Scala UDF test case with a Scalar UDF registered as the name 'udf'.
 *
 *  - Python UDF test case with a Python UDF registered as the name 'udf'
 *    iff Python executable and pyspark are available.
 *
 *  - Scalar Pandas UDF test case with a Scalar Pandas UDF registered as the name 'udf'
 *    iff Python executable, pyspark, pandas and pyarrow are available.
 *
 * Therefore, UDF test cases should have single input and output files but executed by three
 * different types of UDFs. See 'udf/udf-inner-join.sql' as an example.
 *
 * This test suite also implements end-to-end test cases using golden files for the purposes of
 * exercising the analysis of SQL queries. The output of each test case for this suite is the string
 * representation of the logical plan returned as output from the analyzer, rather than the result
 * data from executing the query end-to-end.
 *
 * Each case has a golden result file in "spark/sql/core/src/test/resources/sql-tests/analyzer-results".
 */
// scalastyle:on line.size.limit
@ExtendedSQLTest
class SQLQueryTestSuite extends QueryTest with SharedSparkSession with SQLHelper
    with SQLQueryTestHelper {

  import IntegratedUDFTestUtils._

  protected val baseResourcePath = {
    // We use a path based on Spark home for 2 reasons:
    //   1. Maven can't get correct resource directory when resources in other jars.
    //   2. We test subclasses in the hive-thriftserver module.
    getWorkspaceFilePath("sql", "core", "src", "test", "resources", "sql-tests").toFile
  }

  protected val inputFilePath = new File(baseResourcePath, "inputs").getAbsolutePath
  protected val goldenFilePath = new File(baseResourcePath, "results").getAbsolutePath
  protected val analyzerGoldenFilePath =
    new File(baseResourcePath, "analyzer-results").getAbsolutePath

  protected override def sparkConf: SparkConf = super.sparkConf
    // Fewer shuffle partitions to speed up testing.
    .set(SQLConf.SHUFFLE_PARTITIONS, 4)
    // use Java 8 time API to handle negative years properly
    .set(SQLConf.DATETIME_JAVA8API_ENABLED, true)
    // SPARK-39564: don't print out serde to avoid introducing complicated and error-prone
    // regex magic.
    .set("spark.test.noSerdeInExplain", "true")

  // SPARK-32106 Since we add SQL test 'transform.sql' will use `cat` command,
  // here we need to ignore it.
  private val otherIgnoreList =
    if (TestUtils.testCommandAvailable("/bin/bash")) Nil else Set("transform.sql")
  /** List of test cases to ignore, in lower cases. */
  protected def ignoreList: Set[String] = Set(
    "ignored.sql" // Do NOT remove this one. It is here to test the ignore functionality.
  ) ++ otherIgnoreList

  // Create all the test cases.
  listTestCases.foreach(createScalaTestCase)

  protected def createScalaTestCase(testCase: TestCase): Unit = {
    if (ignoreList.exists(t =>
        testCase.name.toLowerCase(Locale.ROOT).contains(t.toLowerCase(Locale.ROOT)))) {
      // Create a test case to ignore this case.
      ignore(testCase.name) { /* Do nothing */ }
    } else testCase match {
      case udfTestCase: SQLQueryTestSuite#UDFTest
          if udfTestCase.udf.isInstanceOf[TestPythonUDF] && !shouldTestPythonUDFs =>
        ignore(s"${testCase.name} is skipped because " +
          s"[$pythonExec] and/or pyspark were not available.") {
          /* Do nothing */
        }
      case udfTestCase: SQLQueryTestSuite#UDFTest
          if udfTestCase.udf.isInstanceOf[TestScalarPandasUDF] && !shouldTestPandasUDFs =>
        ignore(s"${testCase.name} is skipped because pyspark," +
          s"pandas and/or pyarrow were not available in [$pythonExec].") {
          /* Do nothing */
        }
      case udfTestCase: SQLQueryTestSuite#UDFTest
          if udfTestCase.udf.isInstanceOf[TestGroupedAggPandasUDF] &&
            !shouldTestPandasUDFs =>
        ignore(s"${testCase.name} is skipped because pyspark," +
          s"pandas and/or pyarrow were not available in [$pythonExec].") {
          /* Do nothing */
        }
      case _ =>
        // Create a test case to run this case.
        test(testCase.name) {
          runSqlTestCase(testCase, listTestCases)
        }
    }
  }


  protected def runQueriesWithSparkConfigDimensions(
      queries: Seq[String],
      testCase: TestCase,
      sparkConfigSet: Array[(String, String)],
      sparkConfigDims: Seq[Seq[(String, String)]]): Unit = {
    sparkConfigDims.foreach { configDim =>
      try {
        runQueries(queries, testCase, (sparkConfigSet ++ configDim).toImmutableArraySeq)
      } catch {
        case e: Throwable =>
          val configs = configDim.map {
            case (k, v) => s"$k=$v"
          }
          logError(s"Error using configs: ${configs.mkString(",")}")
          throw e
      }
    }
  }

  /** Run a test case. */
  protected def runSqlTestCase(testCase: TestCase, listTestCases: Seq[TestCase]): Unit = {
    val input = fileToString(new File(testCase.inputFile))
    val (comments, code) = splitCommentsAndCodes(input)
    val queries = getQueries(code, comments, listTestCases)
    val settings = getSparkSettings(comments)

    if (regenerateGoldenFiles) {
      runQueries(queries, testCase, settings.toImmutableArraySeq)
    } else {
      val configSets = getSparkConfigDimensions(comments)
      runQueriesWithSparkConfigDimensions(
        queries, testCase, settings, configSets)
    }
  }

  def hasNoDuplicateColumns(schema: String): Boolean = {
    val columnAndTypes = schema.replaceFirst("^struct<", "").stripSuffix(">").split(",")
    columnAndTypes.size == columnAndTypes.distinct.length
  }

  def expandCTEQueryAndCompareResult(
      session: SparkSession,
      query: String,
      output: ExecutionOutput): Unit = {
    val triggerCreateViewTest = try {
      val logicalPlan: LogicalPlan = session.sessionState.sqlParser.parsePlan(query)
      !logicalPlan.isInstanceOf[Command] &&
      output.schema.get != emptySchema &&
      hasNoDuplicateColumns(output.schema.get)
    } catch {
      case _: ParseException => return
    }

    // For non-command query with CTE, compare the results of selecting from view created on the
    // original query.
    if (triggerCreateViewTest) {
      val createView = s"CREATE temporary VIEW cte_view AS $query"
      val selectFromView = "SELECT * FROM cte_view"
      val dropViewIfExists = "DROP VIEW IF EXISTS cte_view"
      session.sql(createView)
      val (selectViewSchema, selectViewOutput) =
        handleExceptions(getNormalizedQueryExecutionResult(session, selectFromView))
      // Compare results.
      assertResult(
        output.schema.get,
        s"Schema did not match for CTE query and select from its view: \n$output") {
        selectViewSchema
      }
      assertResult(
        output.output,
        s"Result did not match for CTE query and select from its view: \n${output.sql}") {
        selectViewOutput.mkString("\n").replaceAll("\\s+$", "")
      }
      // Drop view.
      session.sql(dropViewIfExists)
    }
  }

  protected def runQueries(
      queries: Seq[String],
      testCase: TestCase,
      sparkConfigSet: Seq[(String, String)]): Unit = {
    // Create a local SparkSession to have stronger isolation between different test cases.
    // This does not isolate catalog changes.
    val localSparkSession = spark.newSession()

    testCase match {
      case udfTestCase: SQLQueryTestSuite#UDFTest =>
        registerTestUDF(udfTestCase.udf, localSparkSession)
      case udtfTestCase: SQLQueryTestSuite#UDTFSetTest =>
        registerTestUDTFs(udtfTestCase.udtfSet, localSparkSession)
      case _ =>
    }

    testCase match {
      case _: SQLQueryTestSuite#PgSQLTest =>
        // booleq/boolne used by boolean.sql
        localSparkSession.udf.register("booleq", (b1: Boolean, b2: Boolean) => b1 == b2)
        localSparkSession.udf.register("boolne", (b1: Boolean, b2: Boolean) => b1 != b2)
        // vol used by boolean.sql and case.sql.
        localSparkSession.udf.register("vol", (s: String) => s)
        localSparkSession.conf.set(SQLConf.ANSI_ENABLED.key, true)
        localSparkSession.conf.set(SQLConf.LEGACY_INTERVAL_ENABLED.key, true)
      case _: SQLQueryTestSuite#NonAnsiTest =>
        localSparkSession.conf.set(SQLConf.ANSI_ENABLED.key, false)
      case _: SQLQueryTestSuite#TimestampNTZTest =>
        localSparkSession.conf.set(SQLConf.TIMESTAMP_TYPE.key,
          TimestampTypes.TIMESTAMP_NTZ.toString)
      case _ =>
        localSparkSession.conf.set(SQLConf.ANSI_ENABLED.key, true)
    }

    if (sparkConfigSet.nonEmpty) {
      // Execute the list of set operation in order to add the desired configs
      val setOperations = sparkConfigSet.map { case (key, value) => s"set $key=$value" }
      logInfo(s"Setting configs: ${setOperations.mkString(", ")}")
      setOperations.foreach(localSparkSession.sql)
    }

    // Run the SQL queries preparing them for comparison.
    val outputs: Seq[QueryTestOutput] = queries.map { sql =>
      testCase match {
        case _: AnalyzerTest =>
          val (_, output) =
            handleExceptions(getNormalizedQueryAnalysisResult(localSparkSession, sql))
          // We do some query canonicalization now.
          AnalyzerOutput(
            sql = sql,
            schema = None,
            output = normalizeTestResults(output.mkString("\n")))
        case _ =>
          val (schema, output) =
            handleExceptions(getNormalizedQueryExecutionResult(localSparkSession, sql))
          // We do some query canonicalization now.
          val executionOutput = ExecutionOutput(
            sql = sql,
            schema = Some(schema),
            output = normalizeTestResults(output.mkString("\n")))
          if (testCase.isInstanceOf[CTETest]) {
            expandCTEQueryAndCompareResult(localSparkSession, sql, executionOutput)
          }
          executionOutput
      }
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
    val clue = testCase match {
      case udfTestCase: SQLQueryTestSuite#UDFTest
          if udfTestCase.udf.isInstanceOf[TestPythonUDF] && shouldTestPythonUDFs =>
        s"${testCase.name}${System.lineSeparator()}Python: $pythonVer${System.lineSeparator()}"
      case udfTestCase: SQLQueryTestSuite#UDFTest
          if udfTestCase.udf.isInstanceOf[TestScalarPandasUDF] && shouldTestPandasUDFs =>
        s"${testCase.name}${System.lineSeparator()}" +
          s"Python: $pythonVer Pandas: $pandasVer PyArrow: $pyarrowVer${System.lineSeparator()}"
      case udfTestCase: SQLQueryTestSuite#UDFTest
          if udfTestCase.udf.isInstanceOf[TestGroupedAggPandasUDF] &&
            shouldTestPandasUDFs =>
        s"${testCase.name}${System.lineSeparator()}" +
          s"Python: $pythonVer Pandas: $pandasVer PyArrow: $pyarrowVer${System.lineSeparator()}"
      case udtfTestCase: SQLQueryTestSuite#UDTFSetTest
          if udtfTestCase.udtfSet.udtfs.forall(_.isInstanceOf[TestPythonUDTF]) &&
            shouldTestPythonUDFs =>
        s"${testCase.name}${System.lineSeparator()}Python: $pythonVer${System.lineSeparator()}"
      case _ =>
        s"${testCase.name}${System.lineSeparator()}"
    }

    withClue(clue) {
      testCase match {
        case _: AnalyzerTest =>
          readGoldenFileAndCompareResults(testCase.resultFile, outputs, AnalyzerOutput)
        case _ =>
          readGoldenFileAndCompareResults(testCase.resultFile, outputs, ExecutionOutput)
      }
    }
  }

  /**
   * Returns the desired file path for results, given the input file. This is implemented as a
   * function because differente Suites extending this class may want their results files with
   * different names or in different locations.
   */
  protected def resultFileForInputFile(file: File): String = {
    file.getAbsolutePath.replace(inputFilePath, goldenFilePath) + ".out"
  }

  protected lazy val listTestCases: Seq[TestCase] = {
    listFilesRecursively(new File(inputFilePath)).flatMap { file =>
      var resultFile = resultFileForInputFile(file)
      var analyzerResultFile =
        file.getAbsolutePath.replace(inputFilePath, analyzerGoldenFilePath) + ".out"
      // JDK-4511638 changes 'toString' result of Float/Double
      // JDK-8282081 changes DataTimeFormatter 'F' symbol
      if (Utils.isJavaVersionAtLeast21) {
        if (new File(resultFile + ".java21").exists()) resultFile += ".java21"
        if (new File(analyzerResultFile + ".java21").exists()) analyzerResultFile += ".java21"
      }
      val absPath = file.getAbsolutePath
      val testCaseName = absPath.stripPrefix(inputFilePath).stripPrefix(File.separator)

      // Create test cases of test types that depend on the input filename.
      val newTestCases: Seq[TestCase] = if (file.getAbsolutePath.startsWith(
        s"$inputFilePath${File.separator}udf${File.separator}postgreSQL")) {
        Seq(TestScalaUDF("udf"), TestPythonUDF("udf"), TestScalarPandasUDF("udf")).map { udf =>
          UDFPgSQLTestCase(
            s"$testCaseName - ${udf.prettyName}", absPath, resultFile, udf)
        }
      } else if (file.getAbsolutePath.startsWith(s"$inputFilePath${File.separator}udf")) {
        Seq(TestScalaUDF("udf"), TestPythonUDF("udf"), TestScalarPandasUDF("udf")).map { udf =>
          UDFTestCase(
            s"$testCaseName - ${udf.prettyName}", absPath, resultFile, udf)
        }
      } else if (file.getAbsolutePath.startsWith(s"$inputFilePath${File.separator}udaf")) {
        Seq(TestGroupedAggPandasUDF("udaf")).map { udf =>
          UDAFTestCase(
            s"$testCaseName - ${udf.prettyName}", absPath, resultFile, udf)
        }
      } else if (file.getAbsolutePath.startsWith(s"$inputFilePath${File.separator}udtf")) {
        Seq(TestUDTFSet(AllTestUDTFs)).map { udtfSet =>
          UDTFSetTestCase(
            s"$testCaseName - Python UDTFs", absPath, resultFile, udtfSet)
        }
      } else if (file.getAbsolutePath.startsWith(s"$inputFilePath${File.separator}postgreSQL")) {
        PgSQLTestCase(testCaseName, absPath, resultFile) :: Nil
      } else if (file.getAbsolutePath.startsWith(s"$inputFilePath${File.separator}nonansi")) {
        NonAnsiTestCase(testCaseName, absPath, resultFile) :: Nil
      } else if (file.getAbsolutePath.startsWith(s"$inputFilePath${File.separator}timestampNTZ")) {
        TimestampNTZTestCase(testCaseName, absPath, resultFile) :: Nil
      } else if (file.getAbsolutePath.startsWith(s"$inputFilePath${File.separator}cte.sql")) {
        CTETestCase(testCaseName, absPath, resultFile) :: Nil
      } else {
        RegularTestCase(testCaseName, absPath, resultFile) :: Nil
      }
      // Also include a copy of each of the above test cases as an analyzer test.
      newTestCases.flatMap { test =>
        test match {
          case _: UDAFTestCase =>
            // Skip creating analyzer test cases for UDAF tests as they are hard to update locally.
            Seq(test)
          case _ =>
            Seq(
              test,
              test.asAnalyzerTest(
                newName = s"${test.name}_analyzer_test",
                newResultFile = analyzerResultFile))
        }
      }
    }.sortBy(_.name)
  }

  /** Load built-in test tables into the SparkSession. */
  protected def createTestTables(session: SparkSession): Unit = {
    import session.implicits._

    // Before creating test tables, deletes warehouse dir
    val f = new File(new URI(conf.warehousePath))
    if (f.exists()) {
      Utils.deleteRecursively(f)
    }

    (1 to 100).map(i => (i, i.toString)).toDF("key", "value")
      .repartition(1)
      .write
      .format("parquet")
      .saveAsTable("testdata")

    ((Seq(1, 2, 3), Seq(Seq(1, 2, 3))) :: (Seq(2, 3, 4), Seq(Seq(2, 3, 4))) :: Nil)
      .toDF("arraycol", "nestedarraycol")
      .write
      .format("parquet")
      .saveAsTable("arraydata")

    (Tuple1(Map(1 -> "a1", 2 -> "b1", 3 -> "c1", 4 -> "d1", 5 -> "e1")) ::
      Tuple1(Map(1 -> "a2", 2 -> "b2", 3 -> "c2", 4 -> "d2")) ::
      Tuple1(Map(1 -> "a3", 2 -> "b3", 3 -> "c3")) ::
      Tuple1(Map(1 -> "a4", 2 -> "b4")) ::
      Tuple1(Map(1 -> "a5")) :: Nil)
      .toDF("mapcol")
      .write
      .format("parquet")
      .saveAsTable("mapdata")

    session
      .read
      .format("csv")
      .options(Map("delimiter" -> "\t", "header" -> "false"))
      .schema("a int, b float")
      .load(testFile("test-data/postgresql/agg.data"))
      .write
      .format("parquet")
      .saveAsTable("aggtest")

    session
      .read
      .format("csv")
      .options(Map("delimiter" -> "\t", "header" -> "false"))
      .schema(
        """
          |unique1 int,
          |unique2 int,
          |two int,
          |four int,
          |ten int,
          |twenty int,
          |hundred int,
          |thousand int,
          |twothousand int,
          |fivethous int,
          |tenthous int,
          |odd int,
          |even int,
          |stringu1 string,
          |stringu2 string,
          |string4 string
        """.stripMargin)
      .load(testFile("test-data/postgresql/onek.data"))
      .write
      .format("parquet")
      .saveAsTable("onek")

    session
      .read
      .format("csv")
      .options(Map("delimiter" -> "\t", "header" -> "false"))
      .schema(
        """
          |unique1 int,
          |unique2 int,
          |two int,
          |four int,
          |ten int,
          |twenty int,
          |hundred int,
          |thousand int,
          |twothousand int,
          |fivethous int,
          |tenthous int,
          |odd int,
          |even int,
          |stringu1 string,
          |stringu2 string,
          |string4 string
        """.stripMargin)
      .load(testFile("test-data/postgresql/tenk.data"))
      .write
      .format("parquet")
      .saveAsTable("tenk1")
  }

  protected def removeTestTables(session: SparkSession): Unit = {
    session.sql("DROP TABLE IF EXISTS testdata")
    session.sql("DROP TABLE IF EXISTS arraydata")
    session.sql("DROP TABLE IF EXISTS mapdata")
    session.sql("DROP TABLE IF EXISTS aggtest")
    session.sql("DROP TABLE IF EXISTS onek")
    session.sql("DROP TABLE IF EXISTS tenk1")
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    createTestTables(spark)
    RuleExecutor.resetMetrics()
    CodeGenerator.resetCompileTime()
    WholeStageCodegenExec.resetCodeGenTime()
  }

  override def afterAll(): Unit = {
    try {
      removeTestTables(spark)

      // For debugging dump some statistics about how much time was spent in various optimizer rules
      logWarning(RuleExecutor.dumpTimeSpent())

      val codeGenTime = WholeStageCodegenExec.codeGenTime.toDouble / NANOS_PER_SECOND
      val compileTime = CodeGenerator.compileTime.toDouble / NANOS_PER_SECOND
      val codegenInfo =
        s"""
           |=== Metrics of Whole-stage Codegen ===
           |Total code generation time: $codeGenTime seconds
           |Total compile time: $compileTime seconds
         """.stripMargin
      logWarning(codegenInfo)
    } finally {
      super.afterAll()
    }
  }

  /**
   * Consumes contents from a single golden file and compares the expected results against the
   * output of running a query.
   */
  def readGoldenFileAndCompareResults(
      resultFile: String,
      outputs: Seq[QueryTestOutput],
      makeOutput: (String, Option[String], String) => QueryTestOutput): Unit = {
    // Read back the golden file.
    val expectedOutputs: Seq[QueryTestOutput] = {
      val goldenOutput = fileToString(new File(resultFile))
      val segments = goldenOutput.split("-- !query.*\n")

      val numSegments = outputs.map(_.numSegments).sum + 1
      assertResult(
        numSegments,
        s"blocks in result file '$resultFile'. Try regenerating the result files.") {
        segments.size
      }
      var curSegment = 0

      outputs.map { output =>
        val result = if (output.numSegments == 3) {
          makeOutput(
            segments(curSegment + 1).trim, // SQL
            Some(segments(curSegment + 2).trim), // Schema
            normalizeTestResults(segments(curSegment + 3))) // Output
        } else {
          makeOutput(
            segments(curSegment + 1).trim, // SQL
            None, // Schema
            normalizeTestResults(segments(curSegment + 2))) // Output
        }
        curSegment += output.numSegments
        result
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
      assertResult(expected.output, s"Result did not match" +
        s" for query #$i\n${expected.sql}") {
        output.output
      }
    }
  }

  test("test splitCommentsAndCodes") {
    {
      // Correctly split comments and codes
      val input =
        """-- Comment 1
          |SELECT * FROM table1;
          |-- Comment 2
          |SELECT * FROM table2;
          |""".stripMargin

      val (comments, codes) = splitCommentsAndCodes(input)
      assert(comments.toSet == Set("-- Comment 1", "-- Comment 2"))
      assert(codes.toSet == Set("SELECT * FROM table1;", "SELECT * FROM table2;"))
    }

    {
      // Handle input with no comments
      val input = "SELECT * FROM table;"
      val (comments, codes) = splitCommentsAndCodes(input)
      assert(comments.isEmpty)
      assert(codes.toSet == Set("SELECT * FROM table;"))
    }

    {
      // Handle input with no codes
      val input =
        """-- Comment 1
          |-- Comment 2
          |""".stripMargin

      val (comments, codes) = splitCommentsAndCodes(input)
      assert(comments.toSet == Set("-- Comment 1", "-- Comment 2"))
      assert(codes.isEmpty)
    }
  }

  test("Test logic for determining whether a query is semantically sorted") {
    withTempView("t1", "t2") {
      spark.sql("CREATE TEMP VIEW t1 AS SELECT * FROM VALUES (1, 1) AS t1(a, b)")
      spark.sql("CREATE TEMP VIEW t2 AS SELECT * FROM VALUES (1, 2) AS t2(a, b)")

      val unsortedSelectQuery = "select * from t1"
      val sortedSelectQuery = "select * from t1 order by a, b"

      val unsortedJoinQuery = "select * from t1 join t2 on t1.a = t2.a"
      val sortedJoinQuery = "select * from t1 join t2 on t1.a = t2.a order by t1.a"

      val unsortedAggQuery = "select a, max(b) from t1 group by a"
      val sortedAggQuery = "select a, max(b) from t1 group by a order by a"

      val unsortedDistinctQuery = "select distinct a from t1"
      val sortedDistinctQuery = "select distinct a from t1 order by a"

      val unsortedWindowQuery = "SELECT a, b, SUM(b) OVER (ORDER BY a) AS cumulative_sum FROM t1;"
      val sortedWindowQuery = "SELECT a, b, SUM(b) OVER (ORDER BY a) AS cumulative_sum FROM " +
        "t1 ORDER BY a, b;"

      assert(!isSemanticallySorted(spark.sql(unsortedSelectQuery).logicalPlan))
      assert(!isSemanticallySorted(spark.sql(unsortedJoinQuery).logicalPlan))
      assert(!isSemanticallySorted(spark.sql(unsortedDistinctQuery).logicalPlan))
      assert(!isSemanticallySorted(spark.sql(unsortedWindowQuery).logicalPlan))
      assert(!isSemanticallySorted(spark.sql(unsortedAggQuery).logicalPlan))
      assert(isSemanticallySorted(spark.sql(sortedSelectQuery).logicalPlan))
      assert(isSemanticallySorted(spark.sql(sortedJoinQuery).logicalPlan))
      assert(isSemanticallySorted(spark.sql(sortedAggQuery).logicalPlan))
      assert(isSemanticallySorted(spark.sql(sortedWindowQuery).logicalPlan))
      assert(isSemanticallySorted(spark.sql(sortedDistinctQuery).logicalPlan))
    }
  }
}
