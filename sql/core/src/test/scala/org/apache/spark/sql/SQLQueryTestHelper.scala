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

import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

import org.apache.spark.{SparkException, SparkThrowable}
import org.apache.spark.ErrorMessageFormat.MINIMAL
import org.apache.spark.SparkThrowableHelper.getMessage
import org.apache.spark.internal.Logging
import org.apache.spark.sql.IntegratedUDFTestUtils.{TestUDF, TestUDTFSet}
import org.apache.spark.sql.catalyst.expressions.{CurrentDate, CurrentTimestampLike, CurrentUser, Literal}
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.util.fileToString
import org.apache.spark.sql.execution.HiveResult.hiveResultString
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.execution.command.{DescribeColumnCommand, DescribeCommandBase}
import org.apache.spark.sql.types.{DateType, StructType, TimestampType}
import org.apache.spark.util.ArrayImplicits.SparkArrayOps

trait SQLQueryTestHelper extends Logging {

  private val notIncludedMsg = "[not included in comparison]"
  private val clsName = this.getClass.getCanonicalName
  protected val emptySchema = StructType(Seq.empty).catalogString

  protected val validFileExtensions = ".sql"

  protected def replaceNotIncludedMsg(line: String): String = {
    line.replaceAll("#\\d+", "#x")
      .replaceAll("plan_id=\\d+", "plan_id=x")
      .replaceAll(
        s"Location.*$clsName/",
        s"Location $notIncludedMsg/{warehouse_dir}/")
      .replaceAll(s"file:[^\\s,]*$clsName", s"file:$notIncludedMsg/{warehouse_dir}")
      .replaceAll("Created By.*", s"Created By $notIncludedMsg")
      .replaceAll("Created Time.*", s"Created Time $notIncludedMsg")
      .replaceAll("Last Access.*", s"Last Access $notIncludedMsg")
      .replaceAll("Owner\t.*", s"Owner\t$notIncludedMsg")
      .replaceAll("Partition Statistics\t\\d+", s"Partition Statistics\t$notIncludedMsg")
      .replaceAll("CTERelationDef \\d+,", s"CTERelationDef xxxx,")
      .replaceAll("CTERelationRef \\d+,", s"CTERelationRef xxxx,")
      .replaceAll("@\\w*,", s"@xxxxxxxx,")
      .replaceAll("\\*\\(\\d+\\) ", "*") // remove the WholeStageCodegen codegenStageIds
  }

  /**
   * Analyzes a query and returns the result as (schema of the output, normalized resolved plan
   * tree string representation).
   */
  protected def getNormalizedQueryAnalysisResult(
      session: SparkSession, sql: String): (String, Seq[String]) = {
    // Note that creating the following DataFrame includes eager execution for commands that create
    // objects such as views. Therefore any following queries that reference these objects should
    // find them in the catalog.
    val df = session.sql(sql)
    val schema = df.schema.catalogString
    val analyzed = df.queryExecution.analyzed
    // Determine if the analyzed plan contains any nondeterministic expressions.
    var deterministic = true
    analyzed.transformAllExpressionsWithSubqueries {
      case expr: CurrentDate =>
        deterministic = false
        expr
      case expr: CurrentTimestampLike =>
        deterministic = false
        expr
      case expr: CurrentUser =>
        deterministic = false
        expr
      case expr: Literal if expr.dataType == DateType || expr.dataType == TimestampType =>
        deterministic = false
        expr
      case expr if !expr.deterministic =>
        deterministic = false
        expr
    }
    if (deterministic) {
      // Perform query analysis, but also get rid of the #1234 expression IDs that show up in the
      // resolved plans.
      (schema, Seq(replaceNotIncludedMsg(analyzed.toString)))
    } else {
      // The analyzed plan is nondeterministic so elide it from the result to keep tests reliable.
      (schema, Seq("[Analyzer test output redacted due to nondeterminism]"))
    }
  }

  /**
   * Uses the Spark logical plan to determine whether the plan is semantically sorted. This is
   * important to make non-sorted queries test cases more deterministic.
   */
  protected def isSemanticallySorted(plan: LogicalPlan): Boolean = plan match {
    case _: Join | _: Aggregate | _: Generate | _: Sample | _: Distinct => false
    case _: DescribeCommandBase
         | _: DescribeColumnCommand
         | _: DescribeRelation
         | _: DescribeColumn => true
    case PhysicalOperation(_, _, Sort(_, true, _, _)) => true
    case _ => plan.children.iterator.exists(isSemanticallySorted)
  }

  /** Executes a query and returns the result as (schema of the output, normalized output). */
  protected def getNormalizedQueryExecutionResult(
      session: SparkSession, sql: String): (String, Seq[String]) = {
    // Returns true if the plan is supposed to be sorted.
    val df = session.sql(sql)
    val schema = df.schema.catalogString
    // Get answer, but also get rid of the #1234 expression ids that show up in explain plans
    val answer = SQLExecution.withNewExecutionId(df.queryExecution, Some(sql)) {
      hiveResultString(df.queryExecution.executedPlan).map(replaceNotIncludedMsg)
    }

    // If the output is not pre-sorted, sort it.
    if (isSemanticallySorted(df.queryExecution.analyzed)) {
      (schema, answer)
    } else {
      (schema, answer.sorted)
    }
  }

  /**
   * This method handles exceptions occurred during query execution as they may need special care
   * to become comparable to the expected output.
   *
   * @param result a function that returns a pair of schema and output
   */
  protected def handleExceptions(result: => (String, Seq[String])): (String, Seq[String]) = {
    val format = MINIMAL
    try {
      result
    } catch {
      case e: SparkThrowable with Throwable if e.getCondition != null =>
        (emptySchema, Seq(e.getClass.getName, getMessage(e, format)))
      case a: AnalysisException =>
        // Do not output the logical plan tree which contains expression IDs.
        // Also implement a crude way of masking expression IDs in the error message
        // with a generic pattern "###".
        (emptySchema, Seq(a.getClass.getName, a.getSimpleMessage.replaceAll("#\\d+", "#x")))
      case s: SparkException if s.getCause != null =>
        // For a runtime exception, it is hard to match because its message contains
        // information of stage, task ID, etc.
        // To make result matching simpler, here we match the cause of the exception if it exists.
        s.getCause match {
          case e: SparkThrowable with Throwable if e.getCondition != null =>
            (emptySchema, Seq(e.getClass.getName, getMessage(e, format)))
          case cause =>
            (emptySchema, Seq(cause.getClass.getName, cause.getMessage))
        }
      case NonFatal(e) =>
        // If there is an exception, put the exception class followed by the message.
        (emptySchema, Seq(e.getClass.getName, e.getMessage))
    }
  }

  /** A test case. */
  protected trait TestCase {
    val name: String
    val inputFile: String
    val resultFile: String
    def asAnalyzerTest(newName: String, newResultFile: String): TestCase
  }

  /**
   * traits that indicate UDF or PgSQL to trigger the code path specific to each. For instance,
   * PgSQL tests require to register some UDF functions.
   */
  protected trait PgSQLTest

  /** Trait that indicates Non-ANSI-related tests with the ANSI mode disabled. */
  protected trait NonAnsiTest

  /** Trait that indicates an analyzer test that shows the analyzed plan string as output. */
  protected trait AnalyzerTest extends TestCase {
    override def asAnalyzerTest(newName: String, newResultFile: String): AnalyzerTest = this
  }

  /** Trait that indicates the default timestamp type is TimestampNTZType. */
  protected trait TimestampNTZTest

  /** Trait that indicates CTE test cases need their create view versions */
  protected trait CTETest

  protected trait UDFTest {
    val udf: TestUDF
  }

  protected trait UDTFSetTest {
    val udtfSet: TestUDTFSet
  }

  protected case class RegularTestCase(
      name: String, inputFile: String, resultFile: String) extends TestCase {
    override def asAnalyzerTest(newName: String, newResultFile: String): TestCase =
      RegularAnalyzerTestCase(newName, inputFile, newResultFile)
  }

  /** An ANSI-related test case. */
  protected case class NonAnsiTestCase(
      name: String, inputFile: String, resultFile: String) extends TestCase with NonAnsiTest {
    override def asAnalyzerTest(newName: String, newResultFile: String): TestCase =
      NonAnsiAnalyzerTestCase(newName, inputFile, newResultFile)
  }

  /** An analyzer test that shows the analyzed plan string as output. */
  protected case class AnalyzerTestCase(
      name: String, inputFile: String, resultFile: String) extends TestCase with AnalyzerTest

  /** A PostgreSQL test case. */
  protected case class PgSQLTestCase(
      name: String, inputFile: String, resultFile: String) extends TestCase with PgSQLTest {
    override def asAnalyzerTest(newName: String, newResultFile: String): TestCase =
      PgSQLAnalyzerTestCase(newName, inputFile, newResultFile)
  }

  /** A UDF test case. */
  protected case class UDFTestCase(
      name: String,
      inputFile: String,
      resultFile: String,
      udf: TestUDF) extends TestCase with UDFTest {
    override def asAnalyzerTest(newName: String, newResultFile: String): TestCase =
      UDFAnalyzerTestCase(newName, inputFile, newResultFile, udf)
  }

  protected case class UDTFSetTestCase(
      name: String,
      inputFile: String,
      resultFile: String,
      udtfSet: TestUDTFSet) extends TestCase with UDTFSetTest {

    override def asAnalyzerTest(newName: String, newResultFile: String): TestCase =
      UDTFSetAnalyzerTestCase(newName, inputFile, newResultFile, udtfSet)
  }

  /** A UDAF test case. */
  protected case class UDAFTestCase(
      name: String,
      inputFile: String,
      resultFile: String,
      udf: TestUDF) extends TestCase with UDFTest {
    override def asAnalyzerTest(newName: String, newResultFile: String): TestCase =
      UDAFAnalyzerTestCase(newName, inputFile, newResultFile, udf)
  }

  /** A UDF PostgreSQL test case. */
  protected case class UDFPgSQLTestCase(
      name: String,
      inputFile: String,
      resultFile: String,
      udf: TestUDF) extends TestCase with UDFTest with PgSQLTest {
    override def asAnalyzerTest(newName: String, newResultFile: String): TestCase =
      UDFPgSQLAnalyzerTestCase(newName, inputFile, newResultFile, udf)
  }

  /** An date time test case with default timestamp as TimestampNTZType */
  protected case class TimestampNTZTestCase(
      name: String, inputFile: String, resultFile: String) extends TestCase with TimestampNTZTest {
    override def asAnalyzerTest(newName: String, newResultFile: String): TestCase =
      TimestampNTZAnalyzerTestCase(newName, inputFile, newResultFile)
  }

  /** A CTE test case with special handling */
  protected case class CTETestCase(name: String, inputFile: String, resultFile: String)
    extends TestCase
      with CTETest {
    override def asAnalyzerTest(newName: String, newResultFile: String): TestCase =
      CTEAnalyzerTestCase(newName, inputFile, newResultFile)
  }

  /** These are versions of the above test cases, but only exercising analysis. */
  protected case class RegularAnalyzerTestCase(
      name: String, inputFile: String, resultFile: String)
    extends AnalyzerTest
  protected case class NonAnsiAnalyzerTestCase(
      name: String, inputFile: String, resultFile: String)
    extends AnalyzerTest with NonAnsiTest
  protected case class PgSQLAnalyzerTestCase(
      name: String, inputFile: String, resultFile: String)
    extends AnalyzerTest with PgSQLTest
  protected case class UDFAnalyzerTestCase(
      name: String, inputFile: String, resultFile: String, udf: TestUDF)
    extends AnalyzerTest with UDFTest
  protected case class UDTFSetAnalyzerTestCase(
      name: String, inputFile: String, resultFile: String, udtfSet: TestUDTFSet)
    extends AnalyzerTest with UDTFSetTest
  protected case class UDAFAnalyzerTestCase(
      name: String, inputFile: String, resultFile: String, udf: TestUDF)
    extends AnalyzerTest with UDFTest
  protected case class UDFPgSQLAnalyzerTestCase(
      name: String, inputFile: String, resultFile: String, udf: TestUDF)
    extends AnalyzerTest with UDFTest with PgSQLTest
  protected case class TimestampNTZAnalyzerTestCase(
      name: String, inputFile: String, resultFile: String)
    extends AnalyzerTest with TimestampNTZTest
  protected case class CTEAnalyzerTestCase(
      name: String, inputFile: String, resultFile: String)
    extends AnalyzerTest with CTETest

  /** A single SQL query's output. */
  trait QueryTestOutput {
    def sql: String
    def schema: Option[String]
    def output: String
    def numSegments: Int
  }

  /** A single SQL query's execution output. */
  case class ExecutionOutput(
      sql: String,
      schema: Option[String],
      output: String) extends QueryTestOutput {
    override def toString: String = {
      // We are explicitly not using multi-line string due to stripMargin removing "|" in output.
      val schemaString = if (schema.nonEmpty) {
        s"-- !query schema\n" + schema.get + "\n"
      } else {
        ""
      }
      s"-- !query\n" +
        sql + "\n" +
        schemaString +
        s"-- !query output\n" +
        output
    }

    override def numSegments: Int = if (schema.isDefined) { 3 } else { 2 }
  }

  /** A single SQL query's analysis results. */
  case class AnalyzerOutput(
      sql: String,
      schema: Option[String],
      output: String) extends QueryTestOutput {
    override def toString: String = {
      // We are explicitly not using multi-line string due to stripMargin removing "|" in output.
      s"-- !query\n" +
        sql + "\n" +
        s"-- !query analysis\n" +
        output
    }
    override def numSegments: Int = 2
  }

  /** Returns all the files (not directories) in a directory, recursively. */
  protected def listFilesRecursively(path: File): Seq[File] = {
    val (dirs, files) = path.listFiles().partition(_.isDirectory)
    // Filter out test files with invalid extensions such as temp files created
    // by vi (.swp), Mac (.DS_Store) etc.
    val filteredFiles = files.filter(_.getName.endsWith(validFileExtensions))
    (filteredFiles ++ dirs.flatMap(listFilesRecursively)).toImmutableArraySeq
  }

  protected def splitCommentsAndCodes(input: String): (Array[String], Array[String]) =
    input.split("\n").partition { line =>
      val newLine = line.trim
      newLine.startsWith("--") && !newLine.startsWith("--QUERY-DELIMITER")
    }

  protected def getQueries(code: Array[String], comments: Array[String],
      allTestCases: Seq[TestCase]): Seq[String] = {
    def splitWithSemicolon(seq: Seq[String]) = {
      seq.mkString("\n").split("(?<=[^\\\\]);")
    }

    // If `--IMPORT` found, load code from another test case file, then insert them
    // into the head in this test.
    val importedTestCaseName = comments.filter(_.startsWith("--IMPORT ")).map(_.substring(9))
    val importedCode = importedTestCaseName.flatMap { testCaseName =>
      allTestCases.find(_.name == testCaseName).map { testCase =>
        val input = fileToString(new File(testCase.inputFile))
        val (_, code) = splitCommentsAndCodes(input)
        code
      }
    }.flatten

    val allCode = importedCode ++ code
    val tempQueries = if (allCode.exists(_.trim.startsWith("--QUERY-DELIMITER"))) {
      // Although the loop is heavy, only used for bracketed comments test.
      val queries = new ArrayBuffer[String]
      val otherCodes = new ArrayBuffer[String]
      var tempStr = ""
      var start = false
      for (c <- allCode) {
        if (c.trim.startsWith("--QUERY-DELIMITER-START")) {
          start = true
          queries ++= splitWithSemicolon(otherCodes.toSeq)
          otherCodes.clear()
        } else if (c.trim.startsWith("--QUERY-DELIMITER-END")) {
          start = false
          queries += s"\n${tempStr.stripSuffix(";")}"
          tempStr = ""
        } else if (start) {
          tempStr += s"\n$c"
        } else {
          otherCodes += c
        }
      }
      if (otherCodes.nonEmpty) {
        queries ++= splitWithSemicolon(otherCodes.toSeq)
      }
      queries.toSeq
    } else {
      splitWithSemicolon(allCode.toImmutableArraySeq).toSeq
    }

    // List of SQL queries to run
    tempQueries.map(_.trim).filter(_ != "")
      // Fix misplacement when comment is at the end of the query.
      .map(_.split("\n").filterNot(_.startsWith("--")).mkString("\n")).map(_.trim).filter(_ != "")
  }

  protected def getSparkSettings(comments: Array[String]): Array[(String, String)] = {
    val settingLines = comments.filter(_.startsWith("--SET ")).map(_.substring(6))
    settingLines.flatMap(_.split(",").map { kv =>
      val (conf, value) = kv.span(_ != '=')
      conf.trim -> value.substring(1).trim
    })
  }

  protected def getSparkConfigDimensions(comments: Array[String]): Seq[Seq[(String, String)]] = {
    // A config dimension has multiple config sets, and a config set has multiple configs.
    // - config dim:     Seq[Seq[(String, String)]]
    //   - config set:   Seq[(String, String)]
    //     - config:     (String, String))
    // We need to do cartesian product for all the config dimensions, to get a list of
    // config sets, and run the query once for each config set.
    val configDimLines = comments.filter(_.startsWith("--CONFIG_DIM")).map(_.substring(12))
    val configDims = configDimLines.groupBy(_.takeWhile(_ != ' ')).view.mapValues { lines =>
      lines.map(_.dropWhile(_ != ' ').substring(1)).map(_.split(",").map { kv =>
        val (conf, value) = kv.span(_ != '=')
        conf.trim -> value.substring(1).trim
      }.toSeq).toSeq
    }

    configDims.values.foldLeft(Seq(Seq[(String, String)]())) { (res, dim) =>
      dim.flatMap { configSet => res.map(_ ++ configSet) }
    }
  }

  /** This is a helper function to normalize non-deterministic Python error stacktraces. */
  def normalizeTestResults(output: String): String = {
    val strippedPythonErrors: String = {
      var traceback = false
      output.split("\n").filter { line: String =>
        if (line == "Traceback (most recent call last):") {
          traceback = true
        } else if (!line.startsWith(" ")) {
          traceback = false
        }
        !traceback
      }.mkString("\n")
    }
    strippedPythonErrors.replaceAll("\\s+$", "")
  }
}
