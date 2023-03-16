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
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.util.fileToString
import org.apache.spark.sql.execution.HiveResult.hiveResultString
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.execution.command.{DescribeColumnCommand, DescribeCommandBase}
import org.apache.spark.sql.types.StructType

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
      .replaceAll("Partition Statistics\t\\d+", s"Partition Statistics\t$notIncludedMsg")
      .replaceAll("\\*\\(\\d+\\) ", "*") // remove the WholeStageCodegen codegenStageIds
  }


  /**
   * Analyzes a query and returns the result as (schema of the output, normalized resolved plan
   * tree string representation).
   */
  protected def getNormalizedQueryAnalysisResult(
      session: SparkSession, sql: String): (String, Seq[String]) = {
    val df = session.sql(sql)
    val schema = df.schema.catalogString
    // Get the answer, but also get rid of the #1234 expression IDs that show up in analyzer plans.
    (schema, Seq(replaceNotIncludedMsg(df.queryExecution.analyzed.toString)))
  }

  /** Executes a query and returns the result as (schema of the output, normalized output). */
  protected def getNormalizedQueryExecutionResult(
      session: SparkSession, sql: String): (String, Seq[String]) = {
    // Returns true if the plan is supposed to be sorted.
    def isSorted(plan: LogicalPlan): Boolean = plan match {
      case _: Join | _: Aggregate | _: Generate | _: Sample | _: Distinct => false
      case _: DescribeCommandBase
          | _: DescribeColumnCommand
          | _: DescribeRelation
          | _: DescribeColumn => true
      case PhysicalOperation(_, _, Sort(_, true, _)) => true
      case _ => plan.children.iterator.exists(isSorted)
    }

    val df = session.sql(sql)
    val schema = df.schema.catalogString
    // Get answer, but also get rid of the #1234 expression ids that show up in explain plans
    val answer = SQLExecution.withNewExecutionId(df.queryExecution, Some(sql)) {
      hiveResultString(df.queryExecution.executedPlan).map(replaceNotIncludedMsg)
    }

    // If the output is not pre-sorted, sort it.
    if (isSorted(df.queryExecution.analyzed)) (schema, answer) else (schema, answer.sorted)
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
      case e: SparkThrowable with Throwable if e.getErrorClass != null =>
        (emptySchema, Seq(e.getClass.getName, getMessage(e, format)))
      case a: AnalysisException =>
        // Do not output the logical plan tree which contains expression IDs.
        // Also implement a crude way of masking expression IDs in the error message
        // with a generic pattern "###".
        val msg = if (a.plan.nonEmpty) a.getSimpleMessage else a.getMessage
        (emptySchema, Seq(a.getClass.getName, msg.replaceAll("#\\d+", "#x")))
      case s: SparkException if s.getCause != null =>
        // For a runtime exception, it is hard to match because its message contains
        // information of stage, task ID, etc.
        // To make result matching simpler, here we match the cause of the exception if it exists.
        s.getCause match {
          case e: SparkThrowable with Throwable if e.getErrorClass != null =>
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
  }

  /** Run a test case. */
  protected def runSqlTestCase(
      testCase: TestCase,
      listTestCases: Seq[TestCase],
      runQueries: (
        Seq[String], // queries
          TestCase, // test case
          Seq[(String, String)] // config set
        ) => Unit): Unit = {
    val input = fileToString(new File(testCase.inputFile))

    val (comments, code) = splitCommentsAndCodes(input)

    // If `--IMPORT` found, load code from another test case file, then insert them
    // into the head in this test.
    val importedTestCaseName = comments.filter(_.startsWith("--IMPORT ")).map(_.substring(9))
    val importedCode = importedTestCaseName.flatMap { testCaseName =>
      listTestCases.find(_.name == testCaseName).map { testCase =>
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
      queries
    } else {
      splitWithSemicolon(allCode.toSeq).toSeq
    }

    // List of SQL queries to run
    val queries = tempQueries.map(_.trim).filter(_ != "")
      // Fix misplacement when comment is at the end of the query.
      .map(_.split("\n").filterNot(_.startsWith("--")).mkString("\n")).map(_.trim).filter(_ != "")

    val settingLines = comments.filter(_.startsWith("--SET ")).map(_.substring(6))
    val settings = settingLines.flatMap(_.split(",").map { kv =>
      val (conf, value) = kv.span(_ != '=')
      conf.trim -> value.substring(1).trim
    })

    val regenerateGoldenFiles: Boolean = System.getenv("SPARK_GENERATE_GOLDEN_FILES") == "1"
    if (regenerateGoldenFiles) {
      runQueries(queries, testCase, settings)
    } else {
      // A config dimension has multiple config sets, and a config set has multiple configs.
      // - config dim:     Seq[Seq[(String, String)]]
      //   - config set:   Seq[(String, String)]
      //     - config:     (String, String))
      // We need to do cartesian product for all the config dimensions, to get a list of
      // config sets, and run the query once for each config set.
      val configDimLines = comments.filter(_.startsWith("--CONFIG_DIM")).map(_.substring(12))
      val configDims = configDimLines.groupBy(_.takeWhile(_ != ' ')).mapValues { lines =>
        lines.map(_.dropWhile(_ != ' ').substring(1)).map(_.split(",").map { kv =>
          val (conf, value) = kv.span(_ != '=')
          conf.trim -> value.substring(1).trim
        }.toSeq).toSeq
      }

      val configSets = configDims.values.foldLeft(Seq(Seq[(String, String)]())) { (res, dim) =>
        dim.flatMap { configSet => res.map(_ ++ configSet) }
      }

      configSets.foreach { configSet =>
        try {
          runQueries(queries, testCase, settings ++ configSet)
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

  protected def splitWithSemicolon(seq: Seq[String]): Array[String] = {
    seq.mkString("\n").split("(?<=[^\\\\]);")
  }

  protected def splitCommentsAndCodes(input: String): (Array[String], Array[String]) =
    input.split("\n").partition { line =>
      val newLine = line.trim
      newLine.startsWith("--") && !newLine.startsWith("--QUERY-DELIMITER")
    }

  /** Returns all the files (not directories) in a directory, recursively. */
  protected def listFilesRecursively(path: File): Seq[File] = {
    val (dirs, files) = path.listFiles().partition(_.isDirectory)
    // Filter out test files with invalid extensions such as temp files created
    // by vi (.swp), Mac (.DS_Store) etc.
    val filteredFiles = files.filter(_.getName.endsWith(validFileExtensions))
    filteredFiles ++ dirs.flatMap(listFilesRecursively)
  }
}
