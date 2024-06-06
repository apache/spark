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
package org.apache.spark.sql.jdbc

import java.io.File
import java.sql.ResultSet

import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLQueryTestHelper
import org.apache.spark.sql.catalyst.util.fileToString

/**
 * This suite builds off of that to allow us to run other DBMS against the SQL test golden files (on
 * which SQLQueryTestSuite generates and tests against) to perform cross-checking for correctness.
 * Note that this is not currently run on all SQL input files by default because there is
 * incompatibility between SQL dialects for Spark and the other DBMS.
 *
 * This suite adds a new comment argument, --ONLY_IF. This comment is used to indicate the DBMS for
 * which is eligible for the SQL file. These strings are defined in the companion object. For
 * example, if you have a SQL file named `describe.sql`, and you want to indicate that Postgres is
 * incompatible, add the following comment into the input file:
 * --ONLY_IF spark
 */
trait CrossDbmsQueryTestSuite extends DockerJDBCIntegrationSuite with SQLQueryTestHelper {

  val DATABASE_NAME: String

  protected val baseResourcePath = {
    // We use a path based on Spark home for 2 reasons:
    //   1. Maven can't get correct resource directory when resources in other jars.
    //   2. We test subclasses in the hive-thriftserver module.
    getWorkspaceFilePath("sql", "core", "src", "test", "resources", "sql-tests").toFile
  }
  protected val inputFilePath = new File(baseResourcePath, "inputs").getAbsolutePath
  protected val customInputFilePath: String
  protected val goldenFilePath = new File(baseResourcePath, "results").getAbsolutePath

  protected def listTestCases: Seq[TestCase] = {
    listFilesRecursively(new File(customInputFilePath)).flatMap { file =>
      val resultFile = file.getAbsolutePath.replace(inputFilePath, goldenFilePath) + ".out"
      val absPath = file.getAbsolutePath
      val testCaseName = absPath.stripPrefix(customInputFilePath).stripPrefix(File.separator)
      RegularTestCase(testCaseName, absPath, resultFile) :: Nil
    }.sortBy(_.name)
  }

  def createScalaTestCase(testCase: TestCase): Unit = {
    testCase match {
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

  protected def runSqlTestCase(testCase: TestCase, listTestCases: Seq[TestCase]): Unit = {
    val input = fileToString(new File(testCase.inputFile))
    val (comments, code) = splitCommentsAndCodes(input)
    val queries = getQueries(code, comments, listTestCases)

    val dbmsConfig = comments.filter(_.startsWith(CrossDbmsQueryTestSuite.ONLY_IF_ARG))
      .map(_.substring(CrossDbmsQueryTestSuite.ONLY_IF_ARG.length))
    // If `--ONLY_IF` is found, check if the DBMS being used is allowed.
    if (dbmsConfig.nonEmpty && !dbmsConfig.contains(DATABASE_NAME)) {
      log.info(s"This test case (${testCase.name}) is ignored because it indicates that it is " +
        s"not eligible with $DATABASE_NAME.")
    } else {
      runQueriesAndCheckAgainstGoldenFile(queries, testCase)
    }
  }

  protected def runQueriesAndCheckAgainstGoldenFile(
      queries: Seq[String], testCase: TestCase): Unit = {
    // The local Spark session is needed because we use Spark analyzed plan to check if the query
    // result is already semantically sorted, below.
    val localSparkSession = spark.newSession()
    val conn = getConnection()
    val stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)

    val outputs: Seq[QueryTestOutput] = queries.map { sql =>
      val output = {
        try {
          val sparkDf = localSparkSession.sql(sql)
          val isResultSet = stmt.execute(sql)
          val rows = ArrayBuffer[Row]()
          if (isResultSet) {
            val rs = stmt.getResultSet
            val metadata = rs.getMetaData
            while (rs.next()) {
              val row = Row.fromSeq((1 to metadata.getColumnCount).map(i => {
                val value = rs.getObject(i)
                if (value == null) {
                  "NULL"
                } else {
                  value
                }
              }))
              rows.append(row)
            }
          }
          val output = rows.map(_.mkString("\t")).toSeq
          if (isSemanticallySorted(sparkDf.queryExecution.analyzed)) {
            output
          } else {
            // Sort the answer manually if it isn't sorted.
            output.sorted
          }
        } catch {
          case NonFatal(e) => Seq(e.getClass.getName, e.getMessage)
        }
      }

      ExecutionOutput(
        sql = sql,
        // Don't care about the schema for this test. Only care about correctness.
        schema = None,
        output = output.mkString("\n"))
    }
    conn.close()

    // Read back the golden files.
    var curSegment = 0
    val expectedOutputs: Seq[QueryTestOutput] = {
      val goldenOutput = fileToString(new File(testCase.resultFile))
      val segments = goldenOutput.split("-- !query.*\n")
      outputs.map { output =>
        val result =
          ExecutionOutput(
            segments(curSegment + 1).trim, // SQL
            None, // Schema
            normalizeTestResults(segments(curSegment + 3))) // Output
        // Assume that the golden file always has all 3 segments.
        curSegment += 3
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
      assertResult(expected.output, s"Result did not match" +
        s" for query #$i\n${expected.sql}") {
        output.output
      }
    }
  }

}

object CrossDbmsQueryTestSuite {

  final val POSTGRES = "postgres"
  // Argument in input files to indicate that the sql file is restricted to certain systems.
  final val ONLY_IF_ARG = "--ONLY_IF "
}
