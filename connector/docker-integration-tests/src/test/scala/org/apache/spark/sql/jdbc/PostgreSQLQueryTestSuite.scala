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
import java.sql.Connection

import scala.util.control.NonFatal

import org.apache.spark.sql.SQLQueryTestHelper
import org.apache.spark.tags.DockerTest

@DockerTest
class PostgreSQLQueryTestSuite extends DockerJDBCIntegrationSuite with SQLQueryTestHelper {
  override val db = new DatabaseOnDocker {
    override val imageName = sys.env.getOrElse("POSTGRES_DOCKER_IMAGE_NAME", "postgres:15.1-alpine")
    override val env = Map(
      "POSTGRES_PASSWORD" -> "rootpass"
    )
    override val usesIpc = false
    override val jdbcPort = 5432

    override def getJdbcUrl(ip: String, port: Int): String =
      s"jdbc:postgresql://$ip:$port/postgres?user=postgres&password=rootpass"
  }

  override def dataPreparation(conn: Connection): Unit = {}

  protected val baseResourcePath = {
    // We use a path based on Spark home for 2 reasons:
    //   1. Maven can't get correct resource directory when resources in other jars.
    //   2. We test subclasses in the hive-thriftserver module.
    getWorkspaceFilePath("sql", "core", "src", "test", "resources", "sql-tests").toFile
  }
  protected val inputFilePath = new File(baseResourcePath, "inputs").getAbsolutePath
  protected val customInputFilePath: String = new File(inputFilePath, "subquery").getAbsolutePath
  protected val goldenFilePath = new File(baseResourcePath, "results").getAbsolutePath

  def listTestCases: Seq[TestCase] = {
    listFilesRecursively(new File(customInputFilePath)).flatMap { file =>
      val resultFile = file.getAbsolutePath.replace(inputFilePath, goldenFilePath) + ".out"
      val absPath = file.getAbsolutePath
      val testCaseName = absPath.stripPrefix(customInputFilePath).stripPrefix(File.separator)
      RegularTestCase(testCaseName, absPath, resultFile) :: Nil
    }.sortBy(_.name)
  }

  def createScalaTestCase(testCase: TestCase): Unit = {
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

  protected def runSqlTestCase(testCase: TestCase, listTestCases: Seq[TestCase]): Unit = {
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
    } else {
      runQueries(queries, testCase, settings.toImmutableArraySeq)
    }
  }


  protected def runQueries(
      queries: Seq[String],
      testCase: TestCase,
      sparkConfigSet: Seq[(String, String)]): Unit = {
    val localSparkSession = spark.newSession()
    var runner: Option[SQLQueryTestRunner] = None

    val outputs: Seq[QueryTestOutput] = queries.map { sql =>
      val output = {
        val setOperations = sparkConfigSet.map { case (key, value) => s"set $key=$value" }
        setOperations.foreach(localSparkSession.sql)
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
    if (runner.isDefined) {
      runner.foreach(_.cleanUp())
      runner = None
    }

    readGoldenFileAndCompareResults(testCase.resultFile, outputs, ExecutionOutput)
  }

  listTestCases.foreach(createScalaTestCase)
}
