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

import org.apache.spark.sql.catalyst.expressions.ExpressionInfo
import org.apache.spark.sql.catalyst.util.{fileToString, stringToFile}
import org.apache.spark.sql.execution.HiveResult.hiveResultString
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.tags.ExtendedSQLTest

/**
 * End-to-end test cases for SQL schemas of expression examples.
 * The golden result file is "spark/sql/core/src/test/resources/sql-functions/output.out".
 *
 * To run the entire test suite:
 * {{{
 *   build/sbt "sql/test-only *ExpressionsSchemaSuite"
 * }}}
 *
 * To re-generate golden files for entire suite, run:
 * {{{
 *   SPARK_GENERATE_GOLDEN_FILES=1 build/sbt "sql/test-only *ExpressionsSchemaSuite"
 * }}}
 *
 * For example:
 * {{{
 *   ...
 *   @ExpressionDescription(
 *     usage = "_FUNC_(str, n) - Returns the string which repeats the given string value n times.",
 *     examples = """
 *       Examples:
 *         > SELECT _FUNC_('123', 2);
 *          123123
 *     """,
 *     since = "1.5.0")
 *   case class StringRepeat(str: Expression, times: Expression)
 *   ...
 * }}}
 *
 * The format for golden result files look roughly like:
 * {{{
 *   ...
 *   -- Class name: org.apache.spark.sql.catalyst.expressions.StringRepeat
 *
 *   -- Function name: repeat
 *   -- !query
 *   SELECT repeat('123', 2)
 *   -- !query schema
 *   struct<repeat(123, 2):string>
 *   ...
 * }}}
 */
@ExtendedSQLTest
class ExpressionsSchemaSuite extends QueryTest with SharedSparkSession {

  private val regenerateGoldenFiles: Boolean = System.getenv("SPARK_GENERATE_GOLDEN_FILES") == "1"

  private val baseResourcePath = {
    // We use a path based on Spark home for 2 reasons:
    //   1. Maven can't get correct resource directory when resources in other jars.
    //   2. We test subclasses in the hive-thriftserver module.
    val sparkHome = {
      assert(sys.props.contains("spark.test.home") ||
        sys.env.contains("SPARK_HOME"), "spark.test.home or SPARK_HOME is not set.")
      sys.props.getOrElse("spark.test.home", sys.env("SPARK_HOME"))
    }

    java.nio.file.Paths.get(sparkHome,
      "sql", "core", "src", "test", "resources", "sql-functions").toFile
  }

  private val resultFile = new File(baseResourcePath, "output.out")

  val ignoreSet = Set(
    // One of examples shows getting the current timestamp
    "org.apache.spark.sql.catalyst.expressions.UnixTimestamp",
    // Random output without a seed
    "org.apache.spark.sql.catalyst.expressions.Rand",
    "org.apache.spark.sql.catalyst.expressions.Randn",
    "org.apache.spark.sql.catalyst.expressions.Shuffle",
    "org.apache.spark.sql.catalyst.expressions.Uuid",
    // The example calls methods that return unstable results.
    "org.apache.spark.sql.catalyst.expressions.CallMethodViaReflection")

  /** A single SQL query's SQL and schema. */
  protected case class QueryOutput(sql: String, schema: String) {
    override def toString: String = {
      // We are explicitly not using multi-line string due to stripMargin removing "|" in output.
      s"-- !query\n" +
        sql + "\n" +
        s"-- !query schema\n" +
        schema
    }
  }

  test("Check schemas for expression examples") {
    val exampleRe = """^(.+);\n(?s)(.+)$""".r
    val funInfos = spark.sessionState.functionRegistry.listFunction().map { funcId =>
      spark.sessionState.catalog.lookupFunctionInfo(funcId)
    }

    val classFunsMap = funInfos.groupBy(_.getClassName)
    val outputBuffer = new ArrayBuffer[String]
    val outputs = new ArrayBuffer[QueryOutput]

    classFunsMap.foreach { kv =>
      val className = kv._1
      if (!ignoreSet.contains(className)) {
        outputBuffer += s"\n\n-- Class name: $className"
        kv._2.foreach { funInfo =>
          outputBuffer += s"\n-- Function name: ${funInfo.getName}"
          val example = funInfo.getExamples

          // If expression exists 'Examples' segment, the first element is 'Examples'. Because
          // this test case is only used to print aliases of expressions for double checking.
          // Therefore, we only need to output the first SQL and its corresponding schema.
          example.split("  > ").take(2).toList.foreach(_ match {
            case exampleRe(sql, expected) =>
              val df = spark.sql(sql)
              val schema = df.schema.catalogString
              val queryOutput = QueryOutput(sql, schema)
              outputBuffer += queryOutput.toString
              outputs += queryOutput
            case _ =>
          })
        }
      }
    }

    if (regenerateGoldenFiles) {
      val goldenOutput = {
        s"-- Automatically generated by ${getClass.getSimpleName}\n" +
        s"-- Number of queries: ${outputs.size}" +
        outputBuffer.mkString("\n")
      }
      val parent = resultFile.getParentFile
      if (!parent.exists()) {
        assert(parent.mkdirs(), "Could not create directory: " + parent)
      }
      stringToFile(resultFile, goldenOutput)
    }

    val expectedOutputs: Seq[QueryOutput] = {
      val goldenOutput = fileToString(resultFile)
      val classSegments = goldenOutput.split("-- Class name: .*\n")
      val functionSegments = classSegments
        .flatMap(_.split("-- Function name: .*\n")).map(_.trim).filter(_ != "")
      val segments = functionSegments.flatMap(_.split("-- !query.*\n")).filter(_ != "")

      // each query has 2 segments, plus the header
      assert(segments.size == outputs.size * 2 + 1,
        s"Expected ${outputs.size * 2 + 1} blocks in result file but got ${segments.size}. " +
          s"Try regenerate the result files.")
      Seq.tabulate(outputs.size) { i =>
        QueryOutput(
          sql = segments(i * 2 + 1).trim,
          schema = segments(i * 2 + 2).trim
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
    }
  }
}
