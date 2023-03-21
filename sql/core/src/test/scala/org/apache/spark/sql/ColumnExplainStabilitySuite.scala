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

import java.io.{ByteArrayOutputStream, File}
import java.nio.charset.StandardCharsets

import org.apache.commons.io.FileUtils

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.functions.{lit, when}
import org.apache.spark.sql.types.IntegerType

/**
 * Check that the output of Column's explain API don't change.
 * If there are explain differences, the error message looks like this:
 *   The output of Column's explain API did not match:
 *   last approved simplified explain: /path/to/column-stability/q1/simplified.txt
 *   last approved extended explain: /path/to/column-stability/q1/extended.txt
 *
 *   explain(false):
 *   [last approved simplified explain]
 *
 *   explain(true):
 *   [last approved extended explain]
 *
 *   actual simplified explain: /path/to/tmp/q1.actual.simplified.txt
 *   actual extended explain: /path/to/tmp/q1.actual.extended.txt
 *
 *   explain(false):
 *   [actual simplified explain]
 *
 *   explain(true):
 *   [actual extended explain]
 *
 *
 * To run the entire test suite:
 * {{{
 *   build/sbt "sql/testOnly *ColumnExplainStabilitySuite"
 * }}}
 *
 * To re-generate golden files for entire suite, run:
 * {{{
 *   SPARK_GENERATE_GOLDEN_FILES=1 build/sbt "sql/testOnly *ColumnExplainStabilitySuite"
 * }}}
 */
class ColumnExplainStabilitySuite extends SparkFunSuite {

  private val SIMPLIFIED_FILE_NAME = "simplified.txt"
  private val EXTENDED_FILE_NAME = "extended.txt"

  protected val baseResourcePath = {
    // use the same way as `SQLQueryTestSuite` to get the resource path
    getWorkspaceFilePath("sql", "core", "src", "test", "resources", "column-stability").toFile
  }

  private def captureStdOut(block: => Unit): String = {
    val capturedOut = new ByteArrayOutputStream()
    Console.withOut(capturedOut)(block)
    capturedOut.toString()
  }

  private def isApproved(
      dir: File, actualSimplified: String, actualExtended: String): Boolean = {
    val simplifiedFile = new File(dir, SIMPLIFIED_FILE_NAME)
    val expectedSimplified = FileUtils.readFileToString(simplifiedFile, StandardCharsets.UTF_8)
    lazy val extendedFile = new File(dir, EXTENDED_FILE_NAME)
    lazy val expectedExtended = FileUtils.readFileToString(extendedFile, StandardCharsets.UTF_8)
    expectedSimplified == actualSimplified && expectedExtended == actualExtended
  }

  /**
   * Save the explain of Column.
   * The resulting file is used by [[checkWithApproved]] to check stability.
   *
   * @param column  the Column
   * @param name    the name of the Column
   */
  private def generateGoldenFile(column: Column, name: String): Unit = {
    val dir = new File(baseResourcePath, name)

    val simplified = captureStdOut(column.explain(false))
    val extended = captureStdOut(column.explain(true))
    val foundMatch = dir.exists() && isApproved(dir, simplified, extended)

    if (!foundMatch) {
      FileUtils.deleteDirectory(dir)
      assert(dir.mkdirs())

      val simplifiedFile = new File(dir, SIMPLIFIED_FILE_NAME)
      FileUtils.writeStringToFile(simplifiedFile, simplified, StandardCharsets.UTF_8)
      val extendedFile = new File(dir, EXTENDED_FILE_NAME)
      FileUtils.writeStringToFile(extendedFile, extended, StandardCharsets.UTF_8)
    }
  }

  private def checkWithApproved(column: Column, name: String): Unit = {
    val dir = new File(baseResourcePath, name)
    val tempDir = FileUtils.getTempDirectory
    val actualSimplified = captureStdOut(column.explain(false))
    val actualExtended = captureStdOut(column.explain(true))
    val foundMatch = isApproved(dir, actualSimplified, actualExtended)

    if (!foundMatch) {
      // show diff with last approved
      val approvedSimplifiedFile = new File(dir, SIMPLIFIED_FILE_NAME)
      val approvedExtendedFile = new File(dir, EXTENDED_FILE_NAME)

      val actualSimplifiedFile = new File(tempDir, s"$name.actual.simplified.txt")
      val actualExtendedFile = new File(tempDir, s"$name.actual.extended.txt")

      val approvedSimplified = FileUtils.readFileToString(
        approvedSimplifiedFile, StandardCharsets.UTF_8)
      val approvedExtended = FileUtils.readFileToString(
        approvedExtendedFile, StandardCharsets.UTF_8)
      // write out for debugging
      FileUtils.writeStringToFile(actualSimplifiedFile, actualSimplified, StandardCharsets.UTF_8)
      FileUtils.writeStringToFile(actualExtendedFile, actualExtended, StandardCharsets.UTF_8)

      fail(
        s"""
           |The output of Column's explain API did not match:
           |last approved simplified explain: ${approvedSimplifiedFile.getAbsolutePath}
           |last approved extended explain: ${approvedExtendedFile.getAbsolutePath}
           |
           |explain(false):
           |$approvedSimplified
           |
           |explain(true):
           |$approvedExtended
           |
           |actual simplified explain: ${actualSimplifiedFile.getAbsolutePath}
           |actual extended explain: ${actualExtendedFile.getAbsolutePath}
           |
           |explain(false):
           |$actualSimplified
           |
           |explain(true):
           |$actualExtended
        """.stripMargin)
    }
  }

  /**
   * Test a `Column.explain`. Depending on the settings this test will either check if the explain
   * matches a golden file or it will create a new golden file.
   */
  def testExplain(column: Column, name: String): Unit = {
    if (regenerateGoldenFiles) {
      generateGoldenFile(column, name)
    } else {
      checkWithApproved(column, name)
    }
  }

  val colA = Column("a")
  val colB = Column("b")
  val colC = Column("c")
  val columns = Seq(
    colA + colB,
    (colA + colB) - Column(Literal(1)),
    colA * Literal(10) / colB % Literal(3),
    (colA - colB).apply(1),
    (colA * colB).unary_-,
    (colA / colB).unary_!,
    colA === Literal(1) || colB =!= Literal(2),
    (colA % colB) > Literal(1) && (colA % colB) < Literal(9) ||
      colC >= Literal(1) && colC <= Literal(8),
    colA <=> Literal(1),
    when(colA === 1, -1).otherwise(0),
    colA.between(Literal(1), Literal(3)),
    colA.isNaN,
    colA.isNull && colB.isNotNull,
    colA.isin(2, 3),
    colA.like("Tom*") || colB.rlike("^P.*$") || colC.ilike("a"),
    colA.withField("b", lit(2)).dropFields("a"),
    colA.substr(2, 5),
    colA.contains("mer") and colB.startsWith("Ari") or colA.endsWith(colC),
    colA.as("add_alias"),
    colA.as("add_alias").as("key" :: "value" :: Nil),
    colA.cast(IntegerType).cast("int"),
    colA.desc.withField("b1", colB.asc),
    colA.desc_nulls_first.withField("b1", colB.asc_nulls_last),
    colA.bitwiseAND(colB.bitwiseOR(colC.bitwiseXOR(Literal(3)))),
    colA.over()
  )

  columns.zipWithIndex.foreach { case (column, idx) =>
    val name = s"column_${idx + 1}"
    test(s"check column explain ($name)") {
      testExplain(column, name)
    }
  }
}
