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
package org.apache.spark.sql.errors

import org.apache.spark.{QueryContext, SparkArithmeticException, SparkConf, SparkThrowable}
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.parser.{NamedParameterContext, ParseException}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class QueryContextSuite extends SharedSparkSession {
  override def sparkConf: SparkConf = super.sparkConf.set(SQLConf.ANSI_ENABLED.key, "true")

  private val ansiConf = "\"" + SQLConf.ANSI_ENABLED.key + "\""

  test("summary of DataFrame context") {
    withSQLConf(SQLConf.STACK_TRACES_IN_DATAFRAME_CONTEXT.key -> "2") {
      val e = intercept[SparkArithmeticException] {
        spark.range(1).select(lit(1) / lit(0)).collect()
      }
      assert(e.getQueryContext.head.summary() ==
        """== DataFrame ==
          |"div" was called from
          |org.apache.spark.sql.errors.QueryContextSuite.$anonfun$new$3(QueryContextSuite.scala:34)
          |org.scalatest.Assertions.intercept(Assertions.scala:749)
          |""".stripMargin)
    }
  }

  test("SQL query context indexes are Unicode code points") {
    val emoji = new String(Character.toChars(0x1F600))

    def contextOf(thunk: => Any): QueryContext = {
      val e = intercept[SparkThrowable] { thunk }
      e.getQueryContext.head
    }

    val literalSql = s"SELECT '$emoji' AS a, DATE 'bad'"
    val literal = contextOf(spark.sql(literalSql).collect())
    val literalStart = literalSql.codePointCount(0, literalSql.indexOf("DATE"))
    assert(literal.fragment() === "DATE 'bad'")
    assert(literal.startIndex() === literalStart)
    assert(literal.stopIndex() === literalSql.codePointCount(0, literalSql.length) - 1)
    val literalCaret = literal.summary().split('\n')(2)
    assert(literalCaret === " " * literalStart + "^" * "DATE 'bad'".length)

    val asciiSql = "SELECT 'x' AS a, DATE 'bad'"
    val ascii = contextOf(spark.sql(asciiSql).collect())
    assert(ascii.fragment() === "DATE 'bad'")
    assert(ascii.startIndex() === asciiSql.indexOf("DATE"))

    val namedSql = "SELECT :p AS a, DATE 'bad'"
    val named = contextOf(spark.sql(namedSql, Map("p" -> emoji)).collect())
    assert(named.fragment() === "DATE 'bad'")
    assert(named.startIndex() === namedSql.indexOf("DATE"))

    val namedAfterSql = s"SELECT :p AS a, '$emoji' AS b, DATE 'bad'"
    val namedAfter = contextOf(spark.sql(namedAfterSql, Map("p" -> 1)).collect())
    assert(namedAfter.fragment() === "DATE 'bad'")
    assert(namedAfter.startIndex() ===
      namedAfterSql.codePointCount(0, namedAfterSql.indexOf("DATE")))

    // A non-empty parameter context still installs the whole-text origin even when
    // the SQL has no markers, so a syntax error observes SparkSqlParser.stopIndex.
    val syntaxSql = s"SELECT '$emoji' AS a, '$emoji' AS b FRUM t"
    val syntaxEx = intercept[ParseException] {
      spark.sessionState.sqlParser.parsePlanWithParameters(
        syntaxSql,
        NamedParameterContext(Map("p" -> Literal(1))))
    }
    assert(syntaxEx.getQueryContext.nonEmpty)
    val syntax = syntaxEx.getQueryContext.head
    assert(syntax.fragment() === syntaxSql)
    assert(syntax.startIndex() === 0)
    assert(syntax.stopIndex() === syntaxSql.codePointCount(0, syntaxSql.length) - 1)

    val execSql = s"SELECT '$emoji' AS a, DATE 'bad'"
    val exec = contextOf(
      spark.sql(s"EXECUTE IMMEDIATE '${execSql.replace("'", "''")}'").collect())
    assert(exec.fragment() === "DATE 'bad'")
    assert(exec.objectType() === "EXECUTE IMMEDIATE")
    assert(exec.startIndex() === execSql.codePointCount(0, execSql.indexOf("DATE")))

    val execSyntaxSql = s"SELECT '$emoji' FRUM t"
    val execSyntax = contextOf(
      spark.sql(s"EXECUTE IMMEDIATE '${execSyntaxSql.replace("'", "''")}'").collect())
    assert(execSyntax.fragment() === execSyntaxSql)
    assert(execSyntax.objectType() === "EXECUTE IMMEDIATE")
    assert(execSyntax.stopIndex() ===
      execSyntaxSql.codePointCount(0, execSyntaxSql.length) - 1)
  }

  test("SPARK-50290: Add a flag to disable DataFrame context") {
    withSQLConf(SQLConf.DATA_FRAME_QUERY_CONTEXT_ENABLED.key -> "false") {
      val df = spark.range(1).select(lit(1) / col("id"))
      checkError(
        exception = intercept[SparkArithmeticException](df.collect()),
        condition = "DIVIDE_BY_ZERO",
        parameters = Map("config" -> ansiConf),
        context = ExpectedContext("", -1, -1)
      )
    }
  }
}
