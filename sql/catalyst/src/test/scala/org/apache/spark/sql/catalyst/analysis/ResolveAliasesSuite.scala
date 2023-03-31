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

package org.apache.spark.sql.catalyst.analysis

import java.sql.Date

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan, Project}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType}

class ResolveAliasesSuite extends AnalysisTest {

  private lazy val t1 = LocalRelation("a".attr.int)
  private lazy val t2 = LocalRelation("b".attr.long)

  private def checkAliasName(plan: LogicalPlan, expected: String): Unit = {
    val analyzed = getAnalyzer.execute(plan)
    val actual = analyzed.find(_.isInstanceOf[Project]).get.asInstanceOf[Project]
      .projectList.head.asInstanceOf[Alias].name
    assert(actual == expected)
  }

  private def checkAliasName(sql: String, expected: String): Unit = {
    checkAliasName(CatalystSqlParser.parsePlan(sql), expected)
  }

  private def checkSubqueryAliasName(plan: LogicalPlan, expected: String): Unit = {
    val analyzed = getAnalyzer.execute(plan)
    val subqueryExpression = new ArrayBuffer[SubqueryExpression]()
    analyzed.transformExpressions {
      case e: SubqueryExpression =>
        subqueryExpression.append(e)
        e
    }
    assert(subqueryExpression.length == 1)
    val actual = subqueryExpression.head.plan.find(_.isInstanceOf[Project]).get
      .asInstanceOf[Project].projectList.head.asInstanceOf[Alias].name
    assert(actual == expected)
  }

  test("SPARK-33989: test unary expression") {
    checkAliasName(t1.select(Floor(Literal(null))), "FLOOR(NULL)")
    checkAliasName(t1.select(Floor("a".attr)), "FLOOR(a)")
    checkAliasName(t1.select(Floor("a".attr.cast(DoubleType))), "FLOOR(CAST(a AS DOUBLE))")
  }

  test("SPARK-33989: test binary expression") {
    checkAliasName(t1.select(EqualTo("a".attr, Literal(null))), "(a = NULL)")
    checkAliasName(t1.select(EqualTo("a".attr.cast(LongType), Literal(1))),
      "(CAST(a AS BIGINT) = 1)")
    checkAliasName(t1.select(EqualTo("a".attr.cast(LongType), Literal("2").cast(LongType))),
      "(CAST(a AS BIGINT) = CAST(2 AS BIGINT))")
  }

  test("SPARK-33989: test nested expression") {
    checkAliasName(t1.select(StringSplit("a".attr + 1, ",", Literal(-1))),
      "split((a + 1), ,, -1)")
    checkAliasName(t1.select(StringSplit(("a".attr + 1).cast(StringType), ",", Literal(-1))),
      "split(CAST((a + 1) AS STRING), ,, -1)")
  }

  test("SPARK-33989: test subquery expression") {
    checkSubqueryAliasName(
     t1.select(ScalarSubquery(t2.select(EqualTo("b".attr, Literal(null))))),
     "(b = NULL)")
    checkSubqueryAliasName(
      t1.select(ScalarSubquery(t2.select(EqualTo("b".attr.cast(IntegerType), Literal(1))))),
      "(CAST(b AS INT) = 1)")
  }

  test("SPARK-34150: Strip Null literal.sql in resolve alias") {
    checkAliasName(t1.select(Rand(Literal(null))), "rand(NULL)")
    checkAliasName(t1.select(DateSub(Literal(Date.valueOf("2021-01-18")), Literal(null))),
      "date_sub(DATE '2021-01-18', NULL)")
  }

  test("SPARK-40822: Stable derived column aliases") {
    withSQLConf(SQLConf.STABLE_DERIVED_COLUMN_ALIAS_ENABLED.key -> "true") {
      Seq(
        // Literals
        "' 1'" -> "' 1'",
        """"abc"""" -> """"abc"""",
        """'\t\n xyz \t\r'""" -> """'\t\n xyz \t\r'""",
        "1l" -> "1L", "1S" -> "1S",
        "date'-0001-1-28'" -> "DATE'-0001-1-28'",
        "interval 3 year 1 month" -> "INTERVAL3YEAR1MONTH",
        "x'00'" -> "X'00'",
        // Preserve case
        "CAST(1 as tinyint)" -> "CAST(1ASTINYINT)",
        // Brackets
        "getbit(11L, 2 + 1)" -> "getbit(11L,2+1)",
        "string(int(shiftleft(int(-1), 31))+1)" -> "string(int(shiftleft(int(-1),31))+1)",
        "map(1, 'a') [ 5 ]" -> "map(1,'a')[5]",
        // Preserve type
        "CAST('123.a' AS long)" -> "CAST('123.a'ASLONG)",
        // Spaces
        "'1' = 1" -> "'1'=1",
        "upper('a') = upper('A')" -> "upper('a')=upper('A')",
        "FLOOR(5, 0)" -> "FLOOR(5,0)",
        "-1" -> "-1",
        "1 in (1.0)" -> "1IN(1.0)",
        "CAST(null AS ARRAY<String>)" -> "CAST(NULLASARRAY<STRING>)",
        """(
          |  WITH t AS (SELECT 1)
          |  SELECT * FROM t
          |)""".stripMargin -> "(WITHtAS(SELECT1)SELECT*FROMt)",
        // Function invokes
        "like('a', 'Spark_')" -> "like('a','Spark_')",
        "substring('abcdef', 2)" -> "substring('abcdef',2)",
        "split('bcdef', 'e')" -> "split('bcdef','e')",
        "current_timestamp = current_timestamp" -> "CURRENT_TIMESTAMP=CURRENT_TIMESTAMP",
        "'a' || 'b' || 'c'" -> "'a'||'b'||'c'"
      ).foreach { case (selectExpr, expected) =>
        checkAliasName(s"select $selectExpr", expected)
      }
    }
  }
}
