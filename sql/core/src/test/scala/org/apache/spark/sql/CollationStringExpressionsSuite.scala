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

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.DataTypeMismatch
import org.apache.spark.sql.catalyst.dsl.expressions.DslExpression
import org.apache.spark.sql.catalyst.expressions.{Collation, ConcatWs, ExpressionEvalHelper, Literal, StringRepeat}
import org.apache.spark.sql.catalyst.util.CollationFactory
import org.apache.spark.sql.internal.{SqlApiConf, SQLConf}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, StringType}

class CollationStringExpressionsSuite
  extends QueryTest
  with SharedSparkSession
  with ExpressionEvalHelper {

  case class CollationTestCase[R](s1: String, s2: String, collation: String, expectedResult: R)
  case class CollationTestFail[R](s1: String, s2: String, collation: String)


  test("Support ConcatWs string expression with Collation") {
    def prepareConcatWs(
        sep: String,
        collation: String,
        inputs: Any*): ConcatWs = {
      val collationId = CollationFactory.collationNameToId(collation)
      val inputExprs = inputs.map(s => Literal.create(s, StringType(collationId)))
      val sepExpr = Literal.create(sep, StringType(collationId))
      ConcatWs(sepExpr +: inputExprs)
    }
    // Supported Collations
    val checks = Seq(
      CollationTestCase("Spark", "SQL", "UTF8_BINARY", "Spark SQL")
    )
    checks.foreach(ct =>
      checkEvaluation(prepareConcatWs(" ", ct.collation, ct.s1, ct.s2), ct.expectedResult)
    )

    // Unsupported Collations
    val fails = Seq(
      CollationTestFail("ABC", "%b%", "UTF8_BINARY_LCASE"),
      CollationTestFail("ABC", "%B%", "UNICODE"),
      CollationTestFail("ABC", "%b%", "UNICODE_CI")
    )
    fails.foreach(ct =>
      assert(prepareConcatWs(" ", ct.collation, ct.s1, ct.s2)
        .checkInputDataTypes() ==
        DataTypeMismatch(
          errorSubClass = "UNEXPECTED_INPUT_TYPE",
          messageParameters = Map(
            "paramIndex" -> "first",
            "requiredType" -> """"STRING"""",
            "inputSql" -> s""""' ' collate ${ct.collation}"""",
            "inputType" -> s""""STRING COLLATE ${ct.collation}""""
          )
        )
      )
    )
  }

  test("REPEAT check output type on explicitly collated string") {
    def testRepeat(expected: String, collationId: Int, input: String, n: Int): Unit = {
      val s = Literal.create(input, StringType(collationId))

      checkEvaluation(Collation(StringRepeat(s, Literal.create(n))).replacement, expected)
    }

    testRepeat("UTF8_BINARY", 0, "abc", 2)
    testRepeat("UTF8_BINARY_LCASE", 1, "abc", 2)
    testRepeat("UNICODE", 2, "abc", 2)
    testRepeat("UNICODE_CI", 3, "abc", 2)
  }

  test("substring check output type on explicitly collated string") {
    val checks = Seq(
      CollationTestCase("Spark", "2", "UTF8_BINARY", "park"),
      CollationTestCase("Spark", "2", "UTF8_BINARY_LCASE", "park")
    )
    checks.foreach(ct => {
      checkAnswer(sql(s"SELECT substr(collate('${ct.s1}', '${ct.collation}'), 2)"),
        Row(ct.expectedResult))
    })
    def testSubstring(
        expected: String,
        collationId: Int,
        input: String,
        pos: Int): Unit = {
      val s = Literal.create(input, StringType(collationId))
      val l = Literal.create(pos, IntegerType)

      checkEvaluation(Collation(s.substring(s, l)).replacement, expected)
    }

    testSubstring("UTF8_BINARY", 0, "abc", 1)
  }

  test("left/right/substr on collated proper string returns proper value") {
    Seq("utf8_binary_lcase", "utf8_binary", "unicode", "unicode_ci").foreach { collationName =>
      checkAnswerAndType(sql(s"select left(collate('Spark', '$collationName'), 2))"), Row("Sp"),
        StringType(CollationFactory.collationNameToId(collationName)))
      checkAnswer(sql(s"select right(collate('Spark', '$collationName'), 2))"), Row("rk"))
      checkAnswer(sql(s"select right('Def' collate $collationName, 1)"), Row("f"))
      checkAnswer(sql(s"select substr('abc' collate $collationName, 2)"), Row("bc"))
      checkAnswer(sql(s"select substr('example' collate $collationName, 0, 2)"), Row("ex"))
      checkAnswer(sql(s"select substr('example' collate $collationName, 1, 2)"), Row("ex"))
      checkAnswer(sql(s"select substr('example' collate $collationName, 0, 7)"), Row("example"))
      checkAnswer(sql(s"select substr('example' collate $collationName, 1, 7)"), Row("example"))
      checkAnswer(sql(s"select substr('example' collate $collationName, 0, 100)"), Row("example"))
      checkAnswer(sql(s"select substr('example' collate $collationName, 1, 100)"), Row("example"))
      checkAnswer(sql(s"select substr('example' collate $collationName, 2, 2)"), Row("xa"))
      checkAnswer(sql(s"select substr('example' collate $collationName, 1, 6)"), Row("exampl"))
      checkAnswer(sql(s"select substr('example' collate $collationName, 2, 100)"), Row("xample"))
      checkAnswer(sql(s"select substr('example' collate $collationName, 0, 0)"), Row(""))
      checkAnswer(sql(s"select substr('example' collate $collationName, 100, 4)"), Row(""))
      checkAnswer(sql(s"select substr('example' collate $collationName, 0, 100)"), Row("example"))
      checkAnswer(sql(s"select substr('example' collate $collationName, 1, 100)"), Row("example"))
      checkAnswer(sql(s"select substr('example' collate $collationName, 2, 100)"), Row("xample"))
      checkAnswer(sql(s"select substr('example' collate $collationName, -3, 2)"), Row("pl"))
      checkAnswer(sql(s"select substr('example' collate $collationName, -100, 4)"), Row(""))
      checkAnswer(sql(s"select substr('example' collate $collationName, -2147483648, 6)"), Row(""))
      checkAnswer(sql(s"select substr(' a世a ' collate $collationName, 2, 3)"), Row("a世a")) // scalastyle:ignore
      checkAnswer(sql(s"select left(' a世a ' collate $collationName, 3)"), Row(" a世")) // scalastyle:ignore
      checkAnswer(sql(s"select right(' a世a ' collate $collationName, 3)"), Row("世a ")) // scalastyle:ignore
      checkAnswer(sql(s"select substr('AaAaAaAa000000' collate $collationName, 2, 3)"), Row("aAa"))
      checkAnswer(sql(s"select left('AaAaAaAa000000' collate $collationName, 3)"), Row("AaA"))
      checkAnswer(sql(s"select right('AaAaAaAa000000' collate $collationName, 3)"), Row("000"))
      checkAnswer(sql(s"select substr('' collate $collationName, 1, 1)"), Row(""))
      checkAnswer(sql(s"select left('' collate $collationName, 1)"), Row(""))
      checkAnswer(sql(s"select right('' collate $collationName, 1)"), Row(""))
      checkAnswer(sql(s"select left('ghi' collate $collationName, 1)"), Row("g"))
    }
  }

  private def checkAnswerAndType(
      frame: DataFrame,
      row: Row,
      stringType: StringType) = {
    checkAnswer(frame, row)
    assert(frame.schema(0).dataType == stringType)
  }

  test("left/right/substr on collated improper string returns proper value") {
    Seq("utf8_binary_lcase", "utf8_binary", "unicode", "unicode_ci").foreach { collationName =>
      checkAnswer(sql(s"select right(null collate $collationName, 1)"), Row(null))
      checkAnswer(sql(s"select substr(null collate $collationName, 1)"), Row(null))
      checkAnswer(sql(s"select left(null collate $collationName, 1)"), Row(null))
    }
  }

  test("left/right/substr on collated improper length and position returns proper value") {
    Seq("utf8_binary_lcase", "utf8_binary", "unicode", "unicode_ci").foreach { collationName =>
      checkAnswer(sql(s"select left(' a世a ' collate $collationName, '3')"), Row(" a世")) // scalastyle:ignore
      checkAnswer(sql(s"select right(' a世a ' collate $collationName, '3')"), Row("世a ")) // scalastyle:ignore
      checkAnswer(sql(s"select right('' collate $collationName, null)"), Row(null))
      checkAnswer(sql(s"select substr('' collate $collationName, null)"), Row(null))
      checkAnswer(sql(s"select substr('' collate $collationName, null, null)"), Row(null))
      checkAnswer(sql(s"select left('' collate $collationName, null)"), Row(null))
    }
  }

  test("left/right/substr on session-collated string returns proper type") {
    Seq("utf8_binary_lcase", "utf8_binary", "unicode", "unicode_ci").foreach { collationName =>
      val collationId = CollationFactory.collationNameToId(collationName)
      withSQLConf(SqlApiConf.DEFAULT_COLLATION -> collationName) {
        assert(sql("select left('abc', 1)").schema(0).dataType == StringType(collationId))
        assert(sql("select right('def', 1)").schema(0).dataType == StringType(collationId))
        assert(sql("select substr('ghi', 1)").schema(0).dataType == StringType(collationId))
      }
    }
  }

  test("left/right/substr on struct fields that are collated strings") {
    Seq(None, Some("utf8_binary_lcase"), Some("utf8_binary"), Some("unicode"), Some("unicode_ci"))
      .foreach { collationNameMaybe =>
        withTable("t") {
          sql("CREATE TABLE t(i STRING, s" +
            s" struct<a: string${collationNameMaybe.map(cn => " collate " + cn).getOrElse("")}>)" +
            s" USING parquet")
          (1 to 5).map(n => "a" + " " * n).foreach { v =>
            sql(s"INSERT OVERWRITE t VALUES ('1', named_struct('a', '$v'))")
          }
          assert(sql(s"SELECT i, left(s.a, 1) FROM t").schema(1).dataType ==
            collationNameMaybe.map(cn =>
              StringType(CollationFactory.collationNameToId(cn))).getOrElse(StringType))
          assert(sql(s"SELECT i, right(s.a, 1) FROM t").schema(1).dataType ==
            collationNameMaybe.map(cn =>
              StringType(CollationFactory.collationNameToId(cn))).getOrElse(StringType))
          assert(sql(s"SELECT i, substr(s.a, 1, 0) FROM t").schema(1).dataType ==
            collationNameMaybe.map(cn =>
              StringType(CollationFactory.collationNameToId(cn))).getOrElse(StringType))
        }
      }
  }

  test("left/right/substr on collated improper string returns proper type") {
    Seq("utf8_binary_lcase", "utf8_binary", "unicode", "unicode_ci").foreach { collationName =>
      val collationId = CollationFactory.collationNameToId(collationName)
      assert(sql(s"select left(null collate $collationName, 1)").schema(0).dataType
        == StringType(collationId))
      assert(sql(s"select right(null collate $collationName, 1)").schema(0).dataType
        == StringType(collationId))
      assert(sql(s"select substr(null collate $collationName, 1)").schema(0).dataType
        == StringType(collationId))
    }
  }
  test("left/right/substr on collated proper string returns proper type") {
    Seq("utf8_binary_lcase", "utf8_binary", "unicode", "unicode_ci").foreach { collationName =>
      val collationId = CollationFactory.collationNameToId(collationName)
      assert(sql(s"select left('hij' collate $collationName, 1)").schema(0).dataType
        == StringType(collationId))
      assert(sql(s"select right('klm' collate $collationName, 1)").schema(0).dataType
        == StringType(collationId))
      assert(sql(s"select substr('nop' collate $collationName, 1)").schema(0).dataType
        == StringType(collationId))
    }
  }

  // TODO: Add more tests for other string expressions

}

class CollationStringExpressionsANSISuite extends CollationRegexpExpressionsSuite {
  override protected def sparkConf: SparkConf =
    super.sparkConf.set(SQLConf.ANSI_ENABLED, true)
}
