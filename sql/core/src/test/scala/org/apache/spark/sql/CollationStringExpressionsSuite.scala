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

import scala.collection.immutable.Seq

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.DataTypeMismatch
import org.apache.spark.sql.catalyst.expressions.{Collation, ConcatWs, ExpressionEvalHelper, FindInSet, Literal, StringInstr, StringRepeat}
import org.apache.spark.sql.catalyst.util.CollationFactory
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{BooleanType, StringType}

class CollationStringExpressionsSuite
  extends QueryTest
  with SharedSparkSession
  with ExpressionEvalHelper {

  test("Support ConcatWs string expression with collation") {
    // Supported collations
    case class ConcatWsTestCase[R](s: String, a: Array[String], c: String, result: R)
    val testCases = Seq(
      ConcatWsTestCase(" ", Array("Spark", "SQL"), "UTF8_BINARY", "Spark SQL")
    )
    testCases.foreach(t => {
      val arrCollated = t.a.map(s => s"collate('$s', '${t.c}')").mkString(", ")
      var query = s"SELECT concat_ws(collate('${t.s}', '${t.c}'), $arrCollated)"
      // Result & data type
      checkAnswer(sql(query), Row(t.result))
      assert(sql(query).schema.fields.head.dataType.sameType(StringType(t.c)))
      // Implicit casting
      val arr = t.a.map(s => s"'$s'").mkString(", ")
      query = s"SELECT concat_ws(collate('${t.s}', '${t.c}'), $arr)"
      checkAnswer(sql(query), Row(t.result))
      assert(sql(query).schema.fields.head.dataType.sameType(StringType(t.c)))
      query = s"SELECT concat_ws('${t.s}', $arrCollated)"
      checkAnswer(sql(query), Row(t.result))
      assert(sql(query).schema.fields.head.dataType.sameType(StringType(t.c)))
    })
    // Unsupported collations
    case class ConcatWsTestFail(s: String, a: Array[String], c: String)
    val failCases = Seq(
      ConcatWsTestFail(" ", Array("ABC", "%b%"), "UTF8_BINARY_LCASE"),
      ConcatWsTestFail(" ", Array("ABC", "%B%"), "UNICODE"),
      ConcatWsTestFail(" ", Array("ABC", "%b%"), "UNICODE_CI")
    )
    failCases.foreach(t => {
      val arrCollated = t.a.map(s => s"collate('$s', '${t.c}')").mkString(", ")
      val query = s"SELECT concat_ws(collate('${t.s}', '${t.c}'), $arrCollated)"
      val unsupportedCollation = intercept[AnalysisException] { sql(query) }
      assert(unsupportedCollation.getErrorClass === "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE")
    })
    // Collation mismatch
    val collationMismatch = intercept[AnalysisException] {
      sql("SELECT concat_ws(' ',collate('Spark', 'UTF8_BINARY_LCASE'),collate('SQL', 'UNICODE'))")
    }
    assert(collationMismatch.getErrorClass === "COLLATION_MISMATCH.EXPLICIT")
  }

  test("Support Contains string expression with collation") {
    // Supported collations
    case class ContainsTestCase[R](l: String, r: String, c: String, result: R)
    val testCases = Seq(
      ContainsTestCase("", "", "UTF8_BINARY", true),
      ContainsTestCase("abcde", "C", "UNICODE", false),
      ContainsTestCase("abcde", "FGH", "UTF8_BINARY_LCASE", false),
      ContainsTestCase("abcde", "BCD", "UNICODE_CI", true)
    )
    testCases.foreach(t => {
      val query = s"SELECT contains(collate('${t.l}','${t.c}'),collate('${t.r}','${t.c}'))"
      // Result & data type
      checkAnswer(sql(query), Row(t.result))
      assert(sql(query).schema.fields.head.dataType.sameType(BooleanType))
      // Implicit casting
      checkAnswer(sql(s"SELECT contains(collate('${t.l}','${t.c}'),'${t.r}')"), Row(t.result))
      checkAnswer(sql(s"SELECT contains('${t.l}',collate('${t.r}','${t.c}'))"), Row(t.result))
    })
    // Collation mismatch
    val collationMismatch = intercept[AnalysisException] {
      sql("SELECT contains(collate('abcde','UTF8_BINARY_LCASE'),collate('C','UNICODE_CI'))")
    }
    assert(collationMismatch.getErrorClass === "COLLATION_MISMATCH.EXPLICIT")
  }

  test("INSTR check result on explicitly collated strings") {
    def testInStr(str: String, substr: String, collationId: Integer, expected: Integer): Unit = {
      val string = Literal.create(str, StringType(collationId))
      val substring = Literal.create(substr, StringType(collationId))

      checkEvaluation(StringInstr(string, substring), expected)
    }

    var collationId = CollationFactory.collationNameToId("UTF8_BINARY")
    testInStr("aaads", "Aa", collationId, 0)
    testInStr("aaaDs", "de", collationId, 0)
    testInStr("aaads", "ds", collationId, 4)
    testInStr("xxxx", "", collationId, 1)
    testInStr("", "xxxx", collationId, 0)
    // scalastyle:off
    testInStr("test大千世界X大千世界", "大千", collationId, 5)
    testInStr("test大千世界X大千世界", "界X", collationId, 8)
    // scalastyle:on

    collationId = CollationFactory.collationNameToId("UTF8_BINARY_LCASE")
    testInStr("aaads", "Aa", collationId, 1)
    testInStr("aaaDs", "de", collationId, 0)
    testInStr("aaaDs", "ds", collationId, 4)
    testInStr("xxxx", "", collationId, 1)
    testInStr("", "xxxx", collationId, 0)
    // scalastyle:off
    testInStr("test大千世界X大千世界", "大千", collationId, 5)
    testInStr("test大千世界X大千世界", "界x", collationId, 8)
    // scalastyle:on

    collationId = CollationFactory.collationNameToId("UNICODE")
    testInStr("aaads", "Aa", collationId, 0)
    testInStr("aaads", "aa", collationId, 1)
    testInStr("aaads", "de", collationId, 0)
    testInStr("xxxx", "", collationId, 1)
    testInStr("", "xxxx", collationId, 0)
    // scalastyle:off
    testInStr("test大千世界X大千世界", "界x", collationId, 0)
    testInStr("test大千世界X大千世界", "界X", collationId, 8)
    // scalastyle:on

    collationId = CollationFactory.collationNameToId("UNICODE_CI")
    testInStr("aaads", "AD", collationId, 3)
    testInStr("aaads", "dS", collationId, 4)
    // scalastyle:off
    testInStr("test大千世界X大千世界", "界x", collationId, 8)
    // scalastyle:on
  }

  test("FIND_IN_SET check result on explicitly collated strings") {
    def testFindInSet(word: String, set: String, collationId: Integer, expected: Integer): Unit = {
      val w = Literal.create(word, StringType(collationId))
      val s = Literal.create(set, StringType(collationId))

      checkEvaluation(FindInSet(w, s), expected)
    }

    var collationId = CollationFactory.collationNameToId("UTF8_BINARY")
    testFindInSet("AB", "abc,b,ab,c,def", collationId, 0)
    testFindInSet("abc", "abc,b,ab,c,def", collationId, 1)
    testFindInSet("def", "abc,b,ab,c,def", collationId, 5)
    testFindInSet("d,ef", "abc,b,ab,c,def", collationId, 0)
    testFindInSet("", "abc,b,ab,c,def", collationId, 0)

    collationId = CollationFactory.collationNameToId("UTF8_BINARY_LCASE")
    testFindInSet("a", "abc,b,ab,c,def", collationId, 0)
    testFindInSet("c", "abc,b,ab,c,def", collationId, 4)
    testFindInSet("AB", "abc,b,ab,c,def", collationId, 3)
    testFindInSet("AbC", "abc,b,ab,c,def", collationId, 1)
    testFindInSet("abcd", "abc,b,ab,c,def", collationId, 0)
    testFindInSet("d,ef", "abc,b,ab,c,def", collationId, 0)
    testFindInSet("XX", "xx", collationId, 1)
    testFindInSet("", "abc,b,ab,c,def", collationId, 0)
    // scalastyle:off
    testFindInSet("界x", "test,大千,世,界X,大,千,世界", collationId, 4)
    // scalastyle:on

    collationId = CollationFactory.collationNameToId("UNICODE")
    testFindInSet("a", "abc,b,ab,c,def", collationId, 0)
    testFindInSet("ab", "abc,b,ab,c,def", collationId, 3)
    testFindInSet("Ab", "abc,b,ab,c,def", collationId, 0)
    testFindInSet("d,ef", "abc,b,ab,c,def", collationId, 0)
    testFindInSet("xx", "xx", collationId, 1)
    // scalastyle:off
    testFindInSet("界x", "test,大千,世,界X,大,千,世界", collationId, 0)
    testFindInSet("大", "test,大千,世,界X,大,千,世界", collationId, 5)
    // scalastyle:on

    collationId = CollationFactory.collationNameToId("UNICODE_CI")
    testFindInSet("a", "abc,b,ab,c,def", collationId, 0)
    testFindInSet("C", "abc,b,ab,c,def", collationId, 4)
    testFindInSet("DeF", "abc,b,ab,c,dEf", collationId, 5)
    testFindInSet("DEFG", "abc,b,ab,c,def", collationId, 0)
    testFindInSet("XX", "xx", collationId, 1)
    // scalastyle:off
    testFindInSet("界x", "test,大千,世,界X,大,千,世界", collationId, 4)
    testFindInSet("界x", "test,大千,界Xx,世,界X,大,千,世界", collationId, 5)
    testFindInSet("大", "test,大千,世,界X,大,千,世界", collationId, 5)
    // scalastyle:on
  }

  test("REPEAT check output type on explicitly collated string") {
    def testRepeat(input: String, n: Int, collationId: Int, expected: String): Unit = {
      val s = Literal.create(input, StringType(collationId))
  test("Support StartsWith string expression with collation") {
    // Supported collations
    case class StartsWithTestCase[R](l: String, r: String, c: String, result: R)
    val testCases = Seq(
      StartsWithTestCase("", "", "UTF8_BINARY", true),
      StartsWithTestCase("abcde", "A", "UNICODE", false),
      StartsWithTestCase("abcde", "FGH", "UTF8_BINARY_LCASE", false),
      StartsWithTestCase("abcde", "ABC", "UNICODE_CI", true)
    )
    testCases.foreach(t => {
      val query = s"SELECT startswith(collate('${t.l}','${t.c}'),collate('${t.r}','${t.c}'))"
      // Result & data type
      checkAnswer(sql(query), Row(t.result))
      assert(sql(query).schema.fields.head.dataType.sameType(BooleanType))
      // Implicit casting
      checkAnswer(sql(s"SELECT startswith(collate('${t.l}', '${t.c}'),'${t.r}')"), Row(t.result))
      checkAnswer(sql(s"SELECT startswith('${t.l}', collate('${t.r}', '${t.c}'))"), Row(t.result))
    })
    // Collation mismatch
    val collationMismatch = intercept[AnalysisException] {
      sql("SELECT startswith(collate('abcde', 'UTF8_BINARY_LCASE'),collate('C', 'UNICODE_CI'))")
    }
    assert(collationMismatch.getErrorClass === "COLLATION_MISMATCH.EXPLICIT")
  }

  test("Support EndsWith string expression with collation") {
    // Supported collations
    case class EndsWithTestCase[R](l: String, r: String, c: String, result: R)
    val testCases = Seq(
      EndsWithTestCase("", "", "UTF8_BINARY", true),
      EndsWithTestCase("abcde", "E", "UNICODE", false),
      EndsWithTestCase("abcde", "FGH", "UTF8_BINARY_LCASE", false),
      EndsWithTestCase("abcde", "CDE", "UNICODE_CI", true)
    )
    testCases.foreach(t => {
      val query = s"SELECT endswith(collate('${t.l}', '${t.c}'), collate('${t.r}', '${t.c}'))"
      // Result & data type
      checkAnswer(sql(query), Row(t.result))
      assert(sql(query).schema.fields.head.dataType.sameType(BooleanType))
      // Implicit casting
      checkAnswer(sql(s"SELECT endswith(collate('${t.l}', '${t.c}'),'${t.r}')"), Row(t.result))
      checkAnswer(sql(s"SELECT endswith('${t.l}', collate('${t.r}', '${t.c}'))"), Row(t.result))
    })
    // Collation mismatch
    val collationMismatch = intercept[AnalysisException] {
      sql("SELECT endswith(collate('abcde', 'UTF8_BINARY_LCASE'),collate('C', 'UNICODE_CI'))")
    }
    assert(collationMismatch.getErrorClass === "COLLATION_MISMATCH.EXPLICIT")
  }

    // Not important for this test
    val repeatNum = 2;

    var collationId = CollationFactory.collationNameToId("UTF8_BINARY")
    testRepeat("abc", repeatNum, collationId, "UTF8_BINARY")

    collationId = CollationFactory.collationNameToId("UTF8_BINARY_LCASE")
    testRepeat("abc", repeatNum, collationId, "UTF8_BINARY_LCASE")

    collationId = CollationFactory.collationNameToId("UNICODE")
    testRepeat("abc", repeatNum, collationId, "UNICODE")

    collationId = CollationFactory.collationNameToId("UNICODE_CI")
    testRepeat("abc", repeatNum, collationId, "UNICODE_CI")
  test("Support StringRepeat string expression with collation") {
    // Supported collations
    case class StringRepeatTestCase[R](s: String, n: Int, c: String, result: R)
    val testCases = Seq(
      StringRepeatTestCase("", 1, "UTF8_BINARY", ""),
      StringRepeatTestCase("a", 0, "UNICODE", ""),
      StringRepeatTestCase("XY", 3, "UTF8_BINARY_LCASE", "XYXYXY"),
      StringRepeatTestCase("123", 2, "UNICODE_CI", "123123")
    )
    testCases.foreach(t => {
      val query = s"SELECT repeat(collate('${t.s}', '${t.c}'), ${t.n})"
      // Result & data type
      checkAnswer(sql(query), Row(t.result))
      assert(sql(query).schema.fields.head.dataType.sameType(StringType(t.c)))
    })
  }

  // TODO: Add more tests for other string expressions

}

class CollationStringExpressionsANSISuite extends CollationStringExpressionsSuite {
  override protected def sparkConf: SparkConf =
    super.sparkConf.set(SQLConf.ANSI_ENABLED, true)

  // TODO: If needed, add more tests for other string expressions (with ANSI mode enabled)

}
