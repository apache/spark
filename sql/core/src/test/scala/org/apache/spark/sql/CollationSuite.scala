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

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.util.CollationFactory
import org.apache.spark.sql.catalyst.ExtendedAnalysisException
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StringType

class CollationSuite extends QueryTest with SharedSparkSession {
  test("collate returns proper type") {
    Seq("ucs_basic", "ucs_basic_lcase", "unicode", "unicode_ci").foreach { collationName =>
      checkAnswer(sql(s"select 'aaa' collate '$collationName'"), Row("aaa"))
      val collationId = CollationFactory.collationNameToId(collationName)
      assert(sql(s"select 'aaa' collate '$collationName'").schema(0).dataType
        == StringType(collationId))
    }
  }

  test("collation name is case insensitive") {
    Seq("uCs_BasIc", "uCs_baSic_Lcase", "uNicOde", "UNICODE_ci").foreach { collationName =>
      checkAnswer(sql(s"select 'aaa' collate '$collationName'"), Row("aaa"))
      val collationId = CollationFactory.collationNameToId(collationName)
      assert(sql(s"select 'aaa' collate '$collationName'").schema(0).dataType
        == StringType(collationId))
    }
  }

  test("collation expression returns name of collation") {
    Seq("ucs_basic", "ucs_basic_lcase", "unicode", "unicode_ci").foreach { collationName =>
      checkAnswer(
        sql(s"select collation('aaa' collate '$collationName')"), Row(collationName.toUpperCase()))
    }
  }

  test("collate function syntax") {
    assert(sql(s"select collate('aaa', 'ucs_basic')").schema(0).dataType == StringType(0))
    assert(sql(s"select collate('aaa', 'ucs_basic_lcase')").schema(0).dataType == StringType(1))
  }

  test("collate function syntax invalid args") {
    Seq("'aaa','a','b'", "'aaa'", "", "'aaa'").foreach(args => {
      val paramCount = if (args == "") 0 else args.split(',').length.toString
      checkError(
        exception = intercept[AnalysisException] {
          sql(s"select collate($args)")
        },
        errorClass = "WRONG_NUM_ARGS.WITHOUT_SUGGESTION",
        sqlState = "42605",
        parameters = Map(
          "functionName" -> "`collate`",
          "expectedNum" -> "2",
          "actualNum" -> paramCount.toString,
          "docroot" -> "https://spark.apache.org/docs/latest"),
        context = ExpectedContext(
          fragment = s"collate($args)",
          start = 7,
          stop = 15 + args.length)
      )
    })
  }

  test("collate function invalid data type") {
    Seq(
      ("1,'UCS_BASIC'", "INT"),
      ("1.1, 'UCS_BASIC'", "DECIMAL(2,1)"),
      ("true, 'UCS_BASIC'", "BOOLEAN")).foreach(input => {
      val (args, dataType) = input
      checkError(
        exception = intercept[ExtendedAnalysisException] { sql(s"select collate($args)") },
        errorClass = "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE",
        sqlState = "42K09",
        parameters = Map(
          "sqlExpr" -> s"\"collate(${args.split(",")(0)})\"",
          "paramIndex" -> "1",
          "inputSql" -> s"\"${args.split(",")(0)}\"",
          "inputType" -> s"\"$dataType\"",
          "requiredType" -> "\"STRING\""
        ),
        context = ExpectedContext(
          fragment = s"collate($args)",
          start = 7,
          stop = 15 + args.length)
      )
    })
  }

  test("collation expression returns default collation") {
    checkAnswer(sql(s"select collation('aaa')"), Row("UCS_BASIC"))
  }

  test("invalid collation name throws exception") {
    checkError(
      exception = intercept[SparkException] { sql("select 'aaa' collate 'UCS_BASIS'") },
      errorClass = "COLLATION_INVALID_NAME",
      sqlState = "42704",
      parameters = Map("proposal" -> "UCS_BASIC", "collationName" -> "UCS_BASIS"))
  }

  test("equality check respects collation") {
    Seq(
      ("ucs_basic", "aaa", "AAA", false),
      ("ucs_basic", "aaa", "aaa", true),
      ("ucs_basic_lcase", "aaa", "aaa", true),
      ("ucs_basic_lcase", "aaa", "AAA", true),
      ("ucs_basic_lcase", "aaa", "bbb", false),
      ("unicode", "aaa", "aaa", true),
      ("unicode", "aaa", "AAA", false),
      ("unicode_CI", "aaa", "aaa", true),
      ("unicode_CI", "aaa", "AAA", true),
      ("unicode_CI", "aaa", "bbb", false)
    ).foreach {
      case (collationName, left, right, expected) =>
        checkAnswer(
          sql(s"select '$left' collate '$collationName' = '$right' collate '$collationName'"),
          Row(expected))
        checkAnswer(
          sql(s"select collate('$left', '$collationName') = collate('$right', '$collationName')"),
          Row(expected))
    }
  }

  test("comparisons respect collation") {
    Seq(
      ("ucs_basic", "AAA", "aaa", true),
      ("ucs_basic", "aaa", "aaa", false),
      ("ucs_basic", "aaa", "BBB", false),
      ("ucs_basic_lcase", "aaa", "aaa", false),
      ("ucs_basic_lcase", "AAA", "aaa", false),
      ("ucs_basic_lcase", "aaa", "bbb", true),
      ("unicode", "aaa", "aaa", false),
      ("unicode", "aaa", "AAA", true),
      ("unicode", "aaa", "BBB", true),
      ("unicode_CI", "aaa", "aaa", false),
      ("unicode_CI", "aaa", "AAA", false),
      ("unicode_CI", "aaa", "bbb", true)
    ).foreach {
      case (collationName, left, right, expected) =>
        checkAnswer(
          sql(s"select '$left' collate '$collationName' < '$right' collate '$collationName'"),
          Row(expected))
        checkAnswer(
          sql(s"select collate('$left', '$collationName') < collate('$right', '$collationName')"),
          Row(expected))
    }
  }
}
