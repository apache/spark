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

import org.apache.spark.sql.internal.SqlApiConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StringType

// scalastyle:off nonascii
class CollationSQLExpressionsSuite
  extends QueryTest
    with SharedSparkSession {

  test("Conv expression with collation") {
    // Supported collations
    case class ConvTestCase(
        num: String,
        from_base: String,
        to_base: String,
        collationName: String,
        result: String)

    val testCases = Seq(
      ConvTestCase("100", "2", "10", "UTF8_BINARY", "4"),
      ConvTestCase("100", "2", "10", "UTF8_BINARY_LCASE", "4"),
      ConvTestCase("100", "2", "10", "UNICODE", "4"),
      ConvTestCase("100", "2", "10", "UNICODE_CI", "4")
    )
    testCases.foreach(t => {
      val query =
        s"""
           |select conv(collate('${t.num}', '${t.collationName}'), ${t.from_base}, ${t.to_base})
           |""".stripMargin
      // Result & data type
      checkAnswer(sql(query), Row(t.result))
      assert(sql(query).schema.fields.head.dataType.sameType(StringType(t.collationName)))
    })
  }

  test("Bin expression with collation") {
    // Supported collations
    case class BinTestCase(
        num: String,
        collationName: String,
        result: String)

    val testCases = Seq(
      BinTestCase("13", "UTF8_BINARY", "1101"),
      BinTestCase("13", "UTF8_BINARY_LCASE", "1101"),
      BinTestCase("13", "UNICODE", "1101"),
      BinTestCase("13", "UNICODE_CI", "1101")
    )
    testCases.foreach(t => {
      val query =
        s"""
           |select bin(${t.num})
           |""".stripMargin
      // Result & data type
      withSQLConf(SqlApiConf.DEFAULT_COLLATION -> t.collationName) {
        checkAnswer(sql(query), Row(t.result))
        assert(sql(query).schema.fields.head.dataType.sameType(StringType(t.collationName)))
      }
    })
  }

  test("Hex with non-string input expression with collation") {
    case class HexTestCase(
        num: String,
        collationName: String,
        result: String)

    val testCases = Seq(
      HexTestCase("13", "UTF8_BINARY", "D"),
      HexTestCase("13", "UTF8_BINARY_LCASE", "D"),
      HexTestCase("13", "UNICODE", "D"),
      HexTestCase("13", "UNICODE_CI", "D")
    )
    testCases.foreach(t => {
      val query =
        s"""
           |select hex(${t.num})
           |""".stripMargin
      // Result & data type
      withSQLConf(SqlApiConf.DEFAULT_COLLATION -> t.collationName) {
        checkAnswer(sql(query), Row(t.result))
        assert(sql(query).schema.fields.head.dataType.sameType(StringType(t.collationName)))
      }
    })
  }

  test("Hex with string input expression with collation") {
    case class HexTestCase(
        num: String,
        collationName: String,
        result: String)

    val testCases = Seq(
      HexTestCase("Spark SQL", "UTF8_BINARY", "537061726B2053514C"),
      HexTestCase("Spark SQL", "UTF8_BINARY_LCASE", "537061726B2053514C"),
      HexTestCase("Spark SQL", "UNICODE", "537061726B2053514C"),
      HexTestCase("Spark SQL", "UNICODE_CI", "537061726B2053514C")
    )
    testCases.foreach(t => {
      val query =
        s"""
           |select hex(collate('${t.num}', '${t.collationName}'))
           |""".stripMargin
      // Result & data type
      checkAnswer(sql(query), Row(t.result))
      assert(sql(query).schema.fields.head.dataType.sameType(StringType(t.collationName)))
    })
  }

  test("UnHex expression with collation") {
    case class UnHexTestCase(
        num: String,
        collationName: String,
        result: String)

    val testCases = Seq(
      UnHexTestCase("537061726B2053514C", "UTF8_BINARY", "Spark SQL"),
      UnHexTestCase("537061726B2053514C", "UTF8_BINARY_LCASE", "Spark SQL"),
      UnHexTestCase("537061726B2053514C", "UNICODE", "Spark SQL"),
      UnHexTestCase("537061726B2053514C", "UNICODE_CI", "Spark SQL")
    )
    testCases.foreach(t => {
      val query =
        s"""
           |select decode(unhex(collate('${t.num}', '${t.collationName}')), 'UTF-8')
           |""".stripMargin
      // Result & data type
      checkAnswer(sql(query), Row(t.result))
      assert(sql(query).schema.fields.head.dataType.sameType(StringType("UTF8_BINARY")))
    })
  }

  // TODO: Add more tests for other SQL expressions

}
// scalastyle:on nonascii
