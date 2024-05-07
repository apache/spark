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

import org.apache.spark.SparkIllegalArgumentException
import org.apache.spark.sql.internal.SqlApiConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

// scalastyle:off nonascii
class CollationSQLExpressionsSuite
  extends QueryTest
  with SharedSparkSession {

  test("Support Md5 hash expression with collation") {
    case class Md5TestCase(
      input: String,
      collationName: String,
      result: String
    )

    val testCases = Seq(
      Md5TestCase("Spark", "UTF8_BINARY", "8cde774d6f7333752ed72cacddb05126"),
      Md5TestCase("Spark", "UTF8_BINARY_LCASE", "8cde774d6f7333752ed72cacddb05126"),
      Md5TestCase("SQL", "UNICODE", "9778840a0100cb30c982876741b0b5a2"),
      Md5TestCase("SQL", "UNICODE_CI", "9778840a0100cb30c982876741b0b5a2")
    )

    // Supported collations
    testCases.foreach(t => {
      val query =
        s"""
           |select md5('${t.input}')
           |""".stripMargin
      // Result & data type
      withSQLConf(SqlApiConf.DEFAULT_COLLATION -> t.collationName) {
        val testQuery = sql(query)
        checkAnswer(testQuery, Row(t.result))
        val dataType = StringType(t.collationName)
        assert(testQuery.schema.fields.head.dataType.sameType(dataType))
      }
    })
  }

  test("Support Sha2 hash expression with collation") {
    case class Sha2TestCase(
      input: String,
      collationName: String,
      bitLength: Int,
      result: String
    )

    val testCases = Seq(
      Sha2TestCase("Spark", "UTF8_BINARY", 256,
        "529bc3b07127ecb7e53a4dcf1991d9152c24537d919178022b2c42657f79a26b"),
      Sha2TestCase("Spark", "UTF8_BINARY_LCASE", 256,
        "529bc3b07127ecb7e53a4dcf1991d9152c24537d919178022b2c42657f79a26b"),
      Sha2TestCase("SQL", "UNICODE", 256,
        "a7056a455639d1c7deec82ee787db24a0c1878e2792b4597709f0facf7cc7b35"),
      Sha2TestCase("SQL", "UNICODE_CI", 256,
        "a7056a455639d1c7deec82ee787db24a0c1878e2792b4597709f0facf7cc7b35")
    )

    // Supported collations
    testCases.foreach(t => {
      val query =
        s"""
           |select sha2('${t.input}', ${t.bitLength})
           |""".stripMargin
      // Result & data type
      withSQLConf(SqlApiConf.DEFAULT_COLLATION -> t.collationName) {
        val testQuery = sql(query)
        checkAnswer(testQuery, Row(t.result))
        val dataType = StringType(t.collationName)
        assert(testQuery.schema.fields.head.dataType.sameType(dataType))
      }
    })
  }

  test("Support Sha1 hash expression with collation") {
    case class Sha1TestCase(
      input: String,
      collationName: String,
      result: String
    )

    val testCases = Seq(
      Sha1TestCase("Spark", "UTF8_BINARY", "85f5955f4b27a9a4c2aab6ffe5d7189fc298b92c"),
      Sha1TestCase("Spark", "UTF8_BINARY_LCASE", "85f5955f4b27a9a4c2aab6ffe5d7189fc298b92c"),
      Sha1TestCase("SQL", "UNICODE", "2064cb643caa8d9e1de12eea7f3e143ca9f8680d"),
      Sha1TestCase("SQL", "UNICODE_CI", "2064cb643caa8d9e1de12eea7f3e143ca9f8680d")
    )

    // Supported collations
    testCases.foreach(t => {
      val query =
        s"""
           |select sha1('${t.input}')
           |""".stripMargin
      // Result & data type
      withSQLConf(SqlApiConf.DEFAULT_COLLATION -> t.collationName) {
        val testQuery = sql(query)
        checkAnswer(testQuery, Row(t.result))
        val dataType = StringType(t.collationName)
        assert(testQuery.schema.fields.head.dataType.sameType(dataType))
      }
    })
  }

  test("Support Crc32 hash expression with collation") {
    case class Crc321TestCase(
     input: String,
     collationName: String,
     result: Int
    )

    val testCases = Seq(
      Crc321TestCase("Spark", "UTF8_BINARY", 1557323817),
      Crc321TestCase("Spark", "UTF8_BINARY_LCASE", 1557323817),
      Crc321TestCase("SQL", "UNICODE", 1299261525),
      Crc321TestCase("SQL", "UNICODE_CI", 1299261525)
    )

    // Supported collations
    testCases.foreach(t => {
      val query =
        s"""
           |select crc32('${t.input}')
           |""".stripMargin
      // Result
      withSQLConf(SqlApiConf.DEFAULT_COLLATION -> t.collationName) {
        val testQuery = sql(query)
        checkAnswer(testQuery, Row(t.result))
      }
    })
  }

  test("Support Murmur3Hash hash expression with collation") {
    case class Murmur3HashTestCase(
     input: String,
     collationName: String,
     result: Int
    )

    val testCases = Seq(
      Murmur3HashTestCase("Spark", "UTF8_BINARY", 228093765),
      Murmur3HashTestCase("Spark", "UTF8_BINARY_LCASE", 228093765),
      Murmur3HashTestCase("SQL", "UNICODE", 17468742),
      Murmur3HashTestCase("SQL", "UNICODE_CI", 17468742)
    )

    // Supported collations
    testCases.foreach(t => {
      val query =
        s"""
           |select hash('${t.input}')
           |""".stripMargin
      // Result
      withSQLConf(SqlApiConf.DEFAULT_COLLATION -> t.collationName) {
        val testQuery = sql(query)
        checkAnswer(testQuery, Row(t.result))
      }
    })
  }

  test("Support XxHash64 hash expression with collation") {
    case class XxHash64TestCase(
      input: String,
      collationName: String,
      result: Long
    )

    val testCases = Seq(
      XxHash64TestCase("Spark", "UTF8_BINARY", -4294468057691064905L),
      XxHash64TestCase("Spark", "UTF8_BINARY_LCASE", -4294468057691064905L),
      XxHash64TestCase("SQL", "UNICODE", -2147923034195946097L),
      XxHash64TestCase("SQL", "UNICODE_CI", -2147923034195946097L)
    )

    // Supported collations
    testCases.foreach(t => {
      val query =
        s"""
           |select xxhash64('${t.input}')
           |""".stripMargin
      // Result
      withSQLConf(SqlApiConf.DEFAULT_COLLATION -> t.collationName) {
        val testQuery = sql(query)
        checkAnswer(testQuery, Row(t.result))
      }
    })
  }

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

  test("Support StringSpace expression with collation") {
    case class StringSpaceTestCase(
      input: Int,
      collationName: String,
      result: String
    )

    val testCases = Seq(
      StringSpaceTestCase(1, "UTF8_BINARY", " "),
      StringSpaceTestCase(2, "UTF8_BINARY_LCASE", "  "),
      StringSpaceTestCase(3, "UNICODE", "   "),
      StringSpaceTestCase(4, "UNICODE_CI", "    ")
    )

    // Supported collations
    testCases.foreach(t => {
      val query =
        s"""
           |select space(${t.input})
           |""".stripMargin
      // Result & data type
      withSQLConf(SqlApiConf.DEFAULT_COLLATION -> t.collationName) {
        val testQuery = sql(query)
        checkAnswer(testQuery, Row(t.result))
        val dataType = StringType(t.collationName)
        assert(testQuery.schema.fields.head.dataType.sameType(dataType))
      }
    })
  }

  test("Support ToNumber & TryToNumber expressions with collation") {
    case class ToNumberTestCase(
      input: String,
      collationName: String,
      format: String,
      result: Any,
      resultType: DataType
    )

    val testCases = Seq(
      ToNumberTestCase("123", "UTF8_BINARY", "999", 123, DecimalType(3, 0)),
      ToNumberTestCase("1", "UTF8_BINARY_LCASE", "0.00", 1.00, DecimalType(3, 2)),
      ToNumberTestCase("99,999", "UNICODE", "99,999", 99999, DecimalType(5, 0)),
      ToNumberTestCase("$14.99", "UNICODE_CI", "$99.99", 14.99, DecimalType(4, 2))
    )

    // Supported collations (ToNumber)
    testCases.foreach(t => {
      val query =
        s"""
           |select to_number('${t.input}', '${t.format}')
           |""".stripMargin
      // Result & data type
      withSQLConf(SqlApiConf.DEFAULT_COLLATION -> t.collationName) {
        val testQuery = sql(query)
        checkAnswer(testQuery, Row(t.result))
        assert(testQuery.schema.fields.head.dataType.sameType(t.resultType))
      }
    })

    // Supported collations (TryToNumber)
    testCases.foreach(t => {
      val query =
        s"""
           |select try_to_number('${t.input}', '${t.format}')
           |""".stripMargin
      // Result & data type
      withSQLConf(SqlApiConf.DEFAULT_COLLATION -> t.collationName) {
        val testQuery = sql(query)
        checkAnswer(testQuery, Row(t.result))
        assert(testQuery.schema.fields.head.dataType.sameType(t.resultType))
      }
    })
  }

  test("Handle invalid number for ToNumber variant expression with collation") {
    // to_number should throw an exception if the conversion fails
    val number = "xx"
    val query = s"SELECT to_number('$number', '999');"
    withSQLConf(SqlApiConf.DEFAULT_COLLATION -> "UNICODE") {
      val e = intercept[SparkIllegalArgumentException] {
        val testQuery = sql(query)
        testQuery.collect()
      }
      assert(e.getErrorClass === "INVALID_FORMAT.MISMATCH_INPUT")
    }
  }

  test("Handle invalid number for TryToNumber variant expression with collation") {
    // try_to_number shouldn't throw an exception if the conversion fails
    val number = "xx"
    val query = s"SELECT try_to_number('$number', '999');"
    withSQLConf(SqlApiConf.DEFAULT_COLLATION -> "UNICODE") {
      val testQuery = sql(query)
      checkAnswer(testQuery, Row(null))
    }
  }

  test("Support ToChar expression with collation") {
    case class ToCharTestCase(
      input: Int,
      collationName: String,
      format: String,
      result: String
    )

    val testCases = Seq(
      ToCharTestCase(12, "UTF8_BINARY", "999", " 12"),
      ToCharTestCase(34, "UTF8_BINARY_LCASE", "000D00", "034.00"),
      ToCharTestCase(56, "UNICODE", "$99.99", "$56.00"),
      ToCharTestCase(78, "UNICODE_CI", "99D9S", "78.0+")
    )

    // Supported collations
    testCases.foreach(t => {
      val query =
        s"""
           |select to_char(${t.input}, '${t.format}')
           |""".stripMargin
      // Result & data type
      withSQLConf(SqlApiConf.DEFAULT_COLLATION -> t.collationName) {
        val testQuery = sql(query)
        checkAnswer(testQuery, Row(t.result))
        val dataType = StringType(t.collationName)
        assert(testQuery.schema.fields.head.dataType.sameType(dataType))
      }
    })
  }

  test("Support StringToMap expression with collation") {
    // Supported collations
    case class StringToMapTestCase[R](t: String, p: String, k: String, c: String, result: R)
    val testCases = Seq(
      StringToMapTestCase("a:1,b:2,c:3", ",", ":", "UTF8_BINARY",
        Map("a" -> "1", "b" -> "2", "c" -> "3")),
      StringToMapTestCase("A-1;B-2;C-3", ";", "-", "UTF8_BINARY_LCASE",
        Map("A" -> "1", "B" -> "2", "C" -> "3")),
      StringToMapTestCase("1:a,2:b,3:c", ",", ":", "UNICODE",
        Map("1" -> "a", "2" -> "b", "3" -> "c")),
      StringToMapTestCase("1/A!2/B!3/C", "!", "/", "UNICODE_CI",
        Map("1" -> "A", "2" -> "B", "3" -> "C"))
    )
    testCases.foreach(t => {
      val query = s"SELECT str_to_map(collate('${t.t}', '${t.c}'), '${t.p}', '${t.k}');"
      // Result & data type
      checkAnswer(sql(query), Row(t.result))
      val dataType = MapType(StringType(t.c), StringType(t.c), true)
      assert(sql(query).schema.fields.head.dataType.sameType(dataType))
    })
  }

  test("Support Mask expression with collation") {
    // Supported collations
    case class MaskTestCase[R](i: String, u: String, l: String, d: String, o: String, c: String,
      result: R)
    val testCases = Seq(
      MaskTestCase("ab-CD-12-@$", null, null, null, null, "UTF8_BINARY", "ab-CD-12-@$"),
      MaskTestCase("ab-CD-12-@$", "X", null, null, null, "UTF8_BINARY_LCASE", "ab-XX-12-@$"),
      MaskTestCase("ab-CD-12-@$", "X", "x", null, null, "UNICODE", "xx-XX-12-@$"),
      MaskTestCase("ab-CD-12-@$", "X", "x", "0", "#", "UNICODE_CI", "xx#XX#00###")
    )
    testCases.foreach(t => {
      def col(s: String): String = if (s == null) "null" else s"collate('$s', '${t.c}')"
      val query = s"SELECT mask(${col(t.i)}, ${col(t.u)}, ${col(t.l)}, ${col(t.d)}, ${col(t.o)})"
      // Result & data type
      var result = sql(query)
      checkAnswer(result, Row(t.result))
      assert(result.schema.fields.head.dataType.sameType(StringType(t.c)))
    })
    // Implicit casting
    val testCasting = Seq(
      MaskTestCase("ab-CD-12-@$", "X", "x", "0", "#", "UNICODE_CI", "xx#XX#00###")
    )
    testCasting.foreach(t => {
      def col(s: String): String = if (s == null) "null" else s"collate('$s', '${t.c}')"
      def str(s: String): String = if (s == null) "null" else s"'$s'"
      val query1 = s"SELECT mask(${col(t.i)}, ${str(t.u)}, ${str(t.l)}, ${str(t.d)}, ${str(t.o)})"
      val query2 = s"SELECT mask(${str(t.i)}, ${col(t.u)}, ${str(t.l)}, ${str(t.d)}, ${str(t.o)})"
      val query3 = s"SELECT mask(${str(t.i)}, ${str(t.u)}, ${col(t.l)}, ${str(t.d)}, ${str(t.o)})"
      val query4 = s"SELECT mask(${str(t.i)}, ${str(t.u)}, ${str(t.l)}, ${col(t.d)}, ${str(t.o)})"
      val query5 = s"SELECT mask(${str(t.i)}, ${str(t.u)}, ${str(t.l)}, ${str(t.d)}, ${col(t.o)})"
      for (q <- Seq(query1, query2, query3, query4, query5)) {
        val result = sql(q)
        checkAnswer(result, Row(t.result))
        assert(result.schema.fields.head.dataType.sameType(StringType(t.c)))
      }
    })
    // Collation mismatch
    val collationMismatch = intercept[AnalysisException] {
      sql("SELECT mask(collate('ab-CD-12-@$','UNICODE'),collate('X','UNICODE_CI'),'x','0','#')")
    }
    assert(collationMismatch.getErrorClass === "COLLATION_MISMATCH.EXPLICIT")
  }

  // TODO: Add more tests for other SQL expressions

}
// scalastyle:on nonascii
