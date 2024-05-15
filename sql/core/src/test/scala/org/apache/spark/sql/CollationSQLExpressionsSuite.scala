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

import java.text.SimpleDateFormat

import scala.collection.immutable.Seq

import org.apache.spark.{SparkException, SparkIllegalArgumentException, SparkRuntimeException}
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

  test("Support UrlEncode hash expression with collation") {
    case class UrlEncodeTestCase(
     input: String,
     collationName: String,
     result: String
    )

    val testCases = Seq(
      UrlEncodeTestCase("https://spark.apache.org", "UTF8_BINARY",
        "https%3A%2F%2Fspark.apache.org"),
      UrlEncodeTestCase("https://spark.apache.org", "UTF8_BINARY_LCASE",
        "https%3A%2F%2Fspark.apache.org"),
      UrlEncodeTestCase("https://spark.apache.org", "UNICODE",
        "https%3A%2F%2Fspark.apache.org"),
      UrlEncodeTestCase("https://spark.apache.org", "UNICODE_CI",
        "https%3A%2F%2Fspark.apache.org")
    )

    // Supported collations
    testCases.foreach(t => {
      val query =
        s"""
           |select url_encode('${t.input}')
           |""".stripMargin
      // Result
      withSQLConf(SqlApiConf.DEFAULT_COLLATION -> t.collationName) {
        val testQuery = sql(query)
        checkAnswer(testQuery, Row(t.result))
        val dataType = StringType(t.collationName)
        assert(testQuery.schema.fields.head.dataType.sameType(dataType))
      }
    })
  }

  test("Support UrlDecode hash expression with collation") {
    case class UrlDecodeTestCase(
      input: String,
      collationName: String,
      result: String
    )

    val testCases = Seq(
      UrlDecodeTestCase("https%3A%2F%2Fspark.apache.org", "UTF8_BINARY",
        "https://spark.apache.org"),
      UrlDecodeTestCase("https%3A%2F%2Fspark.apache.org", "UTF8_BINARY_LCASE",
        "https://spark.apache.org"),
      UrlDecodeTestCase("https%3A%2F%2Fspark.apache.org", "UNICODE",
        "https://spark.apache.org"),
      UrlDecodeTestCase("https%3A%2F%2Fspark.apache.org", "UNICODE_CI",
        "https://spark.apache.org")
    )

    // Supported collations
    testCases.foreach(t => {
      val query =
        s"""
           |select url_decode('${t.input}')
           |""".stripMargin
      // Result
      withSQLConf(SqlApiConf.DEFAULT_COLLATION -> t.collationName) {
        val testQuery = sql(query)
        checkAnswer(testQuery, Row(t.result))
        val dataType = StringType(t.collationName)
        assert(testQuery.schema.fields.head.dataType.sameType(dataType))
      }
    })
  }

  test("Support ParseUrl hash expression with collation") {
    case class ParseUrlTestCase(
      input: String,
      collationName: String,
      path: String,
      result: String
    )

    val testCases = Seq(
      ParseUrlTestCase("http://spark.apache.org/path?query=1", "UTF8_BINARY", "HOST",
        "spark.apache.org"),
      ParseUrlTestCase("http://spark.apache.org/path?query=2", "UTF8_BINARY_LCASE", "PATH",
        "/path"),
      ParseUrlTestCase("http://spark.apache.org/path?query=3", "UNICODE", "QUERY",
        "query=3"),
      ParseUrlTestCase("http://spark.apache.org/path?query=4", "UNICODE_CI", "PROTOCOL",
        "http")
    )

    // Supported collations
    testCases.foreach(t => {
      val query =
        s"""
           |select parse_url('${t.input}', '${t.path}')
           |""".stripMargin
      // Result
      withSQLConf(SqlApiConf.DEFAULT_COLLATION -> t.collationName) {
        val testQuery = sql(query)
        checkAnswer(testQuery, Row(t.result))
        val dataType = StringType(t.collationName)
        assert(testQuery.schema.fields.head.dataType.sameType(dataType))
      }
    })
  }

  test("Support CsvToStructs csv expression with collation") {
    case class CsvToStructsTestCase(
     input: String,
     collationName: String,
     schema: String,
     options: String,
     result: Row,
     structFields: Seq[StructField]
    )

    val testCases = Seq(
      CsvToStructsTestCase("1", "UTF8_BINARY", "'a INT'", "",
        Row(1), Seq(
          StructField("a", IntegerType, nullable = true)
        )),
      CsvToStructsTestCase("true, 0.8", "UTF8_BINARY_LCASE", "'A BOOLEAN, B DOUBLE'", "",
        Row(true, 0.8), Seq(
          StructField("A", BooleanType, nullable = true),
          StructField("B", DoubleType, nullable = true)
        )),
      CsvToStructsTestCase("\"Spark\"", "UNICODE", "'a STRING'", "",
        Row("Spark"), Seq(
          StructField("a", StringType("UNICODE"), nullable = true)
        )),
      CsvToStructsTestCase("26/08/2015", "UTF8_BINARY", "'time Timestamp'",
        ", map('timestampFormat', 'dd/MM/yyyy')", Row(
          new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S").parse("2015-08-26 00:00:00.0")
        ), Seq(
          StructField("time", TimestampType, nullable = true)
        ))
    )

    // Supported collations
    testCases.foreach(t => {
      val query =
        s"""
           |select from_csv('${t.input}', ${t.schema} ${t.options})
           |""".stripMargin
      // Result
      withSQLConf(SqlApiConf.DEFAULT_COLLATION -> t.collationName) {
        val testQuery = sql(query)
        val queryResult = testQuery.collect().head
        checkAnswer(testQuery, Row(t.result))
        val dataType = StructType(t.structFields)
        assert(testQuery.schema.fields.head.dataType.sameType(dataType))
      }
    })
  }

  test("Support SchemaOfCsv csv expression with collation") {
    case class SchemaOfCsvTestCase(
      input: String,
      collationName: String,
      result: String
    )

    val testCases = Seq(
      SchemaOfCsvTestCase("1", "UTF8_BINARY", "STRUCT<_c0: INT>"),
      SchemaOfCsvTestCase("true,0.8", "UTF8_BINARY_LCASE",
        "STRUCT<_c0: BOOLEAN, _c1: DOUBLE>"),
      SchemaOfCsvTestCase("2015-08-26", "UNICODE", "STRUCT<_c0: DATE>"),
      SchemaOfCsvTestCase("abc", "UNICODE_CI",
        "STRUCT<_c0: STRING>")
    )

    // Supported collations
    testCases.foreach(t => {
      val query =
        s"""
           |select schema_of_csv('${t.input}')
           |""".stripMargin
      // Result
      withSQLConf(SqlApiConf.DEFAULT_COLLATION -> t.collationName) {
        val testQuery = sql(query)
        checkAnswer(testQuery, Row(t.result))
        val dataType = StringType(t.collationName)
        assert(testQuery.schema.fields.head.dataType.sameType(dataType))
      }
    })
  }

  test("Support StructsToCsv csv expression with collation") {
    case class StructsToCsvTestCase(
     input: String,
     collationName: String,
     result: String
    )

    val testCases = Seq(
      StructsToCsvTestCase("named_struct('a', 1, 'b', 2)", "UTF8_BINARY", "1,2"),
      StructsToCsvTestCase("named_struct('A', true, 'B', 2.0)", "UTF8_BINARY_LCASE", "true,2.0"),
      StructsToCsvTestCase("named_struct()", "UNICODE", null),
      StructsToCsvTestCase("named_struct('time', to_timestamp('2015-08-26'))", "UNICODE_CI",
        "2015-08-26T00:00:00.000-07:00")
    )

    // Supported collations
    testCases.foreach(t => {
      val query =
        s"""
           |select to_csv(${t.input})
           |""".stripMargin
      // Result
      withSQLConf(SqlApiConf.DEFAULT_COLLATION -> t.collationName) {
        val testQuery = sql(query)
        checkAnswer(testQuery, Row(t.result))
        val dataType = StringType(t.collationName)
        assert(testQuery.schema.fields.head.dataType.sameType(dataType))
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

  test("Support XPath expressions with collation") {
    case class XPathTestCase(
      xml: String,
      xpath: String,
      functionName: String,
      collationName: String,
      result: Any,
      resultType: DataType
    )

    val testCases = Seq(
      XPathTestCase("<a><b>1</b></a>", "a/b",
        "xpath_boolean", "UTF8_BINARY", true, BooleanType),
      XPathTestCase("<A><B>1</B><B>2</B></A>", "sum(A/B)",
        "xpath_short", "UTF8_BINARY", 3, ShortType),
      XPathTestCase("<a><b>3</b><b>4</b></a>", "sum(a/b)",
        "xpath_int", "UTF8_BINARY_LCASE", 7, IntegerType),
      XPathTestCase("<A><B>5</B><B>6</B></A>", "sum(A/B)",
        "xpath_long", "UTF8_BINARY_LCASE", 11, LongType),
      XPathTestCase("<a><b>7</b><b>8</b></a>", "sum(a/b)",
        "xpath_float", "UNICODE", 15.0, FloatType),
      XPathTestCase("<A><B>9</B><B>0</B></A>", "sum(A/B)",
        "xpath_double", "UNICODE", 9.0, DoubleType),
      XPathTestCase("<a><b>b</b><c>cc</c></a>", "a/c",
        "xpath_string", "UNICODE_CI", "cc", StringType("UNICODE_CI")),
      XPathTestCase("<a><b>b1</b><b>b2</b><b>b3</b><c>c1</c><c>c2</c></a>", "a/b/text()",
        "xpath", "UNICODE_CI", Array("b1", "b2", "b3"), ArrayType(StringType("UNICODE_CI")))
    )

    // Supported collations
    testCases.foreach(t => {
      val query =
        s"""
           |select ${t.functionName}('${t.xml}', '${t.xpath}')
           |""".stripMargin
      // Result & data type
      withSQLConf(SqlApiConf.DEFAULT_COLLATION -> t.collationName) {
        val testQuery = sql(query)
        checkAnswer(testQuery, Row(t.result))
        assert(testQuery.schema.fields.head.dataType.sameType(t.resultType))
      }
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

  test("Support GetJsonObject json expression with collation") {
    case class GetJsonObjectTestCase(
      input: String,
      path: String,
      collationName: String,
      result: String
    )

    val testCases = Seq(
      GetJsonObjectTestCase("{\"a\":\"b\"}", "$.a", "UTF8_BINARY", "b"),
      GetJsonObjectTestCase("{\"A\":\"1\"}", "$.A", "UTF8_BINARY_LCASE", "1"),
      GetJsonObjectTestCase("{\"x\":true}", "$.x", "UNICODE", "true"),
      GetJsonObjectTestCase("{\"X\":1}", "$.X", "UNICODE_CI", "1")
    )

    // Supported collations
    testCases.foreach(t => {
      val query =
        s"""
           |SELECT get_json_object('${t.input}', '${t.path}')
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

  test("Support JsonTuple json expression with collation") {
    case class JsonTupleTestCase(
      input: String,
      names: String,
      collationName: String,
      result: Row
    )

    val testCases = Seq(
      JsonTupleTestCase("{\"a\":1, \"b\":2}", "'a', 'b'", "UTF8_BINARY",
        Row("1", "2")),
      JsonTupleTestCase("{\"A\":\"3\", \"B\":\"4\"}", "'A', 'B'", "UTF8_BINARY_LCASE",
        Row("3", "4")),
      JsonTupleTestCase("{\"x\":true, \"y\":false}", "'x', 'y'", "UNICODE",
        Row("true", "false")),
      JsonTupleTestCase("{\"X\":null, \"Y\":null}", "'X', 'Y'", "UNICODE_CI",
        Row(null, null))
    )

    // Supported collations
    testCases.foreach(t => {
      val query =
        s"""
           |SELECT json_tuple('${t.input}', ${t.names})
           |""".stripMargin
      // Result & data type
      withSQLConf(SqlApiConf.DEFAULT_COLLATION -> t.collationName) {
        val testQuery = sql(query)
        checkAnswer(testQuery, t.result)
        val dataType = StringType(t.collationName)
        assert(testQuery.schema.fields.head.dataType.sameType(dataType))
      }
    })
  }

  test("Support JsonToStructs json expression with collation") {
    case class JsonToStructsTestCase(
      input: String,
      schema: String,
      collationName: String,
      result: Row
    )

    val testCases = Seq(
      JsonToStructsTestCase("{\"a\":1, \"b\":2.0}", "a INT, b DOUBLE",
        "UTF8_BINARY", Row(Row(1, 2.0))),
      JsonToStructsTestCase("{\"A\":\"3\", \"B\":4}", "A STRING COLLATE UTF8_BINARY_LCASE, B INT",
        "UTF8_BINARY_LCASE", Row(Row("3", 4))),
      JsonToStructsTestCase("{\"x\":true, \"y\":null}", "x BOOLEAN, y VOID",
        "UNICODE", Row(Row(true, null))),
      JsonToStructsTestCase("{\"X\":null, \"Y\":false}", "X VOID, Y BOOLEAN",
        "UNICODE_CI", Row(Row(null, false)))
    )

    // Supported collations
    testCases.foreach(t => {
      val query =
        s"""
           |SELECT from_json('${t.input}', '${t.schema}')
           |""".stripMargin
      // Result & data type
      withSQLConf(SqlApiConf.DEFAULT_COLLATION -> t.collationName) {
        val testQuery = sql(query)
        checkAnswer(testQuery, t.result)
        val dataType = StructType.fromDDL(t.schema)
        assert(testQuery.schema.fields.head.dataType.sameType(dataType))
      }
    })
  }

  test("Support StructsToJson json expression with collation") {
    case class StructsToJsonTestCase(
      struct: String,
      collationName: String,
      result: Row
    )

    val testCases = Seq(
      StructsToJsonTestCase("named_struct('a', 1, 'b', 2)",
        "UTF8_BINARY", Row("{\"a\":1,\"b\":2}")),
      StructsToJsonTestCase("array(named_struct('a', 1, 'b', 2))",
        "UTF8_BINARY_LCASE", Row("[{\"a\":1,\"b\":2}]")),
      StructsToJsonTestCase("map('a', named_struct('b', 1))",
        "UNICODE", Row("{\"a\":{\"b\":1}}")),
      StructsToJsonTestCase("array(map('a', 1))",
        "UNICODE_CI", Row("[{\"a\":1}]"))
    )

    // Supported collations
    testCases.foreach(t => {
      val query =
        s"""
           |SELECT to_json(${t.struct})
           |""".stripMargin
      // Result & data type
      withSQLConf(SqlApiConf.DEFAULT_COLLATION -> t.collationName) {
        val testQuery = sql(query)
        checkAnswer(testQuery, t.result)
        val dataType = StringType(t.collationName)
        assert(testQuery.schema.fields.head.dataType.sameType(dataType))
      }
    })
  }

  test("Support LengthOfJsonArray json expression with collation") {
    case class LengthOfJsonArrayTestCase(
      input: String,
      collationName: String,
      result: Row
    )

    val testCases = Seq(
      LengthOfJsonArrayTestCase("'[1,2,3,4]'", "UTF8_BINARY", Row(4)),
      LengthOfJsonArrayTestCase("'[1,2,3,{\"f1\":1,\"f2\":[5,6]},4]'", "UTF8_BINARY_LCASE", Row(5)),
      LengthOfJsonArrayTestCase("'[1,2'", "UNICODE", Row(null)),
      LengthOfJsonArrayTestCase("'['", "UNICODE_CI", Row(null))
    )

    // Supported collations
    testCases.foreach(t => {
      val query =
        s"""
           |SELECT json_array_length(${t.input})
           |""".stripMargin
      // Result & data type
      withSQLConf(SqlApiConf.DEFAULT_COLLATION -> t.collationName) {
        val testQuery = sql(query)
        checkAnswer(testQuery, t.result)
        assert(testQuery.schema.fields.head.dataType.sameType(IntegerType))
      }
    })
  }

  test("Support JsonObjectKeys json expression with collation") {
    case class JsonObjectKeysJsonArrayTestCase(
      input: String,
      collationName: String,
      result: Row
    )

    val testCases = Seq(
      JsonObjectKeysJsonArrayTestCase("{}", "UTF8_BINARY",
        Row(Seq())),
      JsonObjectKeysJsonArrayTestCase("{\"k\":", "UTF8_BINARY_LCASE",
        Row(null)),
      JsonObjectKeysJsonArrayTestCase("{\"k1\": \"v1\"}", "UNICODE",
        Row(Seq("k1"))),
      JsonObjectKeysJsonArrayTestCase("{\"k1\":1,\"k2\":{\"k3\":3, \"k4\":4}}", "UNICODE_CI",
        Row(Seq("k1", "k2")))
    )

    // Supported collations
    testCases.foreach(t => {
      val query =
        s"""
           |SELECT json_object_keys('${t.input}')
           |""".stripMargin
      // Result & data type
      withSQLConf(SqlApiConf.DEFAULT_COLLATION -> t.collationName) {
        val testQuery = sql(query)
        checkAnswer(testQuery, t.result)
        val dataType = ArrayType(StringType(t.collationName))
        assert(testQuery.schema.fields.head.dataType.sameType(dataType))
      }
    })
  }

  test("Support SchemaOfJson json expression with collation") {
    case class SchemaOfJsonTestCase(
     input: String,
     collationName: String,
     result: Row
    )

    val testCases = Seq(
      SchemaOfJsonTestCase("'[{\"col\":0}]'",
        "UTF8_BINARY", Row("ARRAY<STRUCT<col: BIGINT>>")),
      SchemaOfJsonTestCase("'[{\"col\":01}]', map('allowNumericLeadingZeros', 'true')",
        "UTF8_BINARY_LCASE", Row("ARRAY<STRUCT<col: BIGINT>>")),
      SchemaOfJsonTestCase("'[]'",
        "UNICODE", Row("ARRAY<STRING>")),
      SchemaOfJsonTestCase("''",
        "UNICODE_CI", Row("STRING"))
    )

    // Supported collations
    testCases.foreach(t => {
      val query =
        s"""
           |SELECT schema_of_json(${t.input})
           |""".stripMargin
      // Result & data type
      withSQLConf(SqlApiConf.DEFAULT_COLLATION -> t.collationName) {
        val testQuery = sql(query)
        checkAnswer(testQuery, t.result)
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

  test("Support RaiseError misc expression with collation") {
    // Supported collations
    case class RaiseErrorTestCase(errorMessage: String, collationName: String)
    val testCases = Seq(
      RaiseErrorTestCase("custom error message 1", "UTF8_BINARY"),
      RaiseErrorTestCase("custom error message 2", "UTF8_BINARY_LCASE"),
      RaiseErrorTestCase("custom error message 3", "UNICODE"),
      RaiseErrorTestCase("custom error message 4", "UNICODE_CI")
    )
    testCases.foreach(t => {
      withSQLConf(SqlApiConf.DEFAULT_COLLATION -> t.collationName) {
        val query = s"SELECT raise_error('${t.errorMessage}')"
        // Result & data type
        val userException = intercept[SparkRuntimeException] {
          sql(query).collect()
        }
        assert(userException.getErrorClass === "USER_RAISED_EXCEPTION")
        assert(userException.getMessage.contains(t.errorMessage))
      }
    })
  }

  test("Support Uuid misc expression with collation") {
    // Supported collations
    Seq("UTF8_BINARY_LCASE", "UNICODE", "UNICODE_CI").foreach(collationName =>
      withSQLConf(SqlApiConf.DEFAULT_COLLATION -> collationName) {
        val query = s"SELECT uuid()"
        // Result & data type
        val testQuery = sql(query)
        val queryResult = testQuery.collect().head.getString(0)
        val uuidFormat = "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
        assert(queryResult.matches(uuidFormat))
        val dataType = StringType(collationName)
        assert(testQuery.schema.fields.head.dataType.sameType(dataType))
      }
    )
  }

  test("Support SparkVersion misc expression with collation") {
    // Supported collations
    Seq("UTF8_BINARY", "UTF8_BINARY_LCASE", "UNICODE", "UNICODE_CI").foreach(collationName =>
      withSQLConf(SqlApiConf.DEFAULT_COLLATION -> collationName) {
        val query = s"SELECT version()"
        // Result & data type
        val testQuery = sql(query)
        val queryResult = testQuery.collect().head.getString(0)
        val versionFormat = "^[0-9]\\.[0-9]\\.[0-9] [0-9a-f]{40}$"
        assert(queryResult.matches(versionFormat))
        val dataType = StringType(collationName)
        assert(testQuery.schema.fields.head.dataType.sameType(dataType))
      }
    )
  }

  test("Support TypeOf misc expression with collation") {
    // Supported collations
    case class TypeOfTestCase(input: String, collationName: String, result: String)
    val testCases = Seq(
      TypeOfTestCase("1", "UTF8_BINARY", "int"),
      TypeOfTestCase("\"A\"", "UTF8_BINARY_LCASE", "string collate UTF8_BINARY_LCASE"),
      TypeOfTestCase("array(1)", "UNICODE", "array<int>"),
      TypeOfTestCase("null", "UNICODE_CI", "void")
    )
    testCases.foreach(t => {
      withSQLConf(SqlApiConf.DEFAULT_COLLATION -> t.collationName) {
        val query = s"SELECT typeof(${t.input})"
        // Result & data type
        val testQuery = sql(query)
        checkAnswer(testQuery, Row(t.result))
        val dataType = StringType(t.collationName)
        assert(testQuery.schema.fields.head.dataType.sameType(dataType))
      }
    })
  }

  test("Support AesEncrypt misc expression with collation") {
    // Supported collations
    case class AesEncryptTestCase(
     input: String,
     collationName: String,
     params: String,
     result: String
    )
    val testCases = Seq(
      AesEncryptTestCase("Spark", "UTF8_BINARY", "'1234567890abcdef', 'ECB'",
        "8DE7DB79A23F3E8ED530994DDEA98913"),
      AesEncryptTestCase("Spark", "UTF8_BINARY_LCASE", "'1234567890abcdef', 'ECB', 'DEFAULT', ''",
        "8DE7DB79A23F3E8ED530994DDEA98913"),
      AesEncryptTestCase("Spark", "UNICODE", "'1234567890abcdef', 'GCM', 'DEFAULT', " +
        "unhex('000000000000000000000000')",
        "00000000000000000000000046596B2DE09C729FE48A0F81A00A4E7101DABEB61D"),
      AesEncryptTestCase("Spark", "UNICODE_CI", "'1234567890abcdef', 'CBC', 'DEFAULT', " +
        "unhex('00000000000000000000000000000000')",
        "000000000000000000000000000000008DE7DB79A23F3E8ED530994DDEA98913")
    )
    testCases.foreach(t => {
      withSQLConf(SqlApiConf.DEFAULT_COLLATION -> t.collationName) {
        val query = s"SELECT hex(aes_encrypt('${t.input}', ${t.params}))"
        // Result & data type
        val testQuery = sql(query)
        checkAnswer(testQuery, Row(t.result))
        val dataType = StringType(t.collationName)
        assert(testQuery.schema.fields.head.dataType.sameType(dataType))
      }
    })
  }

  test("Support AesDecrypt misc expression with collation") {
    // Supported collations
    case class AesDecryptTestCase(
     input: String,
     collationName: String,
     params: String,
     result: String
    )
    val testCases = Seq(
      AesDecryptTestCase("8DE7DB79A23F3E8ED530994DDEA98913",
        "UTF8_BINARY", "'1234567890abcdef', 'ECB'", "Spark"),
      AesDecryptTestCase("8DE7DB79A23F3E8ED530994DDEA98913",
        "UTF8_BINARY_LCASE", "'1234567890abcdef', 'ECB', 'DEFAULT', ''", "Spark"),
      AesDecryptTestCase("00000000000000000000000046596B2DE09C729FE48A0F81A00A4E7101DABEB61D",
        "UNICODE", "'1234567890abcdef', 'GCM', 'DEFAULT'", "Spark"),
      AesDecryptTestCase("000000000000000000000000000000008DE7DB79A23F3E8ED530994DDEA98913",
        "UNICODE_CI", "'1234567890abcdef', 'CBC', 'DEFAULT'", "Spark")
    )
    testCases.foreach(t => {
      withSQLConf(SqlApiConf.DEFAULT_COLLATION -> t.collationName) {
        val query = s"SELECT aes_decrypt(unhex('${t.input}'), ${t.params})"
        // Result & data type
        val testQuery = sql(query)
        checkAnswer(testQuery, sql(s"SELECT to_binary('${t.result}', 'utf-8')"))
        assert(testQuery.schema.fields.head.dataType.sameType(BinaryType))
      }
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

  test("Support XmlToStructs xml expression with collation") {
    case class XmlToStructsTestCase(
     input: String,
     collationName: String,
     schema: String,
     options: String,
     result: Row,
     structFields: Seq[StructField]
    )

    val testCases = Seq(
      XmlToStructsTestCase("<p><a>1</a></p>", "UTF8_BINARY", "'a INT'", "",
        Row(1), Seq(
          StructField("a", IntegerType, nullable = true)
        )),
      XmlToStructsTestCase("<p><A>true</A><B>0.8</B></p>", "UTF8_BINARY_LCASE",
        "'A BOOLEAN, B DOUBLE'", "", Row(true, 0.8), Seq(
          StructField("A", BooleanType, nullable = true),
          StructField("B", DoubleType, nullable = true)
        )),
      XmlToStructsTestCase("<p><s>Spark</s></p>", "UNICODE", "'s STRING'", "",
        Row("Spark"), Seq(
          StructField("s", StringType("UNICODE"), nullable = true)
        )),
      XmlToStructsTestCase("<p><time>26/08/2015</time></p>", "UNICODE_CI", "'time Timestamp'",
        ", map('timestampFormat', 'dd/MM/yyyy')", Row(
          new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S").parse("2015-08-26 00:00:00.0")
        ), Seq(
          StructField("time", TimestampType, nullable = true)
        ))
    )

    // Supported collations
    testCases.foreach(t => {
      val query =
        s"""
           |select from_xml('${t.input}', ${t.schema} ${t.options})
           |""".stripMargin
      // Result
      withSQLConf(SqlApiConf.DEFAULT_COLLATION -> t.collationName) {
        val testQuery = sql(query)
        checkAnswer(testQuery, Row(t.result))
        val dataType = StructType(t.structFields)
        assert(testQuery.schema.fields.head.dataType.sameType(dataType))
      }
    })
  }

  test("Support SchemaOfXml xml expression with collation") {
    case class SchemaOfXmlTestCase(
     input: String,
     collationName: String,
     result: String
    )

    val testCases = Seq(
      SchemaOfXmlTestCase("<p><a>1</a></p>", "UTF8_BINARY", "STRUCT<a: BIGINT>"),
      SchemaOfXmlTestCase("<p><A>true</A><B>0.8</B></p>", "UTF8_BINARY_LCASE",
        "STRUCT<A: BOOLEAN, B: DOUBLE>"),
      SchemaOfXmlTestCase("<p></p>", "UNICODE", "STRUCT<>"),
      SchemaOfXmlTestCase("<p><A>1</A><A>2</A><A>3</A></p>", "UNICODE_CI",
        "STRUCT<A: ARRAY<BIGINT>>")
    )

    // Supported collations
    testCases.foreach(t => {
      val query =
        s"""
           |select schema_of_xml('${t.input}')
           |""".stripMargin
      // Result
      withSQLConf(SqlApiConf.DEFAULT_COLLATION -> t.collationName) {
        val testQuery = sql(query)
        checkAnswer(testQuery, Row(t.result))
        val dataType = StringType(t.collationName)
        assert(testQuery.schema.fields.head.dataType.sameType(dataType))
      }
    })
  }

  test("Support StructsToXml xml expression with collation") {
    case class StructsToXmlTestCase(
      input: String,
      collationName: String,
      result: String
    )

    val testCases = Seq(
      StructsToXmlTestCase("named_struct('a', 1, 'b', 2)", "UTF8_BINARY",
        s"""<ROW>
           |    <a>1</a>
           |    <b>2</b>
           |</ROW>""".stripMargin),
      StructsToXmlTestCase("named_struct('A', true, 'B', 2.0)", "UTF8_BINARY_LCASE",
        s"""<ROW>
           |    <A>true</A>
           |    <B>2.0</B>
           |</ROW>""".stripMargin),
      StructsToXmlTestCase("named_struct()", "UNICODE",
        "<ROW/>"),
      StructsToXmlTestCase("named_struct('time', to_timestamp('2015-08-26'))", "UNICODE_CI",
        s"""<ROW>
           |    <time>2015-08-26T00:00:00.000-07:00</time>
           |</ROW>""".stripMargin)
    )

    // Supported collations
    testCases.foreach(t => {
      val query =
        s"""
           |select to_xml(${t.input})
           |""".stripMargin
      // Result
      withSQLConf(SqlApiConf.DEFAULT_COLLATION -> t.collationName) {
        val testQuery = sql(query)
        checkAnswer(testQuery, Row(t.result))
        val dataType = StringType(t.collationName)
        assert(testQuery.schema.fields.head.dataType.sameType(dataType))
      }
    })
  }

  test("Support ParseJson & TryParseJson variant expressions with collation") {
    case class ParseJsonTestCase(
      input: String,
      collationName: String,
      result: String
    )

    val testCases = Seq(
      ParseJsonTestCase("{\"a\":1,\"b\":2}", "UTF8_BINARY", "{\"a\":1,\"b\":2}"),
      ParseJsonTestCase("{\"A\":3,\"B\":4}", "UTF8_BINARY_LCASE", "{\"A\":3,\"B\":4}"),
      ParseJsonTestCase("{\"c\":5,\"d\":6}", "UNICODE", "{\"c\":5,\"d\":6}"),
      ParseJsonTestCase("{\"C\":7,\"D\":8}", "UNICODE_CI", "{\"C\":7,\"D\":8}")
    )

    // Supported collations (ParseJson)
    testCases.foreach(t => {
      val query =
        s"""
           |SELECT parse_json('${t.input}')
           |""".stripMargin
      // Result & data type
      withSQLConf(SqlApiConf.DEFAULT_COLLATION -> t.collationName) {
        val testQuery = sql(query)
        val testResult = testQuery.collect().map(_.toString()).mkString("")
        assert(testResult === "[" + t.result + "]") // can't use checkAnswer for Variant
        assert(testQuery.schema.fields.head.dataType.sameType(VariantType))
      }
    })

    // Supported collations (TryParseJson)
    testCases.foreach(t => {
      val query =
        s"""
           |SELECT try_parse_json('${t.input}')
           |""".stripMargin
      // Result & data type
      withSQLConf(SqlApiConf.DEFAULT_COLLATION -> t.collationName) {
        val testQuery = sql(query)
        val testResult = testQuery.collect().map(_.toString()).mkString("")
        assert(testResult === "[" + t.result + "]") // can't use checkAnswer for Variant
        assert(testQuery.schema.fields.head.dataType.sameType(VariantType))
      }
    })
  }

  test("Handle invalid JSON for ParseJson variant expression with collation") {
    // parse_json should throw an exception when the string is not valid JSON value
    val json = "{\"a\":1,"
    val query = s"SELECT parse_json('$json');"
    withSQLConf(SqlApiConf.DEFAULT_COLLATION -> "UNICODE") {
      val e = intercept[SparkException] {
        val testQuery = sql(query)
        testQuery.collect()
      }
      assert(e.getErrorClass === "MALFORMED_RECORD_IN_PARSING.WITHOUT_SUGGESTION")
    }
  }

  test("Handle invalid JSON for TryParseJson variant expression with collation") {
    // try_parse_json shouldn't throw an exception when the string is not valid JSON value
    val json = "{\"a\":1,]"
    val query = s"SELECT try_parse_json('$json');"
    withSQLConf(SqlApiConf.DEFAULT_COLLATION -> "UNICODE") {
      val testQuery = sql(query)
      val testResult = testQuery.collect().map(_.toString()).mkString("")
      assert(testResult === s"[null]")
    }
  }

  test("Support IsVariantNull variant expressions with collation") {
    case class IsVariantNullTestCase(
      input: String,
      collationName: String,
      result: Boolean
    )

    val testCases = Seq(
      IsVariantNullTestCase("'null'", "UTF8_BINARY", result = true),
      IsVariantNullTestCase("'\"null\"'", "UTF8_BINARY_LCASE", result = false),
      IsVariantNullTestCase("'13'", "UNICODE", result = false),
      IsVariantNullTestCase("null", "UNICODE_CI", result = false)
    )

    // Supported collations
    testCases.foreach(t => {
      val query =
        s"""
           |SELECT is_variant_null(parse_json(${t.input}))
           |""".stripMargin
      // Result & data type
      withSQLConf(SqlApiConf.DEFAULT_COLLATION -> t.collationName) {
        val testQuery = sql(query)
        checkAnswer(testQuery, Row(t.result))
      }
    })
  }

  test("Support VariantGet & TryVariantGet variant expressions with collation") {
    case class VariantGetTestCase(
      input: String,
      path: String,
      variantType: String,
      collationName: String,
      result: Any,
      resultType: DataType
    )

    val testCases = Seq(
      VariantGetTestCase("{\"a\": 1}", "$.a", "int", "UTF8_BINARY", 1, IntegerType),
      VariantGetTestCase("{\"a\": 1}", "$.b", "int", "UTF8_BINARY_LCASE", null, IntegerType),
      VariantGetTestCase("[1, \"2\"]", "$[1]", "string", "UNICODE", "2", StringType("UNICODE")),
      VariantGetTestCase("[1, \"2\"]", "$[2]", "string", "UNICODE_CI", null,
        StringType("UNICODE_CI"))
    )

    // Supported collations (VariantGet)
    testCases.foreach(t => {
      val query =
        s"""
           |SELECT variant_get(parse_json('${t.input}'), '${t.path}', '${t.variantType}')
           |""".stripMargin
      // Result & data type
      withSQLConf(SqlApiConf.DEFAULT_COLLATION -> t.collationName) {
        val testQuery = sql(query)
        val testResult = testQuery.collect().map(_.toString()).mkString("")
        assert(testResult === "[" + t.result + "]") // can't use checkAnswer for Variant
        assert(testQuery.schema.fields.head.dataType.sameType(t.resultType))
      }
    })

    // Supported collations (TryVariantGet)
    testCases.foreach(t => {
      val query =
        s"""
           |SELECT try_variant_get(parse_json('${t.input}'), '${t.path}', '${t.variantType}')
           |""".stripMargin
      // Result & data type
      withSQLConf(SqlApiConf.DEFAULT_COLLATION -> t.collationName) {
        val testQuery = sql(query)
        val testResult = testQuery.collect().map(_.toString()).mkString("")
        assert(testResult === "[" + t.result + "]") // can't use checkAnswer for Variant
        assert(testQuery.schema.fields.head.dataType.sameType(t.resultType))
      }
    })
  }

  test("Handle invalid JSON for VariantGet variant expression with collation") {
    // variant_get should throw an exception if the cast fails
    val json = "[1, \"Spark\"]"
    val query = s"SELECT variant_get(parse_json('$json'), '$$[1]', 'int');"
    withSQLConf(SqlApiConf.DEFAULT_COLLATION -> "UNICODE") {
      val e = intercept[SparkRuntimeException] {
        val testQuery = sql(query)
        testQuery.collect()
      }
      assert(e.getErrorClass === "INVALID_VARIANT_CAST")
    }
  }

  test("Handle invalid JSON for TryVariantGet variant expression with collation") {
    // try_variant_get shouldn't throw an exception if the cast fails
    val json = "[1, \"Spark\"]"
    val query = s"SELECT try_variant_get(parse_json('$json'), '$$[1]', 'int');"
    withSQLConf(SqlApiConf.DEFAULT_COLLATION -> "UNICODE") {
      val testQuery = sql(query)
      val testResult = testQuery.collect().map(_.toString()).mkString("")
      assert(testResult === s"[null]")
    }
  }

  test("Support VariantExplode variant expressions with collation") {
    case class VariantExplodeTestCase(
     input: String,
     collationName: String,
     result: String,
     resultType: Seq[StructField]
    )

    val testCases = Seq(
      VariantExplodeTestCase("[\"hello\", \"world\"]", "UTF8_BINARY",
          Row(0, "null", "\"hello\"").toString() + Row(1, "null", "\"world\"").toString(),
          Seq[StructField](
              StructField("pos", IntegerType, nullable = false),
              StructField("key", StringType("UTF8_BINARY")),
              StructField("value", VariantType, nullable = false)
          )
      ),
      VariantExplodeTestCase("[\"Spark\", \"SQL\"]", "UTF8_BINARY_LCASE",
        Row(0, "null", "\"Spark\"").toString() + Row(1, "null", "\"SQL\"").toString(),
        Seq[StructField](
          StructField("pos", IntegerType, nullable = false),
          StructField("key", StringType("UTF8_BINARY_LCASE")),
          StructField("value", VariantType, nullable = false)
        )
      ),
      VariantExplodeTestCase("{\"a\": true, \"b\": 3.14}", "UNICODE",
        Row(0, "a", "true").toString() + Row(1, "b", "3.14").toString(),
        Seq[StructField](
          StructField("pos", IntegerType, nullable = false),
          StructField("key", StringType("UNICODE")),
          StructField("value", VariantType, nullable = false)
        )
      ),
      VariantExplodeTestCase("{\"A\": 9.99, \"B\": false}", "UNICODE_CI",
        Row(0, "A", "9.99").toString() + Row(1, "B", "false").toString(),
        Seq[StructField](
          StructField("pos", IntegerType, nullable = false),
          StructField("key", StringType("UNICODE_CI")),
          StructField("value", VariantType, nullable = false)
        )
      )
    )

    // Supported collations
    testCases.foreach(t => {
      val query =
        s"""
           |SELECT * from variant_explode(parse_json('${t.input}'))
           |""".stripMargin
      // Result & data type
      withSQLConf(SqlApiConf.DEFAULT_COLLATION -> t.collationName) {
        val testQuery = sql(query)
        val testResult = testQuery.collect().map(_.toString()).mkString("")
        assert(testResult === t.result) // can't use checkAnswer for Variant
        assert(testQuery.schema.fields.sameElements(t.resultType))
      }
    })
  }

  test("Support SchemaOfVariant variant expressions with collation") {
    case class SchemaOfVariantTestCase(
     input: String,
     collationName: String,
     result: String
    )

    val testCases = Seq(
      SchemaOfVariantTestCase("null", "UTF8_BINARY", "VOID"),
      SchemaOfVariantTestCase("[]", "UTF8_BINARY_LCASE", "ARRAY<VOID>"),
      SchemaOfVariantTestCase("[{\"a\":true,\"b\":0}]", "UNICODE",
        "ARRAY<STRUCT<a: BOOLEAN, b: BIGINT>>"),
      SchemaOfVariantTestCase("[{\"A\":\"x\",\"B\":-1.00}]", "UNICODE_CI",
        "ARRAY<STRUCT<A: STRING COLLATE UNICODE_CI, B: DECIMAL(1,0)>>")
    )

    // Supported collations
    testCases.foreach(t => {
      val query =
        s"""
           |SELECT schema_of_variant(parse_json('${t.input}'))
           |""".stripMargin
      // Result & data type
      withSQLConf(SqlApiConf.DEFAULT_COLLATION -> t.collationName) {
        val testQuery = sql(query)
        checkAnswer(testQuery, Row(t.result))
        assert(testQuery.schema.fields.head.dataType.sameType(StringType(t.collationName)))
      }
    })
  }

  test("Support SchemaOfVariantAgg variant expressions with collation") {
    case class SchemaOfVariantAggTestCase(
      input: String,
      collationName: String,
      result: String
    )

    val testCases = Seq(
      SchemaOfVariantAggTestCase("('1'), ('2'), ('3')", "UTF8_BINARY", "BIGINT"),
      SchemaOfVariantAggTestCase("('true'), ('false'), ('true')", "UTF8_BINARY_LCASE", "BOOLEAN"),
      SchemaOfVariantAggTestCase("('{\"a\": 1}'), ('{\"b\": true}'), ('{\"c\": 1.23}')",
        "UNICODE", "STRUCT<a: BIGINT, b: BOOLEAN, c: DECIMAL(3,2)>"),
      SchemaOfVariantAggTestCase("('{\"A\": \"x\"}'), ('{\"B\": 9.99}'), ('{\"C\": 0}')",
        "UNICODE_CI", "STRUCT<A: STRING COLLATE UNICODE_CI, B: DECIMAL(3,2), C: BIGINT>")
    )

    // Supported collations
    testCases.foreach(t => {
      val query =
        s"""
           |SELECT schema_of_variant_agg(parse_json(j)) FROM VALUES ${t.input} AS tab(j)
           |""".stripMargin
      // Result & data type
      withSQLConf(SqlApiConf.DEFAULT_COLLATION -> t.collationName) {
        val testQuery = sql(query)
        checkAnswer(testQuery, Row(t.result))
        assert(testQuery.schema.fields.head.dataType.sameType(StringType(t.collationName)))
      }
    })
  }

  test("Support InputFileName expression with collation") {
    // Supported collations
    Seq("UTF8_BINARY", "UTF8_BINARY_LCASE", "UNICODE", "UNICODE_CI").foreach(collationName => {
      val query =
        s"""
           |select input_file_name()
           |""".stripMargin
      // Result
      withSQLConf(SqlApiConf.DEFAULT_COLLATION -> collationName) {
        val testQuery = sql(query)
        checkAnswer(testQuery, Row(""))
        val dataType = StringType(collationName)
        assert(testQuery.schema.fields.head.dataType.sameType(dataType))
      }
    })
  }

  // TODO: Add more tests for other SQL expressions

}
// scalastyle:on nonascii
