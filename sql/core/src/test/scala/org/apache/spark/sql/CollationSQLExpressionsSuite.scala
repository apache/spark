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

import java.sql.{Date, Timestamp}
import java.text.SimpleDateFormat

import scala.collection.immutable.Seq

import org.apache.spark.{SparkConf, SparkException, SparkIllegalArgumentException, SparkRuntimeException}
import org.apache.spark.sql.catalyst.{ExtendedAnalysisException, InternalRow}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.Mode
import org.apache.spark.sql.catalyst.util.CollationFactory
import org.apache.spark.sql.internal.{SqlApiConf, SQLConf}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.collection.OpenHashMap

// scalastyle:off nonascii
class CollationSQLExpressionsSuite
    extends QueryTest
    with SharedSparkSession
    with ExpressionEvalHelper {

  private val testSuppCollations =
    Seq(
      "UTF8_BINARY",
      "UTF8_BINARY_RTRIM",
      "UTF8_LCASE",
      "UTF8_LCASE_RTRIM",
      "UNICODE",
      "UNICODE_RTRIM",
      "UNICODE_CI",
      "UNICODE_CI_RTRIM")
  private val testAdditionalCollations = Seq("UNICODE",
    "SR", "SR_RTRIM", "SR_CI", "SR_AI", "SR_CI_AI")
  private val fullyQualifiedPrefix = s"${CollationFactory.CATALOG}.${CollationFactory.SCHEMA}."

  test("Support Md5 hash expression with collation") {
    case class Md5TestCase(
      input: String,
      collationName: String,
      result: String
    )

    val testCases = Seq(
      Md5TestCase("Spark", "UTF8_BINARY", "8cde774d6f7333752ed72cacddb05126"),
      Md5TestCase("Spark", "UTF8_BINARY_RTRIM", "8cde774d6f7333752ed72cacddb05126"),
      Md5TestCase("Spark", "UTF8_LCASE", "8cde774d6f7333752ed72cacddb05126"),
      Md5TestCase("Spark", "UTF8_LCASE_RTRIM", "8cde774d6f7333752ed72cacddb05126"),
      Md5TestCase("SQL", "UNICODE", "9778840a0100cb30c982876741b0b5a2"),
      Md5TestCase("SQL", "UNICODE_RTRIM", "9778840a0100cb30c982876741b0b5a2"),
      Md5TestCase("SQL", "UNICODE_CI", "9778840a0100cb30c982876741b0b5a2"),
      Md5TestCase("SQL", "UNICODE_CI_RTRIM", "9778840a0100cb30c982876741b0b5a2"),
      Md5TestCase("SQL", "SR_CI_AI", "9778840a0100cb30c982876741b0b5a2")
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
      Sha2TestCase("Spark", "UTF8_BINARY_RTRIM", 256,
        "529bc3b07127ecb7e53a4dcf1991d9152c24537d919178022b2c42657f79a26b"),
      Sha2TestCase("Spark", "UTF8_LCASE", 256,
        "529bc3b07127ecb7e53a4dcf1991d9152c24537d919178022b2c42657f79a26b"),
      Sha2TestCase("Spark", "UTF8_LCASE_RTRIM", 256,
        "529bc3b07127ecb7e53a4dcf1991d9152c24537d919178022b2c42657f79a26b"),
      Sha2TestCase("SQL", "UNICODE", 256,
        "a7056a455639d1c7deec82ee787db24a0c1878e2792b4597709f0facf7cc7b35"),
      Sha2TestCase("SQL", "UNICODE_RTRIM", 256,
        "a7056a455639d1c7deec82ee787db24a0c1878e2792b4597709f0facf7cc7b35"),
      Sha2TestCase("SQL", "UNICODE_CI", 256,
        "a7056a455639d1c7deec82ee787db24a0c1878e2792b4597709f0facf7cc7b35"),
      Sha2TestCase("SQL", "UNICODE_CI_RTRIM", 256,
        "a7056a455639d1c7deec82ee787db24a0c1878e2792b4597709f0facf7cc7b35"),
      Sha2TestCase("SQL", "SR_AI", 256,
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
      Sha1TestCase("Spark", "UTF8_BINARY_RTRIM", "85f5955f4b27a9a4c2aab6ffe5d7189fc298b92c"),
      Sha1TestCase("Spark", "UTF8_LCASE", "85f5955f4b27a9a4c2aab6ffe5d7189fc298b92c"),
      Sha1TestCase("Spark", "UTF8_LCASE_RTRIM", "85f5955f4b27a9a4c2aab6ffe5d7189fc298b92c"),
      Sha1TestCase("SQL", "UNICODE", "2064cb643caa8d9e1de12eea7f3e143ca9f8680d"),
      Sha1TestCase("SQL", "UNICODE_RTRIM", "2064cb643caa8d9e1de12eea7f3e143ca9f8680d"),
      Sha1TestCase("SQL", "UNICODE_CI", "2064cb643caa8d9e1de12eea7f3e143ca9f8680d"),
      Sha1TestCase("SQL", "UNICODE_CI_RTRIM", "2064cb643caa8d9e1de12eea7f3e143ca9f8680d"),
      Sha1TestCase("Spark", "SR_CI", "85f5955f4b27a9a4c2aab6ffe5d7189fc298b92c")
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
      Crc321TestCase("Spark", "UTF8_BINARY_RTRIM", 1557323817),
      Crc321TestCase("Spark", "UTF8_LCASE", 1557323817),
      Crc321TestCase("Spark", "UTF8_LCASE_RTRIM", 1557323817),
      Crc321TestCase("SQL", "UNICODE", 1299261525),
      Crc321TestCase("SQL", "UNICODE_RTRIM", 1299261525),
      Crc321TestCase("SQL", "UNICODE_CI", 1299261525),
      Crc321TestCase("SQL", "UNICODE_CI_RTRIM", 1299261525)
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
      Murmur3HashTestCase("Spark  ", "UTF8_BINARY_RTRIM", 1779328737),
      Murmur3HashTestCase("Spark", "UTF8_LCASE", -1928694360),
      Murmur3HashTestCase("Spark  ", "UTF8_LCASE_RTRIM", -1928694360),
      Murmur3HashTestCase("SQL", "UNICODE", 1483684981),
      Murmur3HashTestCase("SQL ", "UNICODE_RTRIM", 1483684981),
      Murmur3HashTestCase("SQL", "UNICODE_CI", 279787709),
      Murmur3HashTestCase("SQL ", "UNICODE_CI_RTRIM", 279787709)
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
      XxHash64TestCase("Spark ", "UTF8_BINARY_RTRIM", 6480371823304753502L),
      XxHash64TestCase("Spark", "UTF8_LCASE", -3142112654825786434L),
      XxHash64TestCase("Spark ", "UTF8_LCASE_RTRIM", -3142112654825786434L),
      XxHash64TestCase("SQL", "UNICODE", 7549349329256749019L),
      XxHash64TestCase("SQL ", "UNICODE_RTRIM", 7549349329256749019L),
      XxHash64TestCase("SQL", "UNICODE_CI", -3010409544364398863L),
      XxHash64TestCase("SQL ", "UNICODE_CI_RTRIM", -3010409544364398863L)
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
      UrlEncodeTestCase("https://spark.apache.org", "UTF8_BINARY_RTRIM",
        "https%3A%2F%2Fspark.apache.org"),
      UrlEncodeTestCase("https://spark.apache.org", "UTF8_LCASE",
        "https%3A%2F%2Fspark.apache.org"),
      UrlEncodeTestCase("https://spark.apache.org", "UTF8_LCASE_RTRIM",
        "https%3A%2F%2Fspark.apache.org"),
      UrlEncodeTestCase("https://spark.apache.org", "UNICODE",
        "https%3A%2F%2Fspark.apache.org"),
      UrlEncodeTestCase("https://spark.apache.org", "UNICODE_RTRIM",
        "https%3A%2F%2Fspark.apache.org"),
      UrlEncodeTestCase("https://spark.apache.org", "UNICODE_CI",
        "https%3A%2F%2Fspark.apache.org"),
      UrlEncodeTestCase("https://spark.apache.org", "UNICODE_CI_RTRIM",
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
      UrlDecodeTestCase("https%3A%2F%2Fspark.apache.org", "UTF8_BINARY_RTRIM",
        "https://spark.apache.org"),
      UrlDecodeTestCase("https%3A%2F%2Fspark.apache.org", "UTF8_LCASE",
        "https://spark.apache.org"),
      UrlDecodeTestCase("https%3A%2F%2Fspark.apache.org", "UTF8_LCASE_RTRIM",
        "https://spark.apache.org"),
      UrlDecodeTestCase("https%3A%2F%2Fspark.apache.org", "UNICODE",
        "https://spark.apache.org"),
      UrlDecodeTestCase("https%3A%2F%2Fspark.apache.org", "UNICODE_RTRIM",
        "https://spark.apache.org"),
      UrlDecodeTestCase("https%3A%2F%2Fspark.apache.org", "UNICODE_CI",
        "https://spark.apache.org"),
      UrlDecodeTestCase("https%3A%2F%2Fspark.apache.org", "UNICODE_CI_RTRIM",
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
      ParseUrlTestCase("http://spark.apache.org/path?query=1", "UTF8_BINARY_RTRIM", "HOST",
        "spark.apache.org"),
      ParseUrlTestCase("http://spark.apache.org/path?query=2", "UTF8_LCASE", "PATH",
        "/path"),
      ParseUrlTestCase("http://spark.apache.org/path?query=2", "UTF8_LCASE_RTRIM", "PATH",
        "/path"),
      ParseUrlTestCase("http://spark.apache.org/path?query=3", "UNICODE", "QUERY",
        "query=3"),
      ParseUrlTestCase("http://spark.apache.org/path?query=3", "UNICODE_RTRIM", "QUERY",
        "query=3"),
      ParseUrlTestCase("http://spark.apache.org/path?query=4", "UNICODE_CI", "PROTOCOL",
        "http"),
      ParseUrlTestCase("http://spark.apache.org/path?query=4", "UNICODE_CI_RTRIM", "PROTOCOL",
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
      CsvToStructsTestCase("1", "UTF8_BINARY_RTRIM", "'a INT'", "",
        Row(1), Seq(
          StructField("a", IntegerType, nullable = true)
        )),
      CsvToStructsTestCase("true, 0.8", "UTF8_LCASE", "'A BOOLEAN, B DOUBLE'", "",
        Row(true, 0.8), Seq(
          StructField("A", BooleanType, nullable = true),
          StructField("B", DoubleType, nullable = true)
        )),
      CsvToStructsTestCase("true, 0.8", "UTF8_LCASE_RTRIM", "'A BOOLEAN, B DOUBLE'", "",
        Row(true, 0.8), Seq(
          StructField("A", BooleanType, nullable = true),
          StructField("B", DoubleType, nullable = true)
        )),
      CsvToStructsTestCase("\"Spark\"", "UNICODE", "'a STRING'", "",
        Row("Spark"), Seq(
          StructField("a", StringType, nullable = true)
        )),
      CsvToStructsTestCase("\"Spark\"", "UTF8_BINARY", "'a STRING COLLATE UNICODE'", "",
        Row("Spark"), Seq(
          StructField("a", StringType("UNICODE"), nullable = true)
        )),
      CsvToStructsTestCase("\"Spark\"", "UNICODE_RTRIM", "'a STRING COLLATE UNICODE_RTRIM'", "",
        Row("Spark"), Seq(
          StructField("a", StringType("UNICODE_RTRIM"), nullable = true)
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
      SchemaOfCsvTestCase("1", "UTF8_BINARY_RTRIM", "STRUCT<_c0: INT>"),
      SchemaOfCsvTestCase("true,0.8", "UTF8_LCASE",
        "STRUCT<_c0: BOOLEAN, _c1: DOUBLE>"),
      SchemaOfCsvTestCase("true,0.8", "UTF8_LCASE_RTRIM",
        "STRUCT<_c0: BOOLEAN, _c1: DOUBLE>"),
      SchemaOfCsvTestCase("2015-08-26", "UNICODE", "STRUCT<_c0: DATE>"),
      SchemaOfCsvTestCase("2015-08-26", "UNICODE_RTRIM", "STRUCT<_c0: DATE>"),
      SchemaOfCsvTestCase("abc", "UNICODE_CI",
        "STRUCT<_c0: STRING>"),
      SchemaOfCsvTestCase("abc", "UNICODE_CI_RTRIM",
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
      StructsToCsvTestCase("named_struct('a', 1, 'b', 2)", "UTF8_BINARY_RTRIM", "1,2"),
      StructsToCsvTestCase("named_struct('A', true, 'B', 2.0)", "UTF8_LCASE", "true,2.0"),
      StructsToCsvTestCase("named_struct('A', true, 'B', 2.0)", "UTF8_LCASE_RTRIM", "true,2.0"),
      StructsToCsvTestCase("named_struct()", "UNICODE", null),
      StructsToCsvTestCase("named_struct()", "UNICODE_RTRIM", null),
      StructsToCsvTestCase("named_struct('time', to_timestamp('2015-08-26'))", "UNICODE_CI",
        "2015-08-26T00:00:00.000-07:00"),
      StructsToCsvTestCase("named_struct('time', to_timestamp('2015-08-26'))", "UNICODE_CI_RTRIM",
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
      ConvTestCase("100", "2", "10", "UTF8_BINARY_RTRIM", "4"),
      ConvTestCase("100", "2", "10", "UTF8_LCASE", "4"),
      ConvTestCase("100", "2", "10", "UTF8_LCASE_RTRIM", "4"),
      ConvTestCase("100", "2", "10", "UNICODE", "4"),
      ConvTestCase("100", "2", "10", "UNICODE_RTRIM", "4"),
      ConvTestCase("100", "2", "10", "UNICODE_CI", "4"),
      ConvTestCase("100", "2", "10", "UNICODE_CI_RTRIM", "4")
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
      BinTestCase("13", "UTF8_BINARY_RTRIM", "1101"),
      BinTestCase("13", "UTF8_LCASE", "1101"),
      BinTestCase("13", "UTF8_LCASE_RTRIM", "1101"),
      BinTestCase("13", "UNICODE", "1101"),
      BinTestCase("13", "UNICODE_RTRIM", "1101"),
      BinTestCase("13", "UNICODE_CI", "1101"),
      BinTestCase("13", "UNICODE_CI_RTRIM", "1101")
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
      HexTestCase("13", "UTF8_BINARY_RTRIM", "D"),
      HexTestCase("13", "UTF8_LCASE", "D"),
      HexTestCase("13", "UTF8_LCASE_RTRIM", "D"),
      HexTestCase("13", "UNICODE", "D"),
      HexTestCase("13", "UNICODE_RTRIM", "D"),
      HexTestCase("13", "UNICODE_CI", "D"),
      HexTestCase("13", "UNICODE_CI_RTRIM", "D")
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
      HexTestCase("Spark SQL", "UTF8_BINARY_RTRIM", "537061726B2053514C"),
      HexTestCase("Spark SQL", "UTF8_LCASE", "537061726B2053514C"),
      HexTestCase("Spark SQL", "UTF8_LCASE_RTRIM", "537061726B2053514C"),
      HexTestCase("Spark SQL", "UNICODE", "537061726B2053514C"),
      HexTestCase("Spark SQL", "UNICODE_RTRIM", "537061726B2053514C"),
      HexTestCase("Spark SQL", "UNICODE_CI", "537061726B2053514C"),
      HexTestCase("Spark SQL", "UNICODE_CI_RTRIM", "537061726B2053514C"),
      HexTestCase("Spark SQL", "DE_CI_AI", "537061726B2053514C"),
      HexTestCase("Spark SQL", "DE_CI_AI_RTRIM", "537061726B2053514C")
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
      UnHexTestCase("537061726B2053514C", "UTF8_BINARY_RTRIM", "Spark SQL"),
      UnHexTestCase("537061726B2053514C", "UTF8_LCASE", "Spark SQL"),
      UnHexTestCase("537061726B2053514C", "UTF8_LCASE_RTRIM", "Spark SQL"),
      UnHexTestCase("537061726B2053514C", "UNICODE", "Spark SQL"),
      UnHexTestCase("537061726B2053514C", "UNICODE_RTRIM", "Spark SQL"),
      UnHexTestCase("537061726B2053514C", "UNICODE_CI", "Spark SQL"),
      UnHexTestCase("537061726B2053514C", "UNICODE_CI_RTRIM", "Spark SQL"),
      UnHexTestCase("537061726B2053514C", "DE", "Spark SQL")
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
      XPathTestCase("<a><b>1</b></a>", "a/b",
        "xpath_boolean", "UTF8_BINARY_RTRIM", true, BooleanType),
      XPathTestCase("<A><B>1</B><B>2</B></A>", "sum(A/B)",
        "xpath_short", "UTF8_BINARY_RTRIM", 3, ShortType),
      XPathTestCase("<a><b>3</b><b>4</b></a>", "sum(a/b)",
        "xpath_int", "UTF8_LCASE", 7, IntegerType),
      XPathTestCase("<A><B>5</B><B>6</B></A>", "sum(A/B)",
        "xpath_long", "UTF8_LCASE", 11, LongType),
      XPathTestCase("<a><b>3</b><b>4</b></a>", "sum(a/b)",
        "xpath_int", "UTF8_LCASE_RTRIM", 7, IntegerType),
      XPathTestCase("<A><B>5</B><B>6</B></A>", "sum(A/B)",
        "xpath_long", "UTF8_LCASE_RTRIM", 11, LongType),
      XPathTestCase("<a><b>7</b><b>8</b></a>", "sum(a/b)",
        "xpath_float", "UNICODE", 15.0, FloatType),
      XPathTestCase("<A><B>9</B><B>0</B></A>", "sum(A/B)",
        "xpath_double", "UNICODE", 9.0, DoubleType),
      XPathTestCase("<a><b>7</b><b>8</b></a>", "sum(a/b)",
        "xpath_float", "UNICODE_RTRIM", 15.0, FloatType),
      XPathTestCase("<A><B>9</B><B>0</B></A>", "sum(A/B)",
        "xpath_double", "UNICODE_RTRIM", 9.0, DoubleType),
      XPathTestCase("<a><b>b</b><c>cc</c></a>", "a/c",
        "xpath_string", "UNICODE_CI", "cc", StringType("UNICODE_CI")),
      XPathTestCase("<a><b>b</b><c>cc </c></a>", "a/c",
        "xpath_string", "UNICODE_CI_RTRIM", "cc ", StringType("UNICODE_CI_RTRIM")),
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
      StringSpaceTestCase(1, "UTF8_BINARY_RTRIM", " "),
      StringSpaceTestCase(2, "UTF8_LCASE", "  "),
      StringSpaceTestCase(2, "UTF8_LCASE_RTRIM", "  "),
      StringSpaceTestCase(3, "UNICODE", "   "),
      StringSpaceTestCase(3, "UNICODE_RTRIM", "   "),
      StringSpaceTestCase(4, "UNICODE_CI", "    "),
      StringSpaceTestCase(4, "UNICODE_CI_RTRIM", "    "),
      StringSpaceTestCase(5, "AF_CI_AI", "     "),
      StringSpaceTestCase(5, "AF_CI_AI_RTRIM", "     ")
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
      ToNumberTestCase("123", "UTF8_BINARY_RTRIM", "999", 123, DecimalType(3, 0)),
      ToNumberTestCase("1", "UTF8_LCASE", "0.00", 1.00, DecimalType(3, 2)),
      ToNumberTestCase("1", "UTF8_LCASE_RTRIM", "0.00", 1.00, DecimalType(3, 2)),
      ToNumberTestCase("99,999", "UNICODE", "99,999", 99999, DecimalType(5, 0)),
      ToNumberTestCase("99,999", "UNICODE_RTRIM", "99,999", 99999, DecimalType(5, 0)),
      ToNumberTestCase("$14.99", "UNICODE_CI", "$99.99", 14.99, DecimalType(4, 2)),
      ToNumberTestCase("$14.99", "UNICODE_CI_RTRIM", "$99.99", 14.99, DecimalType(4, 2))
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
      checkError(
        exception = intercept[SparkIllegalArgumentException] {
          val testQuery = sql(query)
          testQuery.collect()
        },
        condition = "INVALID_FORMAT.MISMATCH_INPUT",
        parameters = Map("inputType" -> "\"STRING\"", "input" -> "xx", "format" -> "999")
      )
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
      ToCharTestCase(12, "UTF8_BINARY_RTRIM", "999", " 12"),
      ToCharTestCase(34, "UTF8_LCASE", "000D00", "034.00"),
      ToCharTestCase(34, "UTF8_LCASE_RTRIM", "000D00", "034.00"),
      ToCharTestCase(56, "UNICODE", "$99.99", "$56.00"),
      ToCharTestCase(56, "UNICODE_RTRIM", "$99.99", "$56.00"),
      ToCharTestCase(78, "UNICODE_CI", "99D9S", "78.0+"),
      ToCharTestCase(78, "UNICODE_CI_RTRIM", "99D9S", "78.0+")
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
      GetJsonObjectTestCase("{\"a\":\"b\"}", "$.a", "UTF8_BINARY_RTRIM", "b"),
      GetJsonObjectTestCase("{\"A\":\"1\"}", "$.A", "UTF8_LCASE", "1"),
      GetJsonObjectTestCase("{\"A\":\"1\"}", "$.A", "UTF8_LCASE_RTRIM", "1"),
      GetJsonObjectTestCase("{\"x\":true}", "$.x", "UNICODE", "true"),
      GetJsonObjectTestCase("{\"x\":true}", "$.x", "UNICODE_RTRIM", "true"),
      GetJsonObjectTestCase("{\"X\":1}", "$.X", "UNICODE_CI", "1"),
      GetJsonObjectTestCase("{\"X\":1}", "$.X", "UNICODE_CI_RTRIM", "1")
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
      JsonTupleTestCase("{\"a\":1, \"b\":2}", "'a', 'b'", "UTF8_BINARY_RTRIM",
        Row("1", "2")),
      JsonTupleTestCase("{\"A\":\"3\", \"B\":\"4\"}", "'A', 'B'", "UTF8_LCASE",
        Row("3", "4")),
      JsonTupleTestCase("{\"A\":\"3\", \"B\":\"4\"}", "'A', 'B'", "UTF8_LCASE_RTRIM",
        Row("3", "4")),
      JsonTupleTestCase("{\"x\":true, \"y\":false}", "'x', 'y'", "UNICODE",
        Row("true", "false")),
      JsonTupleTestCase("{\"x\":true, \"y\":false}", "'x', 'y'", "UNICODE_RTRIM",
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
      JsonToStructsTestCase("{\"a\":1, \"b\":2.0}", "a INT, b DOUBLE",
        "UTF8_BINARY_RTRIM", Row(Row(1, 2.0))),
      JsonToStructsTestCase("{\"A\":\"3\", \"B\":4}", "A STRING COLLATE UTF8_LCASE, B INT",
        "UTF8_LCASE", Row(Row("3", 4))),
      JsonToStructsTestCase("{\"A\":\"3\", \"B\":4}", "A STRING COLLATE UTF8_LCASE, B INT",
        "UTF8_LCASE_RTRIM", Row(Row("3", 4))),
      JsonToStructsTestCase("{\"x\":true, \"y\":null}", "x BOOLEAN, y VOID",
        "UNICODE", Row(Row(true, null))),
      JsonToStructsTestCase("{\"x\":true, \"y\":null}", "x BOOLEAN, y VOID",
        "UNICODE_RTRIM", Row(Row(true, null))),
      JsonToStructsTestCase("{\"X\":null, \"Y\":false}", "X VOID, Y BOOLEAN",
        "UNICODE_CI", Row(Row(null, false))),
      JsonToStructsTestCase("{\"X\":null, \"Y\":false}", "X VOID, Y BOOLEAN",
        "UNICODE_CI_RTRIM", Row(Row(null, false)))
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
      StructsToJsonTestCase("named_struct('a', 1, 'b', 2)",
        "UTF8_BINARY_RTRIM", Row("{\"a\":1,\"b\":2}")),
      StructsToJsonTestCase("array(named_struct('a', 1, 'b', 2))",
        "UTF8_LCASE", Row("[{\"a\":1,\"b\":2}]")),
      StructsToJsonTestCase("array(named_struct('a', 1, 'b', 2))",
        "UTF8_LCASE_RTRIM", Row("[{\"a\":1,\"b\":2}]")),
      StructsToJsonTestCase("map('a', named_struct('b', 1))",
        "UNICODE", Row("{\"a\":{\"b\":1}}")),
      StructsToJsonTestCase("map('a', named_struct('b', 1))",
        "UNICODE_RTRIM", Row("{\"a\":{\"b\":1}}")),
      StructsToJsonTestCase("array(map('a', 1))",
        "UNICODE_CI", Row("[{\"a\":1}]")),
      StructsToJsonTestCase("array(map('a', 1))",
        "UNICODE_CI_RTRIM", Row("[{\"a\":1}]"))
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
      LengthOfJsonArrayTestCase("'[1,2,3,4]'", "UTF8_BINARY_RTRIM", Row(4)),
      LengthOfJsonArrayTestCase("'[1,2,3,{\"f1\":1,\"f2\":[5,6]},4]'", "UTF8_LCASE", Row(5)),
      LengthOfJsonArrayTestCase("'[1,2,3,{\"f1\":1,\"f2\":[5,6]},4]'", "UTF8_LCASE_RTRIM", Row(5)),
      LengthOfJsonArrayTestCase("'[1,2'", "UNICODE", Row(null)),
      LengthOfJsonArrayTestCase("'[1,2'", "UNICODE_RTRIM", Row(null)),
      LengthOfJsonArrayTestCase("'['", "UNICODE_CI", Row(null)),
      LengthOfJsonArrayTestCase("'['", "UNICODE_CI_RTRIM", Row(null))
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
      JsonObjectKeysJsonArrayTestCase("{}", "UTF8_BINARY_RTRIM",
        Row(Seq())),
      JsonObjectKeysJsonArrayTestCase("{\"k\":", "UTF8_LCASE",
        Row(null)),
      JsonObjectKeysJsonArrayTestCase("{\"k\":", "UTF8_LCASE_RTRIM",
        Row(null)),
      JsonObjectKeysJsonArrayTestCase("{\"k1\": \"v1\"}", "UNICODE",
        Row(Seq("k1"))),
      JsonObjectKeysJsonArrayTestCase("{\"k1\": \"v1\"}", "UNICODE_RTRIM",
        Row(Seq("k1"))),
      JsonObjectKeysJsonArrayTestCase("{\"k1\":1,\"k2\":{\"k3\":3, \"k4\":4}}", "UNICODE_CI",
        Row(Seq("k1", "k2"))),
      JsonObjectKeysJsonArrayTestCase("{\"k1\":1,\"k2\":{\"k3\":3, \"k4\":4}}", "UNICODE_CI_RTRIM",
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
      SchemaOfJsonTestCase("'[{\"col\":0}]'",
        "UTF8_BINARY_RTRIM", Row("ARRAY<STRUCT<col: BIGINT>>")),
      SchemaOfJsonTestCase("'[{\"col\":01}]', map('allowNumericLeadingZeros', 'true')",
        "UTF8_LCASE", Row("ARRAY<STRUCT<col: BIGINT>>")),
      SchemaOfJsonTestCase("'[{\"col\":01}]', map('allowNumericLeadingZeros', 'true')",
        "UTF8_LCASE_RTRIM", Row("ARRAY<STRUCT<col: BIGINT>>")),
      SchemaOfJsonTestCase("'[]'",
        "UNICODE", Row("ARRAY<STRING>")),
      SchemaOfJsonTestCase("'[]'",
        "UNICODE_RTRIM", Row("ARRAY<STRING>")),
      SchemaOfJsonTestCase("''",
        "UNICODE_CI", Row("STRING")),
      SchemaOfJsonTestCase("''",
        "UNICODE_CI_RTRIM", Row("STRING"))
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

  test("Support `StringToMap` expression with collation") {
    case class StringToMapTestCase[R](
        text: String,
        pairDelim: String,
        keyValueDelim: String,
        collation: String,
        result: R)
    val testCases = Seq(
      StringToMapTestCase("a:1,b:2,c:3", ",", ":", "UTF8_BINARY",
        Map("a" -> "1", "b" -> "2", "c" -> "3")),
      StringToMapTestCase("A-1xB-2xC-3", "X", "-", "UTF8_LCASE",
        Map("A" -> "1", "B" -> "2", "C" -> "3")),
      StringToMapTestCase("1:ax2:bx3:c", "x", ":", "UNICODE",
        Map("1" -> "a", "2" -> "b", "3" -> "c")),
      StringToMapTestCase("1/AX2/BX3/C", "x", "/", "UNICODE_CI",
        Map("1" -> "A", "2" -> "B", "3" -> "C")),
      StringToMapTestCase("1:cx2:čx3:ć", "x", ":", "SR_CI_AI",
        Map("1" -> "c", "2" -> "č", "3" -> "ć")),
      StringToMapTestCase("c:1,č:2,ć:3", ",", ":", "SR_CI",
        Map("c" -> "1", "č" -> "2", "ć" -> "3"))
    )
    val unsupportedTestCases = Seq(
      StringToMapTestCase("a:1,b:2,c:3", "?", "?", "UNICODE_AI", null))
    testCases.foreach(t => {
      // Unit test.
      val text = Literal.create(t.text, StringType(t.collation))
      val pairDelim = Literal.create(t.pairDelim, StringType(t.collation))
      val keyValueDelim = Literal.create(t.keyValueDelim, StringType(t.collation))
      checkEvaluation(StringToMap(text, pairDelim, keyValueDelim), t.result)
      // E2E SQL test.
      withSQLConf(SQLConf.DEFAULT_COLLATION.key -> t.collation) {
        val query = s"SELECT str_to_map('${t.text}', '${t.pairDelim}', '${t.keyValueDelim}')"
        checkAnswer(sql(query), Row(t.result))
        val dataType = MapType(StringType(t.collation), StringType(t.collation), true)
        assert(sql(query).schema.fields.head.dataType.sameType(dataType))
      }
    })
    // Test unsupported collation.
    unsupportedTestCases.foreach(t => {
      withSQLConf(SQLConf.DEFAULT_COLLATION.key -> t.collation) {
        val query =
          s"select str_to_map('${t.text}', '${t.pairDelim}', " +
            s"'${t.keyValueDelim}')"
        checkError(
          exception = intercept[AnalysisException] {
            sql(query).collect()
          },
          condition = "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE",
          sqlState = Some("42K09"),
          parameters = Map(
            "sqlExpr" -> ("\"str_to_map('a:1,b:2,c:3' collate " + s"${t.collation}, " +
              "'?' collate " + s"${t.collation}, '?' collate ${t.collation})" + "\""),
            "paramIndex" -> "first",
            "inputSql" -> ("\"'a:1,b:2,c:3' collate " + s"${t.collation}" + "\""),
            "inputType" -> ("\"STRING COLLATE " + s"${t.collation}" + "\""),
            "requiredType" -> "\"STRING\""),
          context = ExpectedContext(
            fragment = "str_to_map('a:1,b:2,c:3', '?', '?')",
            start = 7,
            stop = 41))
      }
    })
  }

  test("Support RaiseError misc expression with collation") {
    // Supported collations
    case class RaiseErrorTestCase(errorMessage: String, collationName: String)
    val testCases = Seq(
      RaiseErrorTestCase("custom error message 1", "UTF8_BINARY"),
      RaiseErrorTestCase("custom error message 1", "UTF8_BINARY_RTRIM"),
      RaiseErrorTestCase("custom error message 2", "UTF8_LCASE"),
      RaiseErrorTestCase("custom error message 2", "UTF8_LCASE_RTRIM"),
      RaiseErrorTestCase("custom error message 3", "UNICODE"),
      RaiseErrorTestCase("custom error message 3", "UNICODE_RTRIM"),
      RaiseErrorTestCase("custom error message 4", "UNICODE_CI"),
      RaiseErrorTestCase("custom error message 4", "UNICODE_CI_RTRIM")
    )
    testCases.foreach(t => {
      withSQLConf(SqlApiConf.DEFAULT_COLLATION -> t.collationName) {
        val query = s"SELECT raise_error('${t.errorMessage}')"
        // Result & data type
        checkError(
          exception = intercept[SparkRuntimeException] {
            sql(query).collect()
          },
          condition = "USER_RAISED_EXCEPTION",
          parameters = Map("errorMessage" -> t.errorMessage)
        )
      }
    })
  }

  test("Support CurrentDatabase/Catalog/User expressions with collation") {
    // Supported collations
    Seq(
      "UTF8_LCASE",
      "UTF8_LCASE_RTRIM",
      "UNICODE",
      "UNICODE_RTRIM",
      "UNICODE_CI",
      "SR_CI_AI").foreach(collationName =>
      withSQLConf(SqlApiConf.DEFAULT_COLLATION -> collationName) {
        val queryDatabase = sql("SELECT current_schema()")
        val queryCatalog = sql("SELECT current_catalog()")
        val queryUser = sql("SELECT current_user()")
        // Data type
        val dataType = StringType(collationName)
        assert(queryDatabase.schema.fields.head.dataType.sameType(dataType))
        assert(queryCatalog.schema.fields.head.dataType.sameType(dataType))
        assert(queryUser.schema.fields.head.dataType.sameType(dataType))
      }
    )
  }

  test("Support Uuid misc expression with collation") {
    // Supported collations
    Seq(
      "UTF8_LCASE",
      "UTF8_LCASE_RTRIM",
      "UNICODE",
      "UNICODE_RTRIM",
      "UNICODE_CI",
      "UNICODE_CI_RTRIM",
      "NO_CI_AI").foreach(collationName =>
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
    Seq("UTF8_BINARY", "UTF8_LCASE", "UNICODE", "UNICODE_CI", "DE").foreach(collationName =>
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
      TypeOfTestCase("\"A\"", "UTF8_LCASE", "string collate UTF8_LCASE"),
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
      AesEncryptTestCase("Spark", "UTF8_LCASE", "'1234567890abcdef', 'ECB', 'DEFAULT', ''",
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
        "UTF8_LCASE", "'1234567890abcdef', 'ECB', 'DEFAULT', ''", "Spark"),
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
      MaskTestCase("ab-CD-12-@$", "X", null, null, null, "UTF8_LCASE", "ab-XX-12-@$"),
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
    checkError(
      exception = intercept[AnalysisException] {
        sql("SELECT mask(collate('ab-CD-12-@$','UNICODE'),collate('X','UNICODE_CI'),'x','0','#')")
      },
      condition = "COLLATION_MISMATCH.EXPLICIT",
      parameters = Map(
        "explicitTypes" -> """"STRING COLLATE UNICODE", "STRING COLLATE UNICODE_CI""""
      )
    )
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
      XmlToStructsTestCase("<p><a>1</a></p>", "UTF8_BINARY_RTRIM", "'a INT'", "",
        Row(1), Seq(
          StructField("a", IntegerType, nullable = true)
        )),
      XmlToStructsTestCase("<p><A>true</A><B>0.8</B></p>", "UTF8_LCASE",
        "'A BOOLEAN, B DOUBLE'", "", Row(true, 0.8), Seq(
          StructField("A", BooleanType, nullable = true),
          StructField("B", DoubleType, nullable = true)
        )),
      XmlToStructsTestCase("<p><A>true</A><B>0.8</B></p>", "UTF8_LCASE_RTRIM",
        "'A BOOLEAN, B DOUBLE'", "", Row(true, 0.8), Seq(
          StructField("A", BooleanType, nullable = true),
          StructField("B", DoubleType, nullable = true)
        )),
      XmlToStructsTestCase("<p><s>Spark</s></p>", "UNICODE", "'s STRING'", "",
        Row("Spark"), Seq(
          StructField("s", StringType, nullable = true)
        )),
      XmlToStructsTestCase("<p><s>Spark</s></p>", "UTF8_BINARY", "'s STRING COLLATE UNICODE'", "",
        Row("Spark"), Seq(
          StructField("s", StringType("UNICODE"), nullable = true)
        )),
      XmlToStructsTestCase("<p><s>Spark</s></p>", "UNICODE_RTRIM",
        "'s STRING COLLATE UNICODE_RTRIM'", "",
        Row("Spark"), Seq(
          StructField("s", StringType("UNICODE_RTRIM"), nullable = true)
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
      SchemaOfXmlTestCase("<p><a>1</a></p>", "UTF8_BINARY_RTRIM", "STRUCT<a: BIGINT>"),
      SchemaOfXmlTestCase("<p><A>true</A><B>0.8</B></p>", "UTF8_LCASE",
        "STRUCT<A: BOOLEAN, B: DOUBLE>"),
      SchemaOfXmlTestCase("<p><A>true</A><B>0.8</B></p>", "UTF8_LCASE_RTRIM",
        "STRUCT<A: BOOLEAN, B: DOUBLE>"),
      SchemaOfXmlTestCase("<p></p>", "UNICODE", "STRUCT<>"),
      SchemaOfXmlTestCase("<p></p>", "UNICODE_RTRIM", "STRUCT<>"),
      SchemaOfXmlTestCase("<p><A>1</A><A>2</A><A>3</A></p>", "UNICODE_CI",
        "STRUCT<A: ARRAY<BIGINT>>"),
      SchemaOfXmlTestCase("<p><A>1</A><A>2</A><A>3</A></p>", "UNICODE_CI_RTRIM",
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
      StructsToXmlTestCase("named_struct('a', 1, 'b', 2)", "UTF8_BINARY_RTRIM",
        s"""<ROW>
           |    <a>1</a>
           |    <b>2</b>
           |</ROW>""".stripMargin),
      StructsToXmlTestCase("named_struct('A', true, 'B', 2.0)", "UTF8_LCASE",
        s"""<ROW>
           |    <A>true</A>
           |    <B>2.0</B>
           |</ROW>""".stripMargin),
      StructsToXmlTestCase("named_struct('A', 'aa', 'B', 'bb')", "UTF8_LCASE",
        s"""<ROW>
           |    <A>aa</A>
           |    <B>bb</B>
           |</ROW>""".stripMargin),
      StructsToXmlTestCase("named_struct('A', 'aa', 'B', 'bb')", "UTF8_LCASE_RTRIM",
        s"""<ROW>
           |    <A>aa</A>
           |    <B>bb</B>
           |</ROW>""".stripMargin),
      StructsToXmlTestCase("named_struct('A', 'aa', 'B', 'bb')", "UTF8_BINARY",
        s"""<ROW>
           |    <A>aa</A>
           |    <B>bb</B>
           |</ROW>""".stripMargin),
      StructsToXmlTestCase("named_struct()", "UNICODE",
        "<ROW/>"),
      StructsToXmlTestCase("named_struct()", "UNICODE_RTRIM",
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
      ParseJsonTestCase("{\"a\":1,\"b\":2}", "UTF8_BINARY_RTRIM", "{\"a\":1,\"b\":2}"),
      ParseJsonTestCase("{\"A\":3,\"B\":4}", "UTF8_LCASE", "{\"A\":3,\"B\":4}"),
      ParseJsonTestCase("{\"A\":3,\"B\":4}", "UTF8_LCASE_RTRIM", "{\"A\":3,\"B\":4}"),
      ParseJsonTestCase("{\"c\":5,\"d\":6}", "UNICODE", "{\"c\":5,\"d\":6}"),
      ParseJsonTestCase("{\"c\":5,\"d\":6}", "UNICODE_RTRIM", "{\"c\":5,\"d\":6}"),
      ParseJsonTestCase("{\"C\":7,\"D\":8}", "UNICODE_CI", "{\"C\":7,\"D\":8}"),
      ParseJsonTestCase("{\"C\":7,\"D\":8}", "UNICODE_CI_RTRIM", "{\"C\":7,\"D\":8}")
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
      checkError(
        exception = intercept[SparkException] {
          val testQuery = sql(query)
          testQuery.collect()
        },
        condition = "MALFORMED_RECORD_IN_PARSING.WITHOUT_SUGGESTION",
        parameters = Map("badRecord" -> "{\"a\":1,", "failFastMode" -> "FAILFAST")
      )
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
      IsVariantNullTestCase("'null'", "UTF8_BINARY_RTRIM", result = true),
      IsVariantNullTestCase("'\"null\"'", "UTF8_LCASE", result = false),
      IsVariantNullTestCase("'\"null\"'", "UTF8_LCASE_RTRIM", result = false),
      IsVariantNullTestCase("'13'", "UNICODE", result = false),
      IsVariantNullTestCase("'13'", "UNICODE_RTRIM", result = false),
      IsVariantNullTestCase("null", "UNICODE_CI", result = false),
      IsVariantNullTestCase("null", "UNICODE_CI_RTRIM", result = false)
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
      VariantGetTestCase("{\"a\": 1}", "$.a", "int", "UTF8_BINARY_RTRIM", 1, IntegerType),
      VariantGetTestCase("{\"a\": 1}", "$.b", "int", "UTF8_LCASE", null, IntegerType),
      VariantGetTestCase("[1, \"2\"]", "$[1]", "string", "UNICODE", "2",
        StringType),
      VariantGetTestCase("[1, \"2\"]", "$[1]", "string collate unicode", "UTF8_BINARY", "2",
        StringType("UNICODE")),
      VariantGetTestCase("[1, \"2\"]", "$[2]", "string", "UNICODE_CI", null,
        StringType),
      VariantGetTestCase("[1, \"2\"]", "$[2]", "string collate unicode_CI", "UTF8_BINARY", null,
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
      checkError(
        exception = intercept[SparkRuntimeException] {
          val testQuery = sql(query)
          testQuery.collect()
        },
        condition = "INVALID_VARIANT_CAST",
        parameters = Map("value" -> "\"Spark\"", "dataType" -> "\"INT\"")
      )
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
      VariantExplodeTestCase("[\"hello\", \"world\"]", "UTF8_BINARY_RTRIM",
        Row(0, "null", "\"hello\"").toString() + Row(1, "null", "\"world\"").toString(),
        Seq[StructField](
          StructField("pos", IntegerType, nullable = false),
          StructField("key", StringType("UTF8_BINARY_RTRIM")),
          StructField("value", VariantType, nullable = false)
        )
      ),
      VariantExplodeTestCase("[\"Spark\", \"SQL\"]", "UTF8_LCASE",
        Row(0, "null", "\"Spark\"").toString() + Row(1, "null", "\"SQL\"").toString(),
        Seq[StructField](
          StructField("pos", IntegerType, nullable = false),
          StructField("key", StringType("UTF8_LCASE")),
          StructField("value", VariantType, nullable = false)
        )
      ),
      VariantExplodeTestCase("[\"Spark\", \"SQL\"]", "UTF8_LCASE_RTRIM",
        Row(0, "null", "\"Spark\"").toString() + Row(1, "null", "\"SQL\"").toString(),
        Seq[StructField](
          StructField("pos", IntegerType, nullable = false),
          StructField("key", StringType("UTF8_LCASE_RTRIM")),
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
      VariantExplodeTestCase("{\"a\": true, \"b\": 3.14}", "UNICODE_RTRIM",
        Row(0, "a", "true").toString() + Row(1, "b", "3.14").toString(),
        Seq[StructField](
          StructField("pos", IntegerType, nullable = false),
          StructField("key", StringType("UNICODE_RTRIM")),
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
      SchemaOfVariantTestCase("null", "UTF8_BINARY_RTRIM", "VOID"),
      SchemaOfVariantTestCase("[]", "UTF8_LCASE", "ARRAY<VOID>"),
      SchemaOfVariantTestCase("[]", "UTF8_LCASE_RTRIM", "ARRAY<VOID>"),
      SchemaOfVariantTestCase("[{\"a\":true,\"b\":0}]", "UNICODE",
        "ARRAY<OBJECT<a: BOOLEAN, b: BIGINT>>"),
      SchemaOfVariantTestCase("[{\"a\":true,\"b\":0}]", "UNICODE_RTRIM",
        "ARRAY<OBJECT<a: BOOLEAN, b: BIGINT>>"),
      SchemaOfVariantTestCase("[{\"A\":\"x\",\"B\":-1.00}]", "UNICODE_CI",
        "ARRAY<OBJECT<A: STRING COLLATE UNICODE_CI, B: DECIMAL(1,0)>>"),
      SchemaOfVariantTestCase("[{\"A\":\"x\",\"B\":-1.00}]", "UNICODE_CI_RTRIM",
        "ARRAY<OBJECT<A: STRING COLLATE UNICODE_CI_RTRIM, B: DECIMAL(1,0)>>")
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
      SchemaOfVariantAggTestCase("('1'), ('2'), ('3')", "UTF8_BINARY_RTRIM", "BIGINT"),
      SchemaOfVariantAggTestCase("('true'), ('false'), ('true')", "UTF8_LCASE", "BOOLEAN"),
      SchemaOfVariantAggTestCase("('true'), ('false'), ('true')", "UTF8_LCASE_RTRIM", "BOOLEAN"),
      SchemaOfVariantAggTestCase("('{\"a\": 1}'), ('{\"b\": true}'), ('{\"c\": 1.23}')",
        "UNICODE", "OBJECT<a: BIGINT, b: BOOLEAN, c: DECIMAL(3,2)>"),
      SchemaOfVariantAggTestCase("('{\"a\": 1}'), ('{\"b\": true}'), ('{\"c\": 1.23}')",
        "UNICODE_RTRIM", "OBJECT<a: BIGINT, b: BOOLEAN, c: DECIMAL(3,2)>"),
      SchemaOfVariantAggTestCase("('{\"A\": \"x\"}'), ('{\"B\": 9.99}'), ('{\"C\": 0}')",
        "UNICODE_CI", "OBJECT<A: STRING COLLATE UNICODE_CI, B: DECIMAL(3,2), C: BIGINT>"),
      SchemaOfVariantAggTestCase("('{\"A\": \"x\"}'), ('{\"B\": 9.99}'), ('{\"C\": 0}')",
        "UNICODE_CI_RTRIM", "OBJECT<A: STRING COLLATE UNICODE_CI_RTRIM, B: DECIMAL(3,2), C: BIGINT>"
      )
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
    Seq(
      "UTF8_BINARY",
      "UTF8_BINARY_RTRIM",
      "UTF8_LCASE",
      "UTF8_LCASE_RTRIM",
      "UNICODE",
      "UNICODE_RTRIM",
      "UNICODE_CI",
      "UNICODE_CI_RTRIM",
      "MT_CI_AI").foreach(collationName => {
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

  test("DateFormat expression with collation") {
    case class DateFormatTestCase[R](date: String, format: String, collation: String, result: R)
    val testCases = Seq(
      DateFormatTestCase("2021-01-01", "yyyy-MM-dd", "UTF8_BINARY", "2021-01-01"),
      DateFormatTestCase("2021-01-01", "yyyy-MM-dd", "UTF8_BINARY_RTRIM", "2021-01-01"),
      DateFormatTestCase("2021-01-01", "yyyy-dd", "UTF8_LCASE", "2021-01"),
      DateFormatTestCase("2021-01-01", "yyyy-dd", "UTF8_LCASE_RTRIM", "2021-01"),
      DateFormatTestCase("2021-01-01", "yyyy-MM-dd", "UNICODE", "2021-01-01"),
      DateFormatTestCase("2021-01-01", "yyyy-MM-dd", "UNICODE_RTRIM", "2021-01-01"),
      DateFormatTestCase("2021-01-01", "yyyy", "UNICODE_CI", "2021"),
      DateFormatTestCase("2021-01-01", "yyyy", "UNICODE_CI_RTRIM", "2021")
    )

    for {
      collateDate <- Seq(true, false)
      collateFormat <- Seq(true, false)
    } {
      testCases.foreach(t => {
        val dateArg = if (collateDate) s"collate('${t.date}', '${t.collation}')" else s"'${t.date}'"
        val formatArg =
          if (collateFormat) {
            s"collate('${t.format}', '${t.collation}')"
          } else {
            s"'${t.format}'"
          }

        withSQLConf(SqlApiConf.DEFAULT_COLLATION -> t.collation) {
          val query = s"SELECT date_format(${dateArg}, ${formatArg})"
          // Result & data type
          checkAnswer(sql(query), Row(t.result))
          assert(sql(query).schema.fields.head.dataType.sameType(StringType(t.collation)))
        }
      })
    }
  }

  test("Support mode for string expression with collation - Basic Test") {
    Seq(
      "utf8_binary",
      "utf8_binary_rtrim",
      "UTF8_LCASE",
      "UTF8_LCASE_RTRIM",
      "unicode_ci",
      "unicode_ci_rtrim",
      "unicode",
      "unicode_rtrim",
      "NL_AI").foreach { collationId =>
      val query = s"SELECT mode(collate('abc', '${collationId}'))"
      checkAnswer(sql(query), Row("abc"))
      assert(sql(query).schema.fields.head.dataType.sameType(StringType(collationId)))
    }
  }

  test("Support mode for string expression with collation - Advanced Test") {
    case class ModeTestCase[R](collationId: String, bufferValues: Map[String, Long], result: R)
    val testCases = Seq(
      ModeTestCase("utf8_binary", Map("a" -> 3L, "b" -> 2L, "B" -> 2L), "a"),
      ModeTestCase("utf8_binary_rtrim", Map("a" -> 3L, "b" -> 2L, "B" -> 2L), "a"),
      ModeTestCase("UTF8_LCASE", Map("a" -> 3L, "b" -> 2L, "B" -> 2L), "b"),
      ModeTestCase("UTF8_LCASE_RTRIM", Map("a" -> 3L, "b" -> 2L, "B" -> 2L), "b"),
      ModeTestCase("unicode_ci", Map("a" -> 3L, "b" -> 2L, "B" -> 2L), "b"),
      ModeTestCase("unicode_ci_rtrim", Map("a" -> 3L, "b" -> 2L, "B" -> 2L), "b"),
      ModeTestCase("unicode", Map("a" -> 3L, "b" -> 2L, "B" -> 2L), "a"),
      ModeTestCase("unicode_rtrim", Map("a" -> 3L, "b" -> 2L, "B" -> 2L), "a"),
      ModeTestCase("SR", Map("c" -> 3L, "č" -> 2L, "Č" -> 2L), "c")
    )
    testCases.foreach(t => {
      val valuesToAdd = t.bufferValues.map { case (elt, numRepeats) =>
        (0L to numRepeats).map(_ => s"('$elt')").mkString(",")
      }.mkString(",")

      val tableName = s"t_${t.collationId}_mode"
      withTable(s"${tableName}") {
        sql(s"CREATE TABLE ${tableName}(i STRING) USING parquet")
        sql(s"INSERT INTO ${tableName} VALUES " + valuesToAdd)
        val query = s"SELECT mode(collate(i, '${t.collationId}')) FROM ${tableName}"
        checkAnswer(sql(query), Row(t.result))
        assert(sql(query).schema.fields.head.dataType.sameType(StringType(t.collationId)))

      }
    })
  }

  test("Support Mode.eval(buffer)") {
    case class UTF8StringModeTestCase[R](
        collationId: String,
        bufferValues: Map[UTF8String, Long],
        result: R)

    val bufferValuesUTF8String = Map(
      UTF8String.fromString("a") -> 5L,
      UTF8String.fromString("b") -> 4L,
      UTF8String.fromString("B") -> 3L,
      UTF8String.fromString("d") -> 2L,
      UTF8String.fromString("e") -> 1L)

    val testCasesUTF8String = Seq(
      UTF8StringModeTestCase("utf8_binary", bufferValuesUTF8String, "a"),
      UTF8StringModeTestCase("utf8_binary_rtrim", bufferValuesUTF8String, "a"),
      UTF8StringModeTestCase("UTF8_LCASE", bufferValuesUTF8String, "b"),
      UTF8StringModeTestCase("UTF8_LCASE_RTRIM", bufferValuesUTF8String, "b"),
      UTF8StringModeTestCase("unicode_ci", bufferValuesUTF8String, "b"),
      UTF8StringModeTestCase("unicode_ci_rtrim", bufferValuesUTF8String, "b"),
      UTF8StringModeTestCase("unicode", bufferValuesUTF8String, "a"),
      UTF8StringModeTestCase("unicode_rtrim", bufferValuesUTF8String, "a")
    )

    testCasesUTF8String.foreach ( t => {
      val buffer = new OpenHashMap[AnyRef, Long](5)
      val myMode = Mode(child = Literal.create("some_column_name", StringType(t.collationId)))
      t.bufferValues.foreach { case (k, v) => buffer.update(k, v) }
      assert(myMode.eval(buffer).toString.toLowerCase() == t.result.toLowerCase())
    })
  }

  test("Support Mode.eval(buffer) with complex types") {
    case class UTF8StringModeTestCase[R](
        collationId: String,
        bufferValues: Map[InternalRow, Long],
        result: R)

    val bufferValuesUTF8String: Map[Any, Long] = Map(
      UTF8String.fromString("a") -> 5L,
      UTF8String.fromString("b") -> 4L,
      UTF8String.fromString("B") -> 3L,
      UTF8String.fromString("d") -> 2L,
      UTF8String.fromString("e") -> 1L)

    val bufferValuesComplex = bufferValuesUTF8String.map{
      case (k, v) => (InternalRow.fromSeq(Seq(k, k, k)), v)
    }
    val testCasesUTF8String = Seq(
      UTF8StringModeTestCase("utf8_binary", bufferValuesComplex, "[a,a,a]"),
      UTF8StringModeTestCase("utf8_binary_rtrim", bufferValuesComplex, "[a,a,a]"),
      UTF8StringModeTestCase("UTF8_LCASE", bufferValuesComplex, "[b,b,b]"),
      UTF8StringModeTestCase("UTF8_LCASE_rtrim", bufferValuesComplex, "[b,b,b]"),
      UTF8StringModeTestCase("unicode_ci", bufferValuesComplex, "[b,b,b]"),
      UTF8StringModeTestCase("unicode_ci_rtrim", bufferValuesComplex, "[b,b,b]"),
      UTF8StringModeTestCase("unicode", bufferValuesComplex, "[a,a,a]"),
      UTF8StringModeTestCase("unicode_rtrim", bufferValuesComplex, "[a,a,a]"))

    testCasesUTF8String.foreach { t =>
      val buffer = new OpenHashMap[AnyRef, Long](5)
      val myMode = Mode(child = Literal.create(null, StructType(Seq(
        StructField("f1", StringType(t.collationId), true),
        StructField("f2", StringType(t.collationId), true),
        StructField("f3", StringType(t.collationId), true)
      ))))
      t.bufferValues.foreach { case (k, v) => buffer.update(k, v) }
      assert(myMode.eval(buffer).toString.toLowerCase() == t.result.toLowerCase())
    }
  }

  test("Support mode for string expression with collated strings in struct") {
    case class ModeTestCase[R](collationId: String, bufferValues: Map[String, Long], result: R)
    val testCases = Seq(
      ModeTestCase("utf8_binary", Map("a" -> 3L, "b" -> 2L, "B" -> 2L), "a"),
      ModeTestCase("utf8_binary_rtrim", Map("a" -> 3L, "b" -> 2L, "B" -> 2L), "a"),
      ModeTestCase("UTF8_LCASE", Map("a" -> 3L, "b" -> 2L, "B" -> 2L), "b"),
      ModeTestCase("UTF8_LCASE_RTRIM", Map("a" -> 3L, "b" -> 2L, "B" -> 2L), "b"),
      ModeTestCase("unicode", Map("a" -> 3L, "b" -> 2L, "B" -> 2L), "a"),
      ModeTestCase("unicode_rtrim", Map("a" -> 3L, "b" -> 2L, "B" -> 2L), "a"),
      ModeTestCase("unicode_ci", Map("a" -> 3L, "b" -> 2L, "B" -> 2L), "b"),
      ModeTestCase("unicode_ci_rtrim", Map("a" -> 3L, "b" -> 2L, "B" -> 2L), "b")
    )
    testCases.foreach(t => {
      val valuesToAdd = t.bufferValues.map { case (elt, numRepeats) =>
        (0L to numRepeats).map(_ => s"named_struct('f1'," +
          s" collate('$elt', '${t.collationId}'), 'f2', 1)").mkString(",")
      }.mkString(",")

      val tableName = s"t_${t.collationId}_mode_struct"
      withTable(tableName) {
        sql(s"CREATE TABLE ${tableName}(i STRUCT<f1: STRING COLLATE " +
          t.collationId + ", f2: INT>) USING parquet")
        sql(s"INSERT INTO ${tableName} VALUES " + valuesToAdd)
        val query = s"SELECT lower(mode(i).f1) FROM ${tableName}"
        checkAnswer(sql(query), Row(t.result))
      }
    })
  }

  test("Support mode for string expression with collated strings in recursively nested struct") {
    case class ModeTestCase[R](collationId: String, bufferValues: Map[String, Long], result: R)
    val testCases = Seq(
      ModeTestCase("utf8_binary", Map("a" -> 3L, "b" -> 2L, "B" -> 2L), "a"),
      ModeTestCase("utf8_binary_rtrim", Map("a" -> 3L, "b" -> 2L, "B" -> 2L), "a"),
      ModeTestCase("UTF8_LCASE", Map("a" -> 3L, "b" -> 2L, "B" -> 2L), "b"),
      ModeTestCase("UTF8_LCASE_rtrim", Map("a" -> 3L, "b" -> 2L, "B" -> 2L), "b"),
      ModeTestCase("unicode", Map("a" -> 3L, "b" -> 2L, "B" -> 2L), "a"),
      ModeTestCase("unicode_rtrim", Map("a" -> 3L, "b" -> 2L, "B" -> 2L), "a"),
      ModeTestCase("unicode_ci", Map("a" -> 3L, "b" -> 2L, "B" -> 2L), "b"),
      ModeTestCase("unicode_ci_rtrim", Map("a" -> 3L, "b" -> 2L, "B" -> 2L), "b")
    )
    testCases.foreach { t =>
      val valuesToAdd = t.bufferValues.map { case (elt, numRepeats) =>
        (0L to numRepeats).map(_ => s"named_struct('f1', " +
          s"named_struct('f2', collate('$elt', '${t.collationId}')), 'f3', 1)").mkString(",")
      }.mkString(",")

      val tableName = s"t_${t.collationId}_mode_nested_struct1"
      withTable(tableName) {
        sql(s"CREATE TABLE ${tableName}(i STRUCT<f1: STRUCT<f2: STRING COLLATE " +
          t.collationId + ">, f3: INT>) USING parquet")
        sql(s"INSERT INTO ${tableName} VALUES " + valuesToAdd)
        val query = s"SELECT lower(mode(i).f1.f2) FROM ${tableName}"
        checkAnswer(sql(query), Row(t.result))
      }
    }
  }

  test("Support mode for string expression with collated strings in array complex type") {
    case class ModeTestCase[R](collationId: String, bufferValues: Map[String, Long], result: R)
    val testCases = Seq(
      ModeTestCase("utf8_binary", Map("a" -> 3L, "b" -> 2L, "B" -> 2L), "a"),
      ModeTestCase("utf8_binary_rtrim", Map("a" -> 3L, "b" -> 2L, "B" -> 2L), "a"),
      ModeTestCase("UTF8_LCASE", Map("a" -> 3L, "b" -> 2L, "B" -> 2L), "b"),
      ModeTestCase("UTF8_LCASE_rtrim", Map("a" -> 3L, "b" -> 2L, "B" -> 2L), "b"),
      ModeTestCase("unicode", Map("a" -> 3L, "b" -> 2L, "B" -> 2L), "a"),
      ModeTestCase("unicode_rtrim", Map("a" -> 3L, "b" -> 2L, "B" -> 2L), "a"),
      ModeTestCase("unicode_ci", Map("a" -> 3L, "b" -> 2L, "B" -> 2L), "b"),
      ModeTestCase("unicode_ci_rtrim", Map("a" -> 3L, "b" -> 2L, "B" -> 2L), "b")
    )
    testCases.foreach { t =>
      val valuesToAdd = t.bufferValues.map { case (elt, numRepeats) =>
        (0L to numRepeats).map(_ => s"array(named_struct('f2', " +
          s"collate('$elt', '${t.collationId}'), 'f3', 1))").mkString(",")
      }.mkString(",")

      val tableName = s"t_${t.collationId}_mode_nested_struct2"
      withTable(tableName) {
        sql(s"CREATE TABLE ${tableName}(" +
          s"i ARRAY< STRUCT<f2: STRING COLLATE ${t.collationId}, f3: INT>>)" +
          s" USING parquet")
        sql(s"INSERT INTO ${tableName} VALUES " + valuesToAdd)
        val query = s"SELECT lower(element_at(mode(i).f2, 1)) FROM ${tableName}"
        checkAnswer(sql(query), Row(t.result))
      }
    }
  }

  test("Support mode for string expression with collated strings in 3D array type") {
    case class ModeTestCase[R](collationId: String, bufferValues: Map[String, Long], result: R)
    val testCases = Seq(
      ModeTestCase("utf8_binary", Map("a" -> 3L, "b" -> 2L, "B" -> 2L), "a"),
      ModeTestCase("utf8_binary_rtrim", Map("a" -> 3L, "b" -> 2L, "B" -> 2L), "a"),
      ModeTestCase("UTF8_LCASE", Map("a" -> 3L, "b" -> 2L, "B" -> 2L), "b"),
      ModeTestCase("UTF8_LCASE_rtrim", Map("a" -> 3L, "b" -> 2L, "B" -> 2L), "b"),
      ModeTestCase("unicode", Map("a" -> 3L, "b" -> 2L, "B" -> 2L), "a"),
      ModeTestCase("unicode_rtrim", Map("a" -> 3L, "b" -> 2L, "B" -> 2L), "a"),
      ModeTestCase("unicode_ci", Map("a" -> 3L, "b" -> 2L, "B" -> 2L), "b"),
      ModeTestCase("unicode_ci_rtrim", Map("a" -> 3L, "b" -> 2L, "B" -> 2L), "b")
    )
    testCases.foreach { t =>
      val valuesToAdd = t.bufferValues.map { case (elt, numRepeats) =>
        (0L to numRepeats).map(_ =>
          s"array(array(array(collate('$elt', '${t.collationId}'))))").mkString(",")
      }.mkString(",")

      val tableName = s"t_${t.collationId}_mode_nested_3d_array"
      withTable(tableName) {
        sql(s"CREATE TABLE ${tableName}(i ARRAY<ARRAY<ARRAY" +
          s"<STRING COLLATE ${t.collationId}>>>) USING parquet")
        sql(s"INSERT INTO ${tableName} VALUES " + valuesToAdd)
        val query = s"SELECT lower(" +
          s"element_at(element_at(element_at(mode(i),1),1),1)) FROM ${tableName}"
        checkAnswer(sql(query), Row(t.result))
      }
    }
  }

  test("Support mode for string expression with collated complex type - Highly nested") {
    case class ModeTestCase[R](collationId: String, bufferValues: Map[String, Long], result: R)
    val testCases = Seq(
      ModeTestCase("utf8_binary", Map("a" -> 3L, "b" -> 2L, "B" -> 2L), "a"),
      ModeTestCase("utf8_binary_rtrim", Map("a" -> 3L, "b" -> 2L, "B" -> 2L), "a"),
      ModeTestCase("UTF8_LCASE", Map("a" -> 3L, "b" -> 2L, "B" -> 2L), "b"),
      ModeTestCase("UTF8_LCASE_rtrim", Map("a" -> 3L, "b" -> 2L, "B" -> 2L), "b"),
      ModeTestCase("unicode", Map("a" -> 3L, "b" -> 2L, "B" -> 2L), "a"),
      ModeTestCase("unicode_rtrim", Map("a" -> 3L, "b" -> 2L, "B" -> 2L), "a"),
      ModeTestCase("unicode_ci", Map("a" -> 3L, "b" -> 2L, "B" -> 2L), "b"),
      ModeTestCase("unicode_ci_rtrim", Map("a" -> 3L, "b" -> 2L, "B" -> 2L), "b")
    )
    testCases.foreach { t =>
      val valuesToAdd = t.bufferValues.map { case (elt, numRepeats) =>
        (0L to numRepeats).map(_ => s"array(named_struct('s1', named_struct('a2', " +
          s"array(collate('$elt', '${t.collationId}'))), 'f3', 1))").mkString(",")
      }.mkString(",")

      val tableName = s"t_${t.collationId}_mode_highly_nested_struct"
      withTable(tableName) {
        sql(s"CREATE TABLE ${tableName}(" +
          s"i ARRAY<STRUCT<s1: STRUCT<a2: ARRAY<STRING COLLATE ${t.collationId}>>, f3: INT>>)" +
          s" USING parquet")
        sql(s"INSERT INTO ${tableName} VALUES " + valuesToAdd)
        val query = s"SELECT lower(element_at(element_at(mode(i), 1).s1.a2, 1)) FROM ${tableName}"

          checkAnswer(sql(query), Row(t.result))
      }
    }
  }

  test("Support mode for string expression with collated complex type - nested map") {
    case class ModeTestCase(collationId: String, bufferValues: Map[String, Long], result: String)
    Seq(
      ModeTestCase("utf8_binary", Map("a" -> 3L, "b" -> 2L, "B" -> 2L), "{a -> 1}"),
      ModeTestCase("utf8_binary_rtrim", Map("a" -> 3L, "b" -> 2L, "B" -> 2L), "{a -> 1}"),
      ModeTestCase("unicode", Map("a" -> 3L, "b" -> 2L, "B" -> 2L), "{a -> 1}"),
      ModeTestCase("unicode_rtrim", Map("a" -> 3L, "b" -> 2L, "B" -> 2L), "{a -> 1}"),
      ModeTestCase("utf8_lcase", Map("a" -> 3L, "b" -> 2L, "B" -> 2L), "{b -> 1}"),
      ModeTestCase("utf8_lcase_rtrim", Map("a" -> 3L, "b" -> 2L, "B" -> 2L), "{b -> 1}"),
      ModeTestCase("unicode_ci", Map("a" -> 3L, "b" -> 2L, "B" -> 2L), "{b -> 1}")
    ).foreach { t1 =>
      def getValuesToAdd(t: ModeTestCase): String = {
        val valuesToAdd = t.bufferValues.map {
          case (elt, numRepeats) =>
            (0L to numRepeats).map(i =>
              s"named_struct('m1', map(collate('$elt', '${t.collationId}'), 1))"
            ).mkString(",")
        }.mkString(",")
        valuesToAdd
      }
      val tableName = s"t_${t1.collationId}_mode_nested_map_struct1"
      withTable(tableName) {
        withSQLConf(SQLConf.ALLOW_COLLATIONS_IN_MAP_KEYS.key -> "true") {
          sql(s"CREATE TABLE ${tableName}(" +
            s"i STRUCT<m1: MAP<STRING COLLATE ${t1.collationId}, INT>>) USING parquet")
          sql(s"INSERT INTO ${tableName} VALUES ${getValuesToAdd(t1)}")
        }
        val query = "SELECT lower(cast(mode(i).m1 as string))" +
          s" FROM ${tableName}"
        val queryResult = sql(query)
        checkAnswer(queryResult, Row(t1.result))
      }
    }
  }

  test("SPARK-48430: Map value extraction with collations") {
    for {
      collateKey <- Seq(true, false)
      collateVal <- Seq(true, false)
      defaultCollation <- Seq(
        "UTF8_BINARY",
        "UTF8_BINARY_RTRIM",
        "UTF8_LCASE",
        "UTF8_LCASE_RTRIM",
        "UNICODE")
    } {
      val mapKey = if (collateKey) "'a' collate utf8_lcase" else "'a'"
      val mapVal = if (collateVal) "'b' collate utf8_lcase" else "'b'"
      val collation = if (collateVal) "UTF8_LCASE" else "UTF8_BINARY"
      val queryExtractor = s"select collation(map($mapKey, $mapVal)[$mapKey])"
      val queryElementAt = s"select collation(element_at(map($mapKey, $mapVal), $mapKey))"

      checkAnswer(sql(queryExtractor), Row(fullyQualifiedPrefix + collation))
      checkAnswer(sql(queryElementAt), Row(fullyQualifiedPrefix + collation))

      withSQLConf(SqlApiConf.DEFAULT_COLLATION -> defaultCollation) {
        val res = fullyQualifiedPrefix + (if (collateVal) "UTF8_LCASE" else defaultCollation)
        checkAnswer(sql(queryExtractor), Row(res))
        checkAnswer(sql(queryElementAt), Row(res))
      }
    }
  }

  test("CurrentTimeZone expression with collation") {
    // Supported collations
    testSuppCollations.foreach(collationName => {
      val query = "select current_timezone()"
      // Data type check
      withSQLConf(SqlApiConf.DEFAULT_COLLATION -> collationName) {
        val testQuery = sql(query)
        val dataType = StringType(collationName)
        assert(testQuery.schema.fields.head.dataType.sameType(dataType))
      }
    })
  }

  test("DayName expression with collation") {
    // Supported collations
    testSuppCollations.foreach(collationName => {
      val query = "select dayname(current_date())"
      // Data type check
      withSQLConf(SqlApiConf.DEFAULT_COLLATION -> collationName) {
        val testQuery = sql(query)
        val dataType = StringType(collationName)
        assert(testQuery.schema.fields.head.dataType.sameType(dataType))
      }
    })
  }

  test("ToUnixTimestamp expression with collation") {
    // Supported collations
    testSuppCollations.foreach(collationName => {
      val query =
        s"""
          |select to_unix_timestamp(collate('2021-01-01 00:00:00', '${collationName}'),
          |collate('yyyy-MM-dd HH:mm:ss', '${collationName}'))
          |""".stripMargin
      // Result & data type check
      val testQuery = sql(query)
      val dataType = LongType
      val expectedResult = 1609488000L
      assert(testQuery.schema.fields.head.dataType.sameType(dataType))
      checkAnswer(testQuery, Row(expectedResult))
    })
  }

  test("FromUnixTime expression with collation") {
    // Supported collations
    testSuppCollations.foreach(collationName => {
      val query =
        s"""
          |select from_unixtime(1609488000, collate('yyyy-MM-dd HH:mm:ss', '${collationName}'))
          |""".stripMargin
      // Result & data type check
      withSQLConf(SqlApiConf.DEFAULT_COLLATION -> collationName) {
        val testQuery = sql(query)
        val dataType = StringType(collationName)
        val expectedResult = "2021-01-01 00:00:00"
        assert(testQuery.schema.fields.head.dataType.sameType(dataType))
        checkAnswer(testQuery, Row(expectedResult))
      }
    })
  }

  test("NextDay expression with collation") {
    // Supported collations
    testSuppCollations.foreach(collationName => {
      val query =
        s"""
          |select next_day('2015-01-14', collate('TU', '${collationName}'))
          |""".stripMargin
      // Result & data type check
      withSQLConf(SqlApiConf.DEFAULT_COLLATION -> collationName) {
        val testQuery = sql(query)
        val dataType = DateType
        val expectedResult = "2015-01-20"
        assert(testQuery.schema.fields.head.dataType.sameType(dataType))
        checkAnswer(testQuery, Row(Date.valueOf(expectedResult)))
      }
    })
  }

  test("FromUTCTimestamp expression with collation") {
    // Supported collations
    testSuppCollations.foreach(collationName => {
      val query =
        s"""
          |select from_utc_timestamp(collate('2016-08-31', '${collationName}'),
          |collate('Asia/Seoul', '${collationName}'))
          |""".stripMargin
      // Result & data type check
      val testQuery = sql(query)
      val dataType = TimestampType
      val expectedResult = "2016-08-31 09:00:00.0"
      assert(testQuery.schema.fields.head.dataType.sameType(dataType))
      checkAnswer(testQuery, Row(Timestamp.valueOf(expectedResult)))
    })
  }

  test("ToUTCTimestamp expression with collation") {
    // Supported collations
    testSuppCollations.foreach(collationName => {
      val query =
        s"""
          |select to_utc_timestamp(collate('2016-08-31 09:00:00', '${collationName}'),
          |collate('Asia/Seoul', '${collationName}'))
          |""".stripMargin
      // Result & data type check
      val testQuery = sql(query)
      val dataType = TimestampType
      val expectedResult = "2016-08-31 00:00:00.0"
      assert(testQuery.schema.fields.head.dataType.sameType(dataType))
      checkAnswer(testQuery, Row(Timestamp.valueOf(expectedResult)))
    })
  }

  test("ParseToDate expression with collation") {
    // Supported collations
    testSuppCollations.foreach(collationName => {
      val query =
        s"""
          |select to_date(collate('2016-12-31', '${collationName}'),
          |collate('yyyy-MM-dd', '${collationName}'))
          |""".stripMargin
      // Result & data type check
      val testQuery = sql(query)
      val dataType = DateType
      val expectedResult = "2016-12-31"
      assert(testQuery.schema.fields.head.dataType.sameType(dataType))
      checkAnswer(testQuery, Row(Date.valueOf(expectedResult)))
    })
  }

  test("ParseToTimestamp expression with collation") {
    // Supported collations
    testSuppCollations.foreach(collationName => {
      val query =
        s"""
          |select to_timestamp(collate('2016-12-31 23:59:59', '${collationName}'),
          |collate('yyyy-MM-dd HH:mm:ss', '${collationName}'))
          |""".stripMargin
      // Result & data type check
      val testQuery = sql(query)
      val dataType = TimestampType
      val expectedResult = "2016-12-31 23:59:59.0"
      assert(testQuery.schema.fields.head.dataType.sameType(dataType))
      checkAnswer(testQuery, Row(Timestamp.valueOf(expectedResult)))
    })
  }

  test("TruncDate expression with collation") {
    // Supported collations
    testSuppCollations.foreach(collationName => {
      val query =
        s"""
          |select trunc(collate('2016-12-31 23:59:59', '${collationName}'), 'MM')
          |""".stripMargin
      // Result & data type check
      val testQuery = sql(query)
      val dataType = DateType
      val expectedResult = "2016-12-01"
      assert(testQuery.schema.fields.head.dataType.sameType(dataType))
      checkAnswer(testQuery, Row(Date.valueOf(expectedResult)))
    })
  }

  test("TruncTimestamp expression with collation") {
    // Supported collations
    testSuppCollations.foreach(collationName => {
      val query =
        s"""
          |select date_trunc(collate('HOUR', '${collationName}'),
          |collate('2015-03-05T09:32:05.359', '${collationName}'))
          |""".stripMargin
      // Result & data type check
      val testQuery = sql(query)
      val dataType = TimestampType
      val expectedResult = "2015-03-05 09:00:00.0"
      assert(testQuery.schema.fields.head.dataType.sameType(dataType))
      checkAnswer(testQuery, Row(Timestamp.valueOf(expectedResult)))
    })
  }

  test("MakeTimestamp expression with collation") {
    // Supported collations
    testSuppCollations.foreach(collationName => {
      val query =
        s"""
          |select make_timestamp(2014, 12, 28, 6, 30, 45.887, collate('CET', '${collationName}'))
          |""".stripMargin
      // Result & data type check
      val testQuery = sql(query)
      val dataType = TimestampType
      val expectedResult = "2014-12-27 21:30:45.887"
      assert(testQuery.schema.fields.head.dataType.sameType(dataType))
      checkAnswer(testQuery, Row(Timestamp.valueOf(expectedResult)))
    })
  }

  test("ExtractValue expression with collation") {
    // Supported collations
    testSuppCollations.foreach(collationName => {
      withSQLConf(SqlApiConf.DEFAULT_COLLATION -> collationName) {
        val query =
          s"""
             |select col['Field1']
             |from values (named_struct('Field1', 'Spark', 'Field2', 5)) as tab(col);
             |""".stripMargin
        // Result & data type check
        val testQuery = sql(query)
        val dataType = StringType(collationName)
        val expectedResult = "Spark"
        assert(testQuery.schema.fields.head.dataType.sameType(dataType))
        checkAnswer(testQuery, Row(expectedResult))
      }
    })
  }

  test("Lag expression with collation") {
    // Supported collations
    testSuppCollations.foreach(collationName => {
      val query =
        s"""
           |SELECT lag(a, -1, 'default' collate $collationName) OVER (PARTITION BY b ORDER BY a)
           |FROM VALUES ('A1', 2), ('A2', 1), ('A2', 3), ('A1', 1) tab(a, b);
           |""".stripMargin
      // Result & data type check
      val testQuery = sql(query)
      val dataType = StringType(collationName)
      val expectedResult = Seq("A2", "default", "default", "default")
      assert(testQuery.schema.fields.head.dataType.sameType(dataType))
      checkAnswer(testQuery, expectedResult.map(Row(_)))
    })
  }

  test("Lead expression with collation") {
    // Supported collations
    testSuppCollations.foreach(collationName => {
      val query =
        s"""
           |SELECT lead(a, -1, 'default' collate $collationName) OVER (PARTITION BY b ORDER BY a)
           |FROM VALUES ('A1', 2), ('A2', 1), ('A2', 3), ('A1', 1) tab(a, b);
           |""".stripMargin
      // Result & data type check
      val testQuery = sql(query)
      val dataType = StringType(collationName)
      val expectedResult = Seq("A1", "default", "default", "default")
      assert(testQuery.schema.fields.head.dataType.sameType(dataType))
      checkAnswer(testQuery, expectedResult.map(Row(_)))
    })
  }

  test("DatePart expression with collation") {
    // Supported collations
    testSuppCollations.foreach(collationName => {
      val query =
        s"""
          |select date_part(collate('Week', '${collationName}'),
          |collate('2019-08-12 01:00:00.123456', '${collationName}'))
          |""".stripMargin
      // Result & data type check
      val testQuery = sql(query)
      val dataType = IntegerType
      val expectedResult = 33
      assert(testQuery.schema.fields.head.dataType.sameType(dataType))
      checkAnswer(testQuery, Row(expectedResult))
    })
  }

  test("DateAdd expression with collation") {
    // Supported collations
    testSuppCollations.foreach(collationName => {
      val query = s"""select date_add(collate('2016-07-30', '${collationName}'), 1)"""
      // Result & data type check
      val testQuery = sql(query)
      val dataType = DateType
      val expectedResult = "2016-07-31"
      assert(testQuery.schema.fields.head.dataType.sameType(dataType))
      checkAnswer(testQuery, Row(Date.valueOf(expectedResult)))
    })
  }

  test("DateSub expression with collation") {
    // Supported collations
    testSuppCollations.foreach(collationName => {
      val query = s"""select date_sub(collate('2016-07-30', '${collationName}'), 1)"""
      // Result & data type check
      val testQuery = sql(query)
      val dataType = DateType
      val expectedResult = "2016-07-29"
      assert(testQuery.schema.fields.head.dataType.sameType(dataType))
      checkAnswer(testQuery, Row(Date.valueOf(expectedResult)))
    })
  }

  test("WindowTime and TimeWindow expressions with collation") {
    // Supported collations
    testSuppCollations.foreach(collationName => {
      withSQLConf(SqlApiConf.DEFAULT_COLLATION -> collationName) {
        val query =
          s"""SELECT window_time(window)
             | FROM (SELECT a, window, count(*) as cnt FROM VALUES
             |('A1', '2021-01-01 00:00:00'),
             |('A1', '2021-01-01 00:04:30'),
             |('A1', '2021-01-01 00:06:00'),
             |('A2', '2021-01-01 00:01:00') AS tab(a, b)
             |GROUP by a, window(b, '5 minutes') ORDER BY a, window.start);
             |""".stripMargin
        // Result & data type check
        val testQuery = sql(query)
        val dataType = TimestampType
        val expectedResults =
          Seq("2021-01-01 00:04:59.999999",
            "2021-01-01 00:09:59.999999",
            "2021-01-01 00:04:59.999999")
        assert(testQuery.schema.fields.head.dataType.sameType(dataType))
        checkAnswer(testQuery, expectedResults.map(ts => Row(Timestamp.valueOf(ts))))
      }
    })
  }

  test("SessionWindow expressions with collation") {
    // Supported collations
    testSuppCollations.foreach(collationName => {
      withSQLConf(SqlApiConf.DEFAULT_COLLATION -> collationName) {
        val query =
          s"""SELECT count(*) as cnt
             | FROM VALUES
             |('A1', '2021-01-01 00:00:00'),
             |('A1', '2021-01-01 00:04:30'),
             |('A1', '2021-01-01 00:10:00'),
             |('A2', '2021-01-01 00:01:00'),
             |('A2', '2021-01-01 00:04:30') AS tab(a, b)
             |GROUP BY a,
             |session_window(b, CASE WHEN a = 'A1' THEN '5 minutes'  ELSE '1 minutes' END)
             |ORDER BY a, session_window.start;
             |""".stripMargin
        // Result & data type check
        val testQuery = sql(query)
        val dataType = LongType
        val expectedResults = Seq(2, 1, 1, 1)
        assert(testQuery.schema.fields.head.dataType.sameType(dataType))
        checkAnswer(testQuery, expectedResults.map(Row(_)))
      }
    })
  }

  test("ConvertTimezone expression with collation") {
    // Supported collations
    testSuppCollations.foreach(collationName => {
      val query =
        s"""
          |select date_format(convert_timezone(collate('America/Los_Angeles', '${collationName}'),
          |collate('UTC', '${collationName}'), collate('2021-12-06 00:00:00', '${collationName}')),
          |'yyyy-MM-dd HH:mm:ss.S')
          |""".stripMargin
      // Result & data type check
      val testQuery = sql(query)
      val dataType = StringType
      val expectedResult = "2021-12-06 08:00:00.0"
      assert(testQuery.schema.fields.head.dataType.sameType(dataType))
      checkAnswer(testQuery, Row(expectedResult))
    })
  }

  test("Reflect expressions with collated strings") {
    // be aware that output of java.util.UUID.fromString is always lowercase

    case class ReflectExpressions(
      left: String,
      leftCollation: String,
      right: String,
      rightCollation: String,
      result: Boolean
    )

    val testCases = Seq(
      ReflectExpressions("a5cf6c42-0c85-418f-af6c-3e4e5b1328f2", "utf8_binary",
        "a5cf6c42-0c85-418f-af6c-3e4e5b1328f2", "utf8_binary", true),
      ReflectExpressions("a5cf6c42-0c85-418f-af6c-3e4e5b1328f2", "utf8_binary",
        "A5Cf6c42-0c85-418f-af6c-3e4e5b1328f2", "utf8_binary", false),
      ReflectExpressions("a5cf6c42-0c85-418f-af6c-3e4e5b1328f2", "utf8_binary",
        "a5cf6c42-0c85-418f-af6c-3e4e5b1328f2", "utf8_binary_rtrim", true),
      ReflectExpressions("A5cf6C42-0C85-418f-af6c-3E4E5b1328f2", "utf8_binary",
        "a5cf6c42-0c85-418f-af6c-3e4e5b1328f2", "utf8_lcase", true),
      ReflectExpressions("A5cf6C42-0C85-418f-af6c-3E4E5b1328f2", "utf8_binary",
        "A5Cf6c42-0c85-418f-af6c-3e4e5b1328f2", "utf8_lcase", true)
    )
    testCases.foreach(testCase => {
      val query =
        s"""
           |SELECT REFLECT('java.util.UUID', 'fromString',
           |collate('${testCase.left}', '${testCase.leftCollation}'))=
           |collate('${testCase.right}', '${testCase.rightCollation}');
           |""".stripMargin

      if (testCase.leftCollation == testCase.rightCollation) {
        checkAnswer(sql(query), Row(testCase.result))
      } else {
        val exception = intercept[AnalysisException] {
          sql(query)
        }
        assert(exception.getCondition === "COLLATION_MISMATCH.EXPLICIT")
      }
    })

    val queryPass =
      s"""
         |SELECT REFLECT('java.lang.Integer', 'toHexString',2);
         |""".stripMargin
    val testQueryPass = sql(queryPass)
    checkAnswer(testQueryPass, Row("2"))

    val queryFail =
      s"""
         |SELECT REFLECT('java.lang.Integer', 'toHexString',"2");
         |""".stripMargin
    checkError(
      exception = intercept[ExtendedAnalysisException] {
        sql(queryFail).collect()
      },
      condition = "DATATYPE_MISMATCH.UNEXPECTED_STATIC_METHOD",
      parameters = Map(
        "methodName" -> "toHexString",
        "className" -> "java.lang.Integer",
        "sqlExpr" -> "\"reflect(java.lang.Integer, toHexString, 2)\""),
      context = ExpectedContext(
        fragment = """REFLECT('java.lang.Integer', 'toHexString',"2")""",
        start = 8,
        stop = 54)
    )
  }

  // common method for subsequent tests verifying various SQL expressions with collations
  private def testCollationSqlExpressionCommon(
      query: String,
      collation: String,
      result: Row,
      expectedType: DataType): Unit = {
    testCollationSqlExpressionCommon(query, collation, Seq(result), Seq(expectedType))
  }

  // common method for subsequent tests verifying various SQL expressions with collations
  private def testCollationSqlExpressionCommon(
      query: String,
      collation: String,
      result: Seq[Row],
      expectedTypes: Seq[DataType]): Unit = {
    withSQLConf(SqlApiConf.DEFAULT_COLLATION -> collation) {
      // check result correctness
      checkAnswer(sql(query), result)
      // check result rows data types
      for (i <- 0 until expectedTypes.length)
        assert(sql(query).schema(i).dataType == expectedTypes(i))
    }
  }

  test("min_by supports collation") {
    testAdditionalCollations.foreach { collation =>
      testCollationSqlExpressionCommon(
        query = "SELECT min_by(x, y) FROM VALUES ('a', 10), ('b', 50), ('c', 20) AS tab(x, y)",
        collation,
        result = Row("a"),
        expectedType = StringType(collation)
      )
    }
  }

  test("max_by supports collation") {
    testAdditionalCollations.foreach { collation =>
      testCollationSqlExpressionCommon(
        query = "SELECT max_by(x, y) FROM VALUES ('a', 10), ('b', 50), ('c', 20) AS tab(x, y)",
        collation,
        result = Row("b"),
        expectedType = StringType(collation)
      )
    }
  }

  test("array supports collation") {
    testAdditionalCollations.foreach { collation =>
      testCollationSqlExpressionCommon(
        query = "SELECT array('a', 'b', 'c')",
        collation,
        result = Row(Seq("a", "b", "c")),
        expectedType = ArrayType(StringType(collation), false)
      )
    }
  }

  test("array_agg supports collation") {
    testAdditionalCollations.foreach { collation =>
      testCollationSqlExpressionCommon(
        query = "SELECT array_agg(col) FROM VALUES ('a'), ('b'), ('c') AS tab(col)",
        collation,
        result = Row(Seq("a", "b", "c")),
        expectedType = ArrayType(StringType(collation), false)
      )
    }
  }

  test("array_contains supports collation") {
    testAdditionalCollations.foreach { collation =>
      testCollationSqlExpressionCommon(
        query = "SELECT array_contains(array('a', 'b', 'c'), 'b')",
        collation,
        result = Row(true),
        expectedType = BooleanType
      )
    }
  }

  test("arrays_overlap supports collation") {
    testAdditionalCollations.foreach { collation =>
      testCollationSqlExpressionCommon(
        query = "SELECT arrays_overlap(array('a', 'b', 'c'), array('c', 'd', 'e'))",
        collation,
        result = Row(true),
        expectedType = BooleanType
      )
    }
  }

  test("array_insert supports collation") {
    testAdditionalCollations.foreach { collation =>
      testCollationSqlExpressionCommon(
        query = "SELECT array_insert(array('a', 'b', 'c', 'd'), 5, 'e')",
        collation,
        result = Row(Seq("a", "b", "c", "d", "e")),
        expectedType = ArrayType(StringType(collation), true)
      )
    }
  }

  test("array_intersect supports collation") {
    testAdditionalCollations.foreach { collation =>
      testCollationSqlExpressionCommon(
        query = "SELECT array_intersect(array('a', 'b', 'c'), array('b', 'c', 'd'))",
        collation,
        result = Row(Seq("b", "c")),
        expectedType = ArrayType(StringType(collation), false)
      )
    }
  }

  test("array_join supports collation") {
    testAdditionalCollations.foreach { collation =>
      testCollationSqlExpressionCommon(
        query = "SELECT array_join(array('hello', 'world'), ' ')",
        collation,
        result = Row("hello world"),
        expectedType = StringType(collation)
      )
    }
  }

  test("array_position supports collation") {
    testAdditionalCollations.foreach { collation =>
      testCollationSqlExpressionCommon(
        query = "SELECT array_position(array('a', 'b', 'c', 'c'), 'c')",
        collation,
        result = Row(3),
        expectedType = LongType
      )
    }
  }

  test("array_size supports collation") {
    testAdditionalCollations.foreach { collation =>
      testCollationSqlExpressionCommon(
        query = "SELECT array_size(array('a', 'b', 'c', 'c'))",
        collation,
        result = Row(4),
        expectedType = IntegerType
      )
    }
  }

  test("array_sort supports collation") {
    testAdditionalCollations.foreach { collation =>
      testCollationSqlExpressionCommon(
        query = "SELECT array_sort(array('b', null, 'A'))",
        collation,
        result = Row(Seq("A", "b", null)),
        expectedType = ArrayType(StringType(collation), true)
      )
    }
  }

  test("array_except supports collation") {
    testAdditionalCollations.foreach { collation =>
      testCollationSqlExpressionCommon(
        query = "SELECT array_except(array('a', 'b', 'c'), array('c', 'd', 'e'))",
        collation,
        result = Row(Seq("a", "b")),
        expectedType = ArrayType(StringType(collation), false)
      )
    }
  }

  test("array_union supports collation") {
    testAdditionalCollations.foreach { collation =>
      testCollationSqlExpressionCommon(
        query = "SELECT array_union(array('a', 'b', 'c'), array('a', 'c', 'd'))",
        collation,
        result = Row(Seq("a", "b", "c", "d")),
        expectedType = ArrayType(StringType(collation), false)
      )
    }
  }

  test("array_compact supports collation") {
    testAdditionalCollations.foreach { collation =>
      testCollationSqlExpressionCommon(
        query = "SELECT array_compact(array('a', 'b', null, 'c'))",
        collation,
        result = Row(Seq("a", "b", "c")),
        expectedType = ArrayType(StringType(collation), false)
      )
    }
  }

  test("arrays_zip supports collation") {
    testAdditionalCollations.foreach { collation =>
      testCollationSqlExpressionCommon(
        query = "SELECT arrays_zip(array('a', 'b', 'c'), array(1, 2, 3))",
        collation,
        result = Row(Seq(Row("a", 1), Row("b", 2), Row("c", 3))),
        expectedType = ArrayType(StructType(
          StructField("0", StringType(collation), true) ::
            StructField("1", IntegerType, true) :: Nil
        ), false)
      )
    }
  }

  test("array_min supports collation") {
    testAdditionalCollations.foreach { collation =>
      testCollationSqlExpressionCommon(
        query = "SELECT array_min(array('a', 'b', null, 'c'))",
        collation,
        result = Row("a"),
        expectedType = StringType(collation)
      )
    }
  }

  test("array_max supports collation") {
    testAdditionalCollations.foreach { collation =>
      testCollationSqlExpressionCommon(
        query = "SELECT array_max(array('a', 'b', null, 'c'))",
        collation,
        result = Row("c"),
        expectedType = StringType(collation)
      )
    }
  }

  test("array_append supports collation") {
    testAdditionalCollations.foreach { collation =>
      testCollationSqlExpressionCommon(
        query = "SELECT array_append(array('b', 'd', 'c', 'a'), 'e')",
        collation,
        result = Row(Seq("b", "d", "c", "a", "e")),
        expectedType = ArrayType(StringType(collation), true)
      )
    }
  }

  test("array_repeat supports collation") {
    testAdditionalCollations.foreach { collation =>
      testCollationSqlExpressionCommon(
        query = "SELECT array_repeat('abc', 2)",
        collation,
        result = Row(Seq("abc", "abc")),
        expectedType = ArrayType(StringType(collation), false)
      )
    }
  }

  test("array_remove supports collation") {
    testAdditionalCollations.foreach { collation =>
      testCollationSqlExpressionCommon(
        query = "SELECT array_remove(array('a', 'b', null, 'c'), 'b')",
        collation,
        result = Row(Seq("a", null, "c")),
        expectedType = ArrayType(StringType(collation), true)
      )
    }
  }

  test("array_prepend supports collation") {
    testAdditionalCollations.foreach { collation =>
      testCollationSqlExpressionCommon(
        query = "SELECT array_prepend(array('b', 'd', 'c', 'a'), 'd')",
        collation,
        result = Row(Seq("d", "b", "d", "c", "a")),
        expectedType = ArrayType(StringType(collation), true)
      )
    }
  }

  test("array_distinct supports collation") {
    testAdditionalCollations.foreach { collation =>
      testCollationSqlExpressionCommon(
        query = "SELECT array_distinct(array('a', 'b', 'c', null, 'c'))",
        collation,
        result = Row(Seq("a", "b", "c", null)),
        expectedType = ArrayType(StringType(collation), true)
      )
    }
  }

  test("collect_list supports collation") {
    testAdditionalCollations.foreach { collation =>
      testCollationSqlExpressionCommon(
        query = "SELECT collect_list(col) FROM VALUES ('a'), ('b'), ('c') AS tab(col)",
        collation,
        result = Row(Seq("a", "b", "c")),
        expectedType = ArrayType(StringType(collation), false)
      )
    }
  }

  test("collect_set does not support collation") {
    testAdditionalCollations.foreach { collation =>
      val query = "SELECT collect_set(col) FROM VALUES ('a'), ('b'), ('a') AS tab(col);"
      withSQLConf(SqlApiConf.DEFAULT_COLLATION -> collation) {
        checkError(
          exception = intercept[AnalysisException] {
            sql(query)
          },
          condition = "DATATYPE_MISMATCH.UNSUPPORTED_INPUT_TYPE",
          sqlState = Some("42K09"),
          parameters = Map(
            "functionName" -> "`collect_set`",
            "dataType" -> "\"MAP\" or \"COLLATED STRING\"",
            "sqlExpr" -> "\"collect_set(col)\""),
          context = ExpectedContext(
            fragment = "collect_set(col)",
            start = 7,
            stop = 22))
      }
    }
  }

  test("element_at supports collation") {
    testAdditionalCollations.foreach { collation =>
      testCollationSqlExpressionCommon(
        query = "SELECT element_at(array('a', 'b', 'c'), 2)",
        collation,
        result = Row("b"),
        expectedType = StringType(collation)
      )
    }
  }

  test("aggregate supports collation") {
    testAdditionalCollations.foreach { collation =>
      testCollationSqlExpressionCommon(
        query = "SELECT aggregate(array('a', 'b', 'c'), '', (acc, x) -> concat(acc, x))",
        collation,
        result = Row("abc"),
        expectedType = StringType(collation)
      )
    }
  }

  test("explode supports collation") {
    testAdditionalCollations.foreach { collation =>
      testCollationSqlExpressionCommon(
        query = "SELECT explode(array('a', 'b'))",
        collation,
        result = Seq(
          Row("a"),
          Row("b")
        ),
        expectedTypes = Seq(
          StringType(collation)
        )
      )
    }
  }

  test("posexplode supports collation") {
    testAdditionalCollations.foreach { collation =>
      testCollationSqlExpressionCommon(
        query = "SELECT posexplode(array('a', 'b'))",
        collation,
        result = Seq(
          Row(0, "a"),
          Row(1, "b")
        ),
        expectedTypes = Seq(
          IntegerType,
          StringType(collation)
        )
      )
    }
  }

  test("filter supports collation") {
    testAdditionalCollations.foreach { collation =>
      testCollationSqlExpressionCommon(
        query = "SELECT filter(array('a', 'b', 'c'), x -> x < 'b')",
        collation,
        result = Row(Seq("a")),
        expectedType = ArrayType(StringType(collation), false)
      )
    }
  }

  test("flatten supports collation") {
    testAdditionalCollations.foreach { collation =>
      testCollationSqlExpressionCommon(
        query = "SELECT flatten(array(array('a', 'b'), array('c', 'd')))",
        collation,
        result = Row(Seq("a", "b", "c", "d")),
        expectedType = ArrayType(StringType(collation), false)
      )
    }
  }

  test("inline supports collation") {
    testAdditionalCollations.foreach { collation =>
      testCollationSqlExpressionCommon(
        query = "SELECT inline(array(struct(1, 'a'), struct(2, 'b')))",
        collation,
        Seq(
          Row(1, "a"),
          Row(2, "b")
        ),
        expectedTypes = Seq(
          IntegerType,
          StringType(collation)
        )
      )
    }
  }

  test("shuffle supports collation") {
    testAdditionalCollations.foreach { collation =>
      val query = "SELECT shuffle(array('a', 'b', 'c', 'd'));"
      withSQLConf(SqlApiConf.DEFAULT_COLLATION -> collation) {
        // check result row data type
        val dataType = ArrayType(StringType(collation), false)
        assert(sql(query).schema.head.dataType == dataType)
      }
    }
  }

  test("slice supports collation") {
    testAdditionalCollations.foreach { collation =>
      testCollationSqlExpressionCommon(
        query = "SELECT slice(array('a', 'b', 'c', 'd'), 2, 2)",
        collation,
        result = Row(Seq("b", "c")),
        expectedType = ArrayType(StringType(collation), false)
      )
    }
  }

  test("sort_array supports collation") {
    testAdditionalCollations.foreach { collation =>
      testCollationSqlExpressionCommon(
        query = "SELECT sort_array(array('b', 'd', null, 'c', 'a'), true)",
        collation,
        result = Row(Seq(null, "a", "b", "c", "d")),
        expectedType = ArrayType(StringType(collation), true)
      )
    }
  }

  test("zip_with supports collation") {
    testAdditionalCollations.foreach { collation =>
      testCollationSqlExpressionCommon(
        query = "SELECT zip_with(array('a', 'b'), array('x', 'y'), (x, y) -> concat(x, y))",
        collation,
        result = Row(Seq("ax", "by")),
        expectedType = ArrayType(
          StringType(collation),
          containsNull = true
        )
      )
    }
  }

  test("map_contains_key supports collation") {
    testAdditionalCollations.foreach { collation =>
      testCollationSqlExpressionCommon(
        query = "SELECT map_contains_key(map('a', 1, 'b', 2), 'a')",
        collation,
        result = Row(true),
        expectedType = BooleanType
      )
    }
  }

  test("map_from_arrays supports collation") {
    testAdditionalCollations.foreach { collation =>
      testCollationSqlExpressionCommon(
        query = "SELECT map_from_arrays(array('a','b','c'), array(1,2,3))",
        collation,
        result = Row(Map("a" -> 1, "b" -> 2, "c" -> 3)),
        expectedType = MapType(
          StringType(collation),
          IntegerType, false
        )
      )
    }
  }

  test("map_keys supports collation") {
    testAdditionalCollations.foreach { collation =>
      testCollationSqlExpressionCommon(
        query = "SELECT map_keys(map('a', 1, 'b', 2))",
        collation,
        result = Row(Seq("a", "b")),
        expectedType = ArrayType(StringType(collation), true)
      )
    }
  }

  test("map_values supports collation") {
    testAdditionalCollations.foreach { collation =>
      testCollationSqlExpressionCommon(
        query = "SELECT map_values(map(1, 'a', 2, 'b'))",
        collation,
        result = Row(Seq("a", "b")),
        expectedType = ArrayType(StringType(collation), true)
      )
    }
  }

  test("map_entries supports collation") {
    testAdditionalCollations.foreach { collation =>
      testCollationSqlExpressionCommon(
        query = "SELECT map_entries(map('a', 1, 'b', 2))",
        collation,
        result = Row(Seq(Row("a", 1), Row("b", 2))),
        expectedType = ArrayType(StructType(
          StructField("key", StringType(collation), false) ::
            StructField("value", IntegerType, false) :: Nil
        ), false)
      )
    }
  }

  test("map_from_entries supports collation") {
    testAdditionalCollations.foreach { collation =>
      testCollationSqlExpressionCommon(
        query = "SELECT map_from_entries(array(struct(1, 'a'), struct(2, 'b')))",
        collation,
        result = Row(Map(1 -> "a", 2 -> "b")),
        expectedType = MapType(
          IntegerType,
          StringType(collation),
          valueContainsNull = false
        )
      )
    }
  }

  test("map_concat supports collation") {
    testAdditionalCollations.foreach { collation =>
      testCollationSqlExpressionCommon(
        query = "SELECT map_concat(map(1, 'a'), map(2, 'b'))",
        collation,
        result = Row(Map(1 -> "a", 2 -> "b")),
        expectedType = MapType(
          IntegerType,
          StringType(collation),
          valueContainsNull = false
        )
      )
    }
  }

  test("map_filter supports collation") {
    testAdditionalCollations.foreach { collation =>
      testCollationSqlExpressionCommon(
        query = "SELECT map_filter(map('a', 1, 'b', 2, 'c', 3), (k, v) -> k < 'c')",
        collation,
        result = Row(Map("a" -> 1, "b" -> 2)),
        expectedType = MapType(
          StringType(collation),
          IntegerType,
          valueContainsNull = false
        )
      )
    }
  }

  test("map_zip_with supports collation") {
    testAdditionalCollations.foreach { collation =>
      testCollationSqlExpressionCommon(
        query = "SELECT map_zip_with(map(1, 'a'), map(1, 'x'), (k, v1, v2) -> concat(v1, v2))",
        collation,
        result = Row(Map(1 -> "ax")),
        expectedType = MapType(
          IntegerType,
          StringType(collation),
          valueContainsNull = true
        )
      )
    }
  }

  test("transform supports collation") {
    testAdditionalCollations.foreach { collation =>
      testCollationSqlExpressionCommon(
        query = "SELECT transform(array('aa', 'bb', 'cc'), x -> substring(x, 2))",
        collation,
        result = Row(Seq("a", "b", "c")),
        expectedType = ArrayType(StringType(collation), false)
      )
    }
  }

  test("transform_values supports collation") {
    testAdditionalCollations.foreach { collation =>
      testCollationSqlExpressionCommon(
        query = "SELECT transform_values(map_from_arrays(array(1, 2, 3)," +
          "array('aa', 'bb', 'cc')), (k, v) -> substring(v, 2))",
        collation,
        result = Row(Map(1 -> "a", 2 -> "b", 3 -> "c")),
        expectedType = MapType(
          IntegerType,
          StringType(collation),
          false
        )
      )
    }
  }

  test("transform_keys supports collation") {
    testAdditionalCollations.foreach { collation =>
      testCollationSqlExpressionCommon(
        query = "SELECT transform_keys(map_from_arrays(array('aa', 'bb', 'cc')," +
          "array(1, 2, 3)), (k, v) -> substring(k, 2))",
        collation,
        result = Row(Map("a" -> 1, "b" -> 2, "c" -> 3)),
        expectedType = MapType(
          StringType(collation),
          IntegerType,
          false
        )
      )
    }
  }

  test("stack supports collation") {
    testAdditionalCollations.foreach { collation =>
      testCollationSqlExpressionCommon(
        query = "SELECT stack(2, 'a', 'b', 'c')",
        collation,
        result = Seq(
          Row("a", "b"),
          Row("c", null)
        ),
        expectedTypes = Seq(
          StringType(collation)
        )
      )
    }
  }

  test("SPARK-50060: set operators with conflicting collations") {
    val setOperators = Seq[(String, Int, Int)](
      ("UNION", 64, 45),
      ("INTERSECT", 68, 49),
      ("EXCEPT", 65, 46))

    for {
      ansiEnabled <- Seq(true, false)
      (operator, stopExplicit, stopDefault) <- setOperators
    } {
      withSQLConf(SQLConf.ANSI_ENABLED.key -> ansiEnabled.toString,
        SqlApiConf.DEFAULT_COLLATION -> "UNICODE_CI") {
        val explicitConflictQuery =
          s"SELECT 'a' COLLATE UTF8_LCASE $operator SELECT 'A' COLLATE UNICODE_CI"
        checkError(
          exception = intercept[AnalysisException] {
            sql(explicitConflictQuery)
          },
          condition = "INCOMPATIBLE_COLUMN_TYPE",
          parameters = Map(
            "columnOrdinalNumber" -> "first",
            "tableOrdinalNumber" -> "second",
            "dataType1" -> "\"STRING COLLATE UNICODE_CI\"",
            "dataType2" -> "\"STRING COLLATE UTF8_LCASE\"",
            "operator" -> operator,
            "hint" -> ""),
          context = ExpectedContext(
            fragment = explicitConflictQuery,
            start = 0,
            stop = stopExplicit))

        val defaultConflictQuery =
          s"SELECT 'a' COLLATE UTF8_LCASE $operator SELECT 'A'"
        checkError(
          exception = intercept[AnalysisException] {
            sql(defaultConflictQuery)
          },
          condition = "INCOMPATIBLE_COLUMN_TYPE",
          parameters = Map(
            "columnOrdinalNumber" -> "first",
            "tableOrdinalNumber" -> "second",
            "dataType1" -> "\"STRING COLLATE UNICODE_CI\"",
            "dataType2" -> "\"STRING COLLATE UTF8_LCASE\"",
            "operator" -> operator,
            "hint" -> ""),
          context = ExpectedContext(
            fragment = defaultConflictQuery,
            start = 0,
            stop = stopDefault))
      }
    }
  }

  test("Support HyperLogLogPlusPlus expression with collation") {
    case class HyperLogLogPlusPlusTestCase(
      collation: String,
      input: Seq[String],
      output: Seq[Row]
    )

    val testCases = Seq(
      HyperLogLogPlusPlusTestCase("utf8_binary", Seq("a", "a", "A", "z", "zz", "ZZ", "w",
        "AA", "aA", "Aa", "aa"), Seq(Row(10))),
      HyperLogLogPlusPlusTestCase("utf8_binary_rtrim", Seq("a ", "a", "a", "A", "z", "zz", "ZZ",
        "w", "AA", "aA", "Aa", "aa"), Seq(Row(10))),
      HyperLogLogPlusPlusTestCase("utf8_lcase", Seq("a", "a", "A", "z", "zz", "ZZ", "w",
        "AA", "aA", "Aa", "aa"), Seq(Row(5))),
      HyperLogLogPlusPlusTestCase("utf8_lcase_rtrim", Seq("a ", "a", "a", "A", "z", "zz", "ZZ", "w",
        "AA", "aA", "Aa", "aa"), Seq(Row(5))),
      HyperLogLogPlusPlusTestCase("UNICODE", Seq("a", "a", "A", "z", "zz", "ZZ", "w", "AA",
        "aA", "Aa", "aa"), Seq(Row(9))),
      HyperLogLogPlusPlusTestCase("UNICODE_RTRIM", Seq("a ", "a", "a", "A", "z", "zz", "ZZ", "w",
        "AA", "aA", "Aa", "aa"), Seq(Row(9))),
      HyperLogLogPlusPlusTestCase("UNICODE_CI", Seq("a", "a", "A", "z", "zz", "ZZ", "w", "AA",
        "aA", "Aa", "aa"), Seq(Row(5))),
      HyperLogLogPlusPlusTestCase("UNICODE_CI_RTRIM", Seq("a ", "a", "a", "A", "z", "zz", "ZZ", "w",
        "AA", "aA", "Aa", "aa"), Seq(Row(5)))
    )

    testCases.foreach( t => {
      // Using explicit collate clause
      val query =
        s"""
           |SELECT approx_count_distinct(col) FROM VALUES
           |${t.input.map(s => s"('${s}' collate ${t.collation})").mkString(", ") } tab(col)
           |""".stripMargin
      checkAnswer(sql(query), t.output)

      // Using default collation
      withSQLConf(SqlApiConf.DEFAULT_COLLATION -> t.collation) {
        val query =
          s"""
             |SELECT approx_count_distinct(col) FROM VALUES
             |${t.input.map(s => s"('${s}')").mkString(", ") } tab(col)
             |""".stripMargin
        checkAnswer(sql(query), t.output)
      }
    })
  }

  // TODO: Add more tests for other SQL expressions

}
// scalastyle:on nonascii

class CollationSQLExpressionsANSIOffSuite extends CollationSQLExpressionsSuite {
  override protected def sparkConf: SparkConf =
    super.sparkConf.set(SQLConf.ANSI_ENABLED, false)

}
