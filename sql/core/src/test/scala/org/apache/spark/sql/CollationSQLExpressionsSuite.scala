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

import org.apache.spark.{SparkException, SparkRuntimeException}
import org.apache.spark.sql.internal.SqlApiConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

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
          ),
      ),
      VariantExplodeTestCase("[\"Spark\", \"SQL\"]", "UTF8_BINARY_LCASE",
        Row(0, "null", "\"Spark\"").toString() + Row(1, "null", "\"SQL\"").toString(),
        Seq[StructField](
          StructField("pos", IntegerType, nullable = false),
          StructField("key", StringType("UTF8_BINARY_LCASE")),
          StructField("value", VariantType, nullable = false)
        ),
      ),
      VariantExplodeTestCase("{\"a\": true, \"b\": 3.14}", "UNICODE",
        Row(0, "a", "true").toString() + Row(1, "b", "3.14").toString(),
        Seq[StructField](
          StructField("pos", IntegerType, nullable = false),
          StructField("key", StringType("UNICODE")),
          StructField("value", VariantType, nullable = false)
        ),
      ),
      VariantExplodeTestCase("{\"A\": 9.99, \"B\": false}", "UNICODE_CI",
        Row(0, "A", "9.99").toString() + Row(1, "B", "false").toString(),
        Seq[StructField](
          StructField("pos", IntegerType, nullable = false),
          StructField("key", StringType("UNICODE_CI")),
          StructField("value", VariantType, nullable = false)
        ),
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

  // TODO: Add more tests for other SQL expressions

}
// scalastyle:on nonascii
