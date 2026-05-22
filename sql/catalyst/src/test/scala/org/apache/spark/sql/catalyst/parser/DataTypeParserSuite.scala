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

package org.apache.spark.sql.catalyst.parser

import org.apache.spark.{SparkException, SparkFunSuite}
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.TimestampTypes
import org.apache.spark.sql.types._

class DataTypeParserSuite extends SparkFunSuite with SQLHelper {

  def parse(sql: String): DataType = DataTypeParser.parseDataType(sql)

  def checkDataType(dataTypeString: String, expectedDataType: DataType): Unit = {
    test(s"parse ${dataTypeString.replace("\n", "")}") {
      assert(parse(dataTypeString) === expectedDataType)
    }
  }

  def intercept(sql: String): ParseException =
    intercept[ParseException](CatalystSqlParser.parseDataType(sql))

  def unsupported(dataTypeString: String): Unit = {
    test(s"$dataTypeString is not supported") {
      intercept(dataTypeString)
    }
  }

  checkDataType("int", IntegerType)
  checkDataType("integer", IntegerType)
  checkDataType("BooLean", BooleanType)
  checkDataType("tinYint", ByteType)
  checkDataType("smallINT", ShortType)
  checkDataType("INT", IntegerType)
  checkDataType("INTEGER", IntegerType)
  checkDataType("bigint", LongType)
  checkDataType("float", FloatType)
  checkDataType("dOUBle", DoubleType)
  checkDataType("decimal(10, 5)", DecimalType(10, 5))
  checkDataType("decimal", DecimalType.USER_DEFAULT)
  checkDataType("Dec(10, 5)", DecimalType(10, 5))
  checkDataType("deC", DecimalType.USER_DEFAULT)
  checkDataType("DATE", DateType)
  checkDataType("TimE", TimeType())
  checkDataType("TimE WiTHOUT TiME ZoNE", TimeType())
  checkDataType("time(0)", TimeType(0))
  checkDataType("time(0) without time zone", TimeType(0))
  checkDataType("TIME(6)", TimeType(6))
  checkDataType("TIME(6) WITHOUT TIME ZONE", TimeType(6))
  checkDataType("timestamp", TimestampType)
  checkDataType("TIMESTAMP WITH LOCAL TIME ZONE", TimestampType)
  checkDataType("TIMESTAMP WITHOUT TIME ZONE", TimestampNTZType)
  checkDataType("timestamp_ntz", TimestampNTZType)
  checkDataType("timestamp_ltz", TimestampType)
  checkDataType("string", StringType)
  checkDataType("ChaR(5)", CharType(5))
  checkDataType("ChaRacter(5)", CharType(5))
  checkDataType("varchAr(20)", VarcharType(20))
  checkDataType("cHaR(27)", CharType(27))
  checkDataType("BINARY", BinaryType)
  checkDataType("void", NullType)
  checkDataType("interval", CalendarIntervalType)
  checkDataType("INTERVAL YEAR TO MONTH", YearMonthIntervalType())
  checkDataType("interval day to second", DayTimeIntervalType())

  checkDataType("array<doublE>", ArrayType(DoubleType, true))
  checkDataType("Array<map<int, tinYint>>", ArrayType(MapType(IntegerType, ByteType, true), true))
  checkDataType(
    "array<struct<tinYint:tinyint>>",
    ArrayType(StructType(StructField("tinYint", ByteType, true) :: Nil), true)
  )
  checkDataType("MAP<int, STRING>", MapType(IntegerType, StringType, true))
  checkDataType("MAp<int, ARRAY<double>>", MapType(IntegerType, ArrayType(DoubleType), true))
  checkDataType(
    "MAP<int, struct<varchar:string>>",
    MapType(IntegerType, StructType(StructField("varchar", StringType, true) :: Nil), true)
  )

  checkDataType(
    "struct<intType: int, ts:timestamp>",
    StructType(
      StructField("intType", IntegerType, true) ::
      StructField("ts", TimestampType, true) :: Nil)
  )
  // It is fine to use the data type string as the column name.
  checkDataType(
    "Struct<int: int, timestamp:timestamp>",
    StructType(
      StructField("int", IntegerType, true) ::
      StructField("timestamp", TimestampType, true) :: Nil)
  )
  checkDataType(
    """
      |struct<
      |  struct:struct<deciMal:DECimal, anotherDecimal:decimAL(5,2)>,
      |  MAP:Map<timestamp, varchar(10)>,
      |  arrAy:Array<double>,
      |  anotherArray:Array<char(9)>>
    """.stripMargin,
    StructType(
      StructField("struct",
        StructType(
          StructField("deciMal", DecimalType.USER_DEFAULT, true) ::
          StructField("anotherDecimal", DecimalType(5, 2), true) :: Nil), true) ::
      StructField("MAP", MapType(TimestampType, VarcharType(10)), true) ::
      StructField("arrAy", ArrayType(DoubleType, true), true) ::
      StructField("anotherArray", ArrayType(CharType(9), true), true) :: Nil)
  )
  // Use backticks to quote column names having special characters.
  checkDataType(
    "struct<`x+y`:int, `!@#$%^&*()`:string, `1_2.345<>:\"`:varchar(20)>",
    StructType(
      StructField("x+y", IntegerType, true) ::
      StructField("!@#$%^&*()", StringType, true) ::
      StructField("1_2.345<>:\"", VarcharType(20), true) :: Nil)
  )
  // Empty struct.
  checkDataType("strUCt<>", StructType(Nil))
  // struct data type definition without ":"
  checkDataType("struct<x int, y string>",
    StructType(
      StructField("x", IntegerType, true) ::
      StructField("y", StringType, true) :: Nil)
  )

  unsupported("it is not a data type")
  unsupported("struct<x+y: int, 1.1:timestamp>")
  unsupported("struct<x: int")

  test("Do not print empty parentheses for no params") {
    checkError(
      exception = intercept("unknown"),
      condition = "UNSUPPORTED_DATATYPE",
      parameters = Map("typeName" -> "\"UNKNOWN\"")
    )
    checkError(
      exception = intercept("unknown(1,2,3)"),
      condition = "UNSUPPORTED_DATATYPE",
      parameters = Map("typeName" -> "\"UNKNOWN(1,2,3)\"")
    )
  }

  test("Set default timestamp type") {
    withSQLConf(SQLConf.TIMESTAMP_TYPE.key -> TimestampTypes.TIMESTAMP_NTZ.toString) {
      assert(parse("timestamp") === TimestampNTZType)
      assert(parse("timestamp with local time zone") === TimestampType)
      assert(parse("timestamp without time zone") === TimestampNTZType)
      withSQLConf(SQLConf.TIMESTAMP_NANOS_TYPES_ENABLED.key -> "true") {
        assert(parse("timestamp(9)") === TimestampNTZNanosType(9))
        // Bare TIMESTAMP(p) routes through SqlApiConf.get.timestampType, so an
        // out-of-range precision must surface as the NTZ error here.
        Seq("6", "10").foreach { p =>
          checkError(
            exception = intercept[SparkException] {
              CatalystSqlParser.parseDataType(s"timestamp($p)")
            },
            condition = "INVALID_TIMESTAMP_PRECISION",
            parameters = Map("precision" -> p, "type" -> "TIMESTAMP_NTZ"))
        }
      }
    }
    withSQLConf(SQLConf.TIMESTAMP_TYPE.key -> TimestampTypes.TIMESTAMP_LTZ.toString) {
      assert(parse("timestamp") === TimestampType)
      assert(parse("timestamp with local time zone") === TimestampType)
      assert(parse("timestamp without time zone") === TimestampNTZType)
      withSQLConf(SQLConf.TIMESTAMP_NANOS_TYPES_ENABLED.key -> "true") {
        assert(parse("timestamp(9)") === TimestampLTZNanosType(9))
        // Bare TIMESTAMP(p) under LTZ default must surface as the LTZ error.
        Seq("6", "10").foreach { p =>
          checkError(
            exception = intercept[SparkException] {
              CatalystSqlParser.parseDataType(s"timestamp($p)")
            },
            condition = "INVALID_TIMESTAMP_PRECISION",
            parameters = Map("precision" -> p, "type" -> "TIMESTAMP_LTZ"))
        }
      }
    }
  }

  test("parse nanos timestamp types when the preview flag is enabled") {
    withSQLConf(SQLConf.TIMESTAMP_NANOS_TYPES_ENABLED.key -> "true") {
      assert(parse("TIMESTAMP_NTZ(7)") === TimestampNTZNanosType(7))
      assert(parse("TIMESTAMP_NTZ(8)") === TimestampNTZNanosType(8))
      assert(parse("TIMESTAMP_NTZ(9)") === TimestampNTZNanosType(9))
      assert(parse("TIMESTAMP_LTZ(7)") === TimestampLTZNanosType(7))
      assert(parse("TIMESTAMP_LTZ(8)") === TimestampLTZNanosType(8))
      assert(parse("TIMESTAMP_LTZ(9)") === TimestampLTZNanosType(9))
      assert(parse("Timestamp_Ntz(9)") === TimestampNTZNanosType(9))
      assert(parse("timestamp_ltz(7)") === TimestampLTZNanosType(7))
      assert(parse("TIMESTAMP(9) WITHOUT TIME ZONE") === TimestampNTZNanosType(9))
      assert(parse("TIMESTAMP(7) WITH LOCAL TIME ZONE") === TimestampLTZNanosType(7))
      assert(parse("timestamp(8) without time zone") === TimestampNTZNanosType(8))
      assert(parse("timestamp(8) with local time zone") === TimestampLTZNanosType(8))
    }
  }

  test("nanos timestamp parser surface is gated by SQL conf, disabled by default") {
    val gatedSpellings = Seq(
      "TIMESTAMP_NTZ(7)",
      "TIMESTAMP_LTZ(9)",
      "TIMESTAMP(9) WITHOUT TIME ZONE",
      "TIMESTAMP(9) WITH LOCAL TIME ZONE",
      "TIMESTAMP(9)")
    gatedSpellings.foreach { spelling =>
      checkError(
        exception = intercept[SparkException] {
          CatalystSqlParser.parseDataType(spelling)
        },
        condition = "FEATURE_NOT_ENABLED",
        parameters = Map(
          "featureName" -> "Nanosecond-precision timestamp types",
          "configKey" -> "spark.sql.timestampNanosTypes.enabled",
          "configValue" -> "true"))
    }
    // Bare unparameterized forms remain accepted even with the gate off.
    assert(parse("TIMESTAMP_NTZ") === TimestampNTZType)
    assert(parse("TIMESTAMP_LTZ") === TimestampType)
    assert(parse("TIMESTAMP WITHOUT TIME ZONE") === TimestampNTZType)
    assert(parse("TIMESTAMP WITH LOCAL TIME ZONE") === TimestampType)
  }

  // DataType parser accepts certain reserved keywords.
  checkDataType(
    "Struct<TABLE: string, DATE:boolean>",
    StructType(
      StructField("TABLE", StringType, true) ::
        StructField("DATE", BooleanType, true) :: Nil)
  )

  // Use SQL keywords.
  checkDataType("struct<end: long, select: int, from: string>",
    (new StructType).add("end", LongType).add("select", IntegerType).add("from", StringType))

  // DataType parser accepts comments.
  checkDataType("Struct<x: INT, y: STRING COMMENT 'test'>",
    (new StructType).add("x", IntegerType).add("y", StringType, true, "test"))

  test("unsupported precision of the time data type") {
    checkError(
      exception = intercept[SparkException] {
        CatalystSqlParser.parseDataType("time(9)")
      },
      condition = "UNSUPPORTED_TIME_PRECISION",
      parameters = Map("precision" -> "9"))
    checkError(
      exception = intercept[SparkException] {
        CatalystSqlParser.parseDataType("time(8) without time zone")
      },
      condition = "UNSUPPORTED_TIME_PRECISION",
      parameters = Map("precision" -> "8"))
    checkError(
      exception = intercept[ParseException] {
        CatalystSqlParser.parseDataType("time(-1)")
      },
      condition = "PARSE_SYNTAX_ERROR",
      parameters = Map("error" -> "'-'", "hint" -> ""))
    checkError(
      exception = intercept[ParseException] {
        CatalystSqlParser.parseDataType("time(-100) WITHOUT TIME ZONE")
      },
      condition = "PARSE_SYNTAX_ERROR",
      parameters = Map("error" -> "'-'", "hint" -> ""))
  }

  test("invalid TIME suffix") {
    checkError(
      exception = intercept[ParseException] {
        CatalystSqlParser.parseDataType("time(0) WITHOUT TIMEZONE")
      },
      condition = "PARSE_SYNTAX_ERROR",
      parameters = Map("error" -> "'WITHOUT'", "hint" -> ""))
    checkError(
      exception = intercept[ParseException] {
        CatalystSqlParser.parseDataType("time(0) WITH TIME ZONE")
      },
      condition = "PARSE_SYNTAX_ERROR",
      parameters = Map("error" -> "'WITH'", "hint" -> ""))
  }

  test("invalid TIMESTAMP suffix") {
    checkError(
      exception = intercept[ParseException] {
        CatalystSqlParser.parseDataType("timestamp WITHOUT TIMEZONE")
      },
      condition = "PARSE_SYNTAX_ERROR",
      parameters = Map("error" -> "'WITHOUT'", "hint" -> ""))
    checkError(
      exception = intercept[ParseException] {
        CatalystSqlParser.parseDataType("timestamp WITH TIME ZONE")
      },
      condition = "PARSE_SYNTAX_ERROR",
      parameters = Map("error" -> "'WITH'", "hint" -> ""))
  }

  test("invalid precision of the nanos timestamp data type") {
    withSQLConf(SQLConf.TIMESTAMP_NANOS_TYPES_ENABLED.key -> "true") {
      Seq("TIMESTAMP_NTZ" -> "TIMESTAMP_NTZ", "TIMESTAMP_LTZ" -> "TIMESTAMP_LTZ").foreach {
        case (spelling, errorType) =>
          Seq(0, 1, 6, 10, 99).foreach { p =>
            checkError(
              exception = intercept[SparkException] {
                CatalystSqlParser.parseDataType(s"$spelling($p)")
              },
              condition = "INVALID_TIMESTAMP_PRECISION",
              parameters = Map("precision" -> p.toString, "type" -> errorType))
          }
      }
      // Integer overflow: regex matches but Int.parseInt fails. Original digits are preserved.
      checkError(
        exception = intercept[SparkException] {
          CatalystSqlParser.parseDataType("TIMESTAMP_NTZ(99999999999)")
        },
        condition = "INVALID_TIMESTAMP_PRECISION",
        parameters = Map("precision" -> "99999999999", "type" -> "TIMESTAMP_NTZ"))
      // TIMESTAMP(p) with zone aliases route to the corresponding nanos type's error.
      checkError(
        exception = intercept[SparkException] {
          CatalystSqlParser.parseDataType("TIMESTAMP(6) WITHOUT TIME ZONE")
        },
        condition = "INVALID_TIMESTAMP_PRECISION",
        parameters = Map("precision" -> "6", "type" -> "TIMESTAMP_NTZ"))
      checkError(
        exception = intercept[SparkException] {
          CatalystSqlParser.parseDataType("TIMESTAMP(10) WITH LOCAL TIME ZONE")
        },
        condition = "INVALID_TIMESTAMP_PRECISION",
        parameters = Map("precision" -> "10", "type" -> "TIMESTAMP_LTZ"))
      // Negative precision is rejected by the parser, not by the type constructor.
      checkError(
        exception = intercept[ParseException] {
          CatalystSqlParser.parseDataType("TIMESTAMP_NTZ(-1)")
        },
        condition = "PARSE_SYNTAX_ERROR",
        parameters = Map("error" -> "'-'", "hint" -> ""))
      checkError(
        exception = intercept[ParseException] {
          CatalystSqlParser.parseDataType("TIMESTAMP_LTZ(-100)")
        },
        condition = "PARSE_SYNTAX_ERROR",
        parameters = Map("error" -> "'-'", "hint" -> ""))
    }
  }
}
