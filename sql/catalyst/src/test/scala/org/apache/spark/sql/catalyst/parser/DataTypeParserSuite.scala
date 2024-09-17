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

import org.apache.spark.SparkFunSuite
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
  checkDataType("timestamp", TimestampType)
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
    }
    withSQLConf(SQLConf.TIMESTAMP_TYPE.key -> TimestampTypes.TIMESTAMP_LTZ.toString) {
      assert(parse("timestamp") === TimestampType)
    }
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
}
