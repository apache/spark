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
import org.apache.spark.sql.types._

class DataTypeParserSuite extends SparkFunSuite {

  def parse(sql: String): DataType = CatalystSqlParser.parseDataType(sql)

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
  checkDataType("DATE", DateType)
  checkDataType("timestamp", TimestampType)
  checkDataType("string", StringType)
  checkDataType("ChaR(5)", StringType)
  checkDataType("varchAr(20)", StringType)
  checkDataType("cHaR(27)", StringType)
  checkDataType("BINARY", BinaryType)

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
      StructField("MAP", MapType(TimestampType, StringType), true) ::
      StructField("arrAy", ArrayType(DoubleType, true), true) ::
      StructField("anotherArray", ArrayType(StringType, true), true) :: Nil)
  )
  // Use backticks to quote column names having special characters.
  checkDataType(
    "struct<`x+y`:int, `!@#$%^&*()`:string, `1_2.345<>:\"`:varchar(20)>",
    StructType(
      StructField("x+y", IntegerType, true) ::
      StructField("!@#$%^&*()", StringType, true) ::
      StructField("1_2.345<>:\"", StringType, true) :: Nil)
  )
  // Empty struct.
  checkDataType("strUCt<>", StructType(Nil))

  unsupported("it is not a data type")
  unsupported("struct<x+y: int, 1.1:timestamp>")
  unsupported("struct<x: int")
  unsupported("struct<x int, y string>")

  test("Do not print empty parentheses for no params") {
    assert(intercept("unkwon").getMessage.contains("unkwon is not supported"))
    assert(intercept("unkwon(1,2,3)").getMessage.contains("unkwon(1,2,3) is not supported"))
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
