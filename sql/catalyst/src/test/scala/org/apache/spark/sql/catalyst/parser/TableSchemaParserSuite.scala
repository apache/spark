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

import org.apache.spark.{SparkFunSuite, SparkThrowable}
import org.apache.spark.sql.types._

class TableSchemaParserSuite extends SparkFunSuite {

  def parse(sql: String): StructType = CatalystSqlParser.parseTableSchema(sql)

  def checkTableSchema(tableSchemaString: String, expectedDataType: DataType): Unit = {
    test(s"parse $tableSchemaString") {
      assert(parse(tableSchemaString) === expectedDataType)
    }
  }

  checkTableSchema("a int", new StructType().add("a", "int"))
  checkTableSchema("A int", new StructType().add("A", "int"))
  checkTableSchema("a INT", new StructType().add("a", "int"))
  checkTableSchema("`!@#$%.^&*()` string", new StructType().add("!@#$%.^&*()", "string"))
  checkTableSchema("a int, b long", new StructType().add("a", "int").add("b", "long"))
  checkTableSchema("a STRUCT<intType: int, ts:timestamp>",
    StructType(
      StructField("a", StructType(
        StructField("intType", IntegerType) ::
        StructField("ts", TimestampType) :: Nil)) :: Nil))
  checkTableSchema(
    "a int comment 'test'",
    new StructType().add("a", "int", nullable = true, "test"))

  test("complex hive type") {
    val tableSchemaString =
      """
        |complexStructCol struct<
        |struct:struct<deciMal:DECimal, anotherDecimal:decimAL(5,2)>,
        |MAP:Map<timestamp, varchar(10)>,
        |arrAy:Array<double>,
        |anotherArray:Array<char(9)>>
      """.stripMargin.replace("\n", "")

    val expectedDataType =
      StructType(
        StructField("complexStructCol", StructType(
          StructField("struct",
            StructType(
              StructField("deciMal", DecimalType.USER_DEFAULT) ::
                StructField("anotherDecimal", DecimalType(5, 2)) :: Nil)) ::
            StructField("MAP", MapType(TimestampType, VarcharType(10))) ::
            StructField("arrAy", ArrayType(DoubleType)) ::
            StructField("anotherArray", ArrayType(CharType(9))) :: Nil)) :: Nil)

    assert(parse(tableSchemaString) === expectedDataType)
  }

  // Negative cases
  test("Negative cases") {
    def parseException(sql: String): SparkThrowable =
      intercept[ParseException](CatalystSqlParser.parseTableSchema(sql))

    checkError(
      exception = parseException(""),
      condition = "PARSE_EMPTY_STATEMENT")
    checkError(
      exception = parseException("a"),
      condition = "PARSE_SYNTAX_ERROR",
      parameters = Map("error" -> "end of input", "hint" -> ""))
    checkError(
      exception = parseException("a INT b long"),
      condition = "PARSE_SYNTAX_ERROR",
      parameters = Map("error" -> "'b'", "hint" -> ""))
    checkError(
      exception = parseException("a INT,, b long"),
      condition = "PARSE_SYNTAX_ERROR",
      parameters = Map("error" -> "','", "hint" -> ""))
    checkError(
      exception = parseException("a INT, b long,,"),
      condition = "PARSE_SYNTAX_ERROR",
      parameters = Map("error" -> "','", "hint" -> ""))
    checkError(
      exception = parseException("a INT, b long, c int,"),
      condition = "PARSE_SYNTAX_ERROR",
      parameters = Map("error" -> "end of input", "hint" -> ""))
  }
}
