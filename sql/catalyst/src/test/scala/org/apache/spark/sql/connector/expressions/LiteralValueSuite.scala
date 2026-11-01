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

package org.apache.spark.sql.connector.expressions

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.types.{BinaryType, BooleanType, CharType, IntegerType, StringType, VarcharType}
import org.apache.spark.unsafe.types.UTF8String

class LiteralValueSuite extends SparkFunSuite {

  test("SPARK-58782: null literals should render as NULL") {
    assert(LiteralValue(null, StringType).toString === "NULL")
    assert(LiteralValue(null, IntegerType).toString === "NULL")
    assert(LiteralValue(null, BooleanType).toString === "NULL")
    assert(LiteralValue(null, BinaryType).toString === "NULL")
  }

  test("non-null string literals should be quoted") {
    assert(LiteralValue("test", StringType).toString === "'test'")
    assert(LiteralValue("", StringType).toString === "''")
    assert(LiteralValue("it's", StringType).toString === "'it''s'")
  }

  for (dataType <- Seq(
      StringType, StringType("UTF8_LCASE"), StringType("UNICODE_CI"),
      CharType(8), VarcharType(8))) {
    test(s"collated and constrained string literals should be quoted: $dataType") {
      for ((value, expected) <- Seq(
          "" -> "''", "a" -> "'a'", "abc" -> "'abc'", "null" -> "'null'",
          "it's" -> "'it''s'", "a%b_c" -> "'a%b_c'", "a\\b" -> "'a\\b'")) {
        assert(LiteralValue(UTF8String.fromString(value), dataType).toString === expected)
      }
      assert(LiteralValue(null, dataType).toString === "NULL")
    }
  }

  test("non-null numeric literals should not be quoted") {
    assert(LiteralValue(42, IntegerType).toString === "42")
    assert(LiteralValue(0, IntegerType).toString === "0")
  }

  test("non-null boolean literals should not be quoted") {
    assert(LiteralValue(true, BooleanType).toString === "true")
    assert(LiteralValue(false, BooleanType).toString === "false")
  }

  test("non-null binary literals should render as hex") {
    val bytes = Array[Byte](0x12, 0x34, 0xAB.toByte, 0xCD.toByte)
    assert(LiteralValue(bytes, BinaryType).toString === "0x1234ABCD")
  }
}
