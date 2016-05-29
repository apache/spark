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

package org.apache.spark.sql.execution.datasources.json

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}

/**
 * Test cases for various [[JSONOptions]].
 */
class JsonParsingOptionsSuite extends QueryTest with SharedSQLContext {

  test("allowComments off") {
    val str = """{'name': /* hello */ 'Reynold Xin'}"""
    val rdd = spark.sparkContext.parallelize(Seq(str))
    val df = spark.read.json(rdd)

    assert(df.schema.head.name == "_corrupt_record")
  }

  test("allowComments on") {
    val str = """{'name': /* hello */ 'Reynold Xin'}"""
    val rdd = spark.sparkContext.parallelize(Seq(str))
    val df = spark.read.option("allowComments", "true").json(rdd)

    assert(df.schema.head.name == "name")
    assert(df.first().getString(0) == "Reynold Xin")
  }

  test("allowSingleQuotes off") {
    val str = """{'name': 'Reynold Xin'}"""
    val rdd = spark.sparkContext.parallelize(Seq(str))
    val df = spark.read.option("allowSingleQuotes", "false").json(rdd)

    assert(df.schema.head.name == "_corrupt_record")
  }

  test("allowSingleQuotes on") {
    val str = """{'name': 'Reynold Xin'}"""
    val rdd = spark.sparkContext.parallelize(Seq(str))
    val df = spark.read.json(rdd)

    assert(df.schema.head.name == "name")
    assert(df.first().getString(0) == "Reynold Xin")
  }

  test("allowUnquotedFieldNames off") {
    val str = """{name: 'Reynold Xin'}"""
    val rdd = spark.sparkContext.parallelize(Seq(str))
    val df = spark.read.json(rdd)

    assert(df.schema.head.name == "_corrupt_record")
  }

  test("allowUnquotedFieldNames on") {
    val str = """{name: 'Reynold Xin'}"""
    val rdd = spark.sparkContext.parallelize(Seq(str))
    val df = spark.read.option("allowUnquotedFieldNames", "true").json(rdd)

    assert(df.schema.head.name == "name")
    assert(df.first().getString(0) == "Reynold Xin")
  }

  test("allowNumericLeadingZeros off") {
    val str = """{"age": 0018}"""
    val rdd = spark.sparkContext.parallelize(Seq(str))
    val df = spark.read.json(rdd)

    assert(df.schema.head.name == "_corrupt_record")
  }

  test("allowNumericLeadingZeros on") {
    val str = """{"age": 0018}"""
    val rdd = spark.sparkContext.parallelize(Seq(str))
    val df = spark.read.option("allowNumericLeadingZeros", "true").json(rdd)

    assert(df.schema.head.name == "age")
    assert(df.first().getLong(0) == 18)
  }

  test("allowNonNumericNumbers off") {
    // non-quoted non-numeric numbers don't work if allowNonNumericNumbers is off.
    var testCases: Seq[String] = Seq("""{"age": NaN}""", """{"age": Infinity}""",
      """{"age": +Infinity}""", """{"age": -Infinity}""", """{"age": INF}""",
      """{"age": +INF}""", """{"age": -INF}""")
    testCases.foreach { str =>
      val rdd = spark.sparkContext.parallelize(Seq(str))
      val df = spark.read.option("allowNonNumericNumbers", "false").json(rdd)

      assert(df.schema.head.name == "_corrupt_record")
    }

    // quoted non-numeric numbers should still work even allowNonNumericNumbers is off.
    testCases = Seq("""{"age": "NaN"}""", """{"age": "Infinity"}""", """{"age": "+Infinity"}""",
      """{"age": "-Infinity"}""", """{"age": "INF"}""", """{"age": "+INF"}""",
      """{"age": "-INF"}""")
    val tests: Seq[Double => Boolean] = Seq(_.isNaN, _.isPosInfinity, _.isPosInfinity,
      _.isNegInfinity, _.isPosInfinity, _.isPosInfinity, _.isNegInfinity)
    val schema = StructType(StructField("age", DoubleType, true) :: Nil)

    testCases.zipWithIndex.foreach { case (str, idx) =>
      val rdd = spark.sparkContext.parallelize(Seq(str))
      val df = spark.read.option("allowNonNumericNumbers", "false").schema(schema).json(rdd)

      assert(df.schema.head.name == "age")
      assert(tests(idx)(df.first().getDouble(0)))
    }
  }

  test("allowNonNumericNumbers on") {
    val testCases: Seq[String] = Seq("""{"age": NaN}""", """{"age": Infinity}""",
      """{"age": +Infinity}""", """{"age": -Infinity}""", """{"age": +INF}""",
      """{"age": -INF}""", """{"age": "NaN"}""", """{"age": "Infinity"}""",
      """{"age": "-Infinity"}""")
    val tests: Seq[Double => Boolean] = Seq(_.isNaN, _.isPosInfinity, _.isPosInfinity,
      _.isNegInfinity, _.isPosInfinity, _.isNegInfinity, _.isNaN, _.isPosInfinity,
      _.isNegInfinity, _.isPosInfinity, _.isNegInfinity)
    val schema = StructType(StructField("age", DoubleType, true) :: Nil)
    testCases.zipWithIndex.foreach { case (str, idx) =>
      val rdd = spark.sparkContext.parallelize(Seq(str))
      val df = spark.read.option("allowNonNumericNumbers", "true").schema(schema).json(rdd)

      assert(df.schema.head.name == "age")
      assert(tests(idx)(df.first().getDouble(0)))
    }
  }

  test("allowBackslashEscapingAnyCharacter off") {
    val str = """{"name": "Cazen Lee", "price": "\$10"}"""
    val rdd = spark.sparkContext.parallelize(Seq(str))
    val df = spark.read.option("allowBackslashEscapingAnyCharacter", "false").json(rdd)

    assert(df.schema.head.name == "_corrupt_record")
  }

  test("allowBackslashEscapingAnyCharacter on") {
    val str = """{"name": "Cazen Lee", "price": "\$10"}"""
    val rdd = spark.sparkContext.parallelize(Seq(str))
    val df = spark.read.option("allowBackslashEscapingAnyCharacter", "true").json(rdd)

    assert(df.schema.head.name == "name")
    assert(df.schema.last.name == "price")
    assert(df.first().getString(0) == "Cazen Lee")
    assert(df.first().getString(1) == "$10")
  }
}
