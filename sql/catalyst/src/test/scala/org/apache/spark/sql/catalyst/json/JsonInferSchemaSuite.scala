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

package org.apache.spark.sql.catalyst.json

import com.fasterxml.jackson.core.JsonFactory

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.types._

class JsonInferSchemaSuite extends SparkFunSuite with SQLHelper {

  def checkType(options: Map[String, String], json: String, dt: DataType): Unit = {
    val jsonOptions = new JSONOptions(options, "UTC", "")
    val inferSchema = new JsonInferSchema(jsonOptions)
    val factory = new JsonFactory()
    jsonOptions.setJacksonOptions(factory)
    val parser = CreateJacksonParser.string(factory, json)
    parser.nextToken()
    val expectedType = StructType(Seq(StructField("a", dt, true)))

    assert(inferSchema.inferField(parser) === expectedType)
  }

  def checkTimestampType(pattern: String, json: String): Unit = {
    checkType(Map("timestampFormat" -> pattern), json, TimestampType)
  }

  test("inferring timestamp type") {
    checkTimestampType("yyyy", """{"a": "2018"}""")
    checkTimestampType("yyyy=MM", """{"a": "2018=12"}""")
    checkTimestampType("yyyy MM dd", """{"a": "2018 12 02"}""")
    checkTimestampType(
      "yyyy-MM-dd'T'HH:mm:ss.SSS",
      """{"a": "2018-12-02T21:04:00.123"}""")
    checkTimestampType(
      "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX",
      """{"a": "2018-12-02T21:04:00.123567+01:00"}""")
  }

  test("prefer decimals over timestamps") {
    checkType(
      options = Map(
        "prefersDecimal" -> "true",
        "timestampFormat" -> "yyyyMMdd.HHmmssSSS"
      ),
      json = """{"a": "20181202.210400123"}""",
      dt = DecimalType(17, 9)
    )
  }

  test("skip decimal type inferring") {
    checkType(
      options = Map(
        "prefersDecimal" -> "false",
        "timestampFormat" -> "yyyyMMdd.HHmmssSSS"
      ),
      json = """{"a": "20181202.210400123"}""",
      dt = TimestampType
    )
  }

  test("fallback to string type") {
    checkType(
      options = Map("timestampFormat" -> "yyyy,MM,dd.HHmmssSSS"),
      json = """{"a": "20181202.210400123"}""",
      dt = StringType
    )
  }

  test("disable timestamp inferring") {
    val json = """{"a": "2019-01-04T21:11:10.123Z"}"""
    checkType(Map("inferTimestamp" -> "true"), json, TimestampType)
    checkType(Map("inferTimestamp" -> "false"), json, StringType)
  }
}
