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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

class JsonInferSchemaSuite extends SparkFunSuite with SQLHelper {

  def checkType(options: Map[String, String], json: String, dt: DataType): Unit = {
    val jsonOptions = new JSONOptions(options, "UTC", "")
    val inferSchema = new JsonInferSchema(jsonOptions)
    val factory = jsonOptions.buildJsonFactory()
    val parser = CreateJacksonParser.string(factory, json)
    parser.nextToken()
    val expectedType = StructType(Seq(StructField("a", dt, true)))

    assert(inferSchema.inferField(parser) === expectedType)
  }

  def checkTimestampType(pattern: String, json: String, inferTimestamp: Boolean): Unit = {
    checkType(
      Map("timestampFormat" -> pattern, "inferTimestamp" -> inferTimestamp.toString),
      json,
      if (inferTimestamp) TimestampType else StringType)
  }

  test("inferring timestamp type") {
    Seq(true, false).foreach { inferTimestamp =>
      Seq("legacy", "corrected").foreach { legacyParserPolicy =>
        withSQLConf(SQLConf.LEGACY_TIME_PARSER_POLICY.key -> legacyParserPolicy) {
          checkTimestampType("yyyy", """{"a": "2018"}""", inferTimestamp)
          checkTimestampType("yyyy=MM", """{"a": "2018=12"}""", inferTimestamp)
          checkTimestampType("yyyy MM dd", """{"a": "2018 12 02"}""", inferTimestamp)
          checkTimestampType(
            "yyyy-MM-dd'T'HH:mm:ss.SSS",
            """{"a": "2018-12-02T21:04:00.123"}""",
            inferTimestamp)
          checkTimestampType(
            "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX",
            """{"a": "2018-12-02T21:04:00.123567+01:00"}""",
            inferTimestamp)
        }
      }
    }
  }

  test("prefer decimals over timestamps") {
    Seq("legacy", "corrected").foreach { legacyParser =>
      withSQLConf(SQLConf.LEGACY_TIME_PARSER_POLICY.key -> legacyParser) {
        checkType(
          options = Map(
            "prefersDecimal" -> "true",
            "timestampFormat" -> "yyyyMMdd.HHmmssSSS"
          ),
          json = """{"a": "20181202.210400123"}""",
          dt = DecimalType(17, 9)
        )
      }
    }
  }

  test("skip decimal type inferring") {
    Seq(true, false).foreach { inferTimestamp =>
      Seq("legacy", "corrected").foreach { legacyParserPolicy =>
        withSQLConf(SQLConf.LEGACY_TIME_PARSER_POLICY.key -> legacyParserPolicy) {
          checkType(
            options = Map(
              "prefersDecimal" -> "false",
              "timestampFormat" -> "yyyyMMdd.HHmmssSSS",
              "inferTimestamp" -> inferTimestamp.toString
            ),
            json = """{"a": "20181202.210400123"}""",
            dt = if (inferTimestamp) TimestampType else StringType
          )
        }
      }
    }
  }

  test("fallback to string type") {
    Seq("legacy", "corrected").foreach { legacyParserPolicy =>
      withSQLConf(SQLConf.LEGACY_TIME_PARSER_POLICY.key -> legacyParserPolicy) {
        checkType(
          options = Map("timestampFormat" -> "yyyy,MM,dd.HHmmssSSS"),
          json = """{"a": "20181202.210400123"}""",
          dt = StringType
        )
      }
    }
  }

  test("disable timestamp inferring") {
    val json = """{"a": "2019-01-04T21:11:10.123Z"}"""
    checkType(Map("inferTimestamp" -> "true"), json, TimestampType)
    checkType(Map("inferTimestamp" -> "false"), json, StringType)
  }

  test("SPARK-45433: inferring the schema when timestamps do not match specified timestampFormat" +
    " with only one row") {
    checkType(
      Map("timestampFormat" -> "yyyy-MM-dd'T'HH:mm:ss", "inferTimestamp" -> "true"),
      """{"a": "2884-06-24T02:45:51.138"}""",
      StringType)
  }
}
