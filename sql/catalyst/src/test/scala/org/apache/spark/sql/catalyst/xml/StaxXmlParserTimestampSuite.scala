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

package org.apache.spark.sql.catalyst.xml

import org.apache.spark.{SparkFunSuite, SparkUnsupportedOperationException}
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.catalyst.util.BadRecordException
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{NullType, StructType, TimestampLTZNanosType}

class StaxXmlParserTimestampSuite extends SparkFunSuite with SQLHelper {
  private val value = "2024-02-29T12:34:56.123456789Z"
  private val schema = new StructType().add("stamp", TimestampLTZNanosType(9))
  private val xml = s"<ROW><stamp>$value</stamp></ROW>"

  test("SPARK-60199: XML retains nanosecond parsing under modern policies") {
    Seq("CORRECTED", "EXCEPTION").foreach { policy =>
      withSQLConf(
        SQLConf.LEGACY_TIME_PARSER_POLICY.key -> policy,
        SQLConf.TIMESTAMP_NANOS_TYPES_ENABLED.key -> "true") {
        val options = new XmlOptions()
        val parser = new StaxXmlParser(schema, options)
        val parsed = parser.parseColumn(xml, schema).getTimestampLTZNanos(0)
        assert(parsed.epochMicros == 1709210096123456L)
        assert(parsed.nanosWithinMicro == 789)
        val inference = new XmlInferSchema(options, caseSensitive = false)
        assert(inference.inferFrom(value, NullType) == TimestampLTZNanosType(9))
      }
    }
  }

  test("SPARK-60199: XML retains the LEGACY error for nanosecond timestamp types") {
    withSQLConf(
      SQLConf.LEGACY_TIME_PARSER_POLICY.key -> "LEGACY",
      SQLConf.TIMESTAMP_NANOS_TYPES_ENABLED.key -> "true") {
      val options = new XmlOptions()
      val parser = new StaxXmlParser(schema, options)
      val inference = new XmlInferSchema(options, caseSensitive = false)
      val readError = intercept[BadRecordException] {
        parser.parseColumn(xml, schema)
      }.getCause
      val inferenceError = intercept[SparkUnsupportedOperationException] {
        inference.inferFrom(value, NullType)
      }
      Seq(readError, inferenceError).foreach { error =>
        checkError(
          exception = error.asInstanceOf[SparkUnsupportedOperationException],
          condition = "UNSUPPORTED_FEATURE.TIMESTAMP_NANOS_WITH_LEGACY_TIME_PARSER",
          parameters = Map("config" -> "\"spark.sql.legacy.timeParserPolicy\""))
      }
    }
  }
}
