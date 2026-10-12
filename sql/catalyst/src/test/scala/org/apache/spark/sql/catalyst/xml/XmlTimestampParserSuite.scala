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
import org.apache.spark.sql.internal.SQLConf

class XmlTimestampParserSuite extends SparkFunSuite with SQLHelper {
  private val value = "2024-02-29T12:34:56.123456789Z"

  test("SPARK-60199: XML retains nanosecond parsing under modern policies") {
    Seq("CORRECTED", "EXCEPTION").foreach { policy =>
      withSQLConf(SQLConf.LEGACY_TIME_PARSER_POLICY.key -> policy) {
        val parser = new XmlTimestampParser(new XmlOptions())
        val parsed = parser.parseNanos(value, 9)
        assert(parsed.epochMicros == 1709210096123456L)
        assert(parsed.nanosWithinMicro == 789)
        assert(parser.parseNanosOptional(value, 9).contains(parsed))
      }
    }
  }

  test("SPARK-60199: XML retains the LEGACY error for nanosecond timestamp types") {
    withSQLConf(SQLConf.LEGACY_TIME_PARSER_POLICY.key -> "LEGACY") {
      val parser = new XmlTimestampParser(new XmlOptions())
      Seq[() => Any](() => parser.parseNanos(value, 9), () => parser.parseNanosOptional(value, 9))
        .foreach { parse =>
          checkError(
            exception = intercept[SparkUnsupportedOperationException](parse()),
            condition = "UNSUPPORTED_FEATURE.TIMESTAMP_NANOS_WITH_LEGACY_TIME_PARSER",
            parameters = Map("config" -> "\"spark.sql.legacy.timeParserPolicy\""))
        }
    }
  }
}
