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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.util.DateTimeTestUtils._

class TimeExpressionsSuite extends SparkFunSuite with ExpressionEvalHelper {
  test("ParseToTime") {
    checkEvaluation(new ParseToTime(Literal("00:00:00")), localTime())
    checkEvaluation(new ParseToTime(Literal("23-59-00.000999"), Literal("HH-mm-ss.SSSSSS")),
      localTime(23, 59, 0, 999))
    checkEvaluation(
      new ParseToTime(Literal("12.00.59.90909"), NonFoldableLiteral("HH.mm.ss.SSSSS")),
      localTime(12, 0, 59, 909090))
    checkEvaluation(
      new ParseToTime(NonFoldableLiteral(" 12:00.909 "), Literal(" HH:mm.SSS ")),
      localTime(12, 0, 0, 909000))
    checkEvaluation(
      new ParseToTime(
        NonFoldableLiteral("12 hours 123 millis"),
        NonFoldableLiteral("HH 'hours' SSS 'millis'")),
      localTime(12, 0, 0, 123000))
  }
}
