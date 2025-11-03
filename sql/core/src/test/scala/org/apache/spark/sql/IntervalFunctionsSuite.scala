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

package org.apache.spark.sql

import java.time.{Duration, Period}

import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{DayTimeIntervalType => DT, YearMonthIntervalType => YM}
import org.apache.spark.sql.types.DataTypeTestUtils._

class IntervalFunctionsSuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  test("SPARK-36022: Respect interval fields in extract") {
    yearMonthIntervalTypes.foreach { dtype =>
      val ymDF = Seq(Period.of(1, 2, 0)).toDF().select($"value" cast dtype as "value")
        .select($"value" cast dtype as "value")
      val expectedMap = Map("year" -> 1, "month" -> 2)
      YM.yearMonthFields.foreach { field =>
        val extractUnit = YM.fieldToString(field)
        val extractExpr = s"extract($extractUnit FROM value)"
        if (dtype.startField <= field && field <= dtype.endField) {
          checkAnswer(ymDF.selectExpr(extractExpr), Row(expectedMap(extractUnit)))
        } else {
          intercept[AnalysisException] {
            ymDF.selectExpr(extractExpr)
          }
        }
      }
    }

    dayTimeIntervalTypes.foreach { dtype =>
      val dtDF = Seq(Duration.ofDays(1).plusHours(2).plusMinutes(3).plusSeconds(4)).toDF()
        .select($"value" cast dtype as "value")
      val expectedMap = Map("day" -> 1, "hour" -> 2, "minute" -> 3, "second" -> 4)
      DT.dayTimeFields.foreach { field =>
        val extractUnit = DT.fieldToString(field)
        val extractExpr = s"extract($extractUnit FROM value)"
        if (dtype.startField <= field && field <= dtype.endField) {
          checkAnswer(dtDF.selectExpr(extractExpr), Row(expectedMap(extractUnit)))
        } else {
          intercept[AnalysisException] {
            dtDF.selectExpr(extractExpr)
          }
        }
      }
    }
  }
}
