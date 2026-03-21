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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

class TimeTypeCoercionSuite extends AnalysisTest {

  test("StringType and TimeType => TimeType (default)") {
    withSQLConf(SQLConf.LEGACY_CAST_DATETIME_TO_STRING.key -> "false") {
      val result1 = TypeCoercion.findCommonTypeForBinaryComparison(StringType, TimeType, conf)
      val result2 = TypeCoercion.findCommonTypeForBinaryComparison(TimeType, StringType, conf)

      assert(result1.contains(TimeType))
      assert(result2.contains(TimeType))
    }
  }

  test("StringType and TimeType => StringType (legacy config)") {
    withSQLConf(SQLConf.LEGACY_CAST_DATETIME_TO_STRING.key -> "true") {
      val result = TypeCoercion.findCommonTypeForBinaryComparison(StringType, TimeType, conf)
      assert(result.contains(StringType))
    }
  }

  test("TimeType and TimestampType => TimestampType") {
    val result = TypeCoercion.findTightestCommonType(TimeType, TimestampType)
    assert(result.contains(TimestampType))
  }

  test("TimeType and TimestampNTZType => TimestampNTZType") {
    val result = TypeCoercion.findTightestCommonType(TimeType, TimestampNTZType)
    assert(result.contains(TimestampNTZType))
  }

  test("TimeType and DateType => TimestampType") {
    val result = TypeCoercion.findTightestCommonType(TimeType, DateType)
    assert(result.contains(TimestampType))
  }

  test("coercion symmetry for TimeType") {
    val pairs = Seq(
      (TimeType, StringType),
      (TimeType, TimestampType),
      (TimeType, DateType)
    )

    pairs.foreach { case (a, b) =>
      val r1 = TypeCoercion.findTightestCommonType(a, b)
      val r2 = TypeCoercion.findTightestCommonType(b, a)
      assert(r1 == r2, s"Mismatch for $a and $b")
    }
  }
}
