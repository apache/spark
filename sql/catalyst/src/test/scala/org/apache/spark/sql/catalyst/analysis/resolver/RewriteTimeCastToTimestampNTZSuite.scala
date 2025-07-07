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

package org.apache.spark.sql.catalyst.analysis.resolver

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions.{Cast, CurrentDate, Literal, MakeTimestampNTZ}
import org.apache.spark.sql.types.{TimestampNTZType, TimeType}

class RewriteTimeCastToTimestampNTZSuite extends SparkFunSuite {

  test("SPARK-52617: rewrite TIME -> TIMESTAMP_NTZ cast to MakeTimestampNTZ") {
    // TIME: 15:30:00 -> seconds = 15*3600 + 30*60 = 55800
    val nanos = 55800L * 1_000_000_000L
    val timeLiteral = Literal(nanos, TimeType(6))

    val castExpr = Cast(timeLiteral, TimestampNTZType)
    val rewrittenExpr = RewriteTimeCastToTimestampNTZ.rewrite(castExpr)

    val expectedExpr = MakeTimestampNTZ(CurrentDate(), timeLiteral)

    assert(
      rewrittenExpr.semanticEquals(expectedExpr),
      s"""
         |Expected:
         |  $expectedExpr
         |But got:
         |  $rewrittenExpr
         |""".stripMargin)
  }

  test("should not rewrite non-time casts") {
    val literal = Literal(42)
    val castExpr = Cast(literal, TimestampNTZType)

    val rewrittenExpr = RewriteTimeCastToTimestampNTZ.rewrite(castExpr)
    assert(rewrittenExpr eq castExpr)
  }
}
