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

import java.time.{LocalDate, LocalDateTime}

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, Cast, EvalMode, Expression, ExpressionEvalHelper, Literal, ScalarSubquery}
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan, OneRowRelation, Project}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.catalyst.util.DateTimeTestUtils._
import org.apache.spark.sql.catalyst.util.DateTimeUtils._
import org.apache.spark.sql.catalyst.util.TimestampNanosTestUtils._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ArrayType, DataType, DateType, IntegerType, MapType, StringType, StructField, StructType, TimestampLTZNanosType, TimestampNTZNanosType, TimestampNTZType, TimestampType}
import org.apache.spark.unsafe.types.TimestampNanosVal

/**
 * Test suite for [[ResolveTimestampNanosCast]], which rewrites `DATE <-> nanos(p)` casts into a
 * two-step cast through the microsecond timestamp type.
 */
class ResolveTimestampNanosCastSuite extends AnalysisTest with ExpressionEvalHelper {

  private val ntzNanos = TimestampNTZNanosType(TimestampNTZNanosType.MAX_PRECISION)
  private val ltzNanos = TimestampLTZNanosType(TimestampLTZNanosType.MAX_PRECISION)

  private val ntzAttr = AttributeReference("ntz", ntzNanos)()
  private val ltzAttr = AttributeReference("ltz", ltzNanos)()
  private val dateAttr = AttributeReference("d", DateType)()

  // Complex-typed inputs that nest a nanos timestamp at various depths.
  private val arrNtzAttr = AttributeReference("arr_ntz", ArrayType(ntzNanos, containsNull = true))()
  private val arrDateAttr = AttributeReference("arr_d", ArrayType(DateType, containsNull = true))()
  private val mapNtzAttr =
    AttributeReference("map_ntz", MapType(StringType, ntzNanos, valueContainsNull = true))()
  private val structNtzAttr =
    AttributeReference("st_ntz", StructType(Seq(StructField("f", ntzNanos))))()
  private val structMixedAttr = AttributeReference(
    "st_mixed",
    StructType(Seq(StructField("a", ntzNanos), StructField("b", IntegerType))))()
  private val arrStructNtzAttr = AttributeReference(
    "arr_st_ntz",
    ArrayType(StructType(Seq(StructField("f", ntzNanos))), containsNull = true))()

  private val relation = LocalRelation(
    ntzAttr,
    ltzAttr,
    dateAttr,
    arrNtzAttr,
    arrDateAttr,
    mapNtzAttr,
    structNtzAttr,
    structMixedAttr,
    arrStructNtzAttr)

  // Rewrite only: keeps the original time zone id so the structure can be compared exactly.
  private object Rewrite extends RuleExecutor[LogicalPlan] {
    val batches = Batch("rewrite", FixedPoint(10), ResolveTimestampNanosCast) :: Nil
  }

  // Rewrite + time zone assignment, used to obtain a fully resolved, evaluable expression.
  private object Analyze extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("analyze", FixedPoint(10), ResolveTimeZone, ResolveTimestampNanosCast) :: Nil
  }

  private def micro(dt: DataType): DataType = dt match {
    case _: TimestampLTZNanosType => TimestampType
    case _: TimestampNTZNanosType => TimestampNTZType
  }

  private def checkRewrite(in: Expression, out: Expression): Unit = {
    comparePlans(Rewrite.execute(relation.select(in.as("c"))), relation.select(out.as("c")))
  }

  private def analyzeExpr(e: Expression): Expression = {
    Analyze.execute(OneRowRelation().select(e.as("c")))
      .asInstanceOf[Project].projectList.head.asInstanceOf[Alias].child
  }

  test("rewrite nanos(p) -> DATE through the microsecond timestamp type") {
    Seq(ntzAttr, ltzAttr).foreach { attr =>
      checkRewrite(
        Cast(attr, DateType),
        Cast(Cast(attr, micro(attr.dataType)), DateType))
    }
  }

  test("rewrite DATE -> nanos(p) through the microsecond timestamp type") {
    Seq(ntzNanos, ltzNanos).foreach { nanos =>
      checkRewrite(
        Cast(dateAttr, nanos),
        Cast(Cast(dateAttr, micro(nanos)), nanos))
    }
  }

  test("rewrite preserves timeZoneId and evalMode") {
    val tz = Option(LA.getId)
    Seq(EvalMode.LEGACY, EvalMode.ANSI, EvalMode.TRY).foreach { mode =>
      // nanos(p) -> DATE
      checkRewrite(
        Cast(ltzAttr, DateType, tz, mode),
        Cast(Cast(ltzAttr, TimestampType, tz, mode), DateType, tz, mode))
      // DATE -> nanos(p)
      checkRewrite(
        Cast(dateAttr, ltzNanos, tz, mode),
        Cast(Cast(dateAttr, TimestampType, tz, mode), ltzNanos, tz, mode))
    }
  }

  test("rewrite is idempotent") {
    val in = relation.select(Cast(ntzAttr, DateType).as("c"))
    val once = Rewrite.execute(in)
    comparePlans(Rewrite.execute(once), once)
  }

  test("rewrite nanos(p) <-> DATE nested in an array") {
    // ARRAY<nanos(p)> -> ARRAY<DATE>
    checkRewrite(
      Cast(arrNtzAttr, ArrayType(DateType, containsNull = true)),
      Cast(
        Cast(arrNtzAttr, ArrayType(TimestampNTZType, containsNull = true)),
        ArrayType(DateType, containsNull = true)))
    // ARRAY<DATE> -> ARRAY<nanos(p)>
    checkRewrite(
      Cast(arrDateAttr, ArrayType(ntzNanos, containsNull = true)),
      Cast(
        Cast(arrDateAttr, ArrayType(TimestampNTZType, containsNull = true)),
        ArrayType(ntzNanos, containsNull = true)))
  }

  test("rewrite nanos(p) -> DATE nested in a map value") {
    checkRewrite(
      Cast(mapNtzAttr, MapType(StringType, DateType, valueContainsNull = true)),
      Cast(
        Cast(mapNtzAttr, MapType(StringType, TimestampNTZType, valueContainsNull = true)),
        MapType(StringType, DateType, valueContainsNull = true)))
  }

  test("rewrite nanos(p) -> DATE nested in a struct field") {
    checkRewrite(
      Cast(structNtzAttr, StructType(Seq(StructField("f", DateType)))),
      Cast(
        Cast(structNtzAttr, StructType(Seq(StructField("f", TimestampNTZType)))),
        StructType(Seq(StructField("f", DateType)))))
  }

  test("rewrite bridges only the nanos field of a mixed struct") {
    // The non-nanos field (b: INT) is left untouched in the intermediate type.
    checkRewrite(
      Cast(structMixedAttr,
        StructType(Seq(StructField("a", DateType), StructField("b", IntegerType)))),
      Cast(
        Cast(structMixedAttr,
          StructType(Seq(StructField("a", TimestampNTZType), StructField("b", IntegerType)))),
        StructType(Seq(StructField("a", DateType), StructField("b", IntegerType)))))
  }

  test("rewrite reaches a nanos(p) -> DATE pair nested two levels deep") {
    // ARRAY<STRUCT<f: nanos(p)>> -> ARRAY<STRUCT<f: DATE>>
    val to = ArrayType(StructType(Seq(StructField("f", DateType))), containsNull = true)
    val mid = ArrayType(StructType(Seq(StructField("f", TimestampNTZType))), containsNull = true)
    checkRewrite(
      Cast(arrStructNtzAttr, to),
      Cast(Cast(arrStructNtzAttr, mid), to))
  }

  test("no rewrite when a complex cast has no DATE <-> nanos pair") {
    // ARRAY<nanos(p)> -> ARRAY<nanos(p)> involves no DATE side: left as-is.
    val same = Cast(arrNtzAttr, ArrayType(ntzNanos, containsNull = true))
    checkRewrite(same, same)
  }

  test("complex rewrite is idempotent") {
    val in = relation.select(
      Cast(arrNtzAttr, ArrayType(DateType, containsNull = true)).as("c"))
    val once = Rewrite.execute(in)
    comparePlans(Rewrite.execute(once), once)
  }

  test("rewrite reaches casts inside a subquery") {
    withSQLConf(SQLConf.TIMESTAMP_NANOS_TYPES_ENABLED.key -> "true") {
      val inner = LocalRelation(ntzAttr).select(Cast(ntzAttr, DateType).as("d"))
      val outer = OneRowRelation().select(ScalarSubquery(inner).as("s"))
      assertAnalysisSuccess(outer)
    }
  }

  test("DATE <-> TIMESTAMP_NTZ(p): values on the UTC wall-clock grid") {
    withSQLConf(
      SQLConf.TIMESTAMP_NANOS_TYPES_ENABLED.key -> "true",
      SQLConf.SESSION_LOCAL_TIMEZONE.key -> LA.getId) {
      val date = LocalDate.of(2020, 1, 1)
      val days = localDateToDays(date)
      foreachNanosPrecision { p =>
        // DATE -> TIMESTAMP_NTZ(p): midnight UTC, sub-microsecond part = 0 (zone-independent).
        checkEvaluation(
          analyzeExpr(Cast(Literal.create(date, DateType), TimestampNTZNanosType(p))),
          TimestampNanosVal.fromParts(daysToMicros(days, UTC), 0.toShort))
        // TIMESTAMP_NTZ(p) -> DATE: drops time-of-day and sub-microsecond digits.
        val noon = localDateTimeToNanosVal(LocalDateTime.of(2020, 1, 1, 12, 30, 15, 123456789))
        checkEvaluation(
          analyzeExpr(Cast(Literal.create(noon, TimestampNTZNanosType(p)), DateType)),
          days)
        // Nulls both directions.
        checkEvaluation(
          analyzeExpr(Cast(Literal.create(null, DateType), TimestampNTZNanosType(p))), null)
        checkEvaluation(
          analyzeExpr(Cast(Literal.create(null, TimestampNTZNanosType(p)), DateType)), null)
      }
    }
  }

  test("DATE <-> TIMESTAMP_LTZ(p): values resolve in the session time zone") {
    val date = LocalDate.of(2020, 1, 1)
    val days = localDateToDays(date)
    foreachNanosPrecision { p =>
      withSQLConf(
        SQLConf.TIMESTAMP_NANOS_TYPES_ENABLED.key -> "true",
        SQLConf.SESSION_LOCAL_TIMEZONE.key -> UTC.getId) {
        // DATE -> TIMESTAMP_LTZ(p): midnight of the date in the session zone (UTC here).
        checkEvaluation(
          analyzeExpr(Cast(Literal.create(date, DateType), TimestampLTZNanosType(p))),
          TimestampNanosVal.fromParts(daysToMicros(days, UTC), 0.toShort))
        // TIMESTAMP_LTZ(p) -> DATE: calendar date in the session zone.
        val noon = instantToNanosVal(timestampLTZ(2020, 1, 1, 12, 30, 15, 123456789))
        checkEvaluation(
          analyzeExpr(Cast(Literal.create(noon, TimestampLTZNanosType(p)), DateType)),
          days)
        // Nulls both directions.
        checkEvaluation(
          analyzeExpr(Cast(Literal.create(null, DateType), TimestampLTZNanosType(p))), null)
        checkEvaluation(
          analyzeExpr(Cast(Literal.create(null, TimestampLTZNanosType(p)), DateType)), null)
      }
      // Zone sensitivity: a western zone maps the date to a later midnight instant.
      withSQLConf(
        SQLConf.TIMESTAMP_NANOS_TYPES_ENABLED.key -> "true",
        SQLConf.SESSION_LOCAL_TIMEZONE.key -> LA.getId) {
        assert(daysToMicros(days, LA) != daysToMicros(days, UTC))
        checkEvaluation(
          analyzeExpr(Cast(Literal.create(date, DateType), TimestampLTZNanosType(p))),
          TimestampNanosVal.fromParts(daysToMicros(days, LA), 0.toShort))
      }
    }
  }
}
