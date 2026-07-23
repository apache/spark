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

package org.apache.spark.sql.catalyst.optimizer

import java.lang.Thread.sleep
import java.time.{LocalDateTime, ZoneId}

import scala.concurrent.duration._
import scala.jdk.CollectionConverters.MapHasAsScala

import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.{Add, Alias, Cast, CurrentDate, CurrentTime, CurrentTimestamp, CurrentTimeZone, Expression, InSubquery, ListQuery, Literal, LocalTimestamp, Now}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LocalRelation, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.catalyst.trees.TreePattern
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, TimestampLTZNanosType, TimestampNTZNanosType, TimestampNTZType, TimestampType, TimeType}
import org.apache.spark.unsafe.types.UTF8String

class ComputeCurrentTimeSuite extends PlanTest {
  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches = Seq(Batch("ComputeCurrentTime", Once, ComputeCurrentTime))
  }

  test("analyzer should replace current_timestamp with literals") {
    val in = Project(Seq(Alias(CurrentTimestamp(), "a")(), Alias(CurrentTimestamp(), "b")()),
      LocalRelation())

    val min = System.currentTimeMillis() * 1000
    val plan = Optimize.execute(in.analyze).asInstanceOf[Project]
    val max = (System.currentTimeMillis() + 1) * 1000

    val lits = literals[Long](plan)
    assert(lits.size == 2)
    assert(lits(0) >= min && lits(0) <= max)
    assert(lits(1) >= min && lits(1) <= max)
    assert(lits(0) == lits(1))
  }

  test("analyzer should replace current_time with literals") {
    // logical plan that calls current_time() twice in the Project
    val planInput = Project(
      Seq(
        Alias(CurrentTime(Literal(3)), "a")(),
        Alias(CurrentTime(Literal(3)), "b")()
      ),
      LocalRelation()
    )

    val analyzed = planInput.analyze
    val optimized = Optimize.execute(analyzed).asInstanceOf[Project]

    // We expect 2 literals in the final Project. Each literal is a Long
    // representing microseconds since midnight, truncated to precision=3.
    val lits = literals[Long](optimized)  // a helper that extracts all Literal values of type Long
    assert(lits.size == 2, s"Expected two literal values, found ${lits.size}")

    // The rule should produce the same microsecond value for both columns "a" and "b".
    assert(lits(0) == lits(1),
      s"Expected both current_time(3) calls to yield the same literal, " +
        s"but got ${lits(0)} vs ${lits(1)}")
  }

  test("analyzer should replace current_time with foldable child expressions") {
    // We build a plan that calls current_time(2 + 1) twice
    val foldableExpr = Add(Literal(2), Literal(1))  // a foldable arithmetic expression => 3
    val planInput = Project(
      Seq(
        Alias(CurrentTime(foldableExpr), "a")(),
        Alias(CurrentTime(foldableExpr), "b")()
      ),
      LocalRelation()
    )

    val analyzed = planInput.analyze
    val optimized = Optimize.execute(analyzed).asInstanceOf[Project]

    // We expect the optimizer to replace current_time(2 + 1) with a literal time value,
    // so let's extract those literal values.
    val lits = literals[Long](optimized)
    assert(lits.size == 2, s"Expected two literal values, found ${lits.size}")

    // Both references to current_time(2 + 1) should be replaced by the same microsecond-of-day
    assert(lits(0) == lits(1),
      s"Expected both current_time(2 + 1) calls to yield the same literal, " +
        s"but got ${lits(0)} vs. ${lits(1)}"
    )
  }

  test("analyzer should replace current_time with foldable casted string-literal") {
    // We'll build a foldable cast expression: CAST(' 0005 ' AS INT) => 5
    val castExpr = Cast(Literal(" 0005 "), IntegerType)

    // Two references to current_time(castExpr) => so we can check they're replaced consistently
    val planInput = Project(
      Seq(
        Alias(CurrentTime(castExpr), "a")(),
        Alias(CurrentTime(castExpr), "b")()
      ),
      LocalRelation()
    )

    val analyzed = planInput.analyze
    val optimized = Optimize.execute(analyzed).asInstanceOf[Project]

    val lits = literals[Long](optimized)
    assert(lits.size == 2, s"Expected two literal values, found ${lits.size}")

    // Both references to current_time(CAST(' 0005 ' AS INT)) in the same query
    // should produce the same microsecond-of-day literal.
    assert(lits(0) == lits(1),
      s"Expected both references to yield the same literal, but got ${lits(0)} vs. ${lits(1)}"
    )
  }


  test("analyzer should respect time flow in current timestamp calls") {
    val in = Project(Alias(CurrentTimestamp(), "t1")() :: Nil, LocalRelation())

    val planT1 = Optimize.execute(in.analyze).asInstanceOf[Project]
    sleep(1)
    val planT2 = Optimize.execute(in.analyze).asInstanceOf[Project]

    val t1 = DateTimeUtils.microsToMillis(literals[Long](planT1)(0))
    val t2 = DateTimeUtils.microsToMillis(literals[Long](planT2)(0))

    assert(t2 - t1 <= 1000 && t2 - t1 > 0)
  }

  test("analyzer should respect time flow in current_time calls") {
    val in = Project(Alias(CurrentTime(Literal(4)), "t1")() :: Nil, LocalRelation())

    val planT1 = Optimize.execute(in.analyze).asInstanceOf[Project]
    sleep(5)
    val planT2 = Optimize.execute(in.analyze).asInstanceOf[Project]

    val t1 = literals[Long](planT1)(0) // the microseconds-of-day for planT1
    val t2 = literals[Long](planT2)(0) // the microseconds-of-day for planT2

    assert(t2 > t1, s"Expected a newer time in the second analysis, but got t1=$t1, t2=$t2")
  }


  test("analyzer should replace current_date with literals") {
    val in = Project(Seq(Alias(CurrentDate(), "a")(), Alias(CurrentDate(), "b")()), LocalRelation())

    val min = DateTimeUtils.currentDate(ZoneId.systemDefault())
    val plan = Optimize.execute(in.analyze).asInstanceOf[Project]
    val max = DateTimeUtils.currentDate(ZoneId.systemDefault())

    val lits = literals[Int](plan)
    assert(lits.size == 2)
    assert(lits(0) >= min && lits(0) <= max)
    assert(lits(1) >= min && lits(1) <= max)
    assert(lits(0) == lits(1))
  }

  test("SPARK-33469: Add current_timezone function") {
    val in = Project(Seq(Alias(CurrentTimeZone(), "c")()), LocalRelation())
    val plan = Optimize.execute(in.analyze).asInstanceOf[Project]
    val lits = literals[UTF8String](plan)
    assert(lits.size == 1)
    assert(lits.head == UTF8String.fromString(SQLConf.get.sessionLocalTimeZone))
  }

  test("analyzer should replace localtimestamp with literals") {
    val in = Project(Seq(Alias(LocalTimestamp(), "a")(), Alias(LocalTimestamp(), "b")()),
      LocalRelation())

    val zoneId = DateTimeUtils.getZoneId(SQLConf.get.sessionLocalTimeZone)

    val min = DateTimeUtils.localDateTimeToMicros(LocalDateTime.now(zoneId))
    val plan = Optimize.execute(in.analyze).asInstanceOf[Project]
    val max = DateTimeUtils.localDateTimeToMicros(LocalDateTime.now(zoneId))

    val lits = literals[Long](plan)
    assert(lits.size == 2)
    assert(lits(0) >= min && lits(0) <= max)
    assert(lits(1) >= min && lits(1) <= max)
    assert(lits(0) == lits(1))
  }

  test("analyzer should use equal timestamps across subqueries") {
    val timestampInSubQuery = Project(Seq(Alias(LocalTimestamp(), "timestamp1")()), LocalRelation())
    val listSubQuery = ListQuery(timestampInSubQuery)
    val valueSearchedInSubQuery = Seq(Alias(LocalTimestamp(), "timestamp2")())
    val inFilterWithSubQuery = InSubquery(valueSearchedInSubQuery, listSubQuery)
    val input = Project(Nil, Filter(inFilterWithSubQuery, LocalRelation()))

    val plan = Optimize.execute(input.analyze).asInstanceOf[Project]

    val lits = literals[Long](plan)
    assert(lits.size == 3) // transformDownWithSubqueries covers the inner timestamp twice
    assert(lits.toSet.size == 1)
  }

  test("analyzer should use consistent timestamps for different timezones") {
    val localTimestamps = ZoneId.SHORT_IDS.asScala
      .map { case (zoneId, _) => Alias(LocalTimestamp(Some(zoneId)), zoneId)() }.toSeq
    val input = Project(localTimestamps, LocalRelation())

    val plan = Optimize.execute(input).asInstanceOf[Project]

    val lits = literals[Long](plan)
    assert(lits.size === localTimestamps.size)
    // there are timezones with a 30 or 45 minute offset
    val offsetsFromQuarterHour = lits.map( _ % Duration(15, MINUTES).toMicros).toSet
    assert(offsetsFromQuarterHour.size == 1)
  }

  test("analyzer should use consistent timestamps for different timestamp functions") {
    val differentTimestamps = Seq(
      Alias(CurrentTimestamp(), "currentTimestamp")(),
      Alias(Now(), "now")(),
      Alias(LocalTimestamp(Some("PLT")), "localTimestampWithTimezone")()
    )
    val input = Project(differentTimestamps, LocalRelation())

    val plan = Optimize.execute(input).asInstanceOf[Project]

    val lits = literals[Long](plan)
    assert(lits.size === differentTimestamps.size)
    // there are timezones with a 30 or 45 minute offset
    val offsetsFromQuarterHour = lits.map( _ % Duration(15, MINUTES).toMicros).toSet
    assert(offsetsFromQuarterHour.size == 1)
  }

  test("No duplicate literals") {
    def checkLiterals(f: (String) => Expression, expected: Int): Unit = {
      val timestamps = ZoneId.SHORT_IDS.asScala.flatMap { case (zoneId, _) =>
        // Request each timestamp multiple times.
        (1 to 5).map { _ => Alias(f(zoneId), zoneId)() }
      }.toSeq

      val input = Project(timestamps, LocalRelation())
      val plan = Optimize.execute(input).asInstanceOf[Project]

      val uniqueLiteralObjectIds = new scala.collection.mutable.HashSet[Int]
      plan.transformWithSubqueries { case subQuery =>
        subQuery.transformAllExpressions { case literal: Literal =>
          uniqueLiteralObjectIds += System.identityHashCode(literal)
          literal
        }
      }

      assert(expected === uniqueLiteralObjectIds.size)
    }

    // Use unique ZoneIds count instead of SHORT_IDS.size because some short zone IDs
    // may map to the same ZoneId (e.g., in Java 25, MST and PNT both map to America/Phoenix)
    val numUniqueZoneIds = ZoneId.SHORT_IDS.asScala.map { case (zoneId, _) =>
      ZoneId.of(zoneId, ZoneId.SHORT_IDS)
    }.toSet.size
    checkLiterals({ _: String => CurrentTimestamp() }, 1)
    checkLiterals({ zoneId: String => LocalTimestamp(Some(zoneId)) }, numUniqueZoneIds)
    checkLiterals({ _: String => Now() }, 1)
    checkLiterals({ zoneId: String => CurrentDate(Some(zoneId)) }, numUniqueZoneIds)
  }

  test("CAST(time AS TIMESTAMP_NTZ) is stabilized with the query current date") {
    val timeLit = Literal(0L, TimeType(6))
    val in = Project(Seq(
      Alias(Cast(timeLit, TimestampNTZType), "a")(),
      Alias(Cast(timeLit, TimestampNTZNanosType(9)), "b")(),
      Alias(CurrentDate(), "c")()), LocalRelation())

    val min = DateTimeUtils.currentDate(ZoneId.systemDefault())
    val plan = Optimize.execute(in.analyze).asInstanceOf[Project]
    val max = DateTimeUtils.currentDate(ZoneId.systemDefault())

    // The two casts and current_date() must all be anchored to the same current-date literal.
    val dateLits = dateLiterals(plan)
    assert(dateLits.size == 3)
    assert(dateLits.toSet.size == 1)
    assert(dateLits.forall(d => d >= min && d <= max))

    // The TIME -> TIMESTAMP_NTZ casts must be rewritten away (replaced by a date+time builder).
    val remainingCasts = plan.flatMap(_.expressions.flatMap(_.collect {
      case c: Cast if Cast.isTimeToTimestampNTZ(c.child.dataType, c.dataType) => c
    }))
    assert(remainingCasts.isEmpty)
  }

  test("CAST(time AS TIMESTAMP_LTZ) is stabilized with the query current date") {
    val timeLit = Literal(0L, TimeType(6))
    val in = Project(Seq(
      Alias(Cast(timeLit, TimestampType), "a")(),
      Alias(Cast(timeLit, TimestampLTZNanosType(9)), "b")(),
      Alias(CurrentDate(), "c")()), LocalRelation())

    val min = DateTimeUtils.currentDate(ZoneId.systemDefault())
    val plan = Optimize.execute(in.analyze).asInstanceOf[Project]
    val max = DateTimeUtils.currentDate(ZoneId.systemDefault())

    // The two casts and current_date() must all be anchored to the same current-date literal.
    val dateLits = dateLiterals(plan)
    assert(dateLits.size == 3)
    assert(dateLits.toSet.size == 1)
    assert(dateLits.forall(d => d >= min && d <= max))

    // The TIME -> TIMESTAMP_LTZ casts must be rewritten away (replaced by a date+time builder).
    val remainingCasts = plan.flatMap(_.expressions.flatMap(_.collect {
      case c: Cast if Cast.isTimeToTimestampLTZ(c.child.dataType, c.dataType) => c
    }))
    assert(remainingCasts.isEmpty)
  }

  private def dateLiterals(plan: LogicalPlan): scala.collection.mutable.ArrayBuffer[Int] = {
    val buf = new scala.collection.mutable.ArrayBuffer[Int]
    plan.transformWithSubqueries { case subQuery =>
      subQuery.transformAllExpressions { case lit: Literal if lit.dataType == DateType =>
        buf += lit.value.asInstanceOf[Int]
        lit
      }
    }
    buf
  }

  private def literals[T](plan: LogicalPlan): scala.collection.mutable.ArrayBuffer[T] = {
    val literals = new scala.collection.mutable.ArrayBuffer[T]
    plan.transformWithSubqueries { case subQuery =>
      subQuery.transformAllExpressions { case expression: Literal =>
        literals += expression.value.asInstanceOf[T]
        expression
      }
    }
    literals
  }

  test("SPARK-57748: TIME->TIMESTAMP cast is rewritten even with no CURRENT_LIKE node") {
    val timeLit = Literal(0L, TimeType(6))
    Seq(TimestampNTZType, TimestampType).foreach { target =>
      val in = Project(Seq(Alias(Cast(timeLit, target), "a")()), LocalRelation())
      val plan = Optimize.execute(in.analyze).asInstanceOf[Project]
      val remaining = plan.expressions.flatMap(_.collect {
        case c: Cast if Cast.isTimeToTimestampNTZ(c.child.dataType, c.dataType)
                     || Cast.isTimeToTimestampLTZ(c.child.dataType, c.dataType) => c
      })
      assert(remaining.isEmpty,
        s"TIME->$target cast should be rewritten with no CURRENT_LIKE present")
    }
  }

  test("SPARK-57748: CAST_TO_TIMESTAMP tree pattern is set for NTZ target types") {
    // Cast with TimestampNTZType target should contain CAST_TO_TIMESTAMP
    val ntzCast = Cast(Literal(0L, TimeType(6)), TimestampNTZType)
    assert(ntzCast.containsPattern(TreePattern.CAST_TO_TIMESTAMP))
    assert(ntzCast.containsPattern(TreePattern.CAST)) // existing CAST tag preserved

    // Cast with TimestampNTZNanosType target should also contain CAST_TO_TIMESTAMP
    val ntzNanosCast = Cast(Literal(0L, TimeType(6)), TimestampNTZNanosType(9))
    assert(ntzNanosCast.containsPattern(TreePattern.CAST_TO_TIMESTAMP))
    assert(ntzNanosCast.containsPattern(TreePattern.CAST))
  }

  test("SPARK-57748: CAST_TO_TIMESTAMP tree pattern is NOT set for non-timestamp targets") {
    // Cast to StringType should NOT contain CAST_TO_TIMESTAMP
    val stringCast = Cast(Literal(0L, TimeType(6)), StringType)
    assert(!stringCast.containsPattern(TreePattern.CAST_TO_TIMESTAMP))
    assert(stringCast.containsPattern(TreePattern.CAST))

    // Cast to IntegerType should NOT contain CAST_TO_TIMESTAMP
    val intCast = Cast(Literal("10"), IntegerType)
    assert(!intCast.containsPattern(TreePattern.CAST_TO_TIMESTAMP))
    assert(intCast.containsPattern(TreePattern.CAST))
  }

  test("SPARK-57748: CAST_TO_TIMESTAMP tree pattern is set for LTZ targets") {
    // Cast to TimestampType (LTZ micro) should contain CAST_TO_TIMESTAMP because
    // ComputeCurrentTime rewrites TIME->LTZ casts via the same predicate.
    val ltzCast = Cast(Literal(0L, TimeType(6)), TimestampType)
    assert(ltzCast.containsPattern(TreePattern.CAST_TO_TIMESTAMP))
    assert(ltzCast.containsPattern(TreePattern.CAST))

    // Cast to TimestampLTZNanosType should also contain CAST_TO_TIMESTAMP
    val ltzNanosCast = Cast(Literal(0L, TimeType(6)), TimestampLTZNanosType(9))
    assert(ltzNanosCast.containsPattern(TreePattern.CAST_TO_TIMESTAMP))
    assert(ltzNanosCast.containsPattern(TreePattern.CAST))
  }

  test("SPARK-57748: CAST_TO_TIMESTAMP is keyed on target type, not source type") {
    // Source type does not matter - only the target determines the pattern bit
    val fromString = Cast(Literal("2024-01-01"), TimestampNTZType)
    assert(fromString.containsPattern(TreePattern.CAST_TO_TIMESTAMP))

    val fromInt = Cast(Literal(42), TimestampNTZType)
    assert(fromInt.containsPattern(TreePattern.CAST_TO_TIMESTAMP))

    // Even with an expression child (rand()), the target type determines the bit
    import org.apache.spark.sql.catalyst.expressions.Rand
    val fromRand = Cast(Rand(Literal(0L)), TimestampNTZType)
    assert(fromRand.containsPattern(TreePattern.CAST_TO_TIMESTAMP))
  }

  test("SPARK-57748: plan with non-timestamp cast only does not contain CAST_TO_TIMESTAMP") {
    val timeLit = Literal(0L, TimeType(6))
    val plan = Project(Seq(
      Alias(Cast(timeLit, IntegerType), "a")()),
      LocalRelation())
    assert(!plan.containsPattern(TreePattern.CAST_TO_TIMESTAMP))
    assert(plan.containsPattern(TreePattern.CAST))
  }
}
