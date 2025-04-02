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
import org.apache.spark.sql.catalyst.expressions.{Alias, CurrentDate, CurrentTimestamp, CurrentTimeZone, Expression, InSubquery, ListQuery, Literal, LocalTimestamp, Now}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LocalRelation, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.internal.SQLConf
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

  test("analyzer should respect time flow in current timestamp calls") {
    val in = Project(Alias(CurrentTimestamp(), "t1")() :: Nil, LocalRelation())

    val planT1 = Optimize.execute(in.analyze).asInstanceOf[Project]
    sleep(1)
    val planT2 = Optimize.execute(in.analyze).asInstanceOf[Project]

    val t1 = DateTimeUtils.microsToMillis(literals[Long](planT1)(0))
    val t2 = DateTimeUtils.microsToMillis(literals[Long](planT2)(0))

    assert(t2 - t1 <= 1000 && t2 - t1 > 0)
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

    val numTimezones = ZoneId.SHORT_IDS.size
    checkLiterals({ _: String => CurrentTimestamp() }, 1)
    checkLiterals({ zoneId: String => LocalTimestamp(Some(zoneId)) }, numTimezones)
    checkLiterals({ _: String => Now() }, 1)
    checkLiterals({ zoneId: String => CurrentDate(Some(zoneId)) }, numTimezones)
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
}
