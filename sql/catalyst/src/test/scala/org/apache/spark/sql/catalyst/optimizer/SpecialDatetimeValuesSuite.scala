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

import java.time.{Instant, LocalDate, LocalDateTime, LocalTime, ZoneId}

import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.{Alias, Cast, Literal}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.catalyst.util.DateTimeConstants.MICROS_PER_MINUTE
import org.apache.spark.sql.catalyst.util.DateTimeUtils.{instantToMicros, localDateTimeToMicros}
import org.apache.spark.sql.types.{AtomicType, DateType, TimestampNTZType, TimestampType}

class SpecialDatetimeValuesSuite extends PlanTest {
  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches = Seq(Batch("SpecialDatetimeValues", Once, SpecialDatetimeValues))
  }

  test("special date values") {
    testSpecialDatetimeValues { zoneId =>
      val expected = Set(
        LocalDate.ofEpochDay(0),
        LocalDate.now(zoneId),
        LocalDate.now(zoneId).minusDays(1),
        LocalDate.now(zoneId).plusDays(1)
      ).map(_.toEpochDay.toInt)
      val in = Project(Seq(
        Alias(Cast(Literal("epoch"), DateType, Some(zoneId.getId)), "epoch")(),
        Alias(Cast(Literal("today"), DateType, Some(zoneId.getId)), "today")(),
        Alias(Cast(Literal("yesterday"), DateType, Some(zoneId.getId)), "yesterday")(),
        Alias(Cast(Literal("tomorrow"), DateType, Some(zoneId.getId)), "tomorrow")()),
        LocalRelation())

      val plan = Optimize.execute(in.analyze).asInstanceOf[Project]
      val lits = new scala.collection.mutable.ArrayBuffer[Int]
      plan.transformAllExpressions { case e: Literal if e.dataType == DateType =>
        lits += e.value.asInstanceOf[Int]
        e
      }
      assert(expected === lits.toSet)
    }
  }

  private def testSpecialTs(tsType: AtomicType, expected: Set[Long], zoneId: ZoneId): Unit = {
    val in = Project(Seq(
      Alias(Cast(Literal("epoch"), tsType, Some(zoneId.getId)), "epoch")(),
      Alias(Cast(Literal("now"), tsType, Some(zoneId.getId)), "now")(),
      Alias(Cast(Literal("tomorrow"), tsType, Some(zoneId.getId)), "tomorrow")(),
      Alias(Cast(Literal("yesterday"), tsType, Some(zoneId.getId)), "yesterday")()),
      LocalRelation())

    val plan = Optimize.execute(in.analyze).asInstanceOf[Project]
    val lits = new scala.collection.mutable.ArrayBuffer[Long]
    plan.transformAllExpressions { case e: Literal if e.dataType == tsType =>
      lits += e.value.asInstanceOf[Long]
      e
    }
    assert(lits.forall(ts => expected.exists(ets => Math.abs(ets -ts) <= MICROS_PER_MINUTE)))
  }

  test("special timestamp_ltz values") {
    testSpecialDatetimeValues { zoneId =>
      val expected = Set(
        Instant.ofEpochSecond(0),
        Instant.now(),
        Instant.now().atZone(zoneId).`with`(LocalTime.MIDNIGHT).plusDays(1).toInstant,
        Instant.now().atZone(zoneId).`with`(LocalTime.MIDNIGHT).minusDays(1).toInstant
      ).map(instantToMicros)
      testSpecialTs(TimestampType, expected, zoneId)
    }
  }

  test("special timestamp_ntz values") {
    testSpecialDatetimeValues { zoneId =>
      val expected = Set(
        LocalDateTime.of(1970, 1, 1, 0, 0),
        LocalDateTime.now(zoneId),
        LocalDateTime.now(zoneId).`with`(LocalTime.MIDNIGHT).plusDays(1),
        LocalDateTime.now(zoneId).`with`(LocalTime.MIDNIGHT).minusDays(1)
      ).map(localDateTimeToMicros)
      testSpecialTs(TimestampNTZType, expected, zoneId)
    }
  }
}
