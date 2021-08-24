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

import java.time.LocalDate

import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.{Alias, Cast, Literal}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.types.DateType

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
}
