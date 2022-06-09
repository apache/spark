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

import scala.concurrent.duration._

import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.{Alias, CurrentDate, CurrentTimestamp, InSubquery, ListQuery, Literal, Now}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LocalRelation, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.catalyst.util.DateTimeUtils

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

  test("analyzer should replace current_date with literals") {
    val in = Project(Seq(Alias(CurrentDate(), "a")(), Alias(CurrentDate(), "b")()), LocalRelation())

    val min = DateTimeUtils.millisToDays(System.currentTimeMillis())
    val plan = Optimize.execute(in.analyze).asInstanceOf[Project]
    val max = DateTimeUtils.millisToDays(System.currentTimeMillis())

    val lits = literals[Int](plan)
    assert(lits.size == 2)
    assert(lits(0) >= min && lits(0) <= max)
    assert(lits(1) >= min && lits(1) <= max)
    assert(lits(0) == lits(1))
  }

  test("analyzer should use equal timestamps across subqueries") {
    val timestampInSubQuery = Project(Seq(Alias(Now(), "timestamp1")()), LocalRelation())
    val listSubQuery = ListQuery(timestampInSubQuery)
    val valueSearchedInSubQuery = Seq(Alias(Now(), "timestamp2")())
    val inFilterWithSubQuery = InSubquery(valueSearchedInSubQuery, listSubQuery)
    val input = Project(Nil, Filter(inFilterWithSubQuery, LocalRelation()))

    val plan = Optimize.execute(input.analyze).asInstanceOf[Project]

    val lits = literals[Long](plan)
    assert(lits.size == 3) // transformDownWithSubqueries covers the inner timestamp twice
    assert(lits.toSet.size == 1)
  }

  test("analyzer should use consistent timestamps for different timestamp functions") {
    val differentTimestamps = Seq(
      Alias(CurrentTimestamp(), "currentTimestamp")(),
      Alias(Now(), "now")()
    )
    val input = Project(differentTimestamps, LocalRelation())

    val plan = Optimize.execute(input).asInstanceOf[Project]

    val lits = literals[Long](plan)
    assert(lits.size === differentTimestamps.size)
    // there are timezones with a 30 or 45 minute offset
    val offsetsFromQuarterHour = lits.map(_ % Duration(15, MINUTES).toMicros).toSet
    assert(offsetsFromQuarterHour.size == 1)
  }

  private def literals[T](plan: LogicalPlan): scala.collection.mutable.ArrayBuffer[T] = {
    val literals = new scala.collection.mutable.ArrayBuffer[T]
    plan.transformDownWithSubqueries { case subQuery =>
      subQuery.transformAllExpressions { case expression: Literal =>
        literals += expression.value.asInstanceOf[T]
        expression
      }
    }
    literals
  }
}
