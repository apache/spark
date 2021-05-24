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

import java.time.ZoneId

import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.{Alias, CurrentDate, CurrentTimestamp, CurrentTimeZone, Literal}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan, Project}
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

    val lits = new scala.collection.mutable.ArrayBuffer[Long]
    plan.transformAllExpressions { case e: Literal =>
      lits += e.value.asInstanceOf[Long]
      e
    }
    assert(lits.size == 2)
    assert(lits(0) >= min && lits(0) <= max)
    assert(lits(1) >= min && lits(1) <= max)
    assert(lits(0) == lits(1))
  }

  test("analyzer should replace current_date with literals") {
    val in = Project(Seq(Alias(CurrentDate(), "a")(), Alias(CurrentDate(), "b")()), LocalRelation())

    val min = DateTimeUtils.currentDate(ZoneId.systemDefault())
    val plan = Optimize.execute(in.analyze).asInstanceOf[Project]
    val max = DateTimeUtils.currentDate(ZoneId.systemDefault())

    val lits = new scala.collection.mutable.ArrayBuffer[Int]
    plan.transformAllExpressions { case e: Literal =>
      lits += e.value.asInstanceOf[Int]
      e
    }
    assert(lits.size == 2)
    assert(lits(0) >= min && lits(0) <= max)
    assert(lits(1) >= min && lits(1) <= max)
    assert(lits(0) == lits(1))
  }

  test("SPARK-33469: Add current_timezone function") {
    val in = Project(Seq(Alias(CurrentTimeZone(), "c")()), LocalRelation())
    val plan = Optimize.execute(in.analyze).asInstanceOf[Project]
    val lits = new scala.collection.mutable.ArrayBuffer[String]
    plan.transformAllExpressions { case e: Literal =>
      lits += e.value.asInstanceOf[UTF8String].toString
      e
    }
    assert(lits.size == 1)
    assert(lits.head == SQLConf.get.sessionLocalTimeZone)
  }
}
