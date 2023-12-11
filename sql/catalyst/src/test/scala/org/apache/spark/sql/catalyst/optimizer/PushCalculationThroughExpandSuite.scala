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

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.types.{IntegerType, StringType}

class PushCalculationThroughExpandSuite extends PlanTest {
  object Optimize extends RuleExecutor[LogicalPlan] {

    val batches =
      Batch("Filter Pushdown", FixedPoint(10),
        PushCalculationThroughExpand,
        CollapseProject) :: Nil
  }

  val a = $"a".int
  val b = $"b".string
  val c = $"c".int
  val d = $"d".int

  val testRelation = LocalRelation(a, b, c, d)


  lazy val gid = $"spark_grouping_id".long.withNullability(false)
  lazy val nulInt = Literal(null, IntegerType)
  lazy val nulStr = Literal(null, StringType)
  lazy val r1 = LocalRelation(a, b, c, d)

  test("rollup") {
    val originalPlan =
      Aggregate(Seq(a, b, gid), Seq(a, b, sum(c + d).as("sum(c + d)")),
        Expand(Seq(Seq(a, b, c, d, a, b, 0L),
          Seq(a, b, c, d, a, nulStr, 1L),
          Seq(a, b, c, d, nulInt, nulStr, 3L)),
          Seq(a, b, c, d, a, b, gid),
          Project(Seq(a, b, c, d, a.as("a"), b.as("b")), testRelation)
        )
      )

    val c_plus_d = c + d
    val alias = c_plus_d.as(c_plus_d.toString)
    val attr = alias.toAttribute
    val expectedPlan =
      Aggregate(Seq(a, b, gid), Seq(a, b, sum(attr).as("sum(c + d)")),
        Expand(Seq(Seq(a, b, c, d, a, b, 0L, attr),
          Seq(a, b, c, d, a, nulStr, 1L, attr),
          Seq(a, b, c, d, nulInt, nulStr, 3L, attr)),
          Seq(a, b, c, d, a, b, gid, attr),
          Project(Seq(a, b, c, d, a.as("a"), b.as("b"), alias), testRelation)
        )
      )

    comparePlans(Optimize.execute(originalPlan), expectedPlan)
  }
}
