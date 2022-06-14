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
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.plans.{Inner, LeftOuter, LeftSemi, PlanTest}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Join, JoinHint, LocalRelation, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.internal.SQLConf

class ConvertInnerJoinToSemiJoinSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Convert inner join to semi", Once, ConvertInnerJoinToSemiJoin) :: Nil
  }

  private var autoBroadcastJoinThreshold: Long = _
  protected override def beforeAll(): Unit = {
    autoBroadcastJoinThreshold = SQLConf.get.autoBroadcastJoinThreshold
    conf.setConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD, -1L)
  }

  protected override def afterAll(): Unit = {
    conf.setConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD, autoBroadcastJoinThreshold)
  }

  test("Convert inner join to semi") {
    val table1 = LocalRelation('a.int, 'b.int)
    val table2 = LocalRelation('c.int, 'd.int, 'e.int)
    val agg = Aggregate(Seq('c, 'e), Seq('c, 'e), table2)
    val join = Join(table1, agg, Inner, Option('a <=> 'c && 'b <=> 'e), JoinHint.NONE)
    val project = Project(Seq('a, 'b), join)

    val optimized = Optimize.execute(project.analyze)

    val correctAnswer =
      Project(Seq('a, 'b),
        Join(table1,
          Project(Seq('c, 'e), table2),
          LeftSemi, Option('a <=> 'c && 'b <=> 'e), JoinHint.NONE)).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Convert inner join to semi - aggregate on the left side") {
    val table1 = LocalRelation('a.int, 'b.int)
    val table2 = LocalRelation('c.int, 'd.int, 'e.int)
    val agg = Aggregate(Seq('c, 'e), Seq('c, 'e), table2)
    val join = Join(agg, table1, Inner, Option('a <=> 'c && 'b <=> 'e), JoinHint.NONE)
    val project = Project(Seq('a, 'b), join)

    val optimized = Optimize.execute(project.analyze)

    val correctAnswer =
      Project(Seq('a, 'b),
        Join(table1,
          Project(Seq('c, 'e), table2),
          LeftSemi, Option('a <=> 'c && 'b <=> 'e), JoinHint.NONE)).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Should not convert inner join to semi - join cond miss some columns") {
    val table1 = LocalRelation('a.int, 'b.int)
    val table2 = LocalRelation('c.int, 'd.int)
    val agg = Aggregate(Seq('c, 'd), Seq('c, 'd), table2)
    val join = Join(table1, agg, Inner, Option('a <=> 'c), JoinHint.NONE)
    val project = Project(Seq('a, 'b), join)

    val optimized = Optimize.execute(project.analyze)
    comparePlans(optimized, project.analyze)
  }

  test("Should not Convert inner join to semi - doesn't produce all grouping columns") {
      val table1 = LocalRelation('a.int, 'b.int)
      val table2 = LocalRelation('c.int, 'd.int)
      val agg = Aggregate(Seq('c, 'd), Seq('c), table2)
      val join = Join(table1, agg, Inner, Option('a <=> 'c), JoinHint.NONE)
      val project = Project(Seq('a, 'b), join)

      val optimized = Optimize.execute(project.analyze)
      comparePlans(optimized, project.analyze)
  }

  test("Should not Convert inner join to semi - not an inner join") {
    val table1 = LocalRelation('a.int, 'b.int)
    val table2 = LocalRelation('c.int, 'd.int, 'e.int)
    val agg = Aggregate(Seq('c, 'e), Seq('c, 'e), table2)
    val join = Join(table1, agg, LeftOuter, Option('a <=> 'c && 'b <=> 'e), JoinHint.NONE)
    val project = Project(Seq('a, 'b), join)

    val optimized = Optimize.execute(project.analyze)
    comparePlans(optimized, project.analyze)
  }

  test("Should not Convert inner join to semi - agg size is too small") {
    conf.setConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD, 1000000L)
    val table1 = LocalRelation('a.int, 'b.int)
    val table2 = LocalRelation('c.int, 'd.int, 'e.int)
    val agg = Aggregate(Seq('c, 'e), Seq('c, 'e), table2)
    val join = Join(table1, agg, Inner, Option('a <=> 'c && 'b <=> 'e), JoinHint.NONE)
    val project = Project(Seq('a, 'b), join)

    val optimized = Optimize.execute(project.analyze)
    comparePlans(optimized, project.analyze)
  }
}
