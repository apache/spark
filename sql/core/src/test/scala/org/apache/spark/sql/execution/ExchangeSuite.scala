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

package org.apache.spark.sql.execution

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.{Alias, Literal}
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, IdentityBroadcastMode, SinglePartition}
import org.apache.spark.sql.execution.exchange.{BroadcastExchange, ReusedExchange, ShuffleExchange}
import org.apache.spark.sql.execution.joins.HashedRelationBroadcastMode
import org.apache.spark.sql.test.SharedSQLContext

class ExchangeSuite extends SparkPlanTest with SharedSQLContext {
  import testImplicits.localSeqToDataFrameHolder

  test("shuffling UnsafeRows in exchange") {
    val input = (1 to 1000).map(Tuple1.apply)
    checkAnswer(
      input.toDF(),
      plan => ShuffleExchange(SinglePartition, plan),
      input.map(Row.fromTuple)
    )
  }

  test("compatible BroadcastMode") {
    val mode1 = IdentityBroadcastMode
    val mode2 = HashedRelationBroadcastMode(true, Literal(1) :: Nil, Seq())
    val mode3 = HashedRelationBroadcastMode(false, Literal("s") :: Nil, Seq())

    assert(mode1.compatibleWith(mode1))
    assert(!mode1.compatibleWith(mode2))
    assert(!mode2.compatibleWith(mode1))
    assert(mode2.compatibleWith(mode2))
    assert(!mode2.compatibleWith(mode3))
    assert(mode3.compatibleWith(mode3))
  }

  test("BroadcastExchange same result") {
    val df = sqlContext.range(10)
    val plan = df.queryExecution.executedPlan
    val output = plan.output
    assert(plan sameResult plan)

    val exchange1 = BroadcastExchange(IdentityBroadcastMode, plan)
    val hashMode = HashedRelationBroadcastMode(true, output, plan.output)
    val exchange2 = BroadcastExchange(hashMode, plan)
    val hashMode2 =
      HashedRelationBroadcastMode(true, Alias(output.head, "id2")() :: Nil, plan.output)
    val exchange3 = BroadcastExchange(hashMode2, plan)
    val exchange4 = ReusedExchange(output, exchange3)

    assert(exchange1 sameResult exchange1)
    assert(exchange2 sameResult exchange2)
    assert(exchange3 sameResult exchange3)
    assert(exchange4 sameResult exchange4)

    assert(!exchange1.sameResult(exchange2))
    assert(!exchange2.sameResult(exchange3))
    assert(!exchange3.sameResult(exchange4))
    assert(exchange4 sameResult exchange3)
  }

  test("ShuffleExchange same result") {
    val df = sqlContext.range(10)
    val plan = df.queryExecution.executedPlan
    val output = plan.output
    assert(plan sameResult plan)

    val part1 = HashPartitioning(output, 1)
    val exchange1 = ShuffleExchange(part1, plan)
    val exchange2 = ShuffleExchange(part1, plan)
    val part2 = HashPartitioning(output, 2)
    val exchange3 = ShuffleExchange(part2, plan)
    val part3 = HashPartitioning(output ++ output, 2)
    val exchange4 = ShuffleExchange(part3, plan)
    val exchange5 = ReusedExchange(output, exchange4)

    assert(exchange1 sameResult exchange1)
    assert(exchange2 sameResult exchange2)
    assert(exchange3 sameResult exchange3)
    assert(exchange4 sameResult exchange4)
    assert(exchange5 sameResult exchange5)

    assert(exchange1 sameResult exchange2)
    assert(!exchange2.sameResult(exchange3))
    assert(!exchange3.sameResult(exchange4))
    assert(!exchange4.sameResult(exchange5))
    assert(exchange5 sameResult exchange4)
  }
}
