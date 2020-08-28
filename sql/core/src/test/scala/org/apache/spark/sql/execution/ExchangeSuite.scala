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

import scala.util.Random

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Alias, Literal}
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, IdentityBroadcastMode, SinglePartition}
import org.apache.spark.sql.execution.exchange._
import org.apache.spark.sql.execution.joins.HashedRelationBroadcastMode
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.vectorized.ColumnarBatch

class RanColumnar extends RuntimeException
class RanRowBased extends RuntimeException

case class ColumnarExchange(child: SparkPlan) extends Exchange {

  override def supportsColumnar: Boolean = true

  override protected def doExecute(): RDD[InternalRow] = throw new RanRowBased

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = throw new RanColumnar
}

class ExchangeSuite extends SparkPlanTest with SharedSparkSession {
  import testImplicits._

  test("shuffling UnsafeRows in exchange") {
    val input = (1 to 1000).map(Tuple1.apply)
    checkAnswer(
      input.toDF(),
      plan => ShuffleExchangeExec(SinglePartition, plan),
      input.map(Row.fromTuple)
    )
  }

  test("BroadcastMode.canonicalized") {
    val mode1 = IdentityBroadcastMode
    val mode2 = HashedRelationBroadcastMode(Literal(1L) :: Nil)
    val mode3 = HashedRelationBroadcastMode(Literal("s") :: Nil)

    assert(mode1.canonicalized == mode1.canonicalized)
    assert(mode1.canonicalized != mode2.canonicalized)
    assert(mode2.canonicalized != mode1.canonicalized)
    assert(mode2.canonicalized == mode2.canonicalized)
    assert(mode2.canonicalized != mode3.canonicalized)
    assert(mode3.canonicalized == mode3.canonicalized)
  }

  test("BroadcastExchange same result") {
    val df = spark.range(10)
    val plan = df.queryExecution.executedPlan
    val output = plan.output
    assert(plan sameResult plan)

    val exchange1 = BroadcastExchangeExec(IdentityBroadcastMode, plan)
    val hashMode = HashedRelationBroadcastMode(output)
    val exchange2 = BroadcastExchangeExec(hashMode, plan)
    val hashMode2 =
      HashedRelationBroadcastMode(Alias(output.head, "id2")() :: Nil)
    val exchange3 = BroadcastExchangeExec(hashMode2, plan)
    val exchange4 = ReusedExchangeExec(output, exchange3)

    assert(exchange1 sameResult exchange1)
    assert(exchange2 sameResult exchange2)
    assert(exchange3 sameResult exchange3)
    assert(exchange4 sameResult exchange4)

    assert(!exchange1.sameResult(exchange2))
    assert(!exchange2.sameResult(exchange3))
    assert(exchange3.sameResult(exchange4))
    assert(exchange4 sameResult exchange3)
  }

  test("ShuffleExchange same result") {
    val df = spark.range(10)
    val plan = df.queryExecution.executedPlan
    val output = plan.output
    assert(plan sameResult plan)

    val part1 = HashPartitioning(output, 1)
    val exchange1 = ShuffleExchangeExec(part1, plan)
    val exchange2 = ShuffleExchangeExec(part1, plan)
    val part2 = HashPartitioning(output, 2)
    val exchange3 = ShuffleExchangeExec(part2, plan)
    val part3 = HashPartitioning(output ++ output, 2)
    val exchange4 = ShuffleExchangeExec(part3, plan)
    val exchange5 = ReusedExchangeExec(output, exchange4)

    assert(exchange1 sameResult exchange1)
    assert(exchange2 sameResult exchange2)
    assert(exchange3 sameResult exchange3)
    assert(exchange4 sameResult exchange4)
    assert(exchange5 sameResult exchange5)

    assert(exchange1 sameResult exchange2)
    assert(!exchange2.sameResult(exchange3))
    assert(!exchange3.sameResult(exchange4))
    assert(exchange4.sameResult(exchange5))
    assert(exchange5 sameResult exchange4)
  }

  test("Columnar exchange works") {
    val df = spark.range(10)
    val plan = df.queryExecution.executedPlan
    val exchange = ColumnarExchange(plan)
    val reused = ReusedExchangeExec(plan.output, exchange)

    assertThrows[RanColumnar](reused.executeColumnar())
  }

  test("SPARK-23207: Make repartition() generate consistent output") {
    def assertConsistency(ds: Dataset[java.lang.Long]): Unit = {
      ds.persist()

      val exchange = ds.mapPartitions { iter =>
        Random.shuffle(iter)
      }.repartition(111)
      val exchange2 = ds.repartition(111)

      assert(exchange.rdd.collectPartitions() === exchange2.rdd.collectPartitions())
    }

    withSQLConf(SQLConf.SORT_BEFORE_REPARTITION.key -> "true") {
      // repartition() should generate consistent output.
      assertConsistency(spark.range(10000))

      // case when input contains duplicated rows.
      assertConsistency(spark.range(10000).map(i => Random.nextInt(1000).toLong))
    }
  }

  test("SPARK-23614: Fix incorrect reuse exchange when caching is used") {
    val cached = spark.createDataset(Seq((1, 2, 3), (4, 5, 6))).cache()
    val projection1 = cached.select("_1", "_2").queryExecution.executedPlan
    val projection2 = cached.select("_1", "_3").queryExecution.executedPlan
    assert(!projection1.sameResult(projection2))
  }
}
