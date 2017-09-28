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

package org.apache.spark.sql.streaming

import java.util.UUID

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.{SparkPlan, SparkPlanTest, UnaryExecNode}
import org.apache.spark.sql.execution.exchange.{Exchange, ShuffleExchangeExec}
import org.apache.spark.sql.execution.streaming.{IncrementalExecution, OffsetSeqMetadata, StatefulOperator, StatefulOperatorStateInfo}
import org.apache.spark.sql.test.SharedSQLContext

class EnsureStatefulOpPartitioningSuite extends SparkPlanTest with SharedSQLContext {

  import testImplicits._

  private var baseDf: DataFrame = null

  override def beforeAll(): Unit = {
    super.beforeAll()
    baseDf = Seq((1, "A"), (2, "b")).toDF("num", "char")
  }

  test("ClusteredDistribution generates Exchange with HashPartitioning") {
    testEnsureStatefulOpPartitioning(
      baseDf.queryExecution.sparkPlan,
      requiredDistribution = keys => ClusteredDistribution(keys),
      expectedPartitioning =
        keys => HashPartitioning(keys, spark.sessionState.conf.numShufflePartitions),
      expectShuffle = true)
  }

  test("ClusteredDistribution with coalesce(1) generates Exchange with HashPartitioning") {
    testEnsureStatefulOpPartitioning(
      baseDf.coalesce(1).queryExecution.sparkPlan,
      requiredDistribution = keys => ClusteredDistribution(keys),
      expectedPartitioning =
        keys => HashPartitioning(keys, spark.sessionState.conf.numShufflePartitions),
      expectShuffle = true)
  }

  test("AllTuples generates Exchange with SinglePartition") {
    testEnsureStatefulOpPartitioning(
      baseDf.queryExecution.sparkPlan,
      requiredDistribution = _ => AllTuples,
      expectedPartitioning = _ => SinglePartition,
      expectShuffle = true)
  }

  test("AllTuples with coalesce(1) doesn't need Exchange") {
    testEnsureStatefulOpPartitioning(
      baseDf.coalesce(1).queryExecution.sparkPlan,
      requiredDistribution = _ => AllTuples,
      expectedPartitioning = _ => SinglePartition,
      expectShuffle = false)
  }

  /**
   * For `StatefulOperator` with the given `requiredChildDistribution`, and child SparkPlan
   * `inputPlan`, ensures that the incremental planner adds exchanges, if required, in order to
   * ensure the expected partitioning.
   */
  private def testEnsureStatefulOpPartitioning(
      inputPlan: SparkPlan,
      requiredDistribution: Seq[Attribute] => Distribution,
      expectedPartitioning: Seq[Attribute] => Partitioning,
      expectShuffle: Boolean): Unit = {
    val operator = TestStatefulOperator(inputPlan, requiredDistribution(inputPlan.output.take(1)))
    val executed = executePlan(operator, OutputMode.Complete())
    if (expectShuffle) {
      val exchange = executed.children.find(_.isInstanceOf[Exchange])
      if (exchange.isEmpty) {
        fail(s"Was expecting an exchange but didn't get one in:\n$executed")
      }
      assert(exchange.get ===
        ShuffleExchangeExec(expectedPartitioning(inputPlan.output.take(1)), inputPlan),
        s"Exchange didn't have expected properties:\n${exchange.get}")
    } else {
      assert(!executed.children.exists(_.isInstanceOf[Exchange]),
        s"Unexpected exchange found in:\n$executed")
    }
  }

  /** Executes a SparkPlan using the IncrementalPlanner used for Structured Streaming. */
  private def executePlan(
      p: SparkPlan,
      outputMode: OutputMode = OutputMode.Append()): SparkPlan = {
    val execution = new IncrementalExecution(
      spark,
      null,
      OutputMode.Complete(),
      "chk",
      UUID.randomUUID(),
      0L,
      OffsetSeqMetadata()) {
      override lazy val sparkPlan: SparkPlan = p transform {
        case plan: SparkPlan =>
          val inputMap = plan.children.flatMap(_.output).map(a => (a.name, a)).toMap
          plan transformExpressions {
            case UnresolvedAttribute(Seq(u)) =>
              inputMap.getOrElse(u,
                sys.error(s"Invalid Test: Cannot resolve $u given input $inputMap"))
          }
      }
    }
    execution.executedPlan
  }
}

/** Used to emulate a `StatefulOperator` with the given requiredDistribution. */
case class TestStatefulOperator(
    child: SparkPlan,
    requiredDist: Distribution) extends UnaryExecNode with StatefulOperator {
  override def output: Seq[Attribute] = child.output
  override def doExecute(): RDD[InternalRow] = child.execute()
  override def requiredChildDistribution: Seq[Distribution] = requiredDist :: Nil
  override def stateInfo: Option[StatefulOperatorStateInfo] = None
}
