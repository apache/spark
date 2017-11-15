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

package org.apache.spark.sql.execution.exchange

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeMap, Expression, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, Partitioning}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{LeafExecNode, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType

/**
 * Base class for operators that exchange data among multiple threads or processes.
 *
 * Exchanges are the key class of operators that enable parallelism. Although the implementation
 * differs significantly, the concept is similar to the exchange operator described in
 * "Volcano -- An Extensible and Parallel Query Evaluation System" by Goetz Graefe.
 */
abstract class Exchange extends UnaryExecNode {
  override def output: Seq[Attribute] = child.output
}

/**
 * A wrapper for reused exchange to have different output, because two exchanges which produce
 * logically identical output will have distinct sets of output attribute ids, so we need to
 * preserve the original ids because they're what downstream operators are expecting.
 */
case class ReusedExchangeExec(override val output: Seq[Attribute], child: Exchange)
  extends LeafExecNode {

  // Ignore this wrapper for canonicalizing.
  override def doCanonicalize(): SparkPlan = child.canonicalized

  def doExecute(): RDD[InternalRow] = {
    child.execute()
  }

  override protected[sql] def doExecuteBroadcast[T](): broadcast.Broadcast[T] = {
    child.executeBroadcast()
  }

  // `ReusedExchangeExec` can have distinct set of output attribute ids from its child, we need
  // to update the attribute ids in `outputPartitioning` and `outputOrdering`.
  private lazy val updateAttr: Expression => Expression = {
    val originalAttrToNewAttr = AttributeMap(child.output.zip(output))
    e => e.transform {
      case attr: Attribute => originalAttrToNewAttr.getOrElse(attr, attr)
    }
  }

  override def outputPartitioning: Partitioning = child.outputPartitioning match {
    case h: HashPartitioning => h.copy(expressions = h.expressions.map(updateAttr))
    case other => other
  }

  override def outputOrdering: Seq[SortOrder] = {
    child.outputOrdering.map(updateAttr(_).asInstanceOf[SortOrder])
  }
}

/**
 * Find out duplicated exchanges in the spark plan, then use the same exchange for all the
 * references.
 */
case class ReuseExchange(conf: SQLConf) extends Rule[SparkPlan] {

  def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.exchangeReuseEnabled) {
      return plan
    }
    // Build a hash map using schema of exchanges to avoid O(N*N) sameResult calls.
    val exchanges = mutable.HashMap[StructType, ArrayBuffer[Exchange]]()
    plan.transformUp {
      case exchange: Exchange =>
        // the exchanges that have same results usually also have same schemas (same column names).
        val sameSchema = exchanges.getOrElseUpdate(exchange.schema, ArrayBuffer[Exchange]())
        val samePlan = sameSchema.find { e =>
          exchange.sameResult(e)
        }
        if (samePlan.isDefined) {
          // Keep the output of this exchange, the following plans require that to resolve
          // attributes.
          ReusedExchangeExec(exchange.output, samePlan.get)
        } else {
          sameSchema += exchange
          exchange
        }
    }
  }
}

/**
 * Find out duplicated coordinated exchanges in the spark plan, then use the same exchange for all
 * the references.
 */
case class ReuseExchangeWithCoordinator(conf: SQLConf) extends Rule[SparkPlan] {

  // Returns true if a SparkPlan has coordinated ShuffleExchangeExec children.
  private def hasCoordinatedExchanges(plan: SparkPlan): Boolean = {
    plan.children.nonEmpty && plan.children.forall {
      case ShuffleExchangeExec(_, _, Some(_)) => true
      case _ => false
    }
  }

  // Returns true if two sequences of exchanges are producing the same results.
  private def hasExchangesWithSameResults(
      source: Seq[ShuffleExchangeExec],
      target: Seq[ShuffleExchangeExec]): Boolean = {
    source.length == target.length &&
      source.zip(target).forall(x => x._1.withoutCoordinator.sameResult(x._2.withoutCoordinator))
  }

  type CoordinatorSignature = (Int, Long, Option[Int])

  def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.exchangeReuseEnabled) {
      return plan
    }

    // Build a hash map using the properties of exchange coordinator to reduce the number of
    // sameResult calls.
    val exchangesMap =
      mutable.HashMap[CoordinatorSignature, ArrayBuffer[Seq[ShuffleExchangeExec]]]()

    plan.transformUp {
      case parentPlan if hasCoordinatedExchanges(parentPlan) =>
        val coordinator = parentPlan.children.collect {
          case ShuffleExchangeExec(_, _, Some(coordinator)) => coordinator
        }.head

        val coordinatorSignature = (coordinator.numExchanges,
          coordinator.advisoryTargetPostShuffleInputSize,
          coordinator.minNumPostShufflePartitions)

        val candidateExchanges = exchangesMap.getOrElseUpdate(coordinatorSignature,
          ArrayBuffer[Seq[ShuffleExchangeExec]]())
        val currentExchanges = parentPlan.children.asInstanceOf[Seq[ShuffleExchangeExec]]
        val sameExchanges = candidateExchanges.find { exchanges =>
          hasExchangesWithSameResults(currentExchanges, exchanges)
        }
        if (sameExchanges.isDefined) {
          val reusedExchanges = sameExchanges.get
          val newExchanges = parentPlan.children.zip(reusedExchanges).map {
            case (exchange, reused) => ReusedExchangeExec(exchange.output, reused)
          }
          parentPlan.withNewChildren(newExchanges)
        } else {
          candidateExchanges += currentExchanges
          parentPlan
        }
    }
  }
}
