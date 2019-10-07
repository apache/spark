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

import org.apache.spark.broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeMap, Expression, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Base class for operators that exchange data among multiple threads or processes.
 *
 * Exchanges are the key class of operators that enable parallelism. Although the implementation
 * differs significantly, the concept is similar to the exchange operator described in
 * "Volcano -- An Extensible and Parallel Query Evaluation System" by Goetz Graefe.
 */
abstract class Exchange extends UnaryExecNode {
  override def output: Seq[Attribute] = child.output

  override def stringArgs: Iterator[Any] = super.stringArgs ++ Iterator(s"[id=#$id]")
}

/**
 * A wrapper for reused exchange to have different output, because two exchanges which produce
 * logically identical output will have distinct sets of output attribute ids, so we need to
 * preserve the original ids because they're what downstream operators are expecting.
 */
case class ReusedExchangeExec(override val output: Seq[Attribute], child: Exchange)
  extends LeafExecNode {

  override def supportsColumnar: Boolean = child.supportsColumnar

  // Ignore this wrapper for canonicalizing.
  override def doCanonicalize(): SparkPlan = child.canonicalized

  def doExecute(): RDD[InternalRow] = {
    child.execute()
  }

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    child.executeColumnar()
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
    case e: Expression => updateAttr(e).asInstanceOf[Partitioning]
    case other => other
  }

  override def outputOrdering: Seq[SortOrder] = {
    child.outputOrdering.map(updateAttr(_).asInstanceOf[SortOrder])
  }

  override def verboseStringWithOperatorId(): String = {
    val cdgen = ExplainUtils.getCodegenId(this)
    val reuse_op_str = ExplainUtils.getOpId(child)
    s"""
       |(${ExplainUtils.getOpId(this)}) $nodeName ${cdgen} [Reuses operator id: $reuse_op_str]
       |Output : ${output}
     """.stripMargin
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
    // To avoid costly canonicalization of an exchange:
    // - we use its schema first to check if it can be replaced to a reused exchange at all
    // - we insert an exchange into the map of canonicalized plans only when at least 2 exchange
    //   have the same schema
    val exchanges = mutable.Map[StructType, (Exchange, mutable.Map[SparkPlan, Exchange])]()

    def reuse(plan: SparkPlan): SparkPlan = {
      plan.transformUp {
        case exchange: Exchange =>
          val (firstSameSchemaExchange, sameResultExchanges) =
            exchanges.getOrElseUpdate(exchange.schema, (exchange, mutable.Map()))
          if (firstSameSchemaExchange.ne(exchange)) {
            if (sameResultExchanges.isEmpty) {
              sameResultExchanges +=
                firstSameSchemaExchange.canonicalized -> firstSameSchemaExchange
            }
            val sameResultExchange =
              sameResultExchanges.getOrElseUpdate(exchange.canonicalized, exchange)
            if (sameResultExchange.ne(exchange)) {
              ReusedExchangeExec(exchange.output, sameResultExchange)
            } else {
              exchange
            }
          } else {
            exchange
          }
        case other => other.transformExpressions {
          case sub: ExecSubqueryExpression =>
            sub.withNewPlan(reuse(sub.plan).asInstanceOf[BaseSubqueryExec])
        }
      }
    }

    reuse(plan)
  }
}
