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
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{LeafNode, SparkPlan, UnaryNode}
import org.apache.spark.sql.types.StructType

/**
 * An interface for exchanges.
 */
abstract class Exchange extends UnaryNode {
  override def output: Seq[Attribute] = child.output
}

/**
 * A wrapper for reused exchange to have different output, because two exchanges which produce
 * logically identical output will have distinct sets of output attribute ids, so we need to
 * preserve the original ids because they're what downstream operators are expecting.
 */
case class ReusedExchange(override val output: Seq[Attribute], child: Exchange) extends LeafNode {

  override def sameResult(plan: SparkPlan): Boolean = {
    // Ignore this wrapper. `plan` could also be a ReusedExchange, so we reverse the order here.
    plan.sameResult(child)
  }

  def doExecute(): RDD[InternalRow] = {
    child.execute()
  }

  override protected[sql] def doExecuteBroadcast[T](): broadcast.Broadcast[T] = {
    child.executeBroadcast()
  }

  // Do not repeat the same tree in explain.
  override def treeChildren: Seq[SparkPlan] = Nil
}

/**
 * Find out duplicated exchanges in the spark plan, then use the same exchange for all the
 * references.
 */
private[sql] case class ReuseExchange(sqlContext: SQLContext) extends Rule[SparkPlan] {

  def apply(plan: SparkPlan): SparkPlan = {
    if (!sqlContext.conf.exchangeReuseEnabled) {
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
          ReusedExchange(exchange.output, samePlan.get)
        } else {
          sameSchema += exchange
          exchange
        }
    }
  }
}
