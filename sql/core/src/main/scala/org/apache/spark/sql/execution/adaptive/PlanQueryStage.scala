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

package org.apache.spark.sql.execution.adaptive

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.command.ExecutedCommandExec
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, Exchange, ShuffleExchangeExec}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType

/**
 * Divide the spark plan into multiple QueryStages. For each Exchange in the plan, it adds a
 * QueryStage and a QueryStageInput. If reusing Exchange is enabled, it finds duplicated exchanges
 * and uses the same QueryStage for all the references.
 */
case class PlanQueryStage(conf: SQLConf) extends Rule[SparkPlan] {

  def apply(plan: SparkPlan): SparkPlan = {

    val newPlan = if (!conf.exchangeReuseEnabled) {
      plan.transformUp {
        case e: ShuffleExchangeExec =>
          ShuffleQueryStageInput(ShuffleQueryStage(e), e.output)
        case e: BroadcastExchangeExec =>
          BroadcastQueryStageInput(BroadcastQueryStage(e), e.output)
      }
    } else {
      // Build a hash map using schema of exchanges to avoid O(N*N) sameResult calls.
      val stages = mutable.HashMap[StructType, ArrayBuffer[QueryStage]]()

      plan.transformUp {
        case exchange: Exchange =>
          val sameSchema = stages.getOrElseUpdate(exchange.schema, ArrayBuffer[QueryStage]())
          val samePlan = sameSchema.find { s =>
            exchange.sameResult(s.child)
          }
          if (samePlan.isDefined) {
            // Keep the output of this exchange, the following plans require that to resolve
            // attributes.
            exchange match {
              case e: ShuffleExchangeExec => ShuffleQueryStageInput(
                samePlan.get.asInstanceOf[ShuffleQueryStage], exchange.output)
              case e: BroadcastExchangeExec => BroadcastQueryStageInput(
                samePlan.get.asInstanceOf[BroadcastQueryStage], exchange.output)
            }
          } else {
            val queryStageInput = exchange match {
              case e: ShuffleExchangeExec =>
                ShuffleQueryStageInput(ShuffleQueryStage(e), e.output)
              case e: BroadcastExchangeExec =>
                BroadcastQueryStageInput(BroadcastQueryStage(e), e.output)
            }
            sameSchema += queryStageInput.childStage
            queryStageInput
          }
      }
    }
    ResultQueryStage(newPlan)
  }
}
