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

import org.apache.spark.annotation.{DeveloperApi, Experimental}
import org.apache.spark.sql.catalyst.SQLConfHelper

/**
 * Provide the functionality to modify the next plan fragment configs in AQE rules.
 * The configs will be cleanup before going to execute next plan fragment.
 * To get instance, use: {{{ AdaptiveRuleContext.get() }}}
 *
 * @param isSubquery if the input query plan is subquery
 * @param isFinalStage if the next stage is final stage
 */
@Experimental
@DeveloperApi
case class AdaptiveRuleContext(isSubquery: Boolean, isFinalStage: Boolean) {

  /**
   * Set SQL configs for next plan fragment. The configs will affect all of rules in AQE,
   * i.e., the runtime optimizer, planner, queryStagePreparationRules, queryStageOptimizerRules,
   * columnarRules.
   * This configs will be cleared before going to get the next plan fragment.
   */
  private val nextPlanFragmentConf = new mutable.HashMap[String, String]()

  private[sql] def withFinalStage(isFinalStage: Boolean): AdaptiveRuleContext = {
    if (this.isFinalStage == isFinalStage) {
      this
    } else {
      val newRuleContext = copy(isFinalStage = isFinalStage)
      newRuleContext.setConfigs(this.configs())
      newRuleContext
    }
  }

  def setConfig(key: String, value: String): Unit = {
    nextPlanFragmentConf.put(key, value)
  }

  def setConfigs(kvs: Map[String, String]): Unit = {
    kvs.foreach(kv => nextPlanFragmentConf.put(kv._1, kv._2))
  }

  private[sql] def configs(): Map[String, String] = nextPlanFragmentConf.toMap

  private[sql] def clearConfigs(): Unit = nextPlanFragmentConf.clear()
}

object AdaptiveRuleContext extends SQLConfHelper {
  private val ruleContextThreadLocal = new ThreadLocal[AdaptiveRuleContext]

  /**
   * If a rule is applied inside AQE then the returned value is always defined, else return None.
   */
  def get(): Option[AdaptiveRuleContext] = Option(ruleContextThreadLocal.get())

  private[sql] def withRuleContext[T](ruleContext: AdaptiveRuleContext)(block: => T): T = {
    assert(ruleContext != null)
    val origin = ruleContextThreadLocal.get()
    ruleContextThreadLocal.set(ruleContext)
    try {
      val conf = ruleContext.configs()
      withSQLConf(conf.toSeq: _*) {
        block
      }
    } finally {
      ruleContextThreadLocal.set(origin)
    }
  }
}
