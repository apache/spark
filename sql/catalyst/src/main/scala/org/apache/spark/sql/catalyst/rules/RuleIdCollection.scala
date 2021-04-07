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

package org.apache.spark.sql.catalyst.rules

import scala.collection.mutable

import org.apache.spark.internal.Logging

// A collection of rules that use rule ids to prune tree traversals.
object RuleIdCollection extends Logging {

  // The rules listed here need a rule id. Rules are in alphabetical order.
  private val rulesNeedingIds: Seq[String] = {
      // Catalyst Optimizer rules
      "org.apache.spark.sql.catalyst.optimizer.CostBasedJoinReorder" ::
      "org.apache.spark.sql.catalyst.optimizer.EliminateOuterJoin" ::
      "org.apache.spark.sql.catalyst.optimizer.OptimizeIn" ::
      "org.apache.spark.sql.catalyst.optimizer.PushDownLeftSemiAntiJoin" ::
      "org.apache.spark.sql.catalyst.optimizer.PushExtraPredicateThroughJoin" ::
      "org.apache.spark.sql.catalyst.optimizer.PushLeftSemiLeftAntiThroughJoin" ::
      "org.apache.spark.sql.catalyst.optimizer.ReorderJoin" :: Nil
  }

  // Maps rule names to ids. Rule ids are continuous natural numbers starting from 0.
  private val ruleToId = new mutable.HashMap[String, Int]

  // Maps rule ids to names. Rule ids are continuous natural numbers starting from 0.
  private val ruleIdToName = new mutable.HashMap[Int, String]

  // Unknown rule id which does not prune tree traversal. It is used as the default rule id for
  // tree transformation functions.
  val UnknownId: Int = -1

  // The total number of rules with ids.
  val NumRules: Int = {
    var ruleId = 0
    rulesNeedingIds.foreach(ruleName => {
      ruleToId.put(ruleName, ruleId)
      ruleIdToName.put(ruleId, ruleName)
      ruleId = ruleId + 1
    })
    assert(ruleId < 192) // The assertion can be relaxed when we have more rules.
    ruleId
  }

  // Return the rule Id for a rule name.
  def getRuleId(ruleName: String): Int = {
    val ruleIdOpt = ruleToId.get(ruleName)
    // Please add the rule name to `rulesWithIds` if this assert fails.
    assert(ruleIdOpt.isDefined, s"add $ruleName into `rulesNeedingIds`")
    ruleIdOpt.get
  }

  // Return the rule name from its id. It is for debugging purpose.
  def getRuleName(ruleId: Int): String = {
    val ruleNameOpt = ruleIdToName.get(ruleId)
    assert(ruleNameOpt.isDefined, s"rule id $ruleId does not exist")
    ruleNameOpt.get
  }
}
