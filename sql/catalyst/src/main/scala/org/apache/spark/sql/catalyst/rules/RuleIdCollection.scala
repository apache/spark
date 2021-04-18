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

// Represent unique rule ids for rules that are invoked multiple times.
case class RuleId(id: Int) {
  // Currently, there are more than 128 but less than 192 rules needing an id. However, the
  // requirement can be relaxed when we have more such rules. Note that increasing the max id can
  // result in increased memory consumption from every TreeNode.
  require(id >= -1 && id < 192)
}

// Unknown rule id which does not prune tree traversals. It is used as the default rule id for
// tree transformation functions.
object UnknownRuleId extends RuleId(-1)

// A collection of rules that use rule ids to prune tree traversals.
object RuleIdCollection {

  // The rules listed here need a rule id. Typically, rules that are in a fixed point batch or
  // invoked multiple times by Analyzer/Optimizer/Planner need a rule id to prune unnecessary
  // tree traversals in the transform function family. Note that those rules should not depend on
  // a changing, external state. Rules here are in alphabetical order.
  private val rulesNeedingIds: Seq[String] = {
      // Catalyst Analyzer rules
      "org.apache.spark.sql.catalyst.analysis.Analyzer$ResolveNaturalAndUsingJoin" ::
      "org.apache.spark.sql.catalyst.analysis.Analyzer$ResolveRandomSeed" ::
      "org.apache.spark.sql.catalyst.analysis.Analyzer$ResolveWindowFrame" ::
      "org.apache.spark.sql.catalyst.analysis.Analyzer$ResolveWindowOrder" ::
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
  private val ruleToId = new mutable.HashMap[String, RuleId]

  // The total number of rules with ids.
  val NumRules: Int = {
    var id = 0
    rulesNeedingIds.foreach(ruleName => {
      ruleToId.put(ruleName, RuleId(id))
      id = id + 1
    })
    id
  }

  // Return the rule id for a rule name.
  def getRuleId(ruleName: String): RuleId = {
    val ruleIdOpt = ruleToId.get(ruleName)
    // Please add the rule name to `rulesWithIds` if rule id is not found.
    if (!ruleIdOpt.isDefined) {
      throw new NoSuchElementException(s"Rule id not found for $ruleName")
    }
    ruleIdOpt.get
  }
}
