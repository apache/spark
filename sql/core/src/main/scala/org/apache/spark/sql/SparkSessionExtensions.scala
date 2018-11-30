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

package org.apache.spark.sql

import scala.collection.mutable

import org.apache.spark.annotation.{DeveloperApi, Experimental, Unstable}
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.ExpressionInfo
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.{Order, Rule, RuleBatch, RuleInOrder}

/**
 * :: Experimental ::
 * Holder for injection points to the [[SparkSession]]. We make NO guarantee about the stability
 * regarding binary compatibility and source compatibility of methods here.
 *
 * This current provides the following extension points:
 *
 * <ul>
 * <li>Analyzer Rules.</li>
 * <li>Check Analysis Rules.</li>
 * <li>Optimizer Rules.</li>
 * <li>Planning Strategies.</li>
 * <li>Customized Parser.</li>
 * <li>(External) Catalog listeners.</li>
 * </ul>
 *
 * The extensions can be used by calling withExtension on the [[SparkSession.Builder]], for
 * example:
 * {{{
 *   SparkSession.builder()
 *     .master("...")
 *     .conf("...", true)
 *     .withExtensions { extensions =>
 *       extensions.injectResolutionRule { session =>
 *         ...
 *       }
 *       extensions.injectParser { (session, parser) =>
 *         ...
 *       }
 *     }
 *     .getOrCreate()
 * }}}
 *
 * Note that none of the injected builders should assume that the [[SparkSession]] is fully
 * initialized and should not touch the session's internals (e.g. the SessionState).
 */
@DeveloperApi
@Experimental
@Unstable
class SparkSessionExtensions {
  type RuleBuilder = SparkSession => Rule[LogicalPlan]
  type CheckRuleBuilder = SparkSession => LogicalPlan => Unit
  type StrategyBuilder = SparkSession => Strategy
  type ParserBuilder = (SparkSession, ParserInterface) => ParserInterface
  type FunctionDescription = (FunctionIdentifier, ExpressionInfo, FunctionBuilder)
  type Order = Order.Order

  private[this] val resolutionRuleBuilders = mutable.Buffer.empty[RuleBuilder]

  /**
   * Build the analyzer resolution `Rule`s using the given [[SparkSession]].
   */
  private[sql] def buildResolutionRules(session: SparkSession): Seq[Rule[LogicalPlan]] = {
    resolutionRuleBuilders.map(_.apply(session))
  }

  /**
   * Inject an analyzer resolution `Rule` builder into the [[SparkSession]]. These analyzer
   * rules will be executed as part of the resolution phase of analysis.
   */
  def injectResolutionRule(builder: RuleBuilder): Unit = {
    resolutionRuleBuilders += builder
  }

  private[this] val postHocResolutionRuleBuilders = mutable.Buffer.empty[RuleBuilder]

  /**
   * Build the analyzer post-hoc resolution `Rule`s using the given [[SparkSession]].
   */
  private[sql] def buildPostHocResolutionRules(session: SparkSession): Seq[Rule[LogicalPlan]] = {
    postHocResolutionRuleBuilders.map(_.apply(session))
  }

  /**
   * Inject an analyzer `Rule` builder into the [[SparkSession]]. These analyzer
   * rules will be executed after resolution.
   */
  def injectPostHocResolutionRule(builder: RuleBuilder): Unit = {
    postHocResolutionRuleBuilders += builder
  }

  private[this] val checkRuleBuilders = mutable.Buffer.empty[CheckRuleBuilder]

  /**
   * Build the check analysis `Rule`s using the given [[SparkSession]].
   */
  private[sql] def buildCheckRules(session: SparkSession): Seq[LogicalPlan => Unit] = {
    checkRuleBuilders.map(_.apply(session))
  }

  /**
   * Inject an check analysis `Rule` builder into the [[SparkSession]]. The injected rules will
   * be executed after the analysis phase. A check analysis rule is used to detect problems with a
   * LogicalPlan and should throw an exception when a problem is found.
   */
  def injectCheckRule(builder: CheckRuleBuilder): Unit = {
    checkRuleBuilders += builder
  }

  private[this] val optimizerRules = mutable.Buffer.empty[RuleBuilder]

  private[this] val optimizerRulesInOrder =
    mutable.Buffer.empty[(String, String, Order, RuleBuilder)]
  private[this] val optimizerBatches =
    mutable.Buffer.empty[(String, Int, String, Order, Seq[RuleBuilder])]

  private[sql] def buildOptimizerRules(session: SparkSession): Seq[Rule[LogicalPlan]] = {
    optimizerRules.map(_.apply(session))
  }

  private[sql] def buildOptimizerRulesInOrder(session: SparkSession): Seq[RuleInOrder] = {
    optimizerRulesInOrder.map(r => RuleInOrder(r._1, r._2, r._3, r._4.apply(session)))
  }

  private[sql] def buildOptimizerBatches(session: SparkSession): Seq[RuleBatch] = {
    optimizerBatches.map(batch => {
      val rules = batch._5
      RuleBatch(batch._1, batch._2, batch._3, batch._4, rules.map(r => r.apply(session)))
    })
  }


  /**
   * Inject an optimizer `Rule` builder into the [[SparkSession]]. The injected rules will be
   * executed during the operator optimization batch. An optimizer rule is used to improve the
   * quality of an analyzed logical plan; these rules should never modify the result of the
   * LogicalPlan.
   */
  def injectOptimizerRule(builder: RuleBuilder): Unit = {
    optimizerRules += builder
  }

  /**
   * Inject an optimizer `Rule` builder into the [[SparkSession]] in a particular batch after
   * or before a specific existing rule in the batch.
   * If the existingRule or the current rule is excluded via the conf
   * spark.sql.optimizer.excludedRules, then the rule will be excluded.
   * If the batchName does not exist, or if the existing rule does not exist in the given
   * batch, then an error will be thrown. (fail fast)
   */
  def injectOptimizerRuleInOrder(
      builder: RuleBuilder,
      batchName: String,
      ruleOrder: Order.Order,
      existingRule: String): Unit = {
    optimizerRulesInOrder += Tuple4(batchName, existingRule, ruleOrder, builder)
  }

  /**
   * Inject a batch of optimizer rules
   * @param batchName - Batch Name to inject
   * @param maxIterations - Iterations
   * @param existingBatchName - Existing batch name in reference to which this batch is injected
   * @param order - Specify the order, if before the existing batch or after the existing batch
   * @param rules - Sequence of RuleBuilder's. New rules in the batch that will be added
   */
  def injectOptimizerBatch(
      batchName: String,
      maxIterations: Int,
      existingBatchName: String,
      order: Order.Value,
      rules: Seq[RuleBuilder]): Unit = {
    optimizerBatches += Tuple5(batchName, maxIterations, existingBatchName, order, rules)
  }

  private[this] val plannerStrategyBuilders = mutable.Buffer.empty[StrategyBuilder]

  private[sql] def buildPlannerStrategies(session: SparkSession): Seq[Strategy] = {
    plannerStrategyBuilders.map(_.apply(session))
  }

  /**
   * Inject a planner `Strategy` builder into the [[SparkSession]]. The injected strategy will
   * be used to convert a `LogicalPlan` into a executable
   * [[org.apache.spark.sql.execution.SparkPlan]].
   */
  def injectPlannerStrategy(builder: StrategyBuilder): Unit = {
    plannerStrategyBuilders += builder
  }

  private[this] val parserBuilders = mutable.Buffer.empty[ParserBuilder]

  private[sql] def buildParser(
      session: SparkSession,
      initial: ParserInterface): ParserInterface = {
    parserBuilders.foldLeft(initial) { (parser, builder) =>
      builder(session, parser)
    }
  }

  /**
   * Inject a custom parser into the [[SparkSession]]. Note that the builder is passed a session
   * and an initial parser. The latter allows for a user to create a partial parser and to delegate
   * to the underlying parser for completeness. If a user injects more parsers, then the parsers
   * are stacked on top of each other.
   */
  def injectParser(builder: ParserBuilder): Unit = {
    parserBuilders += builder
  }

  private[this] val injectedFunctions = mutable.Buffer.empty[FunctionDescription]

  private[sql] def registerFunctions(functionRegistry: FunctionRegistry) = {
    for ((name, expressionInfo, function) <- injectedFunctions) {
      functionRegistry.registerFunction(name, expressionInfo, function)
    }
    functionRegistry
  }

  /**
  * Injects a custom function into the [[org.apache.spark.sql.catalyst.analysis.FunctionRegistry]]
  * at runtime for all sessions.
  */
  def injectFunction(functionDescription: FunctionDescription): Unit = {
    injectedFunctions += functionDescription
  }
}
