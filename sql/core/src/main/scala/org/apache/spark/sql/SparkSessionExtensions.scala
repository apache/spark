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
import org.apache.spark.sql.catalyst.analysis.{FunctionRegistry, TableFunctionRegistry}
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.analysis.TableFunctionRegistry.TableFunctionBuilder
import org.apache.spark.sql.catalyst.expressions.ExpressionInfo
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{ColumnarRule, SparkPlan}

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
 * <li>Cache Plan Normalization Rules.</li>
 * <li>Optimizer Rules.</li>
 * <li>Pre CBO Rules.</li>
 * <li>Planning Strategies.</li>
 * <li>Customized Parser.</li>
 * <li>(External) Catalog listeners.</li>
 * <li>Columnar Rules.</li>
 * <li>Adaptive Query Stage Preparation Rules.</li>
 * <li>Adaptive Query Execution Runtime Optimizer Rules.</li>
 * </ul>
 *
 * The extensions can be used by calling `withExtensions` on the [[SparkSession.Builder]], for
 * example:
 * {{{
 *   SparkSession.builder()
 *     .master("...")
 *     .config("...", true)
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
 * The extensions can also be used by setting the Spark SQL configuration property
 * `spark.sql.extensions`. Multiple extensions can be set using a comma-separated list. For example:
 * {{{
 *   SparkSession.builder()
 *     .master("...")
 *     .config("spark.sql.extensions", "org.example.MyExtensions,org.example.YourExtensions")
 *     .getOrCreate()
 *
 *   class MyExtensions extends Function1[SparkSessionExtensions, Unit] {
 *     override def apply(extensions: SparkSessionExtensions): Unit = {
 *       extensions.injectResolutionRule { session =>
 *         ...
 *       }
 *       extensions.injectParser { (session, parser) =>
 *         ...
 *       }
 *     }
 *   }
 *
 *   class YourExtensions extends SparkSessionExtensionsProvider {
 *     override def apply(extensions: SparkSessionExtensions): Unit = {
 *       extensions.injectResolutionRule { session =>
 *         ...
 *       }
 *       extensions.injectFunction(...)
 *     }
 *   }
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
  type TableFunctionDescription = (FunctionIdentifier, ExpressionInfo, TableFunctionBuilder)
  type ColumnarRuleBuilder = SparkSession => ColumnarRule
  type QueryStagePrepRuleBuilder = SparkSession => Rule[SparkPlan]

  private[this] val columnarRuleBuilders = mutable.Buffer.empty[ColumnarRuleBuilder]
  private[this] val queryStagePrepRuleBuilders = mutable.Buffer.empty[QueryStagePrepRuleBuilder]
  private[this] val runtimeOptimizerRules = mutable.Buffer.empty[RuleBuilder]

  /**
   * Build the override rules for columnar execution.
   */
  private[sql] def buildColumnarRules(session: SparkSession): Seq[ColumnarRule] = {
    columnarRuleBuilders.map(_.apply(session)).toSeq
  }

  /**
   * Build the override rules for the query stage preparation phase of adaptive query execution.
   */
  private[sql] def buildQueryStagePrepRules(session: SparkSession): Seq[Rule[SparkPlan]] = {
    queryStagePrepRuleBuilders.map(_.apply(session)).toSeq
  }

  /**
   * Build the override rules for the optimizer of adaptive query execution.
   */
  private[sql] def buildRuntimeOptimizerRules(session: SparkSession): Seq[Rule[LogicalPlan]] = {
    runtimeOptimizerRules.map(_.apply(session)).toSeq
  }

  /**
   * Inject a rule that can override the columnar execution of an executor.
   */
  def injectColumnar(builder: ColumnarRuleBuilder): Unit = {
    columnarRuleBuilders += builder
  }

  /**
   * Inject a rule that can override the query stage preparation phase of adaptive query
   * execution.
   */
  def injectQueryStagePrepRule(builder: QueryStagePrepRuleBuilder): Unit = {
    queryStagePrepRuleBuilders += builder
  }

  /**
   * Inject a runtime `Rule` builder into the [[SparkSession]].
   * The injected rules will be executed after built-in
   * [[org.apache.spark.sql.execution.adaptive.AQEOptimizer]] rules are applied.
   * A runtime optimizer rule is used to improve the quality of a logical plan during execution
   * which can leverage accurate statistics from shuffle.
   *
   * Note that, it does not work if adaptive query execution is disabled.
   */
  def injectRuntimeOptimizerRule(builder: RuleBuilder): Unit = {
    runtimeOptimizerRules += builder
  }

  private[this] val resolutionRuleBuilders = mutable.Buffer.empty[RuleBuilder]

  /**
   * Build the analyzer resolution `Rule`s using the given [[SparkSession]].
   */
  private[sql] def buildResolutionRules(session: SparkSession): Seq[Rule[LogicalPlan]] = {
    resolutionRuleBuilders.map(_.apply(session)).toSeq
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
    postHocResolutionRuleBuilders.map(_.apply(session)).toSeq
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
    checkRuleBuilders.map(_.apply(session)).toSeq
  }

  /**
   * Inject an check analysis `Rule` builder into the [[SparkSession]]. The injected rules will
   * be executed after the analysis phase. A check analysis rule is used to detect problems with a
   * LogicalPlan and should throw an exception when a problem is found.
   */
  def injectCheckRule(builder: CheckRuleBuilder): Unit = {
    checkRuleBuilders += builder
  }

  private[this] val planNormalizationRules = mutable.Buffer.empty[RuleBuilder]

  def buildPlanNormalizationRules(session: SparkSession): Seq[Rule[LogicalPlan]] = {
    planNormalizationRules.map(_.apply(session)).toSeq
  }

  /**
   * Inject a plan normalization `Rule` builder into the [[SparkSession]]. The injected rules will
   * be executed just before query caching decisions are made. Such rules can be used to improve the
   * cache hit rate by normalizing different plans to the same form. These rules should never modify
   * the result of the LogicalPlan.
   */
  def injectPlanNormalizationRule(builder: RuleBuilder): Unit = {
    planNormalizationRules += builder
  }

  private[this] val optimizerRules = mutable.Buffer.empty[RuleBuilder]

  private[sql] def buildOptimizerRules(session: SparkSession): Seq[Rule[LogicalPlan]] = {
    optimizerRules.map(_.apply(session)).toSeq
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

  private[this] val preCBORules = mutable.Buffer.empty[RuleBuilder]

  private[sql] def buildPreCBORules(session: SparkSession): Seq[Rule[LogicalPlan]] = {
    preCBORules.map(_.apply(session)).toSeq
  }

  /**
   * Inject an optimizer `Rule` builder that rewrites logical plans into the [[SparkSession]].
   * The injected rules will be executed once after the operator optimization batch and
   * before any cost-based optimization rules that depend on stats.
   */
  def injectPreCBORule(builder: RuleBuilder): Unit = {
    preCBORules += builder
  }

  private[this] val plannerStrategyBuilders = mutable.Buffer.empty[StrategyBuilder]

  private[sql] def buildPlannerStrategies(session: SparkSession): Seq[Strategy] = {
    plannerStrategyBuilders.map(_.apply(session)).toSeq
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

  private[this] val injectedTableFunctions = mutable.Buffer.empty[TableFunctionDescription]

  private[sql] def registerFunctions(functionRegistry: FunctionRegistry) = {
    for ((name, expressionInfo, function) <- injectedFunctions) {
      functionRegistry.registerFunction(name, expressionInfo, function)
    }
    functionRegistry
  }

  private[sql] def registerTableFunctions(tableFunctionRegistry: TableFunctionRegistry) = {
    for ((name, expressionInfo, function) <- injectedTableFunctions) {
      tableFunctionRegistry.registerFunction(name, expressionInfo, function)
    }
    tableFunctionRegistry
  }

  /**
  * Injects a custom function into the [[org.apache.spark.sql.catalyst.analysis.FunctionRegistry]]
  * at runtime for all sessions.
  */
  def injectFunction(functionDescription: FunctionDescription): Unit = {
    injectedFunctions += functionDescription
  }

  /**
   * Injects a custom function into the
   * [[org.apache.spark.sql.catalyst.analysis.TableFunctionRegistry]] at runtime for all sessions.
   */
  def injectTableFunction(functionDescription: TableFunctionDescription): Unit = {
    injectedTableFunctions += functionDescription
  }
}
