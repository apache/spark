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

package org.apache.spark.sql.catalyst.plans.logical

import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.trees.CurrentOrigin
import org.apache.spark.util.Utils


/**
 * [[AnalysisHelper]] defines some infrastructure for the query analyzer. In particular, in query
 * analysis we don't want to repeatedly re-analyze sub-plans that have previously been analyzed.
 *
 * This trait defines a flag `analyzed` that can be set to true once analysis is done on the tree.
 * This also provides a set of resolve methods that do not recurse down to sub-plans that have the
 * analyzed flag set to true.
 *
 * The analyzer rules should use the various resolve methods, in lieu of the various transform
 * methods defined in [[org.apache.spark.sql.catalyst.trees.TreeNode]] and [[QueryPlan]].
 *
 * To prevent accidental use of the transform methods, this trait also overrides the transform
 * methods to throw exceptions in test mode, if they are used in the analyzer.
 */
trait AnalysisHelper extends QueryPlan[LogicalPlan] { self: LogicalPlan =>

  private var _analyzed: Boolean = false

  /**
   * Recursively marks all nodes in this plan tree as analyzed.
   * This should only be called by
   * [[org.apache.spark.sql.catalyst.analysis.CheckAnalysis]].
   */
  private[sql] def setAnalyzed(): Unit = {
    if (!_analyzed) {
      _analyzed = true
      children.foreach(_.setAnalyzed())
    }
  }

  /**
   * Returns true if this node and its children have already been gone through analysis and
   * verification.  Note that this is only an optimization used to avoid analyzing trees that
   * have already been analyzed, and can be reset by transformations.
   */
  def analyzed: Boolean = _analyzed

  /**
   * Returns a copy of this node where `rule` has been recursively applied to the tree. When
   * `rule` does not apply to a given node, it is left unchanged. This function is similar to
   * `transform`, but skips sub-trees that have already been marked as analyzed.
   * Users should not expect a specific directionality. If a specific directionality is needed,
   * [[resolveOperatorsUp]] or [[resolveOperatorsDown]] should be used.
   *
   * @param rule the function use to transform this nodes children
   */
  def resolveOperators(rule: PartialFunction[LogicalPlan, LogicalPlan]): LogicalPlan = {
    resolveOperatorsDown(rule)
  }

  /**
   * Returns a copy of this node where `rule` has been recursively applied first to all of its
   * children and then itself (post-order, bottom-up). When `rule` does not apply to a given node,
   * it is left unchanged.  This function is similar to `transformUp`, but skips sub-trees that
   * have already been marked as analyzed.
   *
   * @param rule the function use to transform this nodes children
   */
  def resolveOperatorsUp(rule: PartialFunction[LogicalPlan, LogicalPlan]): LogicalPlan = {
    if (!analyzed) {
      AnalysisHelper.allowInvokingTransformsInAnalyzer {
        val afterRuleOnChildren = mapChildren(_.resolveOperatorsUp(rule))
        if (self fastEquals afterRuleOnChildren) {
          CurrentOrigin.withOrigin(origin) {
            rule.applyOrElse(self, identity[LogicalPlan])
          }
        } else {
          CurrentOrigin.withOrigin(origin) {
            val afterRule = rule.applyOrElse(afterRuleOnChildren, identity[LogicalPlan])
            afterRule.copyTagsFrom(self)
            afterRule
          }
        }
      }
    } else {
      self
    }
  }

  /** Similar to [[resolveOperatorsUp]], but does it top-down. */
  def resolveOperatorsDown(rule: PartialFunction[LogicalPlan, LogicalPlan]): LogicalPlan = {
    if (!analyzed) {
      AnalysisHelper.allowInvokingTransformsInAnalyzer {
        val afterRule = CurrentOrigin.withOrigin(origin) {
          rule.applyOrElse(self, identity[LogicalPlan])
        }

        // Check if unchanged and then possibly return old copy to avoid gc churn.
        if (self fastEquals afterRule) {
          mapChildren(_.resolveOperatorsDown(rule))
        } else {
          afterRule.mapChildren(_.resolveOperatorsDown(rule))
        }
      }
    } else {
      self
    }
  }

  /**
   * A variant of `transformUpWithNewOutput`, which skips touching already analyzed plan.
   */
  def resolveOperatorsUpWithNewOutput(
      rule: PartialFunction[LogicalPlan, (LogicalPlan, Seq[(Attribute, Attribute)])])
  : LogicalPlan = {
    if (!analyzed) {
      transformUpWithNewOutput(rule, skipCond = _.analyzed, canGetOutput = _.resolved)
    } else {
      self
    }
  }

  override def transformUpWithNewOutput(
      rule: PartialFunction[LogicalPlan, (LogicalPlan, Seq[(Attribute, Attribute)])],
      skipCond: LogicalPlan => Boolean,
      canGetOutput: LogicalPlan => Boolean): LogicalPlan = {
    AnalysisHelper.allowInvokingTransformsInAnalyzer {
      super.transformUpWithNewOutput(rule, skipCond, canGetOutput)
    }
  }

  /**
   * Recursively transforms the expressions of a tree, skipping nodes that have already
   * been analyzed.
   */
  def resolveExpressions(r: PartialFunction[Expression, Expression]): LogicalPlan = {
    resolveOperators  {
      case p => p.transformExpressions(r)
    }
  }

  protected def assertNotAnalysisRule(): Unit = {
    if (Utils.isTesting &&
        AnalysisHelper.inAnalyzer.get > 0 &&
        AnalysisHelper.resolveOperatorDepth.get == 0) {
      throw new RuntimeException("This method should not be called in the analyzer")
    }
  }

  /**
   * In analyzer, use [[resolveOperatorsDown()]] instead. If this is used in the analyzer,
   * an exception will be thrown in test mode. It is however OK to call this function within
   * the scope of a [[resolveOperatorsDown()]] call.
   * @see [[org.apache.spark.sql.catalyst.trees.TreeNode.transformDown()]].
   */
  override def transformDown(rule: PartialFunction[LogicalPlan, LogicalPlan]): LogicalPlan = {
    assertNotAnalysisRule()
    super.transformDown(rule)
  }

  /**
   * Use [[resolveOperators()]] in the analyzer.
   * @see [[org.apache.spark.sql.catalyst.trees.TreeNode.transformUp()]]
   */
  override def transformUp(rule: PartialFunction[LogicalPlan, LogicalPlan]): LogicalPlan = {
    assertNotAnalysisRule()
    super.transformUp(rule)
  }

  /**
   * Use [[resolveExpressions()]] in the analyzer.
   * @see [[QueryPlan.transformAllExpressions()]]
   */
  override def transformAllExpressions(rule: PartialFunction[Expression, Expression]): this.type = {
    assertNotAnalysisRule()
    super.transformAllExpressions(rule)
  }

  override def clone(): LogicalPlan = {
    val cloned = super.clone()
    if (analyzed) cloned.setAnalyzed()
    cloned
  }
}


object AnalysisHelper {

  /**
   * A thread local to track whether we are in a resolveOperator call (for the purpose of analysis).
   * This is an int because resolve* calls might be be nested (e.g. a rule might trigger another
   * query compilation within the rule itself), so we are tracking the depth here.
   */
  private val resolveOperatorDepth: ThreadLocal[Int] = new ThreadLocal[Int] {
    override def initialValue(): Int = 0
  }

  /**
   * A thread local to track whether we are in the analysis phase of query compilation. This is an
   * int rather than a boolean in case our analyzer recursively calls itself.
   */
  private val inAnalyzer: ThreadLocal[Int] = new ThreadLocal[Int] {
    override def initialValue(): Int = 0
  }

  def allowInvokingTransformsInAnalyzer[T](f: => T): T = {
    resolveOperatorDepth.set(resolveOperatorDepth.get + 1)
    try f finally {
      resolveOperatorDepth.set(resolveOperatorDepth.get - 1)
    }
  }

  def markInAnalyzer[T](f: => T): T = {
    inAnalyzer.set(inAnalyzer.get + 1)
    try f finally {
      inAnalyzer.set(inAnalyzer.get - 1)
    }
  }
}
