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

import org.apache.spark.sql.catalyst.analysis.AnalysisContext
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.trees.{BinaryLike, LeafLike, UnaryLike}
import org.apache.spark.sql.catalyst.trees.TreePattern.{COMMAND, TreePattern}

/**
 * A logical node that represents a non-query command to be executed by the system.  For example,
 * commands can be used by parsers to represent DDL operations.  Commands, unlike queries, are
 * eagerly executed.
 */
trait Command extends LogicalPlan {
  override def output: Seq[Attribute] = Seq.empty
  override def producedAttributes: AttributeSet = outputSet
  // Commands are eagerly executed. They will be converted to LocalRelation after the DataFrame
  // is created. That said, the statistics of a command is useless. Here we just return a dummy
  // statistics to avoid unnecessary statistics calculation of command's children.
  override def stats: Statistics = Statistics.DUMMY
  final override val nodePatterns: Seq[TreePattern] = Seq(COMMAND)
}

trait LeafCommand extends Command with LeafLike[LogicalPlan]
trait UnaryCommand extends Command with UnaryLike[LogicalPlan]
trait BinaryCommand extends Command with BinaryLike[LogicalPlan]

/**
 * A logical node that can be used for a command that requires its children to be only analyzed,
 * but not optimized. An example would be "create view": we don't need to optimize the view subtree
 * because we will just store the entire view text as is in the catalog.
 *
 * The way we do this is by setting the children to empty once the subtree is analyzed. This will
 * prevent the optimizer (or the analyzer from that point on) from traversing into the children.
 *
 * There's a corresponding rule
 * [[org.apache.spark.sql.catalyst.analysis.Analyzer.HandleSpecialCommand]] that marks these
 * commands analyzed.
 */
trait AnalysisOnlyCommand extends Command {
  val isAnalyzed: Boolean
  def childrenToAnalyze: Seq[LogicalPlan]
  override final def children: Seq[LogicalPlan] = if (isAnalyzed) Nil else childrenToAnalyze
  override def innerChildren: Seq[QueryPlan[_]] = if (isAnalyzed) childrenToAnalyze else Nil
  // After the analysis finished, we give the command a chance to update it's state based
  // on the `AnalysisContext`
  def markAsAnalyzed(analysisContext: AnalysisContext): LogicalPlan
}

/**
 * A logical node that does not expose its sub-nodes as children, but rather supervises them
 * in an implementation-defined manner.
 */
trait SupervisingCommand extends LeafCommand {
  /**
   * Transforms its supervised plan using `transformer` and returns a copy of `SupervisingCommand`
   */
  def withTransformedSupervisedPlan(transformer: LogicalPlan => LogicalPlan): LogicalPlan
}
