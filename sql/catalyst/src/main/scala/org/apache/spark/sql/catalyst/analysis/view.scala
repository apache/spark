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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.CatalystConf
import org.apache.spark.sql.catalyst.expressions.{UpCast, Alias, Attribute, Cast}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project, View}
import org.apache.spark.sql.catalyst.rules.Rule

/**
 * This file defines analysis rules related to views.
 */

/**
 * Make sure that a view's child plan produces the view's output attributes. We wrap the child
 * with a Project and add an alias for each output attribute by mapping the child output by index,
 * if the view output doesn't have the same number of columns with the child output, throw an
 * AnalysisException.
 * This should be only done after the batch of Resolution, because the view attributes are not
 * completely resolved during the batch of Resolution.
 */
case class AliasViewChild(conf: CatalystConf) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case v @ View(_, output, child) if child.resolved =>
      if (output.length != child.output.length) {
        throw new AnalysisException(
          s"The view output ${output.mkString("[", ",", "]")} doesn't have the same number of " +
            s"columns with the child output ${child.output.mkString("[", ",", "]")}")
      }
      val newOutput = output.zip(child.output).map {
        case (attr, originAttr) if attr != originAttr =>
          // The dataType of the output attributes may be not the same with that of the view
          // output, so we should cast the attribute to the dataType of the view output attribute.
          // Will throw an AnalysisException if the cast can't perform or might truncate.
          Alias(UpCast(originAttr, attr.dataType, Nil), attr.name)(exprId = attr.exprId,
            qualifier = attr.qualifier, explicitMetadata = Some(attr.metadata))
        case (_, originAttr) => originAttr
      }
      v.copy(child = Project(newOutput, child))
  }
}

/**
 * Removes [[View]] operators from the plan. The operator is respected till the end of analysis
 * stage because we want to see which part of an analyzed logical plan is generated from a view.
 */
object EliminateView extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    // The child should have the same output attributes with the View operator, so we simply
    // remove the View operator.
    case View(_, output, child) =>
      assert(output == child.output,
        s"The output of the child ${child.output.mkString("[", ",", "]")} is different from the " +
          s"view output ${output.mkString("[", ",", "]")}")
      child
  }
}
