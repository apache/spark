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
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, Cast}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project, View}
import org.apache.spark.sql.catalyst.rules.Rule

/**
 * This file defines analysis rules related to views.
 */

/**
 * Make sure that a view's child plan produces the view's output attributes. We wrap the child
 * with a Project and add an alias for each output attribute. The attributes are resolved by
 * name. This should be only done after the resolution batch, because the view attributes are
 * not stable during resolution.
 */
case class AliasViewChild(conf: CatalystConf) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case v @ View(_, output, child) if child.resolved =>
      val resolver = conf.resolver
      val newOutput = child.output.map { attr =>
        val newAttr = findAttributeByName(attr.name, output, resolver)
        // Check the dataType of the output attributes, throw an AnalysisException if they don't
        // match up.
        checkDataType(attr, newAttr)
        Alias(attr, attr.name)(exprId = newAttr.exprId, qualifier = newAttr.qualifier,
          explicitMetadata = Some(newAttr.metadata))
      }
      v.copy(child = Project(newOutput, child))
  }

  /**
   * Find the attribute that has the expected attribute name from an attribute list, the names
   * are compared using conf.resolver.
   * If the expected attribute is not found, throw an AnalysisException.
   */
  private def findAttributeByName(
      name: String,
      attrs: Seq[Attribute],
      resolver: Resolver): Attribute = {
    attrs.collectFirst {
      case attr if resolver(attr.name, name) => attr
    }.getOrElse(throw new AnalysisException(
      s"Attribute with name '$name' is not found in " +
        s"'${attrs.map(_.name).mkString("(", ",", ")")}'"))
  }

  /**
   * Check whether the dataType of `attr` could be casted to that of `other`, throw an
   * AnalysisException if the both attributes don't match up.
   */
  private def checkDataType(attr: Attribute, other: Attribute): Unit = {
    if (!Cast.canCast(attr.dataType, other.dataType)) {
      throw new AnalysisException(
        s"Attribute '$other' don't match up with the output attribute '$attr'")
    }
  }
}

/**
 * Removes [[View]] operators from the plan. The operator is respected till the end of analysis
 * stage because we want to see which part of a analyzed logical plan is generated from a view.
 */
object EliminateView extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
    // The child should have the same output attributes with the View operator, so we simply
    // remove the View operator.
    case View(_, output, child) => child
  }
}
