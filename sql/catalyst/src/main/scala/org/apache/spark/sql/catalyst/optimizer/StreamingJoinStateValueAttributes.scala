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

package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet}
import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys
import org.apache.spark.sql.catalyst.plans.logical.{EventTimeWatermark, Join, LogicalPlan, Project, Union}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreeNodeTag

/** Attributes that a stream-stream join must retain in its buffered state. */
private[sql] case class StreamingJoinStateValueAttributes(
    leftStateValueOrdinals: Seq[Int],
    rightStateValueOrdinals: Seq[Int])

private[sql] object StreamingJoinStateValueAttributes {
  private val tag = TreeNodeTag[StreamingJoinStateValueAttributes](
    "streaming_join_state_value_attributes")

  def get(join: Join): Option[StreamingJoinStateValueAttributes] = join.getTagValue(tag)

  def set(join: Join, value: StreamingJoinStateValueAttributes): Unit = {
    join.setTagValue(tag, value)
  }

  def clear(join: Join): Unit = join.unsetTagValue(tag)
}

/** Separates stream-stream join input attributes from the attributes buffered in state. */
private[sql] object AnnotateStreamingJoinStateValueAttributes extends Rule[LogicalPlan] {
  private def compactStateValueOrdinals(
      input: Seq[Attribute],
      required: AttributeSet): Seq[Int] = {
    val compacted = input.zipWithIndex.collect {
      case (attr, ordinal)
          if !attr.metadata.contains(EventTimeWatermark.delayKey) || required.contains(attr) =>
        ordinal
    }
    if (compacted.nonEmpty) compacted else input.indices
  }

  private def requiredByOperator(
      plan: LogicalPlan,
      requiredOutput: AttributeSet): AttributeSet = plan match {
    case project: Project =>
      AttributeSet(project.projectList.filter { expression =>
        requiredOutput.contains(expression.toAttribute)
      }.flatMap(_.references))
    case join: Join =>
      AttributeSet(join.condition.toSeq.flatMap(_.references))
    case _ => plan.references
  }

  private def requiredChildOutput(
      plan: LogicalPlan,
      child: LogicalPlan,
      required: AttributeSet): AttributeSet = plan match {
    case union: Union =>
      val requiredOrdinals = union.output.zipWithIndex.collect {
        case (attribute, ordinal) if required.contains(attribute) => ordinal
      }
      AttributeSet(requiredOrdinals.filter(_ < child.output.length).map(child.output))
    case _ if plan.output.isEmpty =>
      // Write commands expose no output of their own but consume every child output attribute.
      child.outputSet
    case _ => required.intersect(child.outputSet)
  }

  private def annotate(plan: LogicalPlan, requiredOutput: AttributeSet): Unit = {
    val operatorReferences = requiredByOperator(plan, requiredOutput)
    plan match {
      case join: Join if join.isStateful && ExtractEquiJoinKeys.unapply(join).isDefined =>
        val required = requiredOutput ++ operatorReferences
        val leftOrdinals = compactStateValueOrdinals(join.left.output, required)
        val rightOrdinals = compactStateValueOrdinals(join.right.output, required)
        if (leftOrdinals == join.left.output.indices &&
            rightOrdinals == join.right.output.indices) {
          StreamingJoinStateValueAttributes.clear(join)
        } else {
          StreamingJoinStateValueAttributes.set(join,
            StreamingJoinStateValueAttributes(leftOrdinals, rightOrdinals))
        }
      case join: Join =>
        StreamingJoinStateValueAttributes.clear(join)
      case _ =>
    }

    val required = requiredOutput ++ operatorReferences
    plan.children.foreach { child =>
      annotate(child, requiredChildOutput(plan, child, required))
    }
  }

  override def apply(plan: LogicalPlan): LogicalPlan = {
    annotate(plan, plan.outputSet)
    plan
  }
}
