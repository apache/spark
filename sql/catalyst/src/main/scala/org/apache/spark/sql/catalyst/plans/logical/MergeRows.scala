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

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, Expression, Unevaluable}
import org.apache.spark.sql.catalyst.plans.logical.MergeRows.{Instruction, ROW_ID}
import org.apache.spark.sql.catalyst.trees.UnaryLike
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.types.{DataType, NullType}

case class MergeRows(
    isSourceRowPresent: Expression,
    isTargetRowPresent: Expression,
    matchedInstructions: Seq[Instruction],
    notMatchedInstructions: Seq[Instruction],
    notMatchedBySourceInstructions: Seq[Instruction],
    checkCardinality: Boolean,
    output: Seq[Attribute],
    child: LogicalPlan) extends UnaryNode {

  override lazy val producedAttributes: AttributeSet = {
    AttributeSet(output.filterNot(attr => inputSet.contains(attr)))
  }

  @transient
  override lazy val references: AttributeSet = {
    val usedExprs = if (checkCardinality) {
      val rowIdAttr = child.output.find(attr => conf.resolver(attr.name, ROW_ID))
      assert(rowIdAttr.isDefined, "Cannot find row ID attr")
      rowIdAttr.get +: expressions
    } else {
      expressions
    }
    AttributeSet.fromAttributeSets(usedExprs.map(_.references)) -- producedAttributes
  }

  def instructions: Seq[Instruction] = {
    matchedInstructions ++ notMatchedInstructions ++ notMatchedBySourceInstructions
  }

  def outputs: Seq[Seq[Expression]] = instructions.flatMap(_.outputs)

  override def simpleString(maxFields: Int): String = {
    s"MergeRows${truncatedString(output, "[", ", ", "]", maxFields)}"
  }

  override protected def withNewChildInternal(newChild: LogicalPlan): LogicalPlan = {
    copy(child = newChild)
  }
}

object MergeRows {
  final val ROW_ID = "__row_id"

  /**
   * When a MERGE operation is rewritten, the target table is joined with the source and each
   * MATCHED/NOT MATCHED/NOT MATCHED BY SOURCE clause is converted into a corresponding instruction
   * on top of the joined plan. The purpose of an instruction is to derive an output row
   * based on a joined row.
   *
   * Instructions are valid expressions so that they will be properly transformed by the analyzer
   * and optimizer.
   */
  sealed trait Instruction extends Expression with Unevaluable {
    def condition: Expression
    def outputs: Seq[Seq[Expression]]
    override def nullable: Boolean = false
    // We return NullType here as only the `MergeRows` operator can contain `Instruction`
    // expressions and it doesn't care about the data type. Some external optimizer rules may
    // assume optimized plan is always resolved and Expression#dataType is always available, so
    // we can't just fail here.
    override def dataType: DataType = NullType
  }

  case class Keep(condition: Expression, output: Seq[Expression]) extends Instruction {
    def children: Seq[Expression] = condition +: output
    override def outputs: Seq[Seq[Expression]] = Seq(output)

    override protected def withNewChildrenInternal(
        newChildren: IndexedSeq[Expression]): Expression = {
      copy(condition = newChildren.head, output = newChildren.tail)
    }
  }

  case class Discard(condition: Expression) extends Instruction with UnaryLike[Expression] {
    override def outputs: Seq[Seq[Expression]] = Seq.empty
    override def child: Expression = condition

    override protected def withNewChildInternal(newChild: Expression): Expression = {
      copy(condition = newChild)
    }
  }

  case class Split(
      condition: Expression,
      output: Seq[Expression],
      otherOutput: Seq[Expression]) extends Instruction {

    def children: Seq[Expression] = Seq(condition) ++ output ++ otherOutput
    override def outputs: Seq[Seq[Expression]] = Seq(output, otherOutput)

    override protected def withNewChildrenInternal(
        newChildren: IndexedSeq[Expression]): Expression = {
      val newCondition = newChildren.head
      val newOutput = newChildren.slice(from = 1, until = output.size + 1)
      val newOtherOutput = newChildren.takeRight(otherOutput.size)
      copy(condition = newCondition, output = newOutput, otherOutput = newOtherOutput)
    }
  }
}
