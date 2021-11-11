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

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, Expression, PythonUDF}

/**
 * FlatMap groups using a udf: pandas.Dataframe -> pandas.DataFrame.
 * This is used by DataFrame.groupby().apply().
 */
case class FlatMapGroupsInPandas(
    groupingAttributes: Seq[Attribute],
    functionExpr: Expression,
    output: Seq[Attribute],
    child: LogicalPlan) extends UnaryNode {

  /**
   * This is needed because output attributes are considered `references` when
   * passed through the constructor.
   *
   * Without this, catalyst will complain that output attributes are missing
   * from the input.
   */
  override val producedAttributes = AttributeSet(output)

  override protected def withNewChildInternal(newChild: LogicalPlan): FlatMapGroupsInPandas =
    copy(child = newChild)
}

/**
 * Map partitions using a udf: iter(pandas.Dataframe) -> iter(pandas.DataFrame).
 * This is used by DataFrame.mapInPandas()
 */
case class MapInPandas(
    functionExpr: Expression,
    output: Seq[Attribute],
    child: LogicalPlan) extends UnaryNode {

  override val producedAttributes = AttributeSet(output)

  override protected def withNewChildInternal(newChild: LogicalPlan): MapInPandas =
    copy(child = newChild)
}

/**
 * Flatmap cogroups using a udf: pandas.Dataframe, pandas.Dataframe -> pandas.Dataframe
 * This is used by DataFrame.groupby().cogroup().apply().
 */
case class FlatMapCoGroupsInPandas(
    leftGroupingLen: Int,
    rightGroupingLen: Int,
    functionExpr: Expression,
    output: Seq[Attribute],
    left: LogicalPlan,
    right: LogicalPlan) extends BinaryNode {

  override val producedAttributes = AttributeSet(output)
  override lazy val references: AttributeSet =
    AttributeSet(leftAttributes ++ rightAttributes ++ functionExpr.references) -- producedAttributes

  def leftAttributes: Seq[Attribute] = left.output.take(leftGroupingLen)

  def rightAttributes: Seq[Attribute] = right.output.take(rightGroupingLen)

  override protected def withNewChildrenInternal(
      newLeft: LogicalPlan, newRight: LogicalPlan): FlatMapCoGroupsInPandas =
    copy(left = newLeft, right = newRight)
}

trait BaseEvalPython extends UnaryNode {

  def udfs: Seq[PythonUDF]

  def resultAttrs: Seq[Attribute]

  override def output: Seq[Attribute] = child.output ++ resultAttrs

  override def producedAttributes: AttributeSet = AttributeSet(resultAttrs)
}

/**
 * A logical plan that evaluates a [[PythonUDF]]
 */
case class BatchEvalPython(
    udfs: Seq[PythonUDF],
    resultAttrs: Seq[Attribute],
    child: LogicalPlan) extends BaseEvalPython {
  override protected def withNewChildInternal(newChild: LogicalPlan): BatchEvalPython =
    copy(child = newChild)
}

/**
 * A logical plan that evaluates a [[PythonUDF]] with Apache Arrow.
 */
case class ArrowEvalPython(
    udfs: Seq[PythonUDF],
    resultAttrs: Seq[Attribute],
    child: LogicalPlan,
    evalType: Int) extends BaseEvalPython {
  override protected def withNewChildInternal(newChild: LogicalPlan): ArrowEvalPython =
    copy(child = newChild)
}

/**
 * A logical plan that adds a new long column with the name `name` that
 * increases one by one. This is for 'distributed-sequence' default index
 * in pandas API on Spark.
 */
case class AttachDistributedSequence(
    sequenceAttr: Attribute,
    child: LogicalPlan) extends UnaryNode {

  override val producedAttributes: AttributeSet = AttributeSet(sequenceAttr)

  override val output: Seq[Attribute] = sequenceAttr +: child.output

  override protected def withNewChildInternal(newChild: LogicalPlan): AttachDistributedSequence =
    copy(child = newChild)
}
