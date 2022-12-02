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
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.streaming.{GroupStateTimeout, OutputMode}
import org.apache.spark.sql.types.StructType

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
 * Map partitions using a udf: iter(pyarrow.RecordBatch) -> iter(pyarrow.RecordBatch).
 * This is used by DataFrame.mapInArrow() in PySpark
 */
case class PythonMapInArrow(
    functionExpr: Expression,
    output: Seq[Attribute],
    child: LogicalPlan) extends UnaryNode {

  override val producedAttributes = AttributeSet(output)

  override protected def withNewChildInternal(newChild: LogicalPlan): PythonMapInArrow =
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

/**
 * Similar with [[FlatMapGroupsWithState]]. Applies func to each unique group
 * in `child`, based on the evaluation of `groupingAttributes`,
 * while using state data.
 * `functionExpr` is invoked with an pandas DataFrame representation and the
 * grouping key (tuple).
 *
 * @param functionExpr function called on each group
 * @param groupingAttributes used to group the data
 * @param outputAttrs used to define the output rows
 * @param stateType used to serialize/deserialize state before calling `functionExpr`
 * @param outputMode the output mode of `func`
 * @param timeout used to timeout groups that have not received data in a while
 * @param child logical plan of the underlying data
 */
case class FlatMapGroupsInPandasWithState(
    functionExpr: Expression,
    groupingAttributes: Seq[Attribute],
    outputAttrs: Seq[Attribute],
    stateType: StructType,
    outputMode: OutputMode,
    timeout: GroupStateTimeout,
    child: LogicalPlan) extends UnaryNode {

  override def output: Seq[Attribute] = outputAttrs

  override def producedAttributes: AttributeSet = AttributeSet(outputAttrs)

  override protected def withNewChildInternal(
    newChild: LogicalPlan): FlatMapGroupsInPandasWithState = copy(child = newChild)
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

  override def simpleString(maxFields: Int): String = {
    val truncatedOutputString = truncatedString(output, "[", ", ", "]", maxFields)
    val indexColumn = s"Index: $sequenceAttr"
    s"$nodeName$truncatedOutputString $indexColumn"
  }
}
