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

import org.apache.spark.resource.ResourceProfile
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, Expression, PythonUDF, PythonUDTF}
import org.apache.spark.sql.catalyst.trees.TreePattern._
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.streaming.{GroupStateTimeout, OutputMode, TimeMode}
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
 * FlatMap groups using a udf: iter(pyarrow.RecordBatch) -> iter(pyarrow.RecordBatch).
 * This is used by DataFrame.groupby().applyInArrow().
 */
case class FlatMapGroupsInArrow(
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

  override protected def withNewChildInternal(newChild: LogicalPlan): FlatMapGroupsInArrow =
    copy(child = newChild)
}

/**
 * Map partitions using a udf: iter(pandas.Dataframe) -> iter(pandas.DataFrame).
 * This is used by DataFrame.mapInPandas()
 */
case class MapInPandas(
    functionExpr: Expression,
    output: Seq[Attribute],
    child: LogicalPlan,
    isBarrier: Boolean,
    profile: Option[ResourceProfile]) extends UnaryNode {

  override val producedAttributes = AttributeSet(output)

  override protected def withNewChildInternal(newChild: LogicalPlan): MapInPandas =
    copy(child = newChild)
}

/**
 * Map partitions using a udf: iter(pyarrow.RecordBatch) -> iter(pyarrow.RecordBatch).
 * This is used by DataFrame.mapInArrow() in PySpark
 */
case class MapInArrow(
    functionExpr: Expression,
    output: Seq[Attribute],
    child: LogicalPlan,
    isBarrier: Boolean,
    profile: Option[ResourceProfile]) extends UnaryNode {

  override val producedAttributes = AttributeSet(output)

  override protected def withNewChildInternal(newChild: LogicalPlan): MapInArrow =
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

/**
 * Invokes methods defined in the stateful processor used in arbitrary state API v2. We allow the
 * user to act on per-group set of input rows along with keyed state and the user can choose to
 * output/return 0 or more rows. For a streaming dataframe, we will repeatedly invoke the interface
 * methods for new rows in each trigger and the user's state/state variables will be stored
 * persistently across invocations.
 * @param functionExpr function called on each group
 * @param groupingAttributesLen length of the seq of grouping attributes for input dataframe
 * @param outputAttrs used to define the output rows
 * @param outputMode defines the output mode for the statefulProcessor
 * @param timeMode the time mode semantics of the stateful processor for timers and TTL.
 * @param child logical plan of the underlying data
 * @param initialState logical plan of initial state
 * @param initGroupingAttrsLen length of the seq of grouping attributes for initial state dataframe
 */
case class TransformWithStateInPandas(
    functionExpr: Expression,
    groupingAttributesLen: Int,
    outputAttrs: Seq[Attribute],
    outputMode: OutputMode,
    timeMode: TimeMode,
    child: LogicalPlan,
    hasInitialState: Boolean,
    initialState: LogicalPlan,
    initGroupingAttrsLen: Int,
    initialStateSchema: StructType) extends BinaryNode {
  override def left: LogicalPlan = child

  override def right: LogicalPlan = initialState

  override def output: Seq[Attribute] = outputAttrs

  override def producedAttributes: AttributeSet = AttributeSet(outputAttrs)

  override lazy val references: AttributeSet =
    AttributeSet(leftAttributes ++ rightAttributes ++ functionExpr.references) -- producedAttributes

  override protected def withNewChildrenInternal(
      newLeft: LogicalPlan, newRight: LogicalPlan): TransformWithStateInPandas =
    copy(child = newLeft, initialState = newRight)

  def leftAttributes: Seq[Attribute] = left.output.take(groupingAttributesLen)

  def rightAttributes: Seq[Attribute] = if (hasInitialState) {
    right.output.take(initGroupingAttrsLen)
  } else {
    // Dummy variables for passing the distribution & ordering check
    // in physical operators.
    left.output.take(groupingAttributesLen)
  }
}

/**
 * Flatmap cogroups using a udf: iter(pyarrow.RecordBatch) -> iter(pyarrow.RecordBatch)
 * This is used by DataFrame.groupby().cogroup().applyInArrow().
 */
case class FlatMapCoGroupsInArrow(
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
      newLeft: LogicalPlan, newRight: LogicalPlan): FlatMapCoGroupsInArrow =
    copy(left = newLeft, right = newRight)
}

trait BaseEvalPython extends UnaryNode {

  def udfs: Seq[PythonUDF]

  def resultAttrs: Seq[Attribute]

  override def output: Seq[Attribute] = child.output ++ resultAttrs

  override def producedAttributes: AttributeSet = AttributeSet(resultAttrs)

  final override val nodePatterns: Seq[TreePattern] = Seq(EVAL_PYTHON_UDF)
}

trait BaseEvalPythonUDTF extends UnaryNode {

  def udtf: PythonUDTF

  def requiredChildOutput: Seq[Attribute]

  def resultAttrs: Seq[Attribute]

  override def output: Seq[Attribute] = requiredChildOutput ++ resultAttrs

  override def producedAttributes: AttributeSet = AttributeSet(resultAttrs)

  final override val nodePatterns: Seq[TreePattern] = Seq(EVAL_PYTHON_UDTF)
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
 * A logical plan that evaluates a [[PythonUDTF]].
 *
 * @param udtf the user-defined Python function
 * @param requiredChildOutput the required output of the child plan. It's used for omitting data
 *                            generation that will be discarded next by a projection.
 * @param resultAttrs the output schema of the Python UDTF.
 * @param child the child plan
 */
case class BatchEvalPythonUDTF(
    udtf: PythonUDTF,
    requiredChildOutput: Seq[Attribute],
    resultAttrs: Seq[Attribute],
    child: LogicalPlan) extends BaseEvalPythonUDTF {
  override protected def withNewChildInternal(newChild: LogicalPlan): BatchEvalPythonUDTF =
    copy(child = newChild)
}

/**
 * A logical plan that evaluates a [[PythonUDTF]] using Apache Arrow.
 *
 * @param udtf the user-defined Python function
 * @param requiredChildOutput the required output of the child plan. It's used for omitting data
 *                            generation that will be discarded next by a projection.
 * @param resultAttrs the output schema of the Python UDTF.
 * @param child the child plan
 */
case class ArrowEvalPythonUDTF(
    udtf: PythonUDTF,
    requiredChildOutput: Seq[Attribute],
    resultAttrs: Seq[Attribute],
    child: LogicalPlan,
    evalType: Int) extends BaseEvalPythonUDTF {
  override protected def withNewChildInternal(newChild: LogicalPlan): ArrowEvalPythonUDTF =
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
