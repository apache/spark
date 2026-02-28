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

package org.apache.spark.sql.execution.python

import org.apache.spark.sql.catalyst.expressions.{
  Attribute, AttributeReference, AttributeSet, Expression, ExprId, NamedExpression, Unevaluable
}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, UnaryNode}
import org.apache.spark.sql.types.DataType

/**
 * A cloudpickle-serialized Python function to be executed in-process via jep
 * (Java Embedded Python).
 *
 * Distinct from [[org.apache.spark.sql.catalyst.expressions.PythonUDF]] which uses an
 * out-of-process Python worker connected via socket.
 *
 * Evaluated by [[InProcessArrowEvalExec]], which passes Arrow column buffers to CPython
 * as PyArrow arrays via native memory addresses (zero-copy input), then copies the
 * PyArrow result array back into a JVM-managed Arrow buffer (one copy on output).
 *
 * @param name            display name for plan explain output
 * @param serializedFunc  cloudpickle-serialized Python function bytes
 * @param children        input column expressions
 * @param dataType        return type (Phase 1: fixed-width types only)
 * @param udfDeterministic whether the UDF is deterministic
 * @param resultId        unique identifier for this UDF result
 */
case class InProcessPythonUDF(
    name: String,
    serializedFunc: Array[Byte],
    children: Seq[Expression],
    dataType: DataType,
    udfDeterministic: Boolean = true,
    resultId: ExprId = NamedExpression.newExprId)
  extends Expression with Unevaluable {

  override def nullable: Boolean = true
  override def prettyName: String = name

  override lazy val deterministic: Boolean =
    udfDeterministic && children.forall(_.deterministic)

  lazy val resultAttribute: Attribute =
    AttributeReference(name, dataType, nullable)(exprId = resultId)

  override def toString: String = s"$name(${children.mkString(", ")})#${resultId.id}"

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): InProcessPythonUDF =
    copy(children = newChildren)
}

/**
 * Logical plan node that evaluates [[InProcessPythonUDF]]s in-process via jep.
 * Inserted by [[ExtractInProcessPythonUDFs]] during query optimization, before physical planning.
 * Planned as [[InProcessArrowEvalExec]] by [[org.apache.spark.sql.execution.SparkStrategies]].
 */
case class InProcessEvalPython(
    udfs: Seq[InProcessPythonUDF],
    resultAttrs: Seq[Attribute],
    child: LogicalPlan) extends UnaryNode {

  override def output: Seq[Attribute] = child.output ++ resultAttrs
  override def producedAttributes: AttributeSet = AttributeSet(resultAttrs)

  override protected def withNewChildInternal(newChild: LogicalPlan): InProcessEvalPython =
    copy(child = newChild)
}
