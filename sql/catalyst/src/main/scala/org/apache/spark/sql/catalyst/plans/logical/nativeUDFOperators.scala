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

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, Expression}

/**
 * Evaluates calls of native functions on the rows of its child, and outputs each row followed by
 * the results of its calls. The calls are expressions of sql/core, whose arguments have no call of
 * a native function.
 */
case class EvalNativeUDFs(udfs: Seq[Expression], resultAttrs: Seq[Attribute], child: LogicalPlan)
  extends UnaryNode {

  override def output: Seq[Attribute] = child.output ++ resultAttrs

  override def producedAttributes: AttributeSet = AttributeSet(resultAttrs)

  override def maxRows: Option[Long] = child.maxRows

  override def maxRowsPerPartition: Option[Long] = child.maxRowsPerPartition

  override protected def withNewChildInternal(newChild: LogicalPlan): EvalNativeUDFs =
    copy(child = newChild)
}
