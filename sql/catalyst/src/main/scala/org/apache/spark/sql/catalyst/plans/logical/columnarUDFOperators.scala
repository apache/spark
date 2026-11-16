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

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, ColumnarUDF}
import org.apache.spark.sql.catalyst.trees.TreePattern.{EVAL_COLUMNAR_UDF, TreePattern}

/**
 * Evaluates calls of columnar user-defined functions: its output is the output of its child
 * followed by one column per call. The arguments of the calls are attributes of the child.
 */
case class EvalColumnarUDFs(
    udfs: Seq[ColumnarUDF],
    resultAttrs: Seq[Attribute],
    child: LogicalPlan) extends UnaryNode {

  override def output: Seq[Attribute] = child.output ++ resultAttrs

  override def producedAttributes: AttributeSet = AttributeSet(resultAttrs)

  final override val nodePatterns: Seq[TreePattern] = Seq(EVAL_COLUMNAR_UDF)

  override protected def withNewChildInternal(newChild: LogicalPlan): EvalColumnarUDFs =
    copy(child = newChild)
}
