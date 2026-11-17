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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.trees.TreePattern._

/** An analyzer-inserted FILTER guard that legacy checkpoint preparation can reverse. */
private[sql] case class KllFilteredAggregateInput(predicate: Expression, input: Expression)
  extends BinaryExpression with RuntimeReplaceable {
  override def left: Expression = predicate
  override def right: Expression = input
  // Keep compiler ownership distinct from an equal user-written IF until legacy preparation.
  override lazy val canonicalized: Expression = copy(predicate.canonicalized, input.canonicalized)
  override lazy val replacement: Expression =
    If(predicate, input, Literal.create(null, input.dataType))
  override val nodePatterns: Seq[TreePattern] = Seq(RUNTIME_REPLACEABLE, KLL_CHECKPOINT_ALTERNATIVE)
  override protected def withNewChildrenInternal(
      newLeft: Expression, newRight: Expression): Expression = copy(newLeft, newRight)
}

/** Retains a HAVING/ORDER BY occurrence until checkpoint preparation chooses historical reuse. */
private[sql] case class KllLegacyAggregateOutput(child: Expression)
  extends UnaryExpression with RuntimeReplaceable {
  override def replacement: Expression = child
  override val nodePatterns: Seq[TreePattern] = Seq(RUNTIME_REPLACEABLE, KLL_CHECKPOINT_ALTERNATIVE)
  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)
}
