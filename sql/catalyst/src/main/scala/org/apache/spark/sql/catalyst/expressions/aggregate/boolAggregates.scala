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

package org.apache.spark.sql.catalyst.expressions.aggregate

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.trees.UnaryLike
import org.apache.spark.sql.types._

@ExpressionDescription(
  usage = "_FUNC_(expr) - Returns true if all values of `expr` are true.",
  examples = """
    Examples:
      > SELECT _FUNC_(col) FROM VALUES (true), (true), (true) AS tab(col);
       true
      > SELECT _FUNC_(col) FROM VALUES (NULL), (true), (true) AS tab(col);
       true
      > SELECT _FUNC_(col) FROM VALUES (true), (false), (true) AS tab(col);
       false
  """,
  group = "agg_funcs",
  since = "3.0.0")
case class BoolAnd(child: Expression) extends RuntimeReplaceableAggregate
  with ImplicitCastInputTypes with UnaryLike[Expression] {
  override lazy val replacement: Expression = Min(child)
  override def nodeName: String = "bool_and"
  override def inputTypes: Seq[AbstractDataType] = Seq(BooleanType)
  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)
}

@ExpressionDescription(
  usage = "_FUNC_(expr) - Returns true if at least one value of `expr` is true.",
  examples = """
    Examples:
      > SELECT _FUNC_(col) FROM VALUES (true), (false), (false) AS tab(col);
       true
      > SELECT _FUNC_(col) FROM VALUES (NULL), (true), (false) AS tab(col);
       true
      > SELECT _FUNC_(col) FROM VALUES (false), (false), (NULL) AS tab(col);
       false
  """,
  group = "agg_funcs",
  since = "3.0.0")
case class BoolOr(child: Expression) extends RuntimeReplaceableAggregate
  with ImplicitCastInputTypes with UnaryLike[Expression] {
  override lazy val replacement: Expression = Max(child)
  override def nodeName: String = "bool_or"
  override def inputTypes: Seq[AbstractDataType] = Seq(BooleanType)
  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)
}
