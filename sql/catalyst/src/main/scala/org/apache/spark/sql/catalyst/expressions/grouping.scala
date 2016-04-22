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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types._

/**
 * A placeholder expression for cube/rollup, which will be replaced by analyzer
 */
trait GroupingSet extends Expression with CodegenFallback {

  def groupByExprs: Seq[Expression]
  override def children: Seq[Expression] = groupByExprs

  // this should be replaced first
  override lazy val resolved: Boolean = false

  override def dataType: DataType = throw new UnsupportedOperationException
  override def foldable: Boolean = false
  override def nullable: Boolean = true
  override def eval(input: InternalRow): Any = throw new UnsupportedOperationException
}

case class Cube(groupByExprs: Seq[Expression]) extends GroupingSet {}

case class Rollup(groupByExprs: Seq[Expression]) extends GroupingSet {}

/**
 * Indicates whether a specified column expression in a GROUP BY list is aggregated or not.
 * GROUPING returns 1 for aggregated or 0 for not aggregated in the result set.
 */
case class Grouping(child: Expression) extends Expression with Unevaluable {
  override def references: AttributeSet = AttributeSet(VirtualColumn.groupingIdAttribute :: Nil)
  override def children: Seq[Expression] = child :: Nil
  override def dataType: DataType = ByteType
  override def nullable: Boolean = false
}

/**
 * GroupingID is a function that computes the level of grouping.
 *
 * If groupByExprs is empty, it means all grouping expressions in GroupingSets.
 */
case class GroupingID(groupByExprs: Seq[Expression]) extends Expression with Unevaluable {
  override def references: AttributeSet = AttributeSet(VirtualColumn.groupingIdAttribute :: Nil)
  override def children: Seq[Expression] = groupByExprs
  override def dataType: DataType = IntegerType
  override def nullable: Boolean = false
  override def prettyName: String = "grouping_id"
}
