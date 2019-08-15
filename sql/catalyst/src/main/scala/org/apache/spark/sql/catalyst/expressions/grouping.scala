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

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_([col1[, col2 ..]]) - create a multi-dimensional cube using the specified columns
      so that we can run aggregation on them.
  """,
  examples = """
    Examples:
      > SELECT name, age, count(*) FROM VALUES (2, 'Alice'), (5, 'Bob') people(age, name) GROUP BY _FUNC_(name, age);
        NULL    2       1
        NULL    NULL    2
        Alice   2       1
        Bob     5       1
        NULL    5       1
        Bob     NULL    1
        Alice   NULL    1
  """,
  since = "2.0.0")
// scalastyle:on line.size.limit
case class Cube(groupByExprs: Seq[Expression]) extends GroupingSet {}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_([col1[, col2 ..]]) - create a multi-dimensional rollup using the specified columns
      so that we can run aggregation on them.
  """,
  examples = """
    Examples:
      > SELECT name, age, count(*) FROM VALUES (2, 'Alice'), (5, 'Bob') people(age, name) GROUP BY _FUNC_(name, age);
        NULL    NULL    2
        Alice   2       1
        Bob     5       1
        Bob     NULL    1
        Alice   NULL    1
  """,
  since = "2.0.0")
// scalastyle:on line.size.limit
case class Rollup(groupByExprs: Seq[Expression]) extends GroupingSet {}

/**
 * Indicates whether a specified column expression in a GROUP BY list is aggregated or not.
 * GROUPING returns 1 for aggregated or 0 for not aggregated in the result set.
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(col) - indicates whether a specified column in a GROUP BY is aggregated or
      not, returns 1 for aggregated or 0 for not aggregated in the result set.",
  """,
  examples = """
    Examples:
      > SELECT name, _FUNC_(name), sum(age) FROM VALUES (2, 'Alice'), (5, 'Bob') people(age, name) GROUP BY cube(name);
        Alice   0       2
        NULL    1       7
        Bob     0       5
  """,
  since = "2.0.0")
// scalastyle:on line.size.limit
case class Grouping(child: Expression) extends Expression with Unevaluable {
  @transient
  override lazy val references: AttributeSet =
    AttributeSet(VirtualColumn.groupingIdAttribute :: Nil)
  override def children: Seq[Expression] = child :: Nil
  override def dataType: DataType = ByteType
  override def nullable: Boolean = false
}

/**
 * GroupingID is a function that computes the level of grouping.
 *
 * If groupByExprs is empty, it means all grouping expressions in GroupingSets.
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_([col1[, col2 ..]]) - returns the level of grouping, equals to
      `(grouping(c1) << (n-1)) + (grouping(c2) << (n-2)) + ... + grouping(cn)`
  """,
  examples = """
    Examples:
      > SELECT name, _FUNC_(), sum(age), avg(height) FROM VALUES (2, 'Alice', 165), (5, 'Bob', 180) people(age, name, height) GROUP BY cube(name, height);
        NULL    2       2       165.0
        Alice   0       2       165.0
        NULL    2       5       180.0
        NULL    3       7       172.5
        Bob     0       5       180.0
        Bob     1       5       180.0
        Alice   1       2       165.0
  """,
  note = """
    Input columns should match with grouping columns exactly, or empty (means all the grouping
    columns).
  """,
  since = "2.0.0")
// scalastyle:on line.size.limit
case class GroupingID(groupByExprs: Seq[Expression]) extends Expression with Unevaluable {
  @transient
  override lazy val references: AttributeSet =
    AttributeSet(VirtualColumn.groupingIdAttribute :: Nil)
  override def children: Seq[Expression] = groupByExprs
  override def dataType: DataType = IntegerType
  override def nullable: Boolean = false
  override def prettyName: String = "grouping_id"
}
