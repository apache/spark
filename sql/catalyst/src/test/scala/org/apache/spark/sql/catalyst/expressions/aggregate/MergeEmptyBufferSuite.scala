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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, JoinedRow}
import org.apache.spark.sql.types.DoubleType

/**
 * Focused regression coverage for the empty-buffer guards in the `merge` expressions of the
 * statistical aggregates. Merging an empty buffer into a populated one (or vice versa) is an
 * identity, but without the guards a large average makes the intermediate `delta * deltaN` term
 * overflow to `Infinity` before it is multiplied by the zero count, and `Infinity * 0 = NaN`
 * corrupts the moments. The end-to-end `DataFrameAggregateSuite` cases use `repartition(1)`, which
 * only produces the empty-left merge; here both the `isEmptyLeft` and `isEmptyRight` branches are
 * exercised directly by driving the merger projection with the empty buffer on each side, under
 * both interpreted and generated execution.
 */
class MergeEmptyBufferSuite extends TestWithAndWithoutCodegen {
  private val x = AttributeReference("x", DoubleType, nullable = true)()
  private val y = AttributeReference("y", DoubleType, nullable = true)()

  // Merge the empty buffer on each side and assert the populated buffer is returned unchanged (an
  // empty-buffer merge is an identity), which also proves no term overflowed to Infinity/NaN.
  private def checkMergeWithEmptyIsIdentity(
      evaluator: DeclarativeAggregateEvaluator, populated: InternalRow): Unit = {
    val empty = evaluator.initialize()
    val joiner = new JoinedRow
    // isEmptyLeft: empty buffer as the left (accumulator) operand.
    assert(evaluator.merger(joiner(empty, populated)).copy() === populated)
    // isEmptyRight: empty buffer as the right (input) operand.
    assert(evaluator.merger(joiner(populated, empty)).copy() === populated)
  }

  testBothCodegenAndInterpreted("SPARK-58291: var_pop empty-buffer merge is overflow-safe") {
    val evaluator = DeclarativeAggregateEvaluator(VariancePop(x, nullOnDivideByZero = true), Seq(x))
    // Two equal, very large finite values: a populated buffer with a huge average but zero
    // variance, which is exactly the shape that overflowed `delta * deltaN`.
    val populated = evaluator.update(InternalRow(1.0e155), InternalRow(1.0e155))
    checkMergeWithEmptyIsIdentity(evaluator, populated)
  }

  testBothCodegenAndInterpreted("SPARK-58291: covar_pop empty-buffer merge is overflow-safe") {
    val evaluator =
      DeclarativeAggregateEvaluator(CovPopulation(x, y, nullOnDivideByZero = true), Seq(x, y))
    val populated = evaluator.update(InternalRow(1.0e155, 1.0e155), InternalRow(1.0e155, 1.0e155))
    checkMergeWithEmptyIsIdentity(evaluator, populated)
  }

  testBothCodegenAndInterpreted("SPARK-58291: corr empty-buffer merge is overflow-safe") {
    val evaluator = DeclarativeAggregateEvaluator(Corr(x, y, nullOnDivideByZero = true), Seq(x, y))
    val populated = evaluator.update(
      InternalRow(1.0e155, 1.0e-150),
      InternalRow(1.000000000000001e155, 2.0e-150))
    checkMergeWithEmptyIsIdentity(evaluator, populated)
  }
}
