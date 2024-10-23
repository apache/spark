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
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.types.DoubleType

class CentralMomentAggSuite extends TestWithAndWithoutCodegen {
  val input = AttributeReference("input", DoubleType, nullable = true)()

  testBothCodegenAndInterpreted("SPARK-46189: pandas_kurtosis eval") {
    val evaluator = DeclarativeAggregateEvaluator(PandasKurtosis(input), Seq(input))
    val buffer = evaluator.update(
      InternalRow(1.0d),
      InternalRow(2.0d),
      InternalRow(3.0d),
      InternalRow(7.0d),
      InternalRow(9.0d),
      InternalRow(8.0d))
    val result = evaluator.eval(buffer)
    assert(result === InternalRow(-2.5772889417360285d))
  }

  testBothCodegenAndInterpreted("SPARK-46189: pandas_skew eval") {
    val evaluator = DeclarativeAggregateEvaluator(PandasSkewness(input), Seq(input))
    val buffer = evaluator.update(
      InternalRow(1.0d),
      InternalRow(2.0d),
      InternalRow(2.0d),
      InternalRow(2.0d),
      InternalRow(2.0d),
      InternalRow(100.0d))
    val result = evaluator.eval(buffer)
    assert(result === InternalRow(2.4489389171333733d))
  }

  testBothCodegenAndInterpreted("SPARK-46189: pandas_stddev eval") {
    val evaluator = DeclarativeAggregateEvaluator(PandasStddev(input, 1), Seq(input))
    val buffer = evaluator.update(
      InternalRow(1.0d),
      InternalRow(2.0d),
      InternalRow(3.0d),
      InternalRow(7.0d),
      InternalRow(9.0d),
      InternalRow(8.0d))
    val result = evaluator.eval(buffer)
    assert(result === InternalRow(3.40587727318528d))
  }

  testBothCodegenAndInterpreted("SPARK-46189: pandas_variance eval") {
    val evaluator = DeclarativeAggregateEvaluator(PandasVariance(input, 1), Seq(input))
    val buffer = evaluator.update(
      InternalRow(1.0d),
      InternalRow(2.0d),
      InternalRow(3.0d),
      InternalRow(7.0d),
      InternalRow(9.0d),
      InternalRow(8.0d))
    val result = evaluator.eval(buffer)
    assert(result === InternalRow(11.6d))
  }
}
