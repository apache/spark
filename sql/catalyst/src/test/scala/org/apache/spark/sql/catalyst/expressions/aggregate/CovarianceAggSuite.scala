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

class CovarianceAggSuite extends TestWithAndWithoutCodegen {
  val a = AttributeReference("a", DoubleType, nullable = true)()
  val b = AttributeReference("b", DoubleType, nullable = true)()

  testBothCodegenAndInterpreted("SPARK-46189: pandas_covar eval") {
    val evaluator = DeclarativeAggregateEvaluator(PandasCovar(a, b, 1), Seq(a, b))
    val buffer = evaluator.update(
      InternalRow(1.0d, 1.0d),
      InternalRow(2.0d, 2.0d),
      InternalRow(3.0d, 3.0d),
      InternalRow(7.0d, 7.0d),
      InternalRow(9.0, 9.0),
      InternalRow(8.0d, 6.0))
    val result = evaluator.eval(buffer)
    assert(result === InternalRow(10.4d))
  }
}
