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

package org.apache.spark.sql.execution

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, BoundReference, Alias, Literal}
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.IntegerType

class ExpandSuite extends SparkPlanTest with SharedSQLContext {
  import testImplicits.localSeqToDataFrameHolder

  private def testExpand(f: SparkPlan => SparkPlan): Unit = {
    val input = (1 to 1000).map(Tuple1.apply)
    val projections = Seq.tabulate(2) { i =>
      Alias(BoundReference(0, IntegerType, false), "id")() :: Alias(Literal(i), "gid")() :: Nil
    }
    val attributes = projections.head.map(_.toAttribute)
    checkAnswer(
      input.toDF(),
      plan => Expand(projections, attributes, f(plan)),
      input.flatMap(i => Seq.tabulate(2)(j => Row(i._1, j)))
    )
  }

  test("inheriting child row type") {
    val exprs = AttributeReference("a", IntegerType, false)() :: Nil
    val plan = Expand(Seq(exprs), exprs, ConvertToUnsafe(LocalTableScan(exprs, Seq.empty)))
    assert(plan.outputsUnsafeRows, "Expand should inherits the created row type from its child.")
  }

  test("expanding UnsafeRows") {
    testExpand(ConvertToUnsafe)
  }

  test("expanding SafeRows") {
    testExpand(identity)
  }
}
