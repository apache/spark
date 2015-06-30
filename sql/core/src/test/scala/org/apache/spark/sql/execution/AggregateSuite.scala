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

import org.apache.spark.sql.SQLConf
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.test.TestSQLContext
import org.apache.spark.sql.types.DataTypes._

class AggregateSuite extends SparkPlanTest {

  test("SPARK-8357 Memory leakage on unsafe aggregation path with empty input") {

    val input0 = Seq.empty[(String, Int, Double)]
    // in the case of needEmptyBufferForwarded=true, task makes a row from empty buffer
    // even with empty input. And current default parallelism of SparkPlanTest is two (local[2])
    val x0 = Seq(Tuple1(0L), Tuple1(0L))
    val y0 = Seq.empty[Tuple1[Long]]

    val input1 = Seq(("Hello", 4, 2.0))
    val x1 = Seq(Tuple1(0L), Tuple1(1L))
    val y1 = Seq(Tuple1(1L))

    val codegenDefault = TestSQLContext.getConf(SQLConf.CODEGEN_ENABLED)
    TestSQLContext.setConf(SQLConf.CODEGEN_ENABLED, true)
    try {
      for ((input, x, y) <- Seq((input0, x0, y0), (input1, x1, y1))) {
        val df = input.toDF("a", "b", "c")
        val colB = df.col("b").expr
        val colC = df.col("c").expr
        val aggrExpr = Alias(Count(Cast(colC, LongType)), "Count")()

        for (partial <- Seq(false, true); groupExpr <- Seq(Seq(colB), Seq.empty)) {
          val aggregate = GeneratedAggregate(partial, groupExpr, Seq(aggrExpr), true, _: SparkPlan)
          checkAnswer(df,
            aggregate,
            if (aggregate(null).needEmptyBufferForwarded) x else y)
        }
      }
    } finally {
      TestSQLContext.setConf(SQLConf.CODEGEN_ENABLED, codegenDefault)
    }
  }
}
