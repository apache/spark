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

    val input = Seq.empty[(String, Int, Double)]
    val df = input.toDF("a", "b", "c")

    val colB = df.col("b").expr
    val colC = df.col("c").expr
    val aggrExpr = Alias(Count(Cast(colC, LongType)), "Count")()

    // hack : current default parallelism of test local backend is two
    val two = Seq(Tuple1(0L), Tuple1(0L))
    val empty = Seq.empty[Tuple1[Long]]

    val codegenDefault = TestSQLContext.getConf(SQLConf.CODEGEN_ENABLED)
    try {
      for ((codegen, unsafe) <- Seq((false, false), (true, false), (true, true));
           partial <- Seq(false, true); groupExpr <- Seq(colB :: Nil, Seq.empty)) {
        TestSQLContext.setConf(SQLConf.CODEGEN_ENABLED, codegen)
        checkAnswer(df,
          GeneratedAggregate(partial, groupExpr, aggrExpr :: Nil, unsafe, _: SparkPlan),
          if (groupExpr.isEmpty && !partial) two else empty)
      }
    } finally {
      TestSQLContext.setConf(SQLConf.CODEGEN_ENABLED, codegenDefault)
    }
  }
}
