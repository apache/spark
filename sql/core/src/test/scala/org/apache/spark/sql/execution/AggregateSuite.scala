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

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.test.TestSQLContext
import org.apache.spark.sql.types.DataTypes._

class AggregateSuite extends SparkPlanTest {

  test("SPARK-8357 Memory leakage on unsafe aggregation path with empty input") {

    val df = Seq.empty[(String, Int, Double)].toDF("a", "b", "c")

    val groupExpr = df.col("b").expr
    val aggrExpr = Alias(Count(Cast(groupExpr, LongType)), "Count")()

    for ((codegen, unsafe) <- Seq((false, false), (true, false), (true, true));
         partial <- Seq(false, true)) {
      TestSQLContext.conf.setConfString("spark.sql.codegen", String.valueOf(codegen))
      checkAnswer(
        df,
        GeneratedAggregate(partial, groupExpr :: Nil, aggrExpr :: Nil, unsafe, _: SparkPlan),
        Seq.empty[(String, Int, Double)])
    }
  }
}
