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

  test("SPARK-8826 Fix ClassCastException in GeneratedAggregate") {

    // when codegen = false, CCE is thrown if group-by expression is empty or unsafe is disabled
    val input = Seq(("Hello", 4, 2.0))

    val codegenDefault = TestSQLContext.getConf(SQLConf.CODEGEN_ENABLED)
    TestSQLContext.setConf(SQLConf.CODEGEN_ENABLED, false)
    try {
      val df = input.toDF("a", "b", "c")
      val colB = df.col("b").expr
      val colC = df.col("c").expr
      val aggrExpr = Alias(Count(Cast(colC, LongType)), "Count")()

      for (groupExpr <- Seq(Seq.empty, Seq(colB))) {
        val aggregate = GeneratedAggregate(true, groupExpr, Seq(aggrExpr), false, _: SparkPlan)
        // ok if it's not throws exception
        checkAnswer(df, aggregate, (_, _) => None)
      }
    } finally {
      TestSQLContext.setConf(SQLConf.CODEGEN_ENABLED, codegenDefault)
    }
  }
}
