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

class AggregateSuite extends SparkPlanTest {
  private val ctx = sqlContext

  test("SPARK-8357 unsafe aggregation path should not leak memory with empty input") {
    val codegenDefault = ctx.getConf(SQLConf.CODEGEN_ENABLED)
    val unsafeDefault = ctx.getConf(SQLConf.UNSAFE_ENABLED)
    try {
      ctx.setConf(SQLConf.CODEGEN_ENABLED, true)
      ctx.setConf(SQLConf.UNSAFE_ENABLED, true)
      val df = Seq.empty[(Int, Int)].toDF("a", "b")
      checkAnswer(
        df,
        GeneratedAggregate(
          partial = true,
          Seq(df.col("b").expr),
          Seq(Alias(Count(df.col("a").expr), "cnt")()),
          unsafeEnabled = true,
          _: SparkPlan),
        Seq.empty
      )
    } finally {
      ctx.setConf(SQLConf.CODEGEN_ENABLED, codegenDefault)
      ctx.setConf(SQLConf.UNSAFE_ENABLED, unsafeDefault)
    }
  }
}
