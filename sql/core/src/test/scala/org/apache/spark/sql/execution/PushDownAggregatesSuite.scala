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

import org.apache.spark.sql.{DataFrame, QueryTest}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanHelper, DisableAdaptiveExecutionSuite, EnableAdaptiveExecutionSuite}
import org.apache.spark.sql.execution.aggregate.BaseAggregateExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession


abstract class PushDownAggregatesSuiteBase
    extends QueryTest
    with SharedSparkSession
    with AdaptiveSparkPlanHelper {
  import testImplicits._

  private def assertExchangeBetweenAggregates(df: DataFrame, expected: Boolean): Unit = {
    val plan = df.queryExecution.executedPlan
    assert(collectWithSubqueries(plan) {
      case UnaryExecNode(agg: BaseAggregateExec, UnaryExecNode(_: BaseAggregateExec, _)) => agg
    }.isEmpty == expected)
  }

  test("Partial aggregate is pushed bellow manually inserted repartition") {
    withTempPath { path =>
      spark.range(1000)
        .select('id % 7 as "a")
        .repartition()
        .write.format("parquet").save(path.getAbsolutePath)

      withSQLConf(SQLConf.PUSH_DOWN_AGGREGATES_ENABLED.key -> "false") {
        val df1 = spark.read.parquet(path.getAbsolutePath)
          .repartition('a)
          .groupBy('a)
          .count()

        assertExchangeBetweenAggregates(df1, false)
        val result = df1.collect()
        withSQLConf(SQLConf.PUSH_DOWN_AGGREGATES_ENABLED.key -> "true") {
          val df2 = spark.read.parquet(path.getAbsolutePath)
            .repartition('a)
            .groupBy('a)
            .count()

          assertExchangeBetweenAggregates(df2, true)
          checkAnswer(df2, result)
        }
      }
    }
  }

  test("Partial aggregate is pushed bellow manually inserted repartitionByRange") {
    withTempPath { path =>
      spark.range(1000)
        .select('id % 7 as "a")
        .repartition()
        .write.format("parquet").save(path.getAbsolutePath)

      withSQLConf(SQLConf.PUSH_DOWN_AGGREGATES_ENABLED.key -> "false") {
        val df1 = spark.read.parquet(path.getAbsolutePath)
          .repartitionByRange('a)
          .groupBy('a)
          .count()

        assertExchangeBetweenAggregates(df1, false)
        val result = df1.collect()
        withSQLConf(SQLConf.PUSH_DOWN_AGGREGATES_ENABLED.key -> "true") {
          val df2 = spark.read.parquet(path.getAbsolutePath)
            .repartitionByRange('a)
            .groupBy('a)
            .count()

          assertExchangeBetweenAggregates(df2, true)
          checkAnswer(df2, result)
        }
      }
    }
  }

}

class PushDownAggregatesSuite extends PushDownAggregatesSuiteBase
  with DisableAdaptiveExecutionSuite

class PushDownAggregatesSuiteAE extends PushDownAggregatesSuiteBase
  with EnableAdaptiveExecutionSuite
