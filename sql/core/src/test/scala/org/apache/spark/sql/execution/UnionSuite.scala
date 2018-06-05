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

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.{ExamplePoint, ExamplePointUDT, SharedSQLContext}

class UnionSuite extends QueryTest with SharedSQLContext {
  import testImplicits._

  test("SPARK-24410: Missing optimization for Union on bucketed tables") {
    val N = 10

    withTable("t1", "t2") {
      spark.range(N).selectExpr("id as key", "id % 2 as t1", "id % 3 as t2")
        .repartition(col("key")).write.mode("overwrite")
        .bucketBy(3, "key").sortBy("t1").saveAsTable("a1")

      spark.range(N).selectExpr("id as key", "id % 2 as t1", "id % 3 as t2")
        .repartition(col("key")).write.mode("overwrite")
        .bucketBy(3, "key").sortBy("t1").saveAsTable("a2")

      withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
        withSQLConf(SQLConf.UNION_IN_SAME_PARTITION.key -> "true") {
          val df = sql("select key, count(*) from " +
            "(select * from a1 union all select * from a2) z group by key")
          val shuffles = df.queryExecution.executedPlan.collect {
            case s: ShuffleExchangeExec => s
          }
          assert(shuffles.isEmpty)
          checkAnswer(df.orderBy("key"), Row(0, 2) :: Row(1, 2) :: Row(2, 2) :: Row(3, 2) ::
            Row(4, 2) :: Row(5, 2) :: Row(6, 2) :: Row(7, 2) :: Row(8, 2) :: Row(9, 2) :: Nil)
        }
      }
    }
  }
}
