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

package org.apache.spark.sql

import scala.util.Random

import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.plans.logical.Window
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class ReorderWindowPartitionExpressionsSuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  test("reorder window group expressions by stats") {
    withTable("t") {
      withSQLConf(
        SQLConf.CBO_ENABLED.key -> "true",
        SQLConf.REORDER_WINDOW_PARTITION_EXPRESSIONS.key -> "true") {
        val rand = new Random()
        spark.range(1, 100000, 1, 10)
          .map(i => (i, i % 100, s"name-${rand.nextInt(100000)}"))
          .toDF("a", "b", "c")
          .write.format("parquet").saveAsTable("t")
        spark.sql(s"ANALYZE TABLE t COMPUTE STATISTICS FOR COLUMNS a, b")
        val df = sql("SELECT *, row_number() over(partition by b, a order by c desc) as rn FROM t")
        checkAnswer(
          df,
          sql("SELECT *, row_number() over(partition by a, b order by c) as rn FROM t"))

        assert(df.queryExecution.optimizedPlan.collect {
          case Window(_, partitionSpec, _, _) =>
            partitionSpec.map(_.asInstanceOf[NamedExpression].name)
        }.head === Seq("a", "b"))
      }
    }
  }
}
