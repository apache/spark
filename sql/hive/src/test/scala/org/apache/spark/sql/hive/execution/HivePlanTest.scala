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

package org.apache.spark.sql.hive.execution

import org.apache.spark.sql.functions._
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.hive.test.TestHiveSingleton

class HivePlanTest extends QueryTest with TestHiveSingleton {
  import spark.sql
  import spark.implicits._

  test("udf constant folding") {
    Seq.empty[Tuple1[Int]].toDF("a").createOrReplaceTempView("t")
    val optimized = sql("SELECT cos(null) AS c FROM t").queryExecution.optimizedPlan
    val correctAnswer = sql("SELECT cast(null as double) AS c FROM t").queryExecution.optimizedPlan

    comparePlans(optimized, correctAnswer)
  }

  test("window expressions sharing the same partition by and order by clause") {
    val df = Seq.empty[(Int, String, Int, Int)].toDF("id", "grp", "seq", "val")
    val window = Window.
      partitionBy($"grp").
      orderBy($"val")
    val query = df.select(
      $"id",
      sum($"val").over(window.rowsBetween(-1, 1)),
      sum($"val").over(window.rangeBetween(-1, 1))
    )
    val plan = query.queryExecution.analyzed
    assert(plan.collect{ case w: logical.Window => w }.size === 1,
      "Should have only 1 Window operator.")
  }
}
