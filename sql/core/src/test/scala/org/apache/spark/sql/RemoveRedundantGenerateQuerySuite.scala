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

import org.apache.spark.sql.catalyst.optimizer.RemoveRedundantGenerate
import org.apache.spark.sql.catalyst.plans.logical.Generate
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class RemoveRedundantGenerateQuerySuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  private val ruleName = RemoveRedundantGenerate.ruleName

  private def withRuleDisabled[T](f: => T): T =
    withSQLConf(SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> ruleName)(f)

  private def hasGenerate(df: DataFrame): Boolean =
    df.queryExecution.optimizedPlan.collect { case _: Generate => true }.nonEmpty

  test("DISTINCT over explode drops empty and null arrays") {
    withTempView("t1") {
      Seq[(Int, Seq[Int])](
        (1, Seq(1, 2)),
        (1, Seq(1, 2)),
        (2, Seq.empty[Int]),
        (3, null),
        (4, Seq(5))).toDF("id", "items").createOrReplaceTempView("t1")

      val query = "SELECT DISTINCT id FROM t1 LATERAL VIEW explode(items) v AS item"
      checkAnswer(spark.sql(query), Seq(Row(1), Row(4)))
      assert(!hasGenerate(spark.sql(query)))

      withRuleDisabled {
        checkAnswer(spark.sql(query), Seq(Row(1), Row(4)))
        assert(hasGenerate(spark.sql(query)))
      }
    }
  }

  test("DISTINCT over explode of a map drops empty and null maps") {
    withTempView("t2") {
      Seq[(Int, Map[String, Int])](
        (1, Map("a" -> 1)),
        (2, Map.empty[String, Int]),
        (3, null)).toDF("id", "m").createOrReplaceTempView("t2")

      val query = "SELECT DISTINCT id FROM t2 LATERAL VIEW explode(m) v AS k, val"
      checkAnswer(spark.sql(query), Seq(Row(1)))
      assert(!hasGenerate(spark.sql(query)))

      withRuleDisabled {
        checkAnswer(spark.sql(query), Seq(Row(1)))
      }
    }
  }

  test("DISTINCT over outer explode keeps one row per input row") {
    withTempView("t3") {
      Seq[(Int, Seq[Int])](
        (1, Seq(1, 2)),
        (2, Seq.empty[Int]),
        (3, null),
        (4, Seq(5))).toDF("id", "items").createOrReplaceTempView("t3")

      val query = "SELECT DISTINCT id FROM t3 LATERAL VIEW OUTER explode(items) v AS item"
      checkAnswer(spark.sql(query), Seq(Row(1), Row(2), Row(3), Row(4)))
      assert(!hasGenerate(spark.sql(query)))

      withRuleDisabled {
        checkAnswer(spark.sql(query), Seq(Row(1), Row(2), Row(3), Row(4)))
        assert(hasGenerate(spark.sql(query)))
      }
    }
  }

  test("DISTINCT over explode keeps the Generate when the generated column is selected") {
    withTempView("t4") {
      Seq[(Int, Seq[Int])]((1, Seq(1, 2)), (4, Seq(5)))
        .toDF("id", "items").createOrReplaceTempView("t4")

      val query = "SELECT DISTINCT id, item FROM t4 LATERAL VIEW explode(items) v AS item"
      checkAnswer(spark.sql(query), Seq(Row(1, 1), Row(1, 2), Row(4, 5)))
      assert(hasGenerate(spark.sql(query)))

      withRuleDisabled {
        checkAnswer(spark.sql(query), Seq(Row(1, 1), Row(1, 2), Row(4, 5)))
        assert(hasGenerate(spark.sql(query)))
      }
    }
  }

  test("count over explode keeps the Generate") {
    withTempView("t5") {
      Seq[(Int, Seq[Int])]((1, Seq(1, 2)), (4, Seq(5)))
        .toDF("id", "items").createOrReplaceTempView("t5")

      val query = "SELECT id, count(*) FROM t5 LATERAL VIEW explode(items) v GROUP BY id"
      checkAnswer(spark.sql(query), Seq(Row(1, 2L), Row(4, 1L)))
      assert(hasGenerate(spark.sql(query)))
    }
  }
}
