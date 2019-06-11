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

import org.apache.spark.sql.catalyst.expressions.ScalaUDF
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSQLContext

class DeterministicLiteralUDFFoldingSuite extends QueryTest with SharedSQLContext {
  import testImplicits._

  test("Deterministic and literal UDF optimization") {
    def udfNodesCount(plan: LogicalPlan): Int = {
      plan.expressions.head.children.collect({
        case f: ScalaUDF => f
      }).length
    }

    val foo = udf(() => Math.random()).asNondeterministic()
    spark.udf.register("random0", foo)
    assert(!foo.deterministic)
    val foo2 = udf((x: String, i: Int) => x.length + i)
    spark.udf.register("mystrlen", foo2)
    assert(foo2.deterministic)

    Seq(("true", (1, 0, 0, 1)), ("false", (1, 1, 1, 1))).foreach { case (flag, expectedCounts) =>
      withSQLConf(SQLConf.DETERMINISTIC_LITERAL_UDF_FOLDING_ENABLED.key -> flag) {
        // Non deterministic
        val plan = sql("SELECT random0()").queryExecution.optimizedPlan
        assert(udfNodesCount(plan) == expectedCounts._1)

        // udf is deterministic and args are literal
        assert(sql("SELECT mystrlen('abc', 1)").head().getInt(0) == 4)
        val plan2 = sql("SELECT mystrlen('abc', 1)").queryExecution.optimizedPlan
        assert(udfNodesCount(plan2) == expectedCounts._2)
        val plan3 = sql("SELECT mystrlen('abc', mystrlen('c', 1))").queryExecution.optimizedPlan
        assert(udfNodesCount(plan3) == expectedCounts._3)

        // udf is deterministic and args are not literal
        withTempView("temp1") {
          val df = sparkContext.parallelize(
            (1 to 10).map(i => i.toString)).toDF("i1")
          df.createOrReplaceTempView("temp1")
          val plan = sql("SELECT mystrlen(i1, 1) FROM temp1").queryExecution.optimizedPlan
          assert(udfNodesCount(plan) == expectedCounts._4)
        }
      }
    }
  }

  test("udf folding rule in join") {
    withTempView("temp1") {
      val df = sparkContext.parallelize((1 to 5).map(i => i.toString)).toDF("i1")
      df.createOrReplaceTempView("temp1")
      val foo = udf((x: String, i: Int) => x.length + i)
      spark.udf.register("mystrlen1", foo)
      assert(foo.deterministic)

      val query = "SELECT mystrlen1(i1, 1) FROM temp1, " +
        "(SELECT mystrlen1('abc', mystrlen1('c', 1)) AS ref) WHERE mystrlen1(i1, ref) > 1"
      assert(sql(query).count() == 5)

      withSQLConf(SQLConf.DETERMINISTIC_LITERAL_UDF_FOLDING_ENABLED.key -> "true") {
        val exception = intercept[AnalysisException] {
          sql(query).count()
        }
        assert(exception.message.startsWith("Detected implicit cartesian product"))

        withSQLConf(SQLConf.CROSS_JOINS_ENABLED.key -> "true") {
          assert(sql(query).count() == 5)
        }
      }
    }
  }
}
