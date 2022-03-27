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

import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.BloomFilterAggregate
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.execution.aggregate.BaseAggregateExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Query tests for the Bloom filter aggregate and filter function.
 */
class BloomFilterAggregateQuerySuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  val funcId_bloom_filter_agg = new FunctionIdentifier("bloom_filter_agg")
  val funcId_might_contain = new FunctionIdentifier("might_contain")

  // Register 'bloom_filter_agg' to builtin.
  FunctionRegistry.builtin.registerFunction(funcId_bloom_filter_agg,
    new ExpressionInfo(classOf[BloomFilterAggregate].getName, "bloom_filter_agg"),
    (children: Seq[Expression]) => children.size match {
      case 1 => new BloomFilterAggregate(children.head)
      case 2 => new BloomFilterAggregate(children.head, children(1))
      case 3 => new BloomFilterAggregate(children.head, children(1), children(2))
    })

  // Register 'might_contain' to builtin.
  FunctionRegistry.builtin.registerFunction(funcId_might_contain,
    new ExpressionInfo(classOf[BloomFilterMightContain].getName, "might_contain"),
    (children: Seq[Expression]) => BloomFilterMightContain(children.head, children(1)))

  override def afterAll(): Unit = {
    FunctionRegistry.builtin.dropFunction(funcId_bloom_filter_agg)
    FunctionRegistry.builtin.dropFunction(funcId_might_contain)
    super.afterAll()
  }

  test("Test bloom_filter_agg and might_contain") {
    val conf = SQLConf.get
    val table = "bloom_filter_test"
    for (numEstimatedItems <- Seq(Long.MinValue, -10L, 0L, 4096L, 4194304L, Long.MaxValue,
      conf.getConf(SQLConf.RUNTIME_BLOOM_FILTER_MAX_NUM_ITEMS))) {
      for (numBits <- Seq(Long.MinValue, -10L, 0L, 4096L, 4194304L, Long.MaxValue,
        conf.getConf(SQLConf.RUNTIME_BLOOM_FILTER_MAX_NUM_BITS))) {
        val sqlString = s"""
                           |SELECT every(might_contain(
                           |            (SELECT bloom_filter_agg(col,
                           |              cast($numEstimatedItems as long),
                           |              cast($numBits as long))
                           |             FROM $table),
                           |            col)) positive_membership_test,
                           |       every(might_contain(
                           |            (SELECT bloom_filter_agg(col,
                           |              cast($numEstimatedItems as long),
                           |              cast($numBits as long))
                           |             FROM values (-1L), (100001L), (20000L) as t(col)),
                           |            col)) negative_membership_test
                           |FROM $table
           """.stripMargin
        withTempView(table) {
          (Seq(Long.MinValue, 0, Long.MaxValue) ++ (1L to 10000L))
            .toDF("col").createOrReplaceTempView(table)
          // Validate error messages as well as answers when there's no error.
          if (numEstimatedItems <= 0) {
            val exception = intercept[AnalysisException] {
              spark.sql(sqlString)
            }
            assert(exception.getMessage.contains(
              "The estimated number of items must be a positive value"))
          } else if (numBits <= 0) {
            val exception = intercept[AnalysisException] {
              spark.sql(sqlString)
            }
            assert(exception.getMessage.contains("The number of bits must be a positive value"))
          } else {
            checkAnswer(spark.sql(sqlString), Row(true, false))
          }
        }
      }
    }
  }

  test("Test that bloom_filter_agg errors out disallowed input value types") {
    val exception1 = intercept[AnalysisException] {
      spark.sql("""
        |SELECT bloom_filter_agg(a)
        |FROM values (1.2), (2.5) as t(a)"""
        .stripMargin)
    }
    assert(exception1.getMessage.contains(
      "Input to function bloom_filter_agg should have been a bigint value"))

    val exception2 = intercept[AnalysisException] {
      spark.sql("""
        |SELECT bloom_filter_agg(a, 2)
        |FROM values (cast(1 as long)), (cast(2 as long)) as t(a)"""
        .stripMargin)
    }
    assert(exception2.getMessage.contains(
      "function bloom_filter_agg should have been a bigint value followed with two bigint"))

    val exception3 = intercept[AnalysisException] {
      spark.sql("""
        |SELECT bloom_filter_agg(a, cast(2 as long), 5)
        |FROM values (cast(1 as long)), (cast(2 as long)) as t(a)"""
        .stripMargin)
    }
    assert(exception3.getMessage.contains(
      "function bloom_filter_agg should have been a bigint value followed with two bigint"))

    val exception4 = intercept[AnalysisException] {
      spark.sql("""
        |SELECT bloom_filter_agg(a, null, 5)
        |FROM values (cast(1 as long)), (cast(2 as long)) as t(a)"""
        .stripMargin)
    }
    assert(exception4.getMessage.contains("Null typed values cannot be used as size arguments"))

    val exception5 = intercept[AnalysisException] {
      spark.sql("""
        |SELECT bloom_filter_agg(a, 5, null)
        |FROM values (cast(1 as long)), (cast(2 as long)) as t(a)"""
        .stripMargin)
    }
    assert(exception5.getMessage.contains("Null typed values cannot be used as size arguments"))
  }

  test("Test that might_contain errors out disallowed input value types") {
    val exception1 = intercept[AnalysisException] {
      spark.sql("""|SELECT might_contain(1.0, 1L)"""
        .stripMargin)
    }
    assert(exception1.getMessage.contains(
      "Input to function might_contain should have been binary followed by a value with bigint"))

    val exception2 = intercept[AnalysisException] {
      spark.sql("""|SELECT might_contain(NULL, 0.1)"""
        .stripMargin)
    }
    assert(exception2.getMessage.contains(
      "Input to function might_contain should have been binary followed by a value with bigint"))
  }

  test("Test that might_contain errors out non-constant Bloom filter") {
    val exception1 = intercept[AnalysisException] {
      spark.sql("""
                  |SELECT might_contain(cast(a as binary), cast(5 as long))
                  |FROM values (cast(1 as string)), (cast(2 as string)) as t(a)"""
        .stripMargin)
    }
    assert(exception1.getMessage.contains(
      "The Bloom filter binary input to might_contain should be either a constant value or " +
        "a scalar subquery expression"))

    val exception2 = intercept[AnalysisException] {
      spark.sql("""
                  |SELECT might_contain((select cast(a as binary)), cast(5 as long))
                  |FROM values (cast(1 as string)), (cast(2 as string)) as t(a)"""
        .stripMargin)
    }
    assert(exception2.getMessage.contains(
      "The Bloom filter binary input to might_contain should be either a constant value or " +
        "a scalar subquery expression"))
  }

  test("Test that might_contain can take a constant value input") {
    checkAnswer(spark.sql(
      """SELECT might_contain(
        |X'00000001000000050000000343A2EC6EA8C117E2D3CDB767296B144FC5BFBCED9737F267',
        |cast(201 as long))""".stripMargin),
      Row(false))
  }

  test("Test that bloom_filter_agg produces a NULL with empty input") {
    checkAnswer(spark.sql("""SELECT bloom_filter_agg(cast(id as long)) from range(1, 1)"""),
      Row(null))
  }

  test("Test NULL inputs for might_contain") {
    checkAnswer(spark.sql(
      s"""
         |SELECT might_contain(null, null) both_null,
         |       might_contain(null, 1L) null_bf,
         |       might_contain((SELECT bloom_filter_agg(cast(id as long)) from range(1, 10000)),
         |            null) null_value
         """.stripMargin),
      Row(null, null, null))
  }

  test("Test that a query with bloom_filter_agg has partial aggregates") {
    assert(spark.sql("""SELECT bloom_filter_agg(cast(id as long)) from range(1, 1000000)""")
      .queryExecution.executedPlan.asInstanceOf[AdaptiveSparkPlanExec].inputPlan
      .collect({case agg: BaseAggregateExec => agg}).size == 2)
  }
}
