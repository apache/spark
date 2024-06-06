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
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.Cast.toSQLValue
import org.apache.spark.sql.catalyst.expressions.aggregate.BloomFilterAggregate
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.execution.aggregate.BaseAggregateExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.LongType
import org.apache.spark.tags.ExtendedSQLTest

/**
 * Query tests for the Bloom filter aggregate and filter function.
 */
@ExtendedSQLTest
class BloomFilterAggregateQuerySuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  val funcId_bloom_filter_agg = new FunctionIdentifier("bloom_filter_agg")
  val funcId_might_contain = new FunctionIdentifier("might_contain")

  override def beforeAll(): Unit = {
    super.beforeAll()
    // Register 'bloom_filter_agg' to builtin.
    spark.sessionState.functionRegistry.registerFunction(funcId_bloom_filter_agg,
      new ExpressionInfo(classOf[BloomFilterAggregate].getName, "bloom_filter_agg"),
      (children: Seq[Expression]) => children.size match {
        case 1 => new BloomFilterAggregate(children.head)
        case 2 => new BloomFilterAggregate(children.head, children(1))
        case 3 => new BloomFilterAggregate(children.head, children(1), children(2))
      })

    // Register 'might_contain' to builtin.
    spark.sessionState.functionRegistry.registerFunction(funcId_might_contain,
      new ExpressionInfo(classOf[BloomFilterMightContain].getName, "might_contain"),
      (children: Seq[Expression]) => BloomFilterMightContain(children.head, children(1)))
  }

  override def afterAll(): Unit = {
    spark.sessionState.functionRegistry.dropFunction(funcId_bloom_filter_agg)
    spark.sessionState.functionRegistry.dropFunction(funcId_might_contain)
    super.afterAll()
  }

  test("Test bloom_filter_agg and might_contain") {
    val conf = SQLConf.get
    val table = "bloom_filter_test"
    for (numEstimatedItems <- Seq(Long.MinValue, -10L, 0L, 4096L, 4194304L, Long.MaxValue,
      conf.getConf(SQLConf.RUNTIME_BLOOM_FILTER_MAX_NUM_ITEMS))) {
      for ((numBits, index) <- Seq(Long.MinValue, -10L, 0L, 4096L, 4194304L, Long.MaxValue,
        conf.getConf(SQLConf.RUNTIME_BLOOM_FILTER_MAX_NUM_BITS)).zipWithIndex) {
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
            val stop = numEstimatedItems match {
              case Long.MinValue => Seq(169, 152, 150, 153, 156, 168, 157)
              case -10L => Seq(152, 135, 133, 136, 139, 151, 140)
              case 0L => Seq(150, 133, 131, 134, 137, 149, 138)
            }
            checkError(
              exception = exception,
              errorClass = "DATATYPE_MISMATCH.VALUE_OUT_OF_RANGE",
              parameters = Map(
                "exprName" -> "estimatedNumItems",
                "valueRange" -> "[0, positive]",
                "currentValue" -> toSQLValue(numEstimatedItems, LongType),
                "sqlExpr" -> (s""""bloom_filter_agg(col, CAST($numEstimatedItems AS BIGINT), """ +
                  s"""CAST($numBits AS BIGINT))"""")
              ),
              context = ExpectedContext(
                fragment = "bloom_filter_agg(col,\n" +
                  s"              cast($numEstimatedItems as long),\n" +
                  s"              cast($numBits as long))",
                start = 49,
                stop = stop(index)
              )
            )
          } else if (numBits <= 0) {
            val exception = intercept[AnalysisException] {
              spark.sql(sqlString)
            }
            val stop = numEstimatedItems match {
              case 4096L => Seq(153, 136, 134)
              case 4194304L => Seq(156, 139, 137)
              case Long.MaxValue => Seq(168, 151, 149)
              case 4000000 => Seq(156, 139, 137)
            }
            checkError(
              exception = exception,
              errorClass = "DATATYPE_MISMATCH.VALUE_OUT_OF_RANGE",
              parameters = Map(
                "exprName" -> "numBits",
                "valueRange" -> "[0, positive]",
                "currentValue" -> toSQLValue(numBits, LongType),
                "sqlExpr" -> (s""""bloom_filter_agg(col, CAST($numEstimatedItems AS BIGINT), """ +
                  s"""CAST($numBits AS BIGINT))"""")
              ),
              context = ExpectedContext(
                fragment = "bloom_filter_agg(col,\n" +
                  s"              cast($numEstimatedItems as long),\n" +
                  s"              cast($numBits as long))",
                start = 49,
                stop = stop(index)
              )
            )
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
    checkError(
      exception = exception1,
      errorClass = "DATATYPE_MISMATCH.BLOOM_FILTER_WRONG_TYPE",
      parameters = Map(
        "functionName" -> "`bloom_filter_agg`",
        "sqlExpr" -> "\"bloom_filter_agg(a, 1000000, 8388608)\"",
        "expectedLeft" -> "\"BINARY\"",
        "expectedRight" -> "\"BIGINT\"",
        "actual" -> "\"DECIMAL(2,1)\", \"BIGINT\", \"BIGINT\""
      ),
      context = ExpectedContext(
        fragment = "bloom_filter_agg(a)",
        start = 8,
        stop = 26
      )
    )

    val exception2 = intercept[AnalysisException] {
      spark.sql("""
        |SELECT bloom_filter_agg(a, 2)
        |FROM values (cast(1 as long)), (cast(2 as long)) as t(a)"""
        .stripMargin)
    }
    checkError(
      exception = exception2,
      errorClass = "DATATYPE_MISMATCH.BLOOM_FILTER_WRONG_TYPE",
      parameters = Map(
        "functionName" -> "`bloom_filter_agg`",
        "sqlExpr" -> "\"bloom_filter_agg(a, 2, (2 * 8))\"",
        "expectedLeft" -> "\"BINARY\"",
        "expectedRight" -> "\"BIGINT\"",
        "actual" -> "\"BIGINT\", \"INT\", \"BIGINT\""
      ),
      context = ExpectedContext(
        fragment = "bloom_filter_agg(a, 2)",
        start = 8,
        stop = 29
      )
    )

    val exception3 = intercept[AnalysisException] {
      spark.sql("""
        |SELECT bloom_filter_agg(a, cast(2 as long), 5)
        |FROM values (cast(1 as long)), (cast(2 as long)) as t(a)"""
        .stripMargin)
    }
    checkError(
      exception = exception3,
      errorClass = "DATATYPE_MISMATCH.BLOOM_FILTER_WRONG_TYPE",
      parameters = Map(
        "functionName" -> "`bloom_filter_agg`",
        "sqlExpr" -> "\"bloom_filter_agg(a, CAST(2 AS BIGINT), 5)\"",
        "expectedLeft" -> "\"BINARY\"",
        "expectedRight" -> "\"BIGINT\"",
        "actual" -> "\"BIGINT\", \"BIGINT\", \"INT\""
      ),
      context = ExpectedContext(
        fragment = "bloom_filter_agg(a, cast(2 as long), 5)",
        start = 8,
        stop = 46
      )
    )

    val exception4 = intercept[AnalysisException] {
      spark.sql("""
        |SELECT bloom_filter_agg(a, null, 5)
        |FROM values (cast(1 as long)), (cast(2 as long)) as t(a)"""
        .stripMargin)
    }
    checkError(
      exception = exception4,
      errorClass = "DATATYPE_MISMATCH.UNEXPECTED_NULL",
      parameters = Map(
        "exprName" -> "estimatedNumItems or numBits",
        "sqlExpr" -> "\"bloom_filter_agg(a, NULL, 5)\""
      ),
      context = ExpectedContext(
        fragment = "bloom_filter_agg(a, null, 5)",
        start = 8,
        stop = 35
      )
    )

    val exception5 = intercept[AnalysisException] {
      spark.sql("""
        |SELECT bloom_filter_agg(a, 5, null)
        |FROM values (cast(1 as long)), (cast(2 as long)) as t(a)"""
        .stripMargin)
    }
    checkError(
      exception = exception5,
      errorClass = "DATATYPE_MISMATCH.UNEXPECTED_NULL",
      parameters = Map(
        "exprName" -> "estimatedNumItems or numBits",
        "sqlExpr" -> "\"bloom_filter_agg(a, 5, NULL)\""
      ),
      context = ExpectedContext(
        fragment = "bloom_filter_agg(a, 5, null)",
        start = 8,
        stop = 35
      )
    )
  }

  test("Test that might_contain errors out disallowed input value types") {
    val exception1 = intercept[AnalysisException] {
      spark.sql("""|SELECT might_contain(1.0, 1L)"""
        .stripMargin)
    }
    checkError(
      exception = exception1,
      errorClass = "DATATYPE_MISMATCH.BLOOM_FILTER_WRONG_TYPE",
      parameters = Map(
        "sqlExpr" -> "\"might_contain(1.0, 1)\"",
        "functionName" -> "`might_contain`",
        "expectedLeft" -> "\"BINARY\"",
        "expectedRight" -> "\"BIGINT\"",
        "actual" -> "\"DECIMAL(2,1)\", \"BIGINT\""
      ),
      context = ExpectedContext(
        fragment = "might_contain(1.0, 1L)",
        start = 7,
        stop = 28
      )
    )

    val exception2 = intercept[AnalysisException] {
      spark.sql("""|SELECT might_contain(NULL, 0.1)"""
        .stripMargin)
    }
    checkError(
      exception = exception2,
      errorClass = "DATATYPE_MISMATCH.BLOOM_FILTER_WRONG_TYPE",
      parameters = Map(
        "sqlExpr" -> "\"might_contain(NULL, 0.1)\"",
        "functionName" -> "`might_contain`",
        "expectedLeft" -> "\"BINARY\"",
        "expectedRight" -> "\"BIGINT\"",
        "actual" -> "\"VOID\", \"DECIMAL(1,1)\""
      ),
      context = ExpectedContext(
        fragment = "might_contain(NULL, 0.1)",
        start = 7,
        stop = 30
      )
    )
  }

  test("Test that might_contain errors out non-constant Bloom filter") {
    val exception1 = intercept[AnalysisException] {
      spark.sql("""
                  |SELECT might_contain(cast(a as binary), cast(5 as long))
                  |FROM values (cast(1 as string)), (cast(2 as string)) as t(a)"""
        .stripMargin)
    }
    checkError(
      exception = exception1,
      errorClass = "DATATYPE_MISMATCH.BLOOM_FILTER_BINARY_OP_WRONG_TYPE",
      parameters = Map(
        "sqlExpr" -> "\"might_contain(CAST(a AS BINARY), CAST(5 AS BIGINT))\"",
        "functionName" -> "`might_contain`",
        "actual" -> "\"CAST(a AS BINARY)\""
      ),
      context = ExpectedContext(
        fragment = "might_contain(cast(a as binary), cast(5 as long))",
        start = 8,
        stop = 56
      )
    )

    val exception2 = intercept[AnalysisException] {
      spark.sql("""
                  |SELECT might_contain((select cast(a as binary)), cast(5 as long))
                  |FROM values (cast(1 as string)), (cast(2 as string)) as t(a)"""
        .stripMargin)
    }
    checkError(
      exception = exception2,
      errorClass = "DATATYPE_MISMATCH.BLOOM_FILTER_BINARY_OP_WRONG_TYPE",
      parameters = Map(
        "sqlExpr" -> "\"might_contain(scalarsubquery(a), CAST(5 AS BIGINT))\"",
        "functionName" -> "`might_contain`",
        "actual" -> "\"scalarsubquery(a)\""
      ),
      context = ExpectedContext(
        fragment = "might_contain((select cast(a as binary)), cast(5 as long))",
        start = 8,
        stop = 65
      )
    )
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

  test("Test numBitsExpression") {
    def checkNumBits(estimatedNumItems: Long, numBits: Long): Unit = {
      val agg = new BloomFilterAggregate(Literal(1L), estimatedNumItems)
      assert(agg.numBitsExpression === Literal(numBits))
    }

    checkNumBits(conf.getConf(SQLConf.RUNTIME_BLOOM_FILTER_MAX_NUM_ITEMS) * 100,
      conf.getConf(SQLConf.RUNTIME_BLOOM_FILTER_MAX_NUM_BITS))
    checkNumBits(conf.getConf(SQLConf.RUNTIME_BLOOM_FILTER_MAX_NUM_ITEMS) + 10, 29193836)
    checkNumBits(conf.getConf(SQLConf.RUNTIME_BLOOM_FILTER_MAX_NUM_ITEMS), 29193763)
    checkNumBits(2000000, 17482271)
    checkNumBits(1000000, 10183830)
    checkNumBits(10000, 197688)
    checkNumBits(100, 2935)
    checkNumBits(1, 38)
  }
}
