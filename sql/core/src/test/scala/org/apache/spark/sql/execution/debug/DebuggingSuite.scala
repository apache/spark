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

package org.apache.spark.sql.execution.debug

import java.io.ByteArrayOutputStream

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.test.SQLTestData.TestData

class DebuggingSuite extends SparkFunSuite with SharedSQLContext {

  test("DataFrame.debug()") {
    testData.debug()
  }

  test("Dataset.debug()") {
    import testImplicits._
    testData.as[TestData].debug()
  }

  test("debugCodegen") {
    val res = codegenString(spark.range(10).groupBy(col("id") * 2).count()
      .queryExecution.executedPlan)
    assert(res.contains("Subtree 1 / 2"))
    assert(res.contains("Subtree 2 / 2"))
    assert(res.contains("Object[]"))
  }

  test("debugCodegenStringSeq") {
    val res = codegenStringSeq(spark.range(10).groupBy(col("id") * 2).count()
      .queryExecution.executedPlan)
    assert(res.length == 2)
    assert(res.forall{ case (subtree, code) =>
      subtree.contains("Range") && code.contains("Object[]")})
  }

  test("SPARK-28537: DebugExec cannot debug broadcast related queries") {
    val rightDF = spark.range(10)
    val leftDF = spark.range(10)
    val joinedDF = leftDF.join(rightDF, leftDF("id") === rightDF("id"))

    val captured = new ByteArrayOutputStream()
    Console.withOut(captured) {
      joinedDF.debug()
    }

    val output = captured.toString()
    assert(output.contains(
      """== BroadcastExchange HashedRelationBroadcastMode(List(input[0, bigint, false])) ==
        |Tuples output: 0
        | id LongType: {}
        |== WholeStageCodegen ==
        |Tuples output: 10
        | id LongType: {java.lang.Long}
        |== Range (0, 10, step=1, splits=2) ==
        |Tuples output: 0
        | id LongType: {}""".stripMargin))
  }

  test("SPARK-28537: DebugExec cannot debug columnar related queries") {
    val df = spark.range(5)
    df.persist()

    val captured = new ByteArrayOutputStream()
    Console.withOut(captured) {
      df.debug()
    }
    df.unpersist()

    val output = captured.toString().replaceAll("#\\d+", "#x")
    assert(output.contains(
      """== InMemoryTableScan [id#xL] ==
        |Tuples output: 0
        | id LongType: {}
        |""".stripMargin))
  }
}
