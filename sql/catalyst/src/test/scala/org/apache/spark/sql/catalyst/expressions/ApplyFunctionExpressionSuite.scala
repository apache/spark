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

package org.apache.spark.sql.catalyst.expressions

import java.util.concurrent.{CountDownLatch, TimeUnit}

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog.functions.ScalarFunction
import org.apache.spark.sql.types.{DataType, IntegerType}
import org.apache.spark.util.ThreadUtils

class ApplyFunctionExpressionSuite extends SparkFunSuite {

  private val intIdentity = new ScalarFunction[Int] {
    override def inputTypes(): Array[DataType] = Array(IntegerType)
    override def resultType(): DataType = IntegerType
    override def name(): String = "int_identity"
    override def produceResult(input: InternalRow): Int = input.getInt(0)
  }

  test("SPARK-58578: ApplyFunctionExpression is stateful and produces fresh copies") {
    val expr = ApplyFunctionExpression(
      intIdentity, Seq(BoundReference(0, IntegerType, nullable = false)))
    assert(expr.stateful, "ApplyFunctionExpression.stateful should be true")
    val copy = expr.freshCopyIfContainsStatefulExpression()
    assert(copy ne expr,
      "freshCopyIfContainsStatefulExpression should return a new instance " +
        "for ApplyFunctionExpression")
    assert(copy.eval(InternalRow(7)) === 7)
  }

  test("SPARK-58578: fresh copies do not share the reused input row") {
    val firstEvaluationStarted = new CountDownLatch(1)
    val secondEvaluationStarted = new CountDownLatch(1)
    val blockingIdentity = new ScalarFunction[Int] {
      override def inputTypes(): Array[DataType] = Array(IntegerType)
      override def resultType(): DataType = IntegerType
      override def name(): String = "blocking_identity"
      override def produceResult(input: InternalRow): Int = {
        if (input.getInt(0) == 1) {
          firstEvaluationStarted.countDown()
          assert(secondEvaluationStarted.await(10, TimeUnit.SECONDS))
        } else {
          secondEvaluationStarted.countDown()
        }
        input.getInt(0)
      }
    }

    val expr = ApplyFunctionExpression(
      blockingIdentity, Seq(BoundReference(0, IntegerType, nullable = false)))
    val firstEvaluator = expr.freshCopyIfContainsStatefulExpression()
    val secondEvaluator = expr.freshCopyIfContainsStatefulExpression()

    val executor = ThreadUtils.newDaemonFixedThreadPool(2, "apply-function-expression-test")
    val executionContext = ExecutionContext.fromExecutorService(executor)
    try {
      val firstResult = Future(firstEvaluator.eval(InternalRow(1)))(executionContext)
      assert(firstEvaluationStarted.await(10, TimeUnit.SECONDS))
      val secondResult = Future(secondEvaluator.eval(InternalRow(2)))(executionContext)

      assert(ThreadUtils.awaitResult(firstResult, 10.seconds) === 1)
      assert(ThreadUtils.awaitResult(secondResult, 10.seconds) === 2)
    } finally {
      executor.shutdownNow()
    }
  }
}
