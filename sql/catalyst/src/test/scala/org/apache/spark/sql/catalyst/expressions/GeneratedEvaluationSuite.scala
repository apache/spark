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

package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen._

/**
 * Overrides our expression evaluation tests to use code generation for evaluation.
 */
class GeneratedEvaluationSuite extends ExpressionEvaluationSuite {
  override def checkEvaluation(
      expression: Expression,
      expected: Any,
      inputRow: Row = EmptyRow): Unit = {
    val plan = try {
      GenerateMutableProjection(Alias(expression, s"Optimized($expression)")() :: Nil)()
    } catch {
      case e: Throwable =>
        val evaluated = GenerateProjection.expressionEvaluator(expression)
        fail(
          s"""
            |Code generation of $expression failed:
            |${evaluated.code.mkString("\n")}
            |$e
          """.stripMargin)
    }

    val actual  = plan(inputRow).apply(0)
    if(actual != expected) {
      val input = if(inputRow == EmptyRow) "" else s", input: $inputRow"
      fail(s"Incorrect Evaluation: $expression, actual: $actual, expected: $expected$input")
    }
  }


  test("multithreaded eval") {
    import scala.concurrent._
    import ExecutionContext.Implicits.global
    import scala.concurrent.duration._

    val futures = (1 to 20).map { _ =>
      future {
        GeneratePredicate(EqualTo(Literal(1), Literal(1)))
        GenerateProjection(EqualTo(Literal(1), Literal(1)) :: Nil)
        GenerateMutableProjection(EqualTo(Literal(1), Literal(1)) :: Nil)
        GenerateOrdering(Add(Literal(1), Literal(1)).asc :: Nil)
      }
    }

    futures.foreach(Await.result(_, 10.seconds))
  }
}
