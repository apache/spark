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

package org.apache.spark.sql.catalyst.expressions.codegen

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.{BooleanType, DataType}

/**
 * A test suite that makes sure code generation handles sub-expression elimination correctly.
 */
class CodegenSubexpressionEliminationSuite extends SparkFunSuite {

  test("SPARK-32903: GeneratePredicate should eliminate sub-expressions") {
    Seq(true, false).foreach { useSubexprElimination =>
      val leaf1 = ExprWithEvaluatedState()
      val leaf2 = ExprWithEvaluatedState()
      val leaf3 = ExprWithEvaluatedState()
      val leaf4 = ExprWithEvaluatedState()

      val cond = Or(And(leaf1, leaf2), And(leaf3, leaf4))
      val instance = GeneratePredicate.generate(cond, useSubexprElimination = useSubexprElimination)
      instance.initialize(0)
      assert(instance.eval(null) === false)

      if (useSubexprElimination) {
        // When we do sub-expression elimination, Spark thought left and right side of
        // the `Or` expression are the same. So only left side was evaluated, and Spark
        // reused the evaluation for right side.
        assert(leaf1.evaluated == true)
        assert(leaf2.evaluated == false)
        assert(leaf3.evaluated == false)
        assert(leaf4.evaluated == false)
      } else {
        assert(leaf1.evaluated == true)
        assert(leaf2.evaluated == false)
        assert(leaf3.evaluated == true)
        assert(leaf4.evaluated == false)
      }
    }
  }

  test("SPARK-60332: GeneratePredicate evaluates a common subexpression of later conjuncts " +
      "after the conjuncts before it") {
    // `Not(leaf)` is common to the last two conjuncts, and evaluated just before the first of
    // them, so the first conjunct decides whether `leaf` is evaluated at all: a FALSE one stops
    // the evaluation, while a TRUE or a NULL one does not, as `And.eval` does.
    Seq(true, false).foreach { useSubexprElimination =>
      Seq((false, false, false), (true, true, true), (null, true, false)).foreach {
        case (first, leafEvaluated, result) =>
          val leaf = ExprWithEvaluatedState()
          val cond = And(And(ExprWithEvaluatedState(first, isNullable = first == null),
            Not(leaf)), Not(leaf))
          val instance = GeneratePredicate.generate(cond, useSubexprElimination)
          instance.initialize(0)
          withClue(s"useSubexprElimination = $useSubexprElimination, first = $first: ") {
            assert(instance.eval(null) === result)
            assert(leaf.evaluated === leafEvaluated)
          }
      }
    }
  }
}

/**
 * An expression with evaluated state so we can know whether it is evaluated.
 */
case class ExprWithEvaluatedState(value: Any = false, isNullable: Boolean = false)
  extends LeafExpression with CodegenFallback {
  var evaluated: Boolean = false
  override def eval(input: InternalRow): Any = {
    evaluated = true
    value
  }

  override def nullable: Boolean = isNullable
  override def dataType: DataType = BooleanType
}
