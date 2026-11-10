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
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{BooleanType, DataType, IntegerType}

/**
 * A test suite that makes sure code generation handles sub-expression elimination correctly.
 */
class CodegenSubexpressionEliminationSuite extends SparkFunSuite with SQLHelper {

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
    // the evaluation, while a TRUE or a NULL one does not, as `And.eval` does. With the
    // elimination, `leaf` is evaluated once for both conjuncts, otherwise once per conjunct.
    Seq(true, false).foreach { useSubexprElimination =>
      Seq((false, 0, false), (true, 1, true), (null, 1, false)).foreach {
        case (first, leafEvaluations, result) =>
          // Distinct `id`s, so that the first conjunct and the leaf are not the same expression.
          val leaf = ExprWithEvaluatedState(id = 1)
          val cond = And(And(ExprWithEvaluatedState(first, isNullable = first == null),
            Not(leaf)), Not(leaf))
          val instance = GeneratePredicate.generate(cond, useSubexprElimination)
          instance.initialize(0)
          withClue(s"useSubexprElimination = $useSubexprElimination, first = $first: ") {
            assert(instance.eval(null) === result)
            val expected = if (useSubexprElimination) leafEvaluations else 2 * leafEvaluations
            assert(leaf.evaluations === expected)
          }
      }
    }
  }

  test("SPARK-60332: GeneratePredicate evaluates a common subexpression that one conjunct " +
      "repeats after the conjuncts before it") {
    // `Not(leaf)` is repeated only by the second conjunct, and evaluated just before it, so a
    // FALSE first conjunct stops the evaluation of `leaf`, as `And.eval` does.
    Seq(true, false).foreach { useSubexprElimination =>
      Seq((false, 0, false), (true, 1, true), (null, 1, false)).foreach {
        case (first, leafEvaluations, result) =>
          val leaf = ExprWithEvaluatedState(id = 1)
          val cond = And(ExprWithEvaluatedState(first, isNullable = first == null),
            EqualTo(Not(leaf), Not(leaf)))
          val instance = GeneratePredicate.generate(cond, useSubexprElimination)
          instance.initialize(0)
          withClue(s"useSubexprElimination = $useSubexprElimination, first = $first: ") {
            assert(instance.eval(null) === result)
            val expected = if (useSubexprElimination) leafEvaluations else 2 * leafEvaluations
            assert(leaf.evaluations === expected)
          }
      }
    }
  }

  test("SPARK-60332: GeneratePredicate evaluates nested common subexpressions in the order of " +
      "the conjuncts that first reference them") {
    // The nested ANDs flatten into the conjuncts `first, inner, inner, outer, outer`, so
    // `inner = Not(leaf)` is first referenced by the second conjunct and `outer`, which contains
    // it, by the fourth. Each is evaluated once, just before its conjunct, the inner one first, so
    // the outer one reads the evaluated inner one, not its default. A FALSE first conjunct
    // evaluates neither.
    Seq((false, 0, false), (true, 1, true)).foreach { case (first, leafEvaluations, result) =>
      val leaf = ExprWithEvaluatedState(id = 1)
      val leaf2 = ExprWithEvaluatedState(id = 2)
      val inner = Not(leaf)
      val outer = Or(inner, leaf2)
      val cond = And(And(And(ExprWithEvaluatedState(first), inner), And(inner, outer)), outer)
      val instance = GeneratePredicate.generate(cond, useSubexprElimination = true)
      instance.initialize(0)
      withClue(s"first = $first: ") {
        assert(instance.eval(null) === result)
        assert(leaf.evaluations === leafEvaluations)
        // `Or(TRUE, leaf2)` never evaluates `leaf2`.
        assert(leaf2.evaluations === 0)
      }
    }
  }

  test("SPARK-60332: GeneratePredicate evaluates a contained common subexpression before the " +
      "conjunct that first references the containing one") {
    // `(a + b) + c` is common to the second and the third conjuncts, and the IF branch of the
    // first one holds `(a + c) + b`, equal to it under the canonicalization of `+`, but without
    // `a + b`, which is common to the last three conjuncts. The function of `(a + b) + c` reads the
    // state of `a + b`, so `a + b` is evaluated before the first conjunct, which references the
    // containing subexpression, not before the second, which is the first to reference `a + b`:
    // otherwise `(a + b) + c` would read the default or the previous row's `a + b`.
    val Seq(a, b, c, d, x) = (0 to 4).map(i => BoundReference(i, IntegerType, nullable = true))
    val cond = And(And(And(
      GreaterThan(If(GreaterThan(x, Literal(0)), Add(Add(a, c), b), d), Literal(0)),
      GreaterThan(Add(Add(a, b), c), Literal(1))),
      LessThan(Add(Add(a, b), c), Literal(10))),
      GreaterThan(Add(a, b), Literal(5)))
    val instance = GeneratePredicate.generate(cond, useSubexprElimination = true)
    instance.initialize(0)
    // Rows with different values, in both orders, so that a stale state shows. The last one
    // takes the IF branch and passes every conjunct, so a stale `(a + b) + c` or `a + b` from the
    // row before it, -4 or -5, would drop it.
    val rows = Seq(InternalRow(3, 3, 1, 1, 0), InternalRow(1, 1, 1, 1, 1),
      InternalRow(3, 3, 1, 1, 0), InternalRow(-5, 0, 1, 1, 0), InternalRow(3, 3, 1, 1, 1))
    rows.foreach { row =>
      assert(instance.eval(row) === cond.eval(row), row)
    }
  }

  test("SPARK-60332: GeneratePredicate eliminates the subexpressions of the whole predicate") {
    // The common subexpressions are found in the whole predicate, so that the same ones are
    // eliminated as before: with `skipForShortcutExpr`, only those of the first conjunct. Here
    // `Not(leaf)` is common to the last two conjuncts only, so it is not eliminated, and a FALSE
    // first conjunct still evaluates neither.
    withSQLConf(SQLConf.SUBEXPRESSION_ELIMINATION_SKIP_FOR_SHORTCUT_EXPR.key -> "true") {
      Seq((false, 0, false), (true, 2, true)).foreach { case (first, leafEvaluations, result) =>
        val leaf = ExprWithEvaluatedState(id = 1)
        val cond = And(And(ExprWithEvaluatedState(first), Not(leaf)), Not(leaf))
        val instance = GeneratePredicate.generate(cond, useSubexprElimination = true)
        instance.initialize(0)
        withClue(s"first = $first: ") {
          assert(instance.eval(null) === result)
          assert(leaf.evaluations === leafEvaluations)
        }
      }
    }
  }

  test("SPARK-60332: GeneratePredicate splits the checks of many conjuncts into functions") {
    // Several hundred conjuncts, with a common `a + 1` in two of them, whose checks exceed the
    // length at which they are split into functions, each returning false at a FALSE conjunct.
    val a = BoundReference(0, IntegerType, nullable = true)
    val conjuncts = (0 until 400).map(k => GreaterThan(Add(a, Literal(k)), Literal(0))) :+
      GreaterThan(Add(a, Literal(1)), Literal(-1))
    val cond = conjuncts.reduce[Expression](And)
    withSQLConf(SQLConf.CODEGEN_METHOD_SPLIT_THRESHOLD.key -> "1000") {
      val instance = GeneratePredicate.generate(cond, useSubexprElimination = true)
      instance.initialize(0)
      // All TRUE, a FALSE one, and NULL ones, then all TRUE again, which the NULL conjuncts of
      // the row before it must not affect.
      Seq(InternalRow(1), InternalRow(-1000), InternalRow(null), InternalRow(1)).foreach { row =>
        val expected = cond.eval(row)
        assert(instance.eval(row) === (expected != null && expected.asInstanceOf[Boolean]), row)
      }
    }
  }

  test("SPARK-60332: GeneratePredicate splits the evaluation of many common subexpressions " +
      "into functions") {
    // The second conjunct, an OR of the 100 `Not(leaf)`, is the first to reference each of them,
    // and the 100 conjuncts after it, the ANDs of the same `Not(leaf)` flattened, reference them
    // again. The calls of the functions of all of them, before the second conjunct, exceed the
    // length at which they are split into functions of their own. Each leaf is evaluated once.
    Seq((false, 0, false), (true, 1, true)).foreach { case (first, leafEvaluations, result) =>
      val leaves = (1 to 100).map(i => ExprWithEvaluatedState(id = i))
      val nots = leaves.map(leaf => Not(leaf): Expression)
      val cond = And(And(ExprWithEvaluatedState(first), nots.reduce(Or)), nots.reduce(And))
      val instance = GeneratePredicate.generate(cond, useSubexprElimination = true)
      instance.initialize(0)
      withClue(s"first = $first: ") {
        assert(instance.eval(null) === result)
        assert(leaves.forall(_.evaluations == leafEvaluations))
      }
    }
  }

  test("SPARK-60332: GeneratePredicate keeps an AND of the top-level chain that is a common " +
      "subexpression as one conjunct") {
    // `common`, an AND of 3 `Not(leaf)`, is repeated under NOT in the second conjunct, so it is
    // eliminated as a whole, as before, and its own occurrence in the chain stays one conjunct
    // that reads the state instead of being split into conjuncts that recompute the `Not(leaf)`.
    Seq((false, 0, false), (true, 1, true)).foreach { case (first, leafEvaluations, result) =>
      val leaves = (1 to 3).map(i => ExprWithEvaluatedState(id = i))
      val common = leaves.map(leaf => Not(leaf): Expression).reduce(And)
      val cond = And(And(ExprWithEvaluatedState(first), Not(Not(common))), common)
      val instance = GeneratePredicate.generate(cond, useSubexprElimination = true)
      instance.initialize(0)
      withClue(s"first = $first: ") {
        assert(instance.eval(null) === result)
        assert(leaves.forall(_.evaluations == leafEvaluations))
      }
    }
  }
}

/**
 * An expression with evaluated state so we can know whether it is evaluated.
 */
case class ExprWithEvaluatedState(value: Any = false, isNullable: Boolean = false, id: Int = 0)
  extends LeafExpression with CodegenFallback {
  var evaluations: Int = 0
  def evaluated: Boolean = evaluations > 0
  override def eval(input: InternalRow): Any = {
    evaluations += 1
    value
  }

  override def nullable: Boolean = isNullable
  override def dataType: DataType = BooleanType
}
