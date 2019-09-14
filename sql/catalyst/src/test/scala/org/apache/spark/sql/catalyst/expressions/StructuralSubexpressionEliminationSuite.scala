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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext

class StructuralSubexpressionEliminationSuite extends SparkFunSuite {
  test("Structurally Expression Equivalence") {
    val equivalence = new EquivalentExpressions
    assert(equivalence.getAllStructuralExpressions.isEmpty)

    val oneA = Literal(1)
    val oneB = Literal(1)
    val twoA = Literal(2)

    assert(equivalence.getStructurallyEquivalentExprs(oneA).isEmpty)
    assert(equivalence.getStructurallyEquivalentExprs(twoA).isEmpty)

    // Add oneA and test if it is returned. Since it is a group of one, it does not.
    equivalence.addStructExpr(oneA)
    assert(equivalence.getStructurallyEquivalentExprs(oneA).size == 1)
    assert(equivalence.getStructurallyEquivalentExprs(oneA)(0).size == 1)

    assert(equivalence.getStructurallyEquivalentExprs(twoA).isEmpty)
    equivalence.addStructExpr(oneA)
    assert(equivalence.getStructurallyEquivalentExprs(oneA).size == 1)
    assert(equivalence.getStructurallyEquivalentExprs(oneA)(0).size == 2)

    // Add B and make sure they can see each other.
    equivalence.addStructExpr(oneB)
    // Use exists and reference equality because of how equals is defined.
    assert(equivalence.getStructurallyEquivalentExprs(oneA).flatten.exists(_ eq oneB))
    assert(equivalence.getStructurallyEquivalentExprs(oneA).flatten.exists(_ eq oneA))
    assert(equivalence.getStructurallyEquivalentExprs(oneB).flatten.exists(_ eq oneA))
    assert(equivalence.getStructurallyEquivalentExprs(oneB).flatten.exists(_ eq oneB))
    assert(equivalence.getStructurallyEquivalentExprs(twoA).isEmpty)

    assert(equivalence.getAllStructuralExpressions.size == 1)
    assert(equivalence.getAllStructuralExpressions.values.head.values.flatten.toSeq.size == 3)
    assert(equivalence.getAllStructuralExpressions.values.head.values.flatten.toSeq.contains(oneA))
    assert(equivalence.getAllStructuralExpressions.values.head.values.flatten.toSeq.contains(oneB))

    val add1 = Add(oneA, oneB)
    val add2 = Add(oneA, oneB)

    equivalence.addStructExpr(add1)
    equivalence.addStructExpr(add2)

    assert(equivalence.getAllStructuralExpressions.size == 2)
    assert(equivalence.getStructurallyEquivalentExprs(add2).flatten.exists(_ eq add1))
    assert(equivalence.getStructurallyEquivalentExprs(add2).flatten.size == 2)
    assert(equivalence.getStructurallyEquivalentExprs(add1).flatten.exists(_ eq add2))
  }

  test("Expression equivalence - non deterministic") {
    val sum = Add(Rand(0), Rand(0))
    val equivalence = new EquivalentExpressions
    equivalence.addStructExpr(sum)
    equivalence.addStructExpr(sum)
    assert(equivalence.getAllStructuralExpressions.isEmpty)
  }

  test("CodegenFallback and children") {
    val one = Literal(1)
    val two = Literal(2)
    val add = Add(one, two)
    val fallback = CodegenFallbackExpression(add)
    val add2 = Add(add, fallback)

    val equivalence = new EquivalentExpressions
    equivalence.addStructuralExprTree(add2)
    // `fallback` and the `add` inside should not be added
    assert(equivalence.getAllStructuralExpressions.values
      .map(_.values.count(_.size > 1)).sum == 0)
    assert(equivalence.getAllStructuralExpressions.values
      .map(_.values.count(_.size == 1)).sum == 2)  // add, add2
  }

  test("Children of conditional expressions") {
    val condition = And(Literal(true), Literal(false))
    val add = Add(Literal(1), Literal(2))
    val ifExpr = If(condition, add, add)

    val equivalence = new EquivalentExpressions
    equivalence.addStructuralExprTree(ifExpr)
    // the `add` inside `If` should not be added
    assert(equivalence.getAllStructuralExpressions.values
      .map(_.values.count(_.size > 1)).sum == 0)
    // only ifExpr and its predicate expression
    assert(equivalence.getAllStructuralExpressions.values
      .map(_.values.count(_.size == 1)).sum == 2)
  }

  test("Expressions not for structural expr elimination can go non-structural mode") {
    val fallback1 = CodegenFallbackExpression(Literal(1))
    val fallback2 = CodegenFallbackExpression(Literal(1))

    val ctx = new CodegenContext()
    ctx.generateExpressions(Seq(fallback1, fallback2), doSubexpressionElimination = true)
    assert(ctx.equivalentExpressions.getAllStructuralExpressions.isEmpty)
    assert(ctx.equivalentExpressions.getEquivalentExprs(fallback1).length == 2)
  }
}
