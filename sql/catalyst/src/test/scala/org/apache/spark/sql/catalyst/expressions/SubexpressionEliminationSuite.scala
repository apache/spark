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
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types.{DataType, IntegerType}

class SubexpressionEliminationSuite extends SparkFunSuite {
  test("Semantic equals and hash") {
    val a: AttributeReference = AttributeReference("name", IntegerType)()
    val id = {
      // Make sure we use a "ExprId" different from "a.exprId"
      val _id = ExprId(1)
      if (a.exprId == _id) ExprId(2) else _id
    }
    val b1 = a.withName("name2").withExprId(id)
    val b2 = a.withExprId(id)
    val b3 = a.withQualifier(Seq("qualifierName"))

    assert(b1 != b2)
    assert(a != b1)
    assert(b1.semanticEquals(b2))
    assert(!b1.semanticEquals(a))
    assert(a.hashCode != b1.hashCode)
    assert(b1.hashCode != b2.hashCode)
    assert(b1.semanticHash() == b2.semanticHash())
    assert(a != b3)
    assert(a.hashCode != b3.hashCode)
    assert(a.semanticEquals(b3))
  }

  test("Expression Equivalence - basic") {
    val equivalence = new EquivalentExpressions
    assert(equivalence.getAllEquivalentExprs.isEmpty)

    val oneA = Literal(1)
    val oneB = Literal(1)
    val twoA = Literal(2)
    var twoB = Literal(2)

    assert(equivalence.getEquivalentExprs(oneA).isEmpty)
    assert(equivalence.getEquivalentExprs(twoA).isEmpty)

    // Add oneA and test if it is returned. Since it is a group of one, it does not.
    assert(!equivalence.addExpr(oneA))
    assert(equivalence.getEquivalentExprs(oneA).size == 1)
    assert(equivalence.getEquivalentExprs(twoA).isEmpty)
    assert(equivalence.addExpr((oneA)))
    assert(equivalence.getEquivalentExprs(oneA).size == 2)

    // Add B and make sure they can see each other.
    assert(equivalence.addExpr(oneB))
    // Use exists and reference equality because of how equals is defined.
    assert(equivalence.getEquivalentExprs(oneA).exists(_ eq oneB))
    assert(equivalence.getEquivalentExprs(oneA).exists(_ eq oneA))
    assert(equivalence.getEquivalentExprs(oneB).exists(_ eq oneA))
    assert(equivalence.getEquivalentExprs(oneB).exists(_ eq oneB))
    assert(equivalence.getEquivalentExprs(twoA).isEmpty)
    assert(equivalence.getAllEquivalentExprs.size == 1)
    assert(equivalence.getAllEquivalentExprs.head.size == 3)
    assert(equivalence.getAllEquivalentExprs.head.contains(oneA))
    assert(equivalence.getAllEquivalentExprs.head.contains(oneB))

    val add1 = Add(oneA, oneB)
    val add2 = Add(oneA, oneB)

    equivalence.addExpr(add1)
    equivalence.addExpr(add2)

    assert(equivalence.getAllEquivalentExprs.size == 2)
    assert(equivalence.getEquivalentExprs(add2).exists(_ eq add1))
    assert(equivalence.getEquivalentExprs(add2).size == 2)
    assert(equivalence.getEquivalentExprs(add1).exists(_ eq add2))
  }

  test("Expression Equivalence - Trees") {
    val one = Literal(1)
    val two = Literal(2)

    val add = Add(one, two)
    val abs = Abs(add)
    val add2 = Add(add, add)

    var equivalence = new EquivalentExpressions
    equivalence.addExprTree(add)
    equivalence.addExprTree(abs)
    equivalence.addExprTree(add2)

    // Should only have one equivalence for `one + two`
    assert(equivalence.getAllEquivalentExprs.count(_.size > 1) == 1)
    assert(equivalence.getAllEquivalentExprs.filter(_.size > 1).head.size == 4)

    // Set up the expressions
    //   one * two,
    //   (one * two) * (one * two)
    //   sqrt( (one * two) * (one * two) )
    //   (one * two) + sqrt( (one * two) * (one * two) )
    equivalence = new EquivalentExpressions
    val mul = Multiply(one, two)
    val mul2 = Multiply(mul, mul)
    val sqrt = Sqrt(mul2)
    val sum = Add(mul2, sqrt)
    equivalence.addExprTree(mul)
    equivalence.addExprTree(mul2)
    equivalence.addExprTree(sqrt)
    equivalence.addExprTree(sum)

    // (one * two), (one * two) * (one * two) and sqrt( (one * two) * (one * two) ) should be found
    assert(equivalence.getAllEquivalentExprs.count(_.size > 1) == 3)
    assert(equivalence.getEquivalentExprs(mul).size == 3)
    assert(equivalence.getEquivalentExprs(mul2).size == 3)
    assert(equivalence.getEquivalentExprs(sqrt).size == 2)
    assert(equivalence.getEquivalentExprs(sum).size == 1)
  }

  test("Expression equivalence - non deterministic") {
    val sum = Add(Rand(0), Rand(0))
    val equivalence = new EquivalentExpressions
    equivalence.addExpr(sum)
    equivalence.addExpr(sum)
    assert(equivalence.getAllEquivalentExprs.isEmpty)
  }

  test("Children of CodegenFallback") {
    val one = Literal(1)
    val two = Add(one, one)
    val fallback = CodegenFallbackExpression(two)
    val add = Add(two, fallback)

    val equivalence = new EquivalentExpressions
    equivalence.addExprTree(add)
    // the `two` inside `fallback` should not be added
    assert(equivalence.getAllEquivalentExprs.count(_.size > 1) == 0)
    assert(equivalence.getAllEquivalentExprs.count(_.size == 1) == 3)  // add, two, explode
  }

  test("Children of conditional expressions") {
    val condition = And(Literal(true), Literal(false))
    val add = Add(Literal(1), Literal(2))
    val ifExpr = If(condition, add, add)

    val equivalence = new EquivalentExpressions
    equivalence.addExprTree(ifExpr)
    // the `add` inside `If` should not be added
    assert(equivalence.getAllEquivalentExprs.count(_.size > 1) == 0)
    // only ifExpr and its predicate expression
    assert(equivalence.getAllEquivalentExprs.count(_.size == 1) == 2)
  }
}

case class CodegenFallbackExpression(child: Expression)
  extends UnaryExpression with CodegenFallback {
  override def dataType: DataType = child.dataType
}
