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
package catalyst
package expressions

import org.scalatest.FunSuite

import org.apache.spark.sql.catalyst.types._

/* Implicit conversions */
import org.apache.spark.sql.catalyst.dsl.expressions._


/**
 * Root class of expression evaluation test
 */
trait ExprEvalTest {
  type Execution = (Row => Row)

  def engine: Execution
}

case class InterpretExprEvalTest(exprs: Seq[Expression]) extends ExprEvalTest {
  override def engine: Execution = new InterpretedProjection(exprs)
}

class InterpretExpressionEvaluationSuite extends ExpressionEvaluationSuite {
  override def executor(exprs: Seq[Expression]) = InterpretExprEvalTest(exprs)
}

trait ExpressionEvaluationSuite extends FunSuite {
  /**
   * The sub classes need to create the ExprEvalTest object 
   */
  def executor(exprs: Seq[Expression]): ExprEvalTest
  
  val data: Row = new GenericRow(Array(1, null, 1.0, true, 4, 5, null, "abcccd"))

  // TODO add to DSL
  val c1 = BoundReference(0, AttributeReference("a", IntegerType)())
  val c2 = BoundReference(1, AttributeReference("b", IntegerType)())
  val c3 = BoundReference(2, AttributeReference("c", DoubleType)())
  val c4 = BoundReference(3, AttributeReference("d", BooleanType)())
  val c5 = BoundReference(4, AttributeReference("e", IntegerType)())
  val c6 = BoundReference(5, AttributeReference("f", IntegerType)())
  val c7 = BoundReference(6, AttributeReference("g", StringType)())
  val c8 = BoundReference(7, AttributeReference("h", StringType)())
  
  def verify(expected: Seq[(Boolean, Any)], result: Row, input: Row) {
    Seq.tabulate(expected.size) { i =>
      expected(i) match {
        case (false, expected) => {
          assert(result.isNullAt(i) == false, s"Input:($input), Output field:$i shouldn't be null")
          val real = result.apply(i)
          assert(real == expected, s"Input:($input), Output field:$i is expected as $expected, but got $real")
        }
        case (true, _) => {
          assert(result.isNullAt(i), s"Input:($input), Output field:$i is expected as null")
        }
      }
    }
  }

  def verify(expecteds: Seq[Seq[(Boolean, Any)]], results: Seq[Row], inputs: Seq[Row]) {
    Range(0, expecteds.length).foreach { i =>
      verify(expecteds(i), results(i), inputs(i))
    }
  }
  
  def run(exprs: Seq[Expression], expected: Seq[(Boolean, Any)], input: Row) {
    val tester = executor(exprs)
    verify(expected, tester.engine.apply(input), input)
  }
  
  def run(exprs: Seq[Expression], expecteds: Seq[Seq[(Boolean, Any)]], inputs: Seq[Row]) {
    val tester = executor(exprs)
    
    verify(expecteds, inputs.map(tester.engine.apply(_)), inputs)
  }
  
  test("logical") {
    val expected = Seq[(Boolean, Any)](
        (false, false), 
        (true, -1), 
        (false, true), 
        (false, true), 
        (false, false))

    val exprs = Seq[Expression](And(LessThan(Cast(c1, DoubleType), c3), LessThan(c1, c2)), 
      Or(LessThan(Cast(c1, DoubleType), c3), LessThan(c1, c2)),
      IsNull(c2),
      IsNotNull(c3),
      Not(c4))
    
    run(exprs, expected, data)
  }
  
  test("arithmetic") {
    val exprs = Array[Expression](
      Add(c1, c2),
      Add(c1, c5),
      Divide(c1, c5),
      Subtract(c1, c5),
      Multiply(c1, c5),
      Remainder(c1, c5),
      UnaryMinus(c1)
    )
    val expecteds = Seq[(Boolean, Any)](
        (true, 0), 
        (false, 5), 
        (false, 0), 
        (false, -3), 
        (false, 4),
        (false, 1),
        (false, -1))

    run(exprs, expecteds, data)
  }

  test("literals") {
    assert((Literal(1) + Literal(1)).apply(null) === 2)
  }

  /**
   * Checks for three-valued-logic.  Based on:
   * http://en.wikipedia.org/wiki/Null_(SQL)#Comparisons_with_NULL_and_the_three-valued_logic_.283VL.29
   *
   * p       q       p OR q  p AND q  p = q
   * True    True    True    True     True
   * True    False   True    False    False
   * True    Unknown True    Unknown  Unknown
   * False   True    True    False    False
   * False   False   False   False    True
   * False   Unknown Unknown False    Unknown
   * Unknown True    True    Unknown  Unknown
   * Unknown False   Unknown False    Unknown
   * Unknown Unknown Unknown Unknown  Unknown
   *
   * p       NOT p
   * True    False
   * False   True
   * Unknown Unknown
   */

  val b1 = BoundReference(0, AttributeReference("a", BooleanType)())
  val b2 = BoundReference(1, AttributeReference("b", BooleanType)())
  
  test("3VL Not") {
    val table = (true, false) :: (false, true) :: (null, null) :: Nil

    val exprs = Array[Expression](Not(b1))
    val inputs = table.map { case(v, answer) => new GenericRow(Array(v)) }
    val expected = table.map { case(v, answer) => Seq((answer == null, answer)) }
    
    run(exprs, expected, inputs)
  }

  test("3VL AND") {
    val table = (true,  true,  true) ::
    (true,  false, false) ::
    (true,  null,  null) ::
    (false, true,  false) ::
    (false, false, false) ::
    (false, null,  false) ::
    (null,  true,  null) ::
    (null,  false, false) ::
    (null,  null,  null) :: Nil
    
    val exprs = Seq[Expression](And(b1, b2))
    val inputs = table.map { case(v1, v2, answer) => new GenericRow(Array(v1, v2)) }
    val expected = table.map { case(v1, v2, answer) => Seq((answer == null, answer)) }
    
    run(exprs, expected, inputs)
  }

  test("3VL OR") {
    val table = (true,  true,  true) ::
    (true,  false, true) ::
    (true,  null,  true) ::
    (false, true,  true) ::
    (false, false, false) ::
    (false, null,  null) ::
    (null,  true,  true) ::
    (null,  false, null) ::
    (null,  null,  null) :: Nil
    
    val exprs = Array[Expression](Or(b1, b2))
    val inputs = table.map { case(v1, v2, answer) => new GenericRow(Array(v1, v2)) }
    val expected = table.map { case(v1, v2, answer) => Seq((answer == null, answer)) }
    
    run(exprs, expected, inputs)
  }
    
  test("3VL Equals") {
    val table = (true,  true,  true) ::
    (true,  false, false) ::
    (true,  null,  null) ::
    (false, true,  false) ::
    (false, false, true) ::
    (false, null,  null) ::
    (null,  true,  null) ::
    (null,  false, null) ::
    (null,  null,  null) :: Nil
    
    val exprs = Array[Expression](Equals(b1, b2))
    val inputs = table.map { case(v1, v2, answer) => new GenericRow(Array(v1, v2)) }
    val expected = table.map { case(v1, v2, answer) => Seq((answer == null, answer)) }
    
    run(exprs, expected, inputs)
  }
}
