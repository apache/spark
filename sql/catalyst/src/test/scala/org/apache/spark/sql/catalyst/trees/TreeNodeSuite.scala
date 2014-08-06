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

package org.apache.spark.sql.catalyst.trees

import scala.collection.mutable.ArrayBuffer

import org.scalatest.FunSuite

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.types.{StringType, NullType}

case class Dummy(optKey: Option[Expression]) extends Expression {
  def children = optKey.toSeq
  def references = Set.empty[Attribute]
  def nullable = true
  def dataType = NullType
  override lazy val resolved = true
  type EvaluatedType = Any
  def eval(input: Row) = null.asInstanceOf[Any]
}

class TreeNodeSuite extends FunSuite {
  test("top node changed") {
    val after = Literal(1) transform { case Literal(1, _) => Literal(2) }
    assert(after === Literal(2))
  }

  test("one child changed") {
    val before = Add(Literal(1), Literal(2))
    val after = before transform { case Literal(2, _) => Literal(1) }

    assert(after === Add(Literal(1), Literal(1)))
  }

  test("no change") {
    val before = Add(Literal(1), Add(Literal(2), Add(Literal(3), Literal(4))))
    val after = before transform { case Literal(5, _) => Literal(1)}

    assert(before === after)
    assert(before.map(_.id) === after.map(_.id))
  }

  test("collect") {
    val tree = Add(Literal(1), Add(Literal(2), Add(Literal(3), Literal(4))))
    val literals = tree collect {case l: Literal => l}

    assert(literals.size === 4)
    (1 to 4).foreach(i => assert(literals contains Literal(i)))
  }

  test("pre-order transform") {
    val actual = new ArrayBuffer[String]()
    val expected = Seq("+", "1", "*", "2", "-", "3", "4")
    val expression = Add(Literal(1), Multiply(Literal(2), Subtract(Literal(3), Literal(4))))
    expression transformDown {
      case b: BinaryExpression => actual.append(b.symbol); b
      case l: Literal => actual.append(l.toString); l
    }

    assert(expected === actual)
  }

  test("post-order transform") {
    val actual = new ArrayBuffer[String]()
    val expected = Seq("1", "2", "3", "4", "-", "*", "+")
    val expression = Add(Literal(1), Multiply(Literal(2), Subtract(Literal(3), Literal(4))))
    expression transformUp {
      case b: BinaryExpression => actual.append(b.symbol); b
      case l: Literal => actual.append(l.toString); l
    }

    assert(expected === actual)
  }

  test("transform works on nodes with Option children") {
    val dummy1 = Dummy(Some(Literal("1", StringType)))
    val dummy2 = Dummy(None)
    val toZero: PartialFunction[Expression, Expression] =  { case Literal(_, _) => Literal(0) }

    var actual = dummy1 transformDown toZero
    assert(actual === Dummy(Some(Literal(0))))

    actual = dummy1 transformUp toZero
    assert(actual === Dummy(Some(Literal(0))))

    actual = dummy2 transform toZero
    assert(actual === Dummy(None))
  }

}
