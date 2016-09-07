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

import java.math.BigInteger
import java.util.UUID

import scala.collection.mutable.ArrayBuffer

import org.json4s.jackson.JsonMethods

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.dsl.expressions.DslString
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, Union}
import org.apache.spark.sql.types.{BooleanType, IntegerType, Metadata, NullType, StringType}
import org.apache.spark.storage.StorageLevel

case class Dummy(optKey: Option[Expression]) extends Expression with CodegenFallback {
  override def children: Seq[Expression] = optKey.toSeq
  override def nullable: Boolean = true
  override def dataType: NullType = NullType
  override lazy val resolved = true
  override def eval(input: InternalRow): Any = null.asInstanceOf[Any]
}

case class ComplexPlan(exprs: Seq[Seq[Expression]])
  extends org.apache.spark.sql.catalyst.plans.logical.LeafNode {
  override def output: Seq[Attribute] = Nil
}

case class ExpressionInMap(map: Map[String, Expression]) extends Expression with Unevaluable {
  override def children: Seq[Expression] = map.values.toSeq
  override def nullable: Boolean = true
  override def dataType: NullType = NullType
  override lazy val resolved = true
}

case class JsonTestTreeNode(arg: Any) extends LeafNode {
  override def output: Seq[Attribute] = Seq.empty[Attribute]
}

case class NameValue(name: String, value: Any)

case object DummyObject

case class SelfReferenceUDF(
    var config: Map[String, Any] = Map.empty[String, Any]) extends Function1[String, Boolean] {
  config += "self" -> this
  def apply(key: String): Boolean = config.contains(key)
}

class TreeNodeSuite extends SparkFunSuite {
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
    // Ensure that the objects after are the same objects before the transformation.
    before.map(identity[Expression]).zip(after.map(identity[Expression])).foreach {
      case (b, a) => assert(b eq a)
    }
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
      case b: BinaryOperator => actual.append(b.symbol); b
      case l: Literal => actual.append(l.toString); l
    }

    assert(expected === actual)
  }

  test("post-order transform") {
    val actual = new ArrayBuffer[String]()
    val expected = Seq("1", "2", "3", "4", "-", "*", "+")
    val expression = Add(Literal(1), Multiply(Literal(2), Subtract(Literal(3), Literal(4))))
    expression transformUp {
      case b: BinaryOperator => actual.append(b.symbol); b
      case l: Literal => actual.append(l.toString); l
    }

    assert(expected === actual)
  }

  test("transform works on nodes with Option children") {
    val dummy1 = Dummy(Some(Literal.create("1", StringType)))
    val dummy2 = Dummy(None)
    val toZero: PartialFunction[Expression, Expression] = { case Literal(_, _) => Literal(0) }

    var actual = dummy1 transformDown toZero
    assert(actual === Dummy(Some(Literal(0))))

    actual = dummy1 transformUp toZero
    assert(actual === Dummy(Some(Literal(0))))

    actual = dummy2 transform toZero
    assert(actual === Dummy(None))
  }

  test("preserves origin") {
    CurrentOrigin.setPosition(1, 1)
    val add = Add(Literal(1), Literal(1))
    CurrentOrigin.reset()

    val transformed = add transform {
      case Literal(1, _) => Literal(2)
    }

    assert(transformed.origin.line.isDefined)
    assert(transformed.origin.startPosition.isDefined)
  }

  test("foreach up") {
    val actual = new ArrayBuffer[String]()
    val expected = Seq("1", "2", "3", "4", "-", "*", "+")
    val expression = Add(Literal(1), Multiply(Literal(2), Subtract(Literal(3), Literal(4))))
    expression foreachUp {
      case b: BinaryOperator => actual.append(b.symbol);
      case l: Literal => actual.append(l.toString);
    }

    assert(expected === actual)
  }

  test("find") {
    val expression = Add(Literal(1), Multiply(Literal(2), Subtract(Literal(3), Literal(4))))
    // Find the top node.
    var actual: Option[Expression] = expression.find {
      case add: Add => true
      case other => false
    }
    var expected: Option[Expression] =
      Some(Add(Literal(1), Multiply(Literal(2), Subtract(Literal(3), Literal(4)))))
    assert(expected === actual)

    // Find the first children.
    actual = expression.find {
      case Literal(1, IntegerType) => true
      case other => false
    }
    expected = Some(Literal(1))
    assert(expected === actual)

    // Find an internal node (Subtract).
    actual = expression.find {
      case sub: Subtract => true
      case other => false
    }
    expected = Some(Subtract(Literal(3), Literal(4)))
    assert(expected === actual)

    // Find a leaf node.
    actual = expression.find {
      case Literal(3, IntegerType) => true
      case other => false
    }
    expected = Some(Literal(3))
    assert(expected === actual)

    // Find nothing.
    actual = expression.find {
      case Literal(100, IntegerType) => true
      case other => false
    }
    expected = None
    assert(expected === actual)
  }

  test("collectFirst") {
    val expression = Add(Literal(1), Multiply(Literal(2), Subtract(Literal(3), Literal(4))))

    // Collect the top node.
    {
      val actual = expression.collectFirst {
        case add: Add => add
      }
      val expected =
        Some(Add(Literal(1), Multiply(Literal(2), Subtract(Literal(3), Literal(4)))))
      assert(expected === actual)
    }

    // Collect the first children.
    {
      val actual = expression.collectFirst {
        case l @ Literal(1, IntegerType) => l
      }
      val expected = Some(Literal(1))
      assert(expected === actual)
    }

    // Collect an internal node (Subtract).
    {
      val actual = expression.collectFirst {
        case sub: Subtract => sub
      }
      val expected = Some(Subtract(Literal(3), Literal(4)))
      assert(expected === actual)
    }

    // Collect a leaf node.
    {
      val actual = expression.collectFirst {
        case l @ Literal(3, IntegerType) => l
      }
      val expected = Some(Literal(3))
      assert(expected === actual)
    }

    // Collect nothing.
    {
      val actual = expression.collectFirst {
        case l @ Literal(100, IntegerType) => l
      }
      val expected = None
      assert(expected === actual)
    }
  }

  test("transformExpressions on nested expression sequence") {
    val plan = ComplexPlan(Seq(Seq(Literal(1)), Seq(Literal(2))))
    val actual = plan.transformExpressions {
      case Literal(value, _) => Literal(value.toString)
    }
    val expected = ComplexPlan(Seq(Seq(Literal("1")), Seq(Literal("2"))))
    assert(expected === actual)
  }

  test("expressions inside a map") {
    val expression = ExpressionInMap(Map("1" -> Literal(1), "2" -> Literal(2)))

    {
      val actual = expression.transform {
        case Literal(i: Int, _) => Literal(i + 1)
      }
      val expected = ExpressionInMap(Map("1" -> Literal(2), "2" -> Literal(3)))
      assert(actual === expected)
    }

    {
      val actual = expression.withNewChildren(Seq(Literal(2), Literal(3)))
      val expected = ExpressionInMap(Map("1" -> Literal(2), "2" -> Literal(3)))
      assert(actual === expected)
    }
  }

  test("toJSON") {
    def expected(json: String): String = {
      s"""[{"class":"${classOf[JsonTestTreeNode].getName}","num-children":0,"arg":$json}]"""
    }
    // Converts simple types to JSON
    compareJSON(JsonTestTreeNode(true).toJSON, expected("true"))
    compareJSON(JsonTestTreeNode(33.toByte).toJSON, expected("33"))
    compareJSON(JsonTestTreeNode(44).toJSON, expected("44"))
    compareJSON(JsonTestTreeNode(55L).toJSON, expected("55"))
    compareJSON(JsonTestTreeNode(3.0).toJSON, expected("3.0"))
    compareJSON(JsonTestTreeNode(4.0D).toJSON, expected("4.0"))
    compareJSON(JsonTestTreeNode(BigInt(BigInteger.valueOf(88L))).toJSON, expected("88"))
    compareJSON(JsonTestTreeNode(null).toJSON, expected("null"))
    compareJSON(JsonTestTreeNode("text").toJSON, expected("\"text\""))
    compareJSON(JsonTestTreeNode(Some("text")).toJSON, expected("\"text\""))
    compareJSON(JsonTestTreeNode(None).toJSON,
      s"""[
         |  {
         |    "class":"${classOf[JsonTestTreeNode].getName}",
         |    "num-children":0
         |  }
         |]
       """.stripMargin)

    val uuid = UUID.randomUUID()
    compareJSON(JsonTestTreeNode(uuid).toJSON, expected("\"" + uuid.toString + "\""))

    // Converts some Spark Sql types to JSON
    compareJSON(JsonTestTreeNode(IntegerType).toJSON, expected("\"integer\""))
    compareJSON(JsonTestTreeNode(Metadata.empty).toJSON, expected(Metadata.empty.json))
    compareJSON(JsonTestTreeNode(StorageLevel.NONE).toJSON,
      expected(
        """{
          |  "useDisk":false,
          |  "useMemory":false,
          |  "useOffHeap":false,
          |  "deserialized":false,
          |  "replication":1
          |}
        """.stripMargin))

    // Converts TreeNode argument to JSON
    compareJSON(JsonTestTreeNode(Literal(333)).toJSON,
      expected(
        """[
          |  {
          |    "class":"org.apache.spark.sql.catalyst.expressions.Literal",
          |    "num-children":0,
          |    "value":"333",
          |    "dataType":"integer"
          |  }
          |]
        """.stripMargin))

    // Converts case object to JSON
    compareJSON(JsonTestTreeNode(DummyObject).toJSON,
      expected("{\"object\":\"org.apache.spark.sql.catalyst.trees.DummyObject$\"}"))

    // Convert Case class to JSON
    val bigValue = new Array[Int](10000)
    compareJSON(
      JsonTestTreeNode(NameValue("name", bigValue)).toJSON,
      expected(
        """
          |{
          |  "product-class":"org.apache.spark.sql.catalyst.trees.NameValue",
          |  "name":"name"
          |}
        """.stripMargin))

    // Converts Seq[TreeNode] to JSON recursively
    compareJSON(
      JsonTestTreeNode(Seq(Literal(1), Literal(2))).toJSON,
      expected(
        """
          |[
          |  [
          |    {
          |      "class":"org.apache.spark.sql.catalyst.expressions.Literal",
          |      "num-children":0,
          |      "value":"1",
          |      "dataType":"integer"
          |    }
          |  ],
          |  [
          |    {
          |      "class":"org.apache.spark.sql.catalyst.expressions.Literal",
          |      "num-children":0,
          |      "value":"2",
          |      "dataType":"integer"
          |    }
          |  ]
          |]
        """.stripMargin
      ))

    // Other Seq is converted to JNull, to reduce the risk of out of memory
    compareJSON(JsonTestTreeNode(Seq(1, 2, 3)).toJSON, expected("null"))

    // All Map type is converted to JNull, to reduce the risk of out of memory
    compareJSON(JsonTestTreeNode(Map("key" -> "value")).toJSON, expected("null"))

    // Unknown type is converted to JNull, to reduce the risk of out of memory
    compareJSON(JsonTestTreeNode(new Object {}).toJSON, expected("null"))

    // Convert all TreeNode children to JSON
    compareJSON(
      Union(Seq(JsonTestTreeNode("0"), JsonTestTreeNode("1"))).toJSON,
      """
        |[
        |  {
        |    "class":"org.apache.spark.sql.catalyst.plans.logical.Union",
        |    "num-children":2,
        |    "children":[0, 1]
        |  },
        |  {
        |    "class":"org.apache.spark.sql.catalyst.trees.JsonTestTreeNode",
        |    "num-children":0,
        |    "arg":"0"
        |  },
        |  {
        |    "class":"org.apache.spark.sql.catalyst.trees.JsonTestTreeNode",
        |    "num-children":0,
        |    "arg":"1"
        |  }
        |]
      """.stripMargin
    )
  }

  test("toJSON should not throws java.lang.StackOverflowError") {
    val udf = ScalaUDF(SelfReferenceUDF(), BooleanType, Seq("col1".attr))
    // Should not throw java.lang.StackOverflowError
    udf.toJSON
  }

  private def compareJSON(leftJson: String, rightJson: String): Unit = {
    val left = JsonMethods.parse(leftJson)
    val right = JsonMethods.parse(rightJson)
    assert(left == right)
  }
}
