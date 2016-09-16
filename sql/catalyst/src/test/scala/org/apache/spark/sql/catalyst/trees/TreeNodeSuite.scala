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
import org.apache.spark.sql.catalyst.catalog.{BucketSpec, CatalogStorageFormat, CatalogTable, CatalogTableType, FunctionResource, JarResource}
import org.apache.spark.sql.catalyst.dsl.expressions.DslString
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.plans.{LeftOuter, NaturalJoin}
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, Union}
import org.apache.spark.sql.catalyst.plans.physical.{IdentityBroadcastMode, RoundRobinPartitioning, SinglePartition}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.types.{BooleanType, DoubleType, FloatType, IntegerType, Metadata, NullType, StringType, StructField, StructType}
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
    def assertJSON(input: Any, json: String): Unit = {
      val expected =
        s"""[{"class":"${classOf[JsonTestTreeNode].getName}","num-children":0,"arg":$json}]"""
      compareJSON(JsonTestTreeNode(input).toJSON, expected)
    }

    // Converts simple types to JSON
    assertJSON(true, "true")
    assertJSON(33.toByte, "33")
    assertJSON(44, "44")
    assertJSON(55L, "55")
    assertJSON(3.0, "3.0")
    assertJSON(4.0D, "4.0")
    assertJSON(BigInt(BigInteger.valueOf(88L)), "88")
    assertJSON(null, "null")
    assertJSON("text", "\"text\"")
    assertJSON(Some("text"), "\"text\"")
    compareJSON(JsonTestTreeNode(None).toJSON,
      s"""[
         |  {
         |    "class": "${classOf[JsonTestTreeNode].getName}",
         |    "num-children": 0
         |  }
         |]
       """.stripMargin)

    val uuid = UUID.randomUUID()
    assertJSON(uuid, "\"" + uuid.toString + "\"")

    // Converts Spark Sql DataType to JSON
    assertJSON(IntegerType, "\"integer\"")
    assertJSON(Metadata.empty, Metadata.empty.json)
    assertJSON(
      StorageLevel.NONE,
      """{
        |  "useDisk": false,
        |  "useMemory": false,
        |  "useOffHeap": false,
        |  "deserialized": false,
        |  "replication": 1
        |}
      """.stripMargin)

    // Converts TreeNode argument to JSON
    assertJSON(
      Literal(333),
      """[
        |  {
        |    "class": "org.apache.spark.sql.catalyst.expressions.Literal",
        |    "num-children": 0,
        |    "value": "333",
        |    "dataType": "integer"
        |  }
        |]
      """.stripMargin)

    // Converts Seq[String] to JSON
    assertJSON(Seq("1", "2", "3"), "\"[1, 2, 3]\"")

    // Converts Seq[DataType] to JSON
    assertJSON(Seq(IntegerType, DoubleType, FloatType), """["integer","double","float"]""")

    // Converts Seq[Partitioning] to JSON
    assertJSON(
      Seq(SinglePartition, RoundRobinPartitioning(numPartitions = 3)),
      """
        |[
        |  {
        |    "object": "org.apache.spark.sql.catalyst.plans.physical.SinglePartition$"
        |  },
        |  {
        |    "product-class": "org.apache.spark.sql.catalyst.plans.physical.RoundRobinPartitioning",
        |    "numPartitions": 3
        |  }
        |]
      """.stripMargin)

    // Converts case object to JSON
    assertJSON(
      DummyObject,
      """
        |{
        |  "object": "org.apache.spark.sql.catalyst.trees.DummyObject$"
        |}
      """.stripMargin)

    // Converts ExprId to JSON
    assertJSON(
      ExprId(0, uuid),
      s"""
         |{
         |  "product-class": "org.apache.spark.sql.catalyst.expressions.ExprId",
         |  "id": 0,
         |  "jvmId": "${uuid.toString}"
         |}
       """.stripMargin)

    // Converts StructField to JSON
    assertJSON(
      StructField("field", IntegerType),
      """
        |{
        |  "product-class": "org.apache.spark.sql.types.StructField",
        |  "name": "field",
        |  "dataType": "integer",
        |  "nullable": true,
        |  "metadata": {}
        |}
      """.stripMargin)

    // Converts TableIdentifier to JSON
    assertJSON(
      TableIdentifier("table"),
      """
        |{
        |  "product-class": "org.apache.spark.sql.catalyst.TableIdentifier",
        |  "table": "table"
        |}
      """.stripMargin)

    // Converts JoinType to JSON
    assertJSON(
      NaturalJoin(LeftOuter),
      """
        |{
        |  "product-class": "org.apache.spark.sql.catalyst.plans.NaturalJoin",
        |  "tpe":{
        |    "object": "org.apache.spark.sql.catalyst.plans.LeftOuter$"
        |  }
        |}
      """.stripMargin)

    // Converts FunctionIdentifier to JSON
    assertJSON(
      FunctionIdentifier("function", None),
      """
        |{
        |  "product-class": "org.apache.spark.sql.catalyst.FunctionIdentifier",
        |  "funcName": "function"
        |}
      """.stripMargin)

    // Converts BucketSpec to JSON
    assertJSON(
      BucketSpec(1, Seq("bucket"), Seq("sort")),
      s"""
         |{
         |  "product-class": "org.apache.spark.sql.catalyst.catalog.BucketSpec",
         |  "numBuckets": 1,
         |  "bucketColumnNames": "[bucket]",
         |  "sortColumnNames": "[sort]"
         |}
       """.stripMargin)

    // Converts FrameBoundary to JSON
    assertJSON(
      ValueFollowing(3),
      s"""
         |{
         |  "product-class": "org.apache.spark.sql.catalyst.expressions.ValueFollowing",
         |  "value": 3
         |}
       """.stripMargin)

    // Converts WindowFrame to JSON
    assertJSON(
      SpecifiedWindowFrame(RowFrame, UnboundedFollowing, CurrentRow),
      """
        |{
        |  "product-class": "org.apache.spark.sql.catalyst.expressions.SpecifiedWindowFrame",
        |  "frameType": {
        |    "object": "org.apache.spark.sql.catalyst.expressions.RowFrame$"
        |  },
        |  "frameStart": {
        |    "object": "org.apache.spark.sql.catalyst.expressions.UnboundedFollowing$"
        |  },
        |  "frameEnd": {
        |    "object": "org.apache.spark.sql.catalyst.expressions.CurrentRow$"
        |  }
        |}
       """.stripMargin)

    // Converts Partitioning to JSON
    assertJSON(
      RoundRobinPartitioning(numPartitions = 3),
      """
        |{
        |  "product-class": "org.apache.spark.sql.catalyst.plans.physical.RoundRobinPartitioning",
        |  "numPartitions": 3
        |}
      """.stripMargin)

    // Converts FunctionResource to JSON
    assertJSON(
      FunctionResource(JarResource, "file:///"),
      """
        |{
        |  "product-class": "org.apache.spark.sql.catalyst.catalog.FunctionResource",
        |  "resourceType": {
        |    "object": "org.apache.spark.sql.catalyst.catalog.JarResource$"
        |  },
        |  "uri": "file:///"
        |}
      """.stripMargin)

    // Converts BroadcastMode to JSON
    assertJSON(
      IdentityBroadcastMode,
      """{"object": "org.apache.spark.sql.catalyst.plans.physical.IdentityBroadcastMode$"}""")

    // Converts CatalogTable to JSON
    val struct = StructType(StructField("a", IntegerType, true) :: Nil)
    assertJSON(
      CatalogTable(
        TableIdentifier("table"),
        CatalogTableType.MANAGED,
        CatalogStorageFormat.empty,
        StructType(StructField("a", IntegerType, true) :: Nil),
        createTime = 0L),
      """
        |{
        |  "product-class": "org.apache.spark.sql.catalyst.catalog.CatalogTable",
        |  "identifier": {
        |    "product-class": "org.apache.spark.sql.catalyst.TableIdentifier",
        |    "table": "table"
        |  },
        |  "tableType": null,
        |  "storage": null,
        |  "schema": {
        |    "type": "struct",
        |    "fields": [{
        |      "name": "a",
        |      "type": "integer",
        |      "nullable": true,
        |      "metadata": {}
        |    }]
        |  },
        |  "partitionColumnNames": [],
        |  "owner": "",
        |  "createTime": 0,
        |  "lastAccessTime": -1,
        |  "properties": null,
        |  "unsupportedFeatures": []
        |}
      """.stripMargin)

    // For unknown case class, returns JNull.
    val bigValue = new Array[Int](10000)
    assertJSON(NameValue("name", bigValue), "null")

    // Converts Seq[TreeNode] to JSON recursively
    assertJSON(
      Seq(Literal(1), Literal(2)),
      """
        |[
        |  [
        |    {
        |      "class": "org.apache.spark.sql.catalyst.expressions.Literal",
        |      "num-children": 0,
        |      "value": "1",
        |      "dataType": "integer"
        |    }
        |  ],
        |  [
        |    {
        |      "class": "org.apache.spark.sql.catalyst.expressions.Literal",
        |      "num-children": 0,
        |      "value": "2",
        |      "dataType": "integer"
        |    }
        |  ]
        |]
      """.stripMargin)

    // Other Seq is converted to JNull, to reduce the risk of out of memory
    assertJSON(Seq(1, 2, 3), "null")

    // All Map type is converted to JNull, to reduce the risk of out of memory
    assertJSON(Map("key" -> "value"), "null")

    // Unknown type is converted to JNull, to reduce the risk of out of memory
    assertJSON(new Object {}, "null")

    // Convert all TreeNode children to JSON
    compareJSON(
      Union(Seq(JsonTestTreeNode("0"), JsonTestTreeNode("1"))).toJSON,
      """
        |[
        |  {
        |    "class": "org.apache.spark.sql.catalyst.plans.logical.Union",
        |    "num-children": 2,
        |    "children": [0, 1]
        |  },
        |  {
        |    "class": "org.apache.spark.sql.catalyst.trees.JsonTestTreeNode",
        |    "num-children": 0,
        |    "arg": "0"
        |  },
        |  {
        |    "class": "org.apache.spark.sql.catalyst.trees.JsonTestTreeNode",
        |    "num-children": 0,
        |    "arg": "1"
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
