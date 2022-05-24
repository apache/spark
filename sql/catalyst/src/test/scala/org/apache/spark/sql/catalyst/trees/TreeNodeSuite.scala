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

import org.json4s.JsonAST._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods
import org.json4s.jackson.JsonMethods._

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.{AliasIdentifier, FunctionIdentifier, InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.dsl.expressions.DslString
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.plans.{LeftOuter, NaturalJoin, SQLHelper}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans.physical.{IdentityBroadcastMode, RoundRobinPartitioning, SinglePartition}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel

case class Dummy(optKey: Option[Expression]) extends Expression with CodegenFallback {
  override def children: Seq[Expression] = optKey.toSeq
  override def nullable: Boolean = true
  override def dataType: NullType = NullType
  override lazy val resolved = true
  override def eval(input: InternalRow): Any = null.asInstanceOf[Any]
  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    copy(optKey = if (optKey.isDefined) Some(newChildren(0)) else None)
}

case class ComplexPlan(exprs: Seq[Seq[Expression]])
  extends org.apache.spark.sql.catalyst.plans.logical.LeafNode {
  override def output: Seq[Attribute] = Nil
}

case class ExpressionInMap(map: Map[String, Expression]) extends Unevaluable {
  override def children: Seq[Expression] = map.values.toSeq
  override def nullable: Boolean = true
  override def dataType: NullType = NullType
  override lazy val resolved = true
  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    super.legacyWithNewChildren(newChildren)
}

case class SeqTupleExpression(sons: Seq[(Expression, Expression)],
    nonSons: Seq[(Expression, Expression)]) extends Unevaluable {
  override def children: Seq[Expression] = sons.flatMap(t => Iterator(t._1, t._2))
  override def nullable: Boolean = true
  override def dataType: NullType = NullType
  override lazy val resolved = true

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    super.legacyWithNewChildren(newChildren)
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

case class FakeLeafPlan(child: LogicalPlan)
  extends org.apache.spark.sql.catalyst.plans.logical.LeafNode {
  override def output: Seq[Attribute] = child.output
}

case class FakeCurryingProduct(x: Expression)(val y: Int)

class TreeNodeSuite extends SparkFunSuite with SQLHelper {
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
      case b: BinaryOperator => actual += b.symbol; b
      case l: Literal => actual += l.toString; l
    }

    assert(expected === actual)
  }

  test("post-order transform") {
    val actual = new ArrayBuffer[String]()
    val expected = Seq("1", "2", "3", "4", "-", "*", "+")
    val expression = Add(Literal(1), Multiply(Literal(2), Subtract(Literal(3), Literal(4))))
    expression transformUp {
      case b: BinaryOperator => actual += b.symbol; b
      case l: Literal => actual += l.toString; l
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

  test("mapChildren should only works on children") {
    val children = Seq((Literal(1), Literal(2)))
    val nonChildren = Seq((Literal(3), Literal(4)))
    val before = SeqTupleExpression(children, nonChildren)
    val toZero: PartialFunction[Expression, Expression] = { case Literal(_, _) => Literal(0) }
    val expect = SeqTupleExpression(Seq((Literal(0), Literal(0))), nonChildren)

    val actual = before mapChildren toZero
    assert(actual === expect)
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
      case b: BinaryOperator => actual += b.symbol;
      case l: Literal => actual += l.toString;
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

  test("exists") {
    val expression = Add(Literal(1), Multiply(Literal(2), Subtract(Literal(3), Literal(4))))
    // Check the top node.
    var exists = expression.exists {
      case _: Add => true
      case _ => false
    }
    assert(exists)

    // Check the first children.
    exists = expression.exists {
      case Literal(1, IntegerType) => true
      case _ => false
    }
    assert(exists)

    // Check an internal node (Subtract).
    exists = expression.exists {
      case _: Subtract => true
      case _ => false
    }
    assert(exists)

    // Check a leaf node.
    exists = expression.exists {
      case Literal(3, IntegerType) => true
      case _ => false
    }
    assert(exists)

    // Check not exists.
    exists = expression.exists {
      case Literal(100, IntegerType) => true
      case _ => false
    }
    assert(!exists)
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
    def assertJSON(input: Any, json: JValue): Unit = {
      val expected =
        s"""
           |[{
           |  "class": "${classOf[JsonTestTreeNode].getName}",
           |  "num-children": 0,
           |  "arg": ${compact(render(json))}
           |}]
         """.stripMargin
      compareJSON(JsonTestTreeNode(input).toJSON, expected)
    }

    // Converts simple types to JSON
    assertJSON(true, true)
    assertJSON(33.toByte, 33)
    assertJSON(44, 44)
    assertJSON(55L, 55L)
    assertJSON(3.0, 3.0)
    assertJSON(4.0D, 4.0D)
    assertJSON(BigInt(BigInteger.valueOf(88L)), 88L)
    assertJSON(null, JNull)
    assertJSON("text", "text")
    assertJSON(Some("text"), "text")
    compareJSON(JsonTestTreeNode(None).toJSON,
      s"""[
         |  {
         |    "class": "${classOf[JsonTestTreeNode].getName}",
         |    "num-children": 0
         |  }
         |]
       """.stripMargin)

    val uuid = UUID.randomUUID()
    assertJSON(uuid, uuid.toString)

    // Converts Spark Sql DataType to JSON
    assertJSON(IntegerType, "integer")
    assertJSON(Metadata.empty, JObject(Nil))
    assertJSON(
      StorageLevel.NONE,
      JObject(
        "useDisk" -> false,
        "useMemory" -> false,
        "useOffHeap" -> false,
        "deserialized" -> false,
        "replication" -> 1)
    )

    // Converts TreeNode argument to JSON
    assertJSON(
      Literal(333),
      List(
        JObject(
          "class" -> classOf[Literal].getName,
          "num-children" -> 0,
          "value" -> "333",
          "dataType" -> "integer")))

    // Converts Seq[String] to JSON
    assertJSON(Seq("1", "2", "3"), "[1, 2, 3]")

    // Converts Seq[DataType] to JSON
    assertJSON(Seq(IntegerType, DoubleType, FloatType), List("integer", "double", "float"))

    // Converts Seq[Partitioning] to JSON
    assertJSON(
      Seq(SinglePartition, RoundRobinPartitioning(numPartitions = 3)),
      List(
        JObject("object" -> JString(SinglePartition.getClass.getName)),
        JObject(
          "product-class" -> classOf[RoundRobinPartitioning].getName,
          "numPartitions" -> 3)))

    // Converts case object to JSON
    assertJSON(DummyObject, JObject("object" -> JString(DummyObject.getClass.getName)))

    // Converts ExprId to JSON
    assertJSON(
      ExprId(0, uuid),
      JObject(
        "product-class" -> classOf[ExprId].getName,
        "id" -> 0,
        "jvmId" -> uuid.toString))

    // Converts StructField to JSON
    assertJSON(
      StructField("field", IntegerType),
      JObject(
        "product-class" -> classOf[StructField].getName,
        "name" -> "field",
        "dataType" -> "integer",
        "nullable" -> true,
        "metadata" -> JObject(Nil)))

    // Converts TableIdentifier to JSON
    assertJSON(
      TableIdentifier("table"),
      JObject(
        "product-class" -> classOf[TableIdentifier].getName,
        "table" -> "table"))

    // Converts JoinType to JSON
    assertJSON(
      NaturalJoin(LeftOuter),
      JObject(
        "product-class" -> classOf[NaturalJoin].getName,
        "tpe" -> JObject("object" -> JString(LeftOuter.getClass.getName))))

    // Converts FunctionIdentifier to JSON
    assertJSON(
      FunctionIdentifier("function", None),
      JObject(
        "product-class" -> JString(classOf[FunctionIdentifier].getName),
          "funcName" -> "function"))

    // Converts AliasIdentifier to JSON
    assertJSON(
      AliasIdentifier("alias", Seq("ns1", "ns2")),
      JObject(
        "product-class" -> JString(classOf[AliasIdentifier].getName),
          "name" -> "alias",
          "qualifier" -> "[ns1, ns2]"))

    // Converts SubqueryAlias to JSON
    assertJSON(
      SubqueryAlias("t1", JsonTestTreeNode("0")),
      List(
        JObject(
          "class" -> classOf[SubqueryAlias].getName,
          "num-children" -> 1,
          "identifier" -> JObject("product-class" -> JString(classOf[AliasIdentifier].getName),
            "name" -> "t1",
            "qualifier" -> JArray(Nil)),
          "child" -> 0),
        JObject(
          "class" -> classOf[JsonTestTreeNode].getName,
          "num-children" -> 0,
          "arg" -> "0")))

    // Converts BucketSpec to JSON
    assertJSON(
      BucketSpec(1, Seq("bucket"), Seq("sort")),
      JObject(
        "product-class" -> classOf[BucketSpec].getName,
        "numBuckets" -> 1,
        "bucketColumnNames" -> "[bucket]",
        "sortColumnNames" -> "[sort]"))

    // Converts WindowFrame to JSON
    assertJSON(
      SpecifiedWindowFrame(RowFrame, UnboundedPreceding, CurrentRow),
      List(
        JObject(
          "class" -> classOf[SpecifiedWindowFrame].getName,
          "num-children" -> 2,
          "frameType" -> JObject("object" -> JString(RowFrame.getClass.getName)),
          "lower" -> 0,
          "upper" -> 1),
        JObject(
          "class" -> UnboundedPreceding.getClass.getName,
          "num-children" -> 0),
        JObject(
          "class" -> CurrentRow.getClass.getName,
          "num-children" -> 0)))

    // Converts Partitioning to JSON
    assertJSON(
      RoundRobinPartitioning(numPartitions = 3),
      JObject(
        "product-class" -> classOf[RoundRobinPartitioning].getName,
        "numPartitions" -> 3))

    // Converts FunctionResource to JSON
    assertJSON(
      FunctionResource(JarResource, "file:///"),
      JObject(
        "product-class" -> JString(classOf[FunctionResource].getName),
        "resourceType" -> JObject("object" -> JString(JarResource.getClass.getName)),
        "uri" -> "file:///"))

    // Converts BroadcastMode to JSON
    assertJSON(
      IdentityBroadcastMode,
      JObject("object" -> JString(IdentityBroadcastMode.getClass.getName)))

    // Converts CatalogTable to JSON
    assertJSON(
      CatalogTable(
        TableIdentifier("table"),
        CatalogTableType.MANAGED,
        CatalogStorageFormat.empty,
        StructType(StructField("a", IntegerType, true) :: Nil),
        createTime = 0L,
        createVersion = "2.x"),

      JObject(
        "product-class" -> classOf[CatalogTable].getName,
        "identifier" -> JObject(
          "product-class" -> classOf[TableIdentifier].getName,
          "table" -> "table"
        ),
        "tableType" -> JObject(
          "product-class" -> classOf[CatalogTableType].getName,
          "name" -> "MANAGED"
        ),
        "storage" -> JObject(
          "product-class" -> classOf[CatalogStorageFormat].getName,
          "compressed" -> false,
          "properties" -> JNull
        ),
        "schema" -> JObject(
          "type" -> "struct",
          "fields" -> List(
            JObject(
              "name" -> "a",
              "type" -> "integer",
              "nullable" -> true,
              "metadata" -> JObject(Nil)))),
        "partitionColumnNames" -> List.empty[String],
        "owner" -> "",
        "createTime" -> 0,
        "lastAccessTime" -> -1,
        "createVersion" -> "2.x",
        "tracksPartitionsInCatalog" -> false,
        "properties" -> JNull,
        "unsupportedFeatures" -> List.empty[String],
        "schemaPreservesCase" -> JBool(true),
        "ignoredProperties" -> JNull))

    // For unknown case class, returns JNull.
    val bigValue = new Array[Int](10000)
    assertJSON(NameValue("name", bigValue), JNull)

    // Converts Seq[TreeNode] to JSON recursively
    assertJSON(
      Seq(Literal(1), Literal(2)),
      List(
        List(
          JObject(
            "class" -> JString(classOf[Literal].getName),
            "num-children" -> 0,
            "value" -> "1",
            "dataType" -> "integer")),
        List(
          JObject(
            "class" -> JString(classOf[Literal].getName),
            "num-children" -> 0,
            "value" -> "2",
            "dataType" -> "integer"))))

    // Other Seq is converted to JNull, to reduce the risk of out of memory
    assertJSON(Seq(1, 2, 3), JNull)

    // All Map type is converted to JNull, to reduce the risk of out of memory
    assertJSON(Map("key" -> "value"), JNull)

    // Unknown type is converted to JNull, to reduce the risk of out of memory
    assertJSON(new Object {}, JNull)

    // Convert all TreeNode children to JSON
    assertJSON(
      Union(Seq(JsonTestTreeNode("0"), JsonTestTreeNode("1"))),
      List(
        JObject(
          "class" -> classOf[Union].getName,
          "num-children" -> 2,
          "children" -> List(0, 1),
          "byName" -> JBool(false),
          "allowMissingCol" -> JBool(false)),
        JObject(
          "class" -> classOf[JsonTestTreeNode].getName,
          "num-children" -> 0,
          "arg" -> "0"),
        JObject(
          "class" -> classOf[JsonTestTreeNode].getName,
          "num-children" -> 0,
          "arg" -> "1")))

    // Convert Seq of Product contains TreeNode to JSON.
    assertJSON(
      Seq(("a", JsonTestTreeNode("0")), ("b", JsonTestTreeNode("1"))),
      List(
        JObject(
          "product-class" -> "scala.Tuple2",
          "_1" -> "a",
          "_2" -> List(JObject(
            "class" -> classOf[JsonTestTreeNode].getName,
            "num-children" -> 0,
            "arg" -> "0"
          ))),
        JObject(
          "product-class" -> "scala.Tuple2",
          "_1" -> "b",
          "_2" -> List(JObject(
            "class" -> classOf[JsonTestTreeNode].getName,
            "num-children" -> 0,
            "arg" -> "1"
          )))))

    // Convert currying product contains TreeNode to JSON.
    assertJSON(
      FakeCurryingProduct(Literal(1))(1),
      JObject(
        "product-class" -> classOf[FakeCurryingProduct].getName,
        "x" -> List(
          JObject(
            "class" -> JString(classOf[Literal].getName),
            "num-children" -> 0,
            "value" -> "1",
            "dataType" -> "integer")),
        "y" -> 1
      )
    )
  }

  test("toJSON should not throws java.lang.StackOverflowError") {
    val udf = ScalaUDF(SelfReferenceUDF(), BooleanType, Seq("col1".attr),
      Option(ExpressionEncoder[String]()) :: Nil)
    // Should not throw java.lang.StackOverflowError
    udf.toJSON
  }

  private def compareJSON(leftJson: String, rightJson: String): Unit = {
    val left = JsonMethods.parse(leftJson)
    val right = JsonMethods.parse(rightJson)
    assert(left == right)
  }

  test("transform works on stream of children") {
    val before = Coalesce(Stream(Literal(1), Literal(2)))
    // Note it is a bit tricky to exhibit the broken behavior. Basically we want to create the
    // situation in which the TreeNode.mapChildren function's change detection is not triggered. A
    // stream's first element is typically materialized, so in order to not trip the TreeNode change
    // detection logic, we should not change the first element in the sequence.
    val result = before.transform {
      case Literal(v: Int, IntegerType) if v != 1 =>
        Literal(v + 1, IntegerType)
    }
    val expected = Coalesce(Stream(Literal(1), Literal(3)))
    assert(result === expected)
  }

  test("withNewChildren on stream of children") {
    val before = Coalesce(Stream(Literal(1), Literal(2)))
    val result = before.withNewChildren(Stream(Literal(1), Literal(3)))
    val expected = Coalesce(Stream(Literal(1), Literal(3)))
    assert(result === expected)
  }

  test("treeString limits plan length") {
    withSQLConf(SQLConf.MAX_PLAN_STRING_LENGTH.key -> "200") {
      val ds = (1 until 20).foldLeft(Literal("TestLiteral"): Expression) { case (treeNode, x) =>
        Add(Literal(x), treeNode)
      }

      val planString = ds.treeString
      logWarning("Plan string: " + planString)
      assert(planString.endsWith(" more characters"))
      assert(planString.length <= SQLConf.get.maxPlanStringLength)
    }
  }

  test("treeString limit at zero") {
    withSQLConf(SQLConf.MAX_PLAN_STRING_LENGTH.key -> "0") {
      val ds = (1 until 2).foldLeft(Literal("TestLiteral"): Expression) { case (treeNode, x) =>
        Add(Literal(x), treeNode)
      }

      val planString = ds.treeString
      assert(planString.startsWith("Truncated plan of"))
    }
  }

  test("tags will be carried over after copy & transform") {
    val tag = TreeNodeTag[String]("test")

    withClue("makeCopy") {
      val node = Dummy(None)
      node.setTagValue(tag, "a")
      val copied = node.makeCopy(Array(Some(Literal(1))))
      assert(copied.getTagValue(tag) == Some("a"))
    }

    def checkTransform(
        sameTypeTransform: Expression => Expression,
        differentTypeTransform: Expression => Expression): Unit = {
      val child = Dummy(None)
      child.setTagValue(tag, "child")
      val node = Dummy(Some(child))
      node.setTagValue(tag, "parent")

      val transformed = sameTypeTransform(node)
      // Both the child and parent keep the tags
      assert(transformed.getTagValue(tag) == Some("parent"))
      assert(transformed.children.head.getTagValue(tag) == Some("child"))

      val transformed2 = differentTypeTransform(node)
      // Both the child and parent keep the tags, even if we transform the node to a new one of
      // different type.
      assert(transformed2.getTagValue(tag) == Some("parent"))
      assert(transformed2.children.head.getTagValue(tag) == Some("child"))
    }

    withClue("transformDown") {
      checkTransform(
        sameTypeTransform = _ transformDown {
          case Dummy(None) => Dummy(Some(Literal(1)))
        },
        differentTypeTransform = _ transformDown {
          case Dummy(None) => Literal(1)

        })
    }

    withClue("transformUp") {
      checkTransform(
        sameTypeTransform = _ transformUp {
          case Dummy(None) => Dummy(Some(Literal(1)))
        },
        differentTypeTransform = _ transformUp {
          case Dummy(None) => Literal(1)

        })
    }
  }

  test("clone") {
    def assertDifferentInstance[T <: TreeNode[T]](before: TreeNode[T], after: TreeNode[T]): Unit = {
      assert(before.ne(after) && before == after)
      before.children.zip(after.children).foreach { case (beforeChild, afterChild) =>
        assertDifferentInstance(
          beforeChild.asInstanceOf[TreeNode[T]],
          afterChild.asInstanceOf[TreeNode[T]])
      }
    }

    // Empty constructor
    val rowNumber = RowNumber()
    assertDifferentInstance(rowNumber, rowNumber.clone())

    // Overridden `makeCopy`
    val oneRowRelation = OneRowRelation()
    assertDifferentInstance(oneRowRelation, oneRowRelation.clone())

    // Multi-way operators
    val intersect =
      Intersect(oneRowRelation, Union(Seq(oneRowRelation, oneRowRelation)), isAll = false)
    assertDifferentInstance(intersect, intersect.clone())

    // Leaf node with an inner child
    val leaf = FakeLeafPlan(intersect)
    val leafCloned = leaf.clone()
    assertDifferentInstance(leaf, leafCloned)
    assert(leaf.child.eq(leafCloned.asInstanceOf[FakeLeafPlan].child))
  }

  object MalformedClassObject extends Serializable {
    case class MalformedNameExpression(child: Expression) extends TaggingExpression {
      override protected def withNewChildInternal(newChild: Expression): Expression =
        copy(child = newChild)
    }
  }

  test("SPARK-32999: TreeNode.nodeName should not throw malformed class name error") {
    val testTriggersExpectedError = try {
      classOf[MalformedClassObject.MalformedNameExpression].getSimpleName
      false
    } catch {
      case ex: java.lang.InternalError if ex.getMessage.contains("Malformed class name") =>
        true
      case ex: Throwable => throw ex
    }
    // This test case only applies on older JDK versions (e.g. JDK8u), and doesn't trigger the
    // issue on newer JDK versions (e.g. JDK11u).
    assume(testTriggersExpectedError, "the test case didn't trigger malformed class name error")

    val expr = MalformedClassObject.MalformedNameExpression(Literal(1))
    try {
      expr.nodeName
    } catch {
      case ex: java.lang.InternalError if ex.getMessage.contains("Malformed class name") =>
        fail("TreeNode.nodeName should not throw malformed class name error")
    }
  }

  test("SPARK-37800: TreeNode.argString incorrectly formats arguments of type Set[_]") {
    case class Node(set: Set[String], nested: Seq[Set[Int]]) extends LeafNode {
      val output: Seq[Attribute] = Nil
    }
    val node = Node(Set("second", "first"), Seq(Set(3, 1), Set(2, 1)))
    assert(node.argString(10) == "{first, second}, [{1, 3}, {1, 2}]")
  }

  test("SPARK-38676: truncate before/after sql text if too long") {
    val text =
      """
        |
        |SELECT
        |1234567890 + 1234567890 + 1234567890, cast('a'
        |as /* comment */
        |int), 1234567890 + 1234567890 + 1234567890
        |as foo
        |""".stripMargin
    val origin = Origin(
      line = Some(3),
      startPosition = Some(38),
      startIndex = Some(47),
      stopIndex = Some(77),
      sqlText = Some(text),
      objectType = Some("VIEW"),
      objectName = Some("some_view"))
    val expected =
      """== SQL of VIEW some_view(line 3, position 38) ==
        |...7890 + 1234567890 + 1234567890, cast('a'
        |                                   ^^^^^^^^
        |as /* comment */
        |^^^^^^^^^^^^^^^^
        |int), 1234567890 + 1234567890 + 12345...
        |^^^^^
        |""".stripMargin

    assert(origin.context == expected)
  }

  test("SPARK-39046: Return an empty context string if TreeNode.origin is wrongly set") {
    val text = Some("select a + b")
    // missing start index
    val origin1 = Origin(
      startIndex = Some(7),
      stopIndex = None,
      sqlText = text)
    // missing stop index
    val origin2 = Origin(
      startIndex = None,
      stopIndex = Some(11),
      sqlText = text)
    // missing text
    val origin3 = Origin(
      startIndex = Some(7),
      stopIndex = Some(11),
      sqlText = None)
    // negative start index
    val origin4 = Origin(
      startIndex = Some(-1),
      stopIndex = Some(11),
      sqlText = text)
    // stop index >= text.length
    val origin5 = Origin(
      startIndex = Some(-1),
      stopIndex = Some(text.get.length),
      sqlText = text)
    // start index > stop index
    val origin6 = Origin(
      startIndex = Some(2),
      stopIndex = Some(1),
      sqlText = text)
    Seq(origin1, origin2, origin3, origin4, origin5, origin6).foreach { origin =>
      assert(origin.context.isEmpty)
    }
  }
}
