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
import org.apache.spark.sql.types.{IntegerType, MetadataBuilder, StringType}

class AttributeMapSuite extends SparkFunSuite {

  val aUpper = AttributeReference("A", IntegerType)(exprId = ExprId(1))
  val aLower = AttributeReference("a", IntegerType)(exprId = ExprId(1))
  val fakeA = AttributeReference("a", IntegerType)(exprId = ExprId(3))

  val bUpper = AttributeReference("B", IntegerType)(exprId = ExprId(2))
  val bLower = AttributeReference("b", IntegerType)(exprId = ExprId(2))

  val cAttr = AttributeReference("c", StringType)(exprId = ExprId(4))

  test("basic map operations - get") {
    val map = AttributeMap(Seq((aUpper, "value1"), (bUpper, "value2")))

    // Should find by exprId, not by attribute equality
    assert(map.get(aLower) === Some("value1"))
    assert(map.get(aUpper) === Some("value1"))
    assert(map.get(bLower) === Some("value2"))
    assert(map.get(bUpper) === Some("value2"))

    // Different exprId should not be found
    assert(map.get(fakeA) === None)
  }

  test("basic map operations - contains") {
    val map = AttributeMap(Seq((aUpper, "value1"), (bUpper, "value2")))

    // Should find by exprId, not by attribute equality
    assert(map.contains(aLower))
    assert(map.contains(aUpper))
    assert(map.contains(bUpper))
    assert(!map.contains(fakeA))
  }

  test("basic map operations - getOrElse") {
    val map = AttributeMap(Seq((aUpper, "value1")))

    assert(map.getOrElse(aLower, "default") === "value1")
    assert(map.getOrElse(fakeA, "default") === "default")
  }

  test("+ operator preserves ExprId-based hashing") {
    val map1 = AttributeMap(Seq((aUpper, "value1")))
    val map2 = map1 + (bUpper -> "value2")

    // The resulting map should still be an AttributeMap
    assert(map2.isInstanceOf[AttributeMap[_]])

    // Should look up by exprId, not by attribute equality
    assert(map2.get(aLower) === Some("value1"))
    assert(map2.get(bLower) === Some("value2"))
  }

  test("+ operator with attribute having different metadata") {
    val metadata1 = new MetadataBuilder().putString("key", "value1").build()
    val metadata2 = new MetadataBuilder().putString("key", "value2").build()

    // Two attributes with same exprId but different metadata
    val attrWithMetadata1 = AttributeReference("col", IntegerType, metadata = metadata1)(
      exprId = ExprId(100))
    val attrWithMetadata2 = AttributeReference("col", IntegerType, metadata = metadata2)(
      exprId = ExprId(100))

    // These should have different hashCodes but same exprId
    assert(attrWithMetadata1.hashCode() != attrWithMetadata2.hashCode(),
      "Attributes with different metadata should have different hashCodes")
    assert(attrWithMetadata1.exprId == attrWithMetadata2.exprId,
      "Attributes should have the same exprId")

    // Create a map with the first attribute
    val map1 = AttributeMap(Seq((attrWithMetadata1, "original")))

    // Add an entry using the + operator
    val map2 = map1 + (cAttr -> "new")

    // CRITICAL: The map should still find the original entry using an attribute
    // with the same exprId but different metadata
    assert(map2.get(attrWithMetadata2) === Some("original"),
      "AttributeMap should look up by exprId, not by attribute hashCode")

    // And the new entry should also be present
    assert(map2.get(cAttr) === Some("new"))
  }

  test("+ operator updates existing key") {
    val map1 = AttributeMap(Seq((aUpper, "value1")))
    val map2 = map1 + (aLower -> "updated")

    // Since aLower has the same exprId as aUpper, it should update the value
    assert(map2.get(aUpper) === Some("updated"))
    assert(map2.get(aLower) === Some("updated"))
    assert(map2.size === 1)
  }

  test("+ operator with type widening") {
    val map1: AttributeMap[String] = AttributeMap(Seq((aUpper, "value1")))
    val map2: AttributeMap[Any] = map1 + (bUpper -> 42)

    assert(map2.get(aUpper) === Some("value1"))
    assert(map2.get(bUpper) === Some(42))
  }

  test("++ operator preserves AttributeMap semantics") {
    val map1 = AttributeMap(Seq((aUpper, "value1")))
    val map2 = AttributeMap(Seq((bUpper, "value2")))
    val combined = map1 ++ map2

    assert(combined.isInstanceOf[AttributeMap[_]])
    assert(combined.get(aLower) === Some("value1"))
    assert(combined.get(bLower) === Some("value2"))
  }

  test("updated method") {
    val map1 = AttributeMap(Seq((aUpper, "value1")))
    val map2 = map1.updated(bUpper, "value2")

    // Note: updated returns a Map[Attribute, B1], not AttributeMap
    assert(map2.get(aUpper) === Some("value1"))
    assert(map2.get(bUpper) === Some("value2"))
  }

  test("- operator (removal)") {
    val map1 = AttributeMap(Seq((aUpper, "value1"), (bUpper, "value2")))
    val map2 = map1 - aLower

    // Note: - returns a Map[Attribute, A], not AttributeMap
    assert(map2.get(aUpper) === None)
    assert(map2.get(bUpper) === Some("value2"))
  }

  test("iterator") {
    val map = AttributeMap(Seq((aUpper, "value1"), (bUpper, "value2")))
    val entries = map.iterator.toSeq

    assert(entries.size === 2)
    assert(entries.contains((aUpper, "value1")))
    assert(entries.contains((bUpper, "value2")))
  }

  test("size") {
    val emptyMap = AttributeMap.empty[String]
    assert(emptyMap.size === 0)

    val map1 = AttributeMap(Seq((aUpper, "value1")))
    assert(map1.size === 1)

    val map2 = AttributeMap(Seq((aUpper, "value1"), (bUpper, "value2")))
    assert(map2.size === 2)
  }

  test("empty map") {
    val emptyMap = AttributeMap.empty[String]
    assert(emptyMap.get(aUpper) === None)
    assert(emptyMap.size === 0)
    assert(!emptyMap.contains(aUpper))
  }

  test("duplicate keys in construction") {
    // When constructing with duplicate exprIds, the last one should win
    val map = AttributeMap(Seq((aUpper, "value1"), (aLower, "value2")))
    assert(map.size === 1)
    assert(map.get(aUpper) === Some("value2"))
  }

  test("map construction from Map") {
    val regularMap = Map(aUpper -> "value1", bUpper -> "value2")
    val attrMap = AttributeMap(regularMap)

    assert(attrMap.get(aLower) === Some("value1"))
    assert(attrMap.get(bLower) === Some("value2"))
  }

  test("chained + operations") {
    val map = AttributeMap.empty[String] + (aUpper -> "value1") + (bUpper -> "value2") +
      (cAttr -> "value3")

    assert(map.size === 3)
    assert(map.get(aLower) === Some("value1"))
    assert(map.get(bLower) === Some("value2"))
    assert(map.get(cAttr) === Some("value3"))
  }

  test("+ should be deterministic with Attributes with diff hashcodes and same exprId") {
    // The HashMap needs to be of a certain size before the hashing starts to collide, set up
    // these AttributeMaps to start with size 5.
    var map1 = AttributeMap(
      Seq(
        AttributeReference("a", IntegerType)(exprId = ExprId(1)) -> 1,
        AttributeReference("b", IntegerType)(exprId = ExprId(2)) -> 2,
        AttributeReference("c", IntegerType)(exprId = ExprId(3)) -> 3,
        AttributeReference("d", IntegerType)(exprId = ExprId(4)) -> 4,
        AttributeReference("e", IntegerType)(exprId = ExprId(5)) -> 5
      )
    )
    var map2 = AttributeMap(
      Seq(
        AttributeReference("a", IntegerType)(exprId = ExprId(1)) -> 1,
        AttributeReference("b", IntegerType)(exprId = ExprId(2)) -> 2,
        AttributeReference("c", IntegerType)(exprId = ExprId(3)) -> 3,
        AttributeReference("d", IntegerType)(exprId = ExprId(4)) -> 4,
        AttributeReference("e", IntegerType)(exprId = ExprId(5)) -> 5
      )
    )
    val qualifier1 = Seq("d")
    val qualifier2 = Seq()
    val exprId = ExprId(42)
    val attr1 = AttributeReference("x", IntegerType)(exprId = exprId, qualifier = qualifier1)
    val attr2 = AttributeReference("x", IntegerType)(exprId = exprId, qualifier = qualifier2)
    assert(attr1.hashCode != attr2.hashCode)

    map1 = map1 + (attr1 -> 100)
    map1 = map1 + (attr2 -> 200)
    assert(map1.get(attr2) === Some(200))

    map2 = map2 + (attr2 -> 200)
    map2 = map2 + (attr1 -> 100)
    assert(map2.get(attr2) === Some(100))
  }

  test("updated should be deterministic with Attributes with diff hashcodes and same exprId") {
    // The HashMap needs to be of a certain size before the hashing starts to collide, set up
    // these AttributeMaps to start with size 5.
    var map1: Map[Attribute, Int] = AttributeMap(
      Seq(
        AttributeReference("a", IntegerType)(exprId = ExprId(1)) -> 1,
        AttributeReference("b", IntegerType)(exprId = ExprId(2)) -> 2,
        AttributeReference("c", IntegerType)(exprId = ExprId(3)) -> 3,
        AttributeReference("d", IntegerType)(exprId = ExprId(4)) -> 4,
        AttributeReference("e", IntegerType)(exprId = ExprId(5)) -> 5
      )
    )
    var map2: Map[Attribute, Int] = AttributeMap(
      Seq(
        AttributeReference("a", IntegerType)(exprId = ExprId(1)) -> 1,
        AttributeReference("b", IntegerType)(exprId = ExprId(2)) -> 2,
        AttributeReference("c", IntegerType)(exprId = ExprId(3)) -> 3,
        AttributeReference("d", IntegerType)(exprId = ExprId(4)) -> 4,
        AttributeReference("e", IntegerType)(exprId = ExprId(5)) -> 5
      )
    )
    val qualifier1 = Seq("d")
    val qualifier2 = Seq()
    val exprId = ExprId(42)
    val attr1 = AttributeReference("x", IntegerType)(exprId = exprId, qualifier = qualifier1)
    val attr2 = AttributeReference("x", IntegerType)(exprId = exprId, qualifier = qualifier2)
    assert(attr1.hashCode != attr2.hashCode)

    map1 = map1.updated(attr1, 100)
    map1 = map1.updated(attr2, 200)
    assert(map1.get(attr2) === Some(200))

    map2 = map2.updated(attr2, 200)
    map2 = map2.updated(attr1, 100)
    assert(map2.get(attr2) === Some(100))
  }
}
