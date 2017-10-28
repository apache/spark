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

package org.apache.spark.ml.attribute

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.types._

class AttributeCompatibilitySuite extends SparkFunSuite {
  import AttributeKeys._
  import AttributeType._

  test("NominalAttribute with values") {
    val nominalAttr = new NominalAttribute(isOrdinal = Some(true))
      .withName("col1")
      .withValues(Array("1.0", "2.0"))
    val structField = nominalAttr.toStructField()

    val converted = MLAttributes.fromStructField(structField, preserveName = false)
      .asInstanceOf[NominalAttr]
    assert(converted.name.get == "col1" &&
      converted.isOrdinal.get == true &&
      converted.values.get.toSeq === Seq("1.0", "2.0"))
  }

  test("NominalAttribute with numValues") {
    val nominalAttr = new NominalAttribute(isOrdinal = Some(false))
      .withName("col1")
      .withNumValues(5)
    val structField = nominalAttr.toStructField()

    val converted = MLAttributes.fromStructField(structField, preserveName = false)
      .asInstanceOf[NominalAttr]
    assert(converted.name.get == "col1" &&
      converted.isOrdinal.get == false &&
      converted.values.get.toSeq === Seq("0.0", "1.0", "2.0", "3.0", "4.0"))
  }

  test("NumericAttribute") {
    val numericAttr = new NumericAttribute(
        min = Some(1.0),
        max = Some(10.0),
        std = Some(2.5),
        sparsity = Some(0.5))
      .withName("col1")

    val structField = numericAttr.toStructField()

    val converted = MLAttributes.fromStructField(structField, preserveName = false)
      .asInstanceOf[NumericAttr]
    assert(converted.name.get == "col1" &&
      converted.min.get == 1.0 &&
      converted.max.get == 10.0 &&
      converted.std.get == 2.5 &&
      converted.sparsity.get == 0.5)
  }

  test("BinaryAttribute") {
    val binaryAttr = new BinaryAttribute()
      .withName("col1")
      .withValues("1.0", "2.0")

    val structField = binaryAttr.toStructField()

    val converted = MLAttributes.fromStructField(structField, preserveName = false)
      .asInstanceOf[BinaryAttr]
    assert(converted.name.get == "col1" &&
      converted.values.get.toSeq === Seq("1.0", "2.0"))
  }

  test("AttributeGroup: without attributes") {
    val attrGroup = new AttributeGroup("vecCol1", numAttributes = 10)
    val structField = attrGroup.toStructField()
    val converted = MLAttributes.fromStructField(structField, preserveName = false)
      .asInstanceOf[VectorAttr]

    assert(converted.numOfAttributes == 10 &&
      converted.name.get == "vecCol1" &&
      converted.attributes === Seq.empty)
  }

  test("AttributeGroup: with attributes") {
    val nominalAttr = new NominalAttribute(isOrdinal = Some(true))
      .withName("col1")
      .withValues(Array("1.0", "2.0"))

    val numericAttr = new NumericAttribute(
        min = Some(1.0),
        max = Some(10.0),
        std = Some(2.5),
        sparsity = Some(0.5))
      .withName("col2")

    val binaryAttr = new BinaryAttribute()
      .withName("col1")
      .withValues("1.0", "2.0")

    val attrbutes = Array(
      nominalAttr.withIndex(0),
      nominalAttr.withIndex(1),
      nominalAttr.withIndex(2),
      numericAttr.withIndex(3),
      numericAttr.withIndex(4),
      binaryAttr.withIndex(5))

    val attrGroup = new AttributeGroup("vecCol1", attrbutes)
    val structField = attrGroup.toStructField()
    val converted = MLAttributes.fromStructField(structField, preserveName = false)
      .asInstanceOf[VectorAttr]

    (0 to 2).foreach { idx =>
      val attr = converted.getAttribute(idx).asInstanceOf[NominalAttr]
      val structField = nominalAttr.toStructField()
      val nominal = MLAttributes.fromStructField(structField, preserveName = false)
        .asInstanceOf[NominalAttr].withoutName

      assert(attr.withoutIndicesRange == nominal)
      assert(attr.indicesRange === Seq(0, 2))
    }

    (3 to 4).foreach { idx =>
      val attr = converted.getAttribute(idx).asInstanceOf[NumericAttr]
      val structField = numericAttr.toStructField()
      val numeric = MLAttributes.fromStructField(structField, preserveName = false)
        .asInstanceOf[NumericAttr].withoutName

      assert(attr.withoutIndicesRange == numeric)
      assert(attr.indicesRange === Seq(3, 4))
    }

    (5 to 5).foreach { idx =>
      val attr = converted.getAttribute(idx).asInstanceOf[BinaryAttr]
      val structField = binaryAttr.toStructField()
      val binary = MLAttributes.fromStructField(structField, preserveName = false)
        .asInstanceOf[BinaryAttr].withoutName

      assert(attr.withoutIndicesRange == binary)
      assert(attr.indicesRange === Seq(5))
    }
  }
}
